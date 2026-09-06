// What the Insights panel shows, accumulated from the event bus. The library sends raw
// facts (a sample per peer per second, every UCB pick, every verified piece); everything
// derived from them (rates, distributions, the piece map's arrival order) is built here, and
// bounded so a session that runs for days doesn't grow without end.
import { SvelteMap } from 'svelte/reactivity'
import type { Stamped, Kind } from './events'
import { sourceLabel } from './events'

/** one-second throughput points kept, i.e. five minutes */
const SERIES_POINTS = 300
/** per-peer rate history, in samples */
const PEER_HISTORY = 60
const FEED_LINES = 2000
const PICKS_KEPT = 400
/** peers gone for good are kept this long in the table */
const DEPARTED_KEPT = 200

export interface PeerRecord {
  addr: string
  info_hash: string
  client: string
  dialed: boolean
  encrypted: boolean
  utp: boolean
  connected_at: number
  /** set once it's gone */
  left_at: number | null
  reason: string | null
  downloaded: number
  uploaded: number
  rx_bps: number
  /** the last PEER_HISTORY rate samples */
  history: number[]
  picks: number
  /** the latest pick's two halves of the UCB score */
  exploit: number
  explore: number
  choked_us: boolean
  choked_them: boolean
  pieces: number
}

export interface Pick {
  at: number
  addr: string
  exploit: number
  explore: number
}

export interface PieceMap {
  info_hash: string
  name: string
  total: number
  /** 0 missing, 1 had at start, 2 arrived this session, 3 failed a hash check once */
  state: Uint8Array
  /** for pieces that arrived this session: the order they came in, from 1 */
  order: Uint32Array
  arrived: number
  failed: number
}

export interface Announce {
  at: number
  url: string
  ok: boolean
  peers: number
  detail: string
}

export interface RatePoint {
  /** seconds since the epoch */
  t: number
  down: number
  up: number
}

export class Insights {
  /** the raw stream, newest last */
  feed = $state<Stamped[]>([])
  counts = new SvelteMap<Kind, number>()
  received = $state(0)
  lagged = $state(0)

  rates = $state<RatePoint[]>([])
  peers = new SvelteMap<string, PeerRecord>()
  picks = $state<Pick[]>([])
  pieces = new SvelteMap<string, PieceMap>()
  /** torrent names by info hash, from `torrent_resolved` */
  names = new SvelteMap<string, string>()
  discovery = new SvelteMap<string, number>()
  clients = new SvelteMap<string, number>()
  disconnects = new SvelteMap<string, number>()
  announces = $state<Announce[]>([])
  lifecycle = $state<Stamped[]>([])
  dialsFailed = $state(0)
  wasted = $state(0)
  wastedBlocks = $state(0)

  /** last traffic totals per torrent, to turn them into rates */
  private lastTraffic = new Map<string, { at: number; down: number; up: number }>()
  /** rates land in one-second buckets across torrents */
  private buckets = new Map<number, RatePoint>()
  private departed: string[] = []

  ingest(batch: Stamped[]) {
    for (const e of batch) this.one(e)
    this.feed = this.feed.length + batch.length > FEED_LINES ? [...this.feed, ...batch].slice(-FEED_LINES) : [...this.feed, ...batch]
    this.received += batch.length
  }

  private one(e: Stamped) {
    this.counts.set(e.kind, (this.counts.get(e.kind) ?? 0) + 1)
    switch (e.kind) {
      case 'lagged':
        this.lagged += e.missed
        break
      case 'torrent_resolved':
        this.names.set(e.info_hash, e.name)
        this.pieces.set(e.info_hash, {
          info_hash: e.info_hash,
          name: e.name,
          total: e.pieces,
          state: new Uint8Array(e.pieces),
          order: new Uint32Array(e.pieces),
          arrived: 0,
          failed: 0,
        })
        this.lifecycle = [...this.lifecycle, e].slice(-100)
        break
      case 'torrent_started':
      case 'torrent_queued':
      case 'torrent_paused':
      case 'torrent_checked':
      case 'torrent_completed':
      case 'torrent_failed':
      case 'torrent_removed':
      case 'metadata_fetched':
      case 'listening':
      case 'port_mapping':
      case 'dht_up':
        this.lifecycle = [...this.lifecycle, e].slice(-100)
        if (e.kind === 'torrent_removed') this.pieces.delete(e.info_hash)
        break
      case 'pieces_known': {
        const map = this.pieceMap(e.info_hash, e.bitfield.length * 4)
        for (let i = 0; i < map.total; i++) {
          const nibble = parseInt(e.bitfield[i >> 2], 16)
          if (nibble & (8 >> (i & 3))) map.state[i] = 1
        }
        this.pieces.set(e.info_hash, { ...map })
        break
      }
      case 'piece_verified': {
        const map = this.pieceMap(e.info_hash, e.piece + 1)
        if (map.state[e.piece] !== 2) map.arrived += 1
        map.state[e.piece] = 2
        map.order[e.piece] = map.arrived
        this.pieces.set(e.info_hash, { ...map })
        for (const addr of e.peers) {
          const p = this.peers.get(addr)
          if (p) p.pieces += 1
        }
        break
      }
      case 'piece_failed': {
        const map = this.pieceMap(e.info_hash, e.piece + 1)
        if (map.state[e.piece] !== 3) map.failed += 1
        map.state[e.piece] = 3
        this.pieces.set(e.info_hash, { ...map })
        break
      }
      case 'block_wasted':
        this.wasted += e.len
        this.wastedBlocks += 1
        break
      case 'traffic': {
        const prev = this.lastTraffic.get(e.info_hash)
        this.lastTraffic.set(e.info_hash, { at: e.at_ms, down: e.downloaded, up: e.uploaded })
        if (!prev) break
        // bytes since the last totals go into this second's bucket as they are: a bucket
        // is a second, so its sum is a rate, and two samples landing in the same second
        // (the swarm's tick catching up after a stall) just add up instead of each being
        // divided by a tiny interval
        const t = Math.floor(e.at_ms / 1000)
        const point = this.buckets.get(t) ?? { t, down: 0, up: 0 }
        point.down += Math.max(0, e.downloaded - prev.down)
        point.up += Math.max(0, e.uploaded - prev.up)
        this.buckets.set(t, point)
        // a bucket is final once a later second has data; publish everything but the newest
        const keys = [...this.buckets.keys()].sort((a, b) => a - b)
        while (keys.length > SERIES_POINTS + 1) this.buckets.delete(keys.shift()!)
        this.rates = keys.slice(0, -1).map((k) => this.buckets.get(k)!)
        break
      }
      case 'peers_discovered': {
        const label = sourceLabel(e.source)
        this.discovery.set(label, (this.discovery.get(label) ?? 0) + e.count)
        break
      }
      case 'dial_failed':
        this.dialsFailed += 1
        break
      case 'peer_connected': {
        this.peers.set(e.addr, {
          addr: e.addr,
          info_hash: e.info_hash,
          client: e.client || 'unknown',
          dialed: e.dialed,
          encrypted: e.encrypted,
          utp: e.utp,
          connected_at: e.at_ms,
          left_at: null,
          reason: null,
          downloaded: 0,
          uploaded: 0,
          rx_bps: 0,
          history: [],
          picks: 0,
          exploit: 0,
          explore: 0,
          choked_us: true,
          choked_them: true,
          pieces: 0,
        })
        const client = e.client || 'unknown'
        this.clients.set(client, (this.clients.get(client) ?? 0) + 1)
        break
      }
      case 'peer_disconnected': {
        const p = this.peers.get(e.addr)
        if (p) {
          this.peers.set(e.addr, { ...p, left_at: e.at_ms, reason: e.reason, downloaded: e.downloaded, uploaded: e.uploaded, rx_bps: 0 })
          this.departed.push(e.addr)
          while (this.departed.length > DEPARTED_KEPT) {
            const gone = this.departed.shift()!
            if (this.peers.get(gone)?.left_at !== null) this.peers.delete(gone)
          }
        }
        this.disconnects.set(e.reason, (this.disconnects.get(e.reason) ?? 0) + 1)
        break
      }
      case 'choke_changed': {
        const p = this.peers.get(e.addr)
        if (p) this.peers.set(e.addr, { ...p, ...(e.by_us ? { choked_them: e.choked } : { choked_us: e.choked }) })
        break
      }
      case 'peer_picked': {
        // a first pick has an infinite bonus, which JSON carries as null
        const explore = e.explore ?? 1.5
        const p = this.peers.get(e.addr)
        if (p) this.peers.set(e.addr, { ...p, picks: e.picked_count, exploit: e.exploit, explore })
        this.picks = [...this.picks, { at: e.at_ms, addr: e.addr, exploit: e.exploit, explore }].slice(-PICKS_KEPT)
        break
      }
      case 'peer_sample': {
        const p = this.peers.get(e.addr)
        if (!p) break
        const history = [...p.history, e.rx_bps].slice(-PEER_HISTORY)
        this.peers.set(e.addr, {
          ...p,
          rx_bps: e.rx_bps,
          downloaded: e.downloaded,
          uploaded: e.uploaded,
          choked_us: e.choked_us,
          choked_them: e.choked_them,
          history,
        })
        break
      }
      case 'announced':
        this.announces = [...this.announces, { at: e.at_ms, url: e.url, ok: true, peers: e.peers, detail: `next in ${e.interval_secs}s` }].slice(-100)
        break
      case 'announce_failed':
        this.announces = [...this.announces, { at: e.at_ms, url: e.url, ok: false, peers: 0, detail: e.error }].slice(-100)
        break
      case 'dht_lookup':
        this.announces = [...this.announces, { at: e.at_ms, url: 'DHT', ok: true, peers: e.peers, detail: `${e.took_ms} ms` }].slice(-100)
        break
    }
  }

  private pieceMap(info_hash: string, atLeast: number): PieceMap {
    let map = this.pieces.get(info_hash)
    if (!map || map.total < atLeast) {
      const total = Math.max(atLeast, map?.total ?? 0)
      const grown: PieceMap = {
        info_hash,
        name: map?.name ?? this.names.get(info_hash) ?? info_hash.slice(0, 8),
        total,
        state: new Uint8Array(total),
        order: new Uint32Array(total),
        arrived: map?.arrived ?? 0,
        failed: map?.failed ?? 0,
      }
      if (map) {
        grown.state.set(map.state)
        grown.order.set(map.order)
      }
      map = grown
      this.pieces.set(info_hash, map)
    }
    return map
  }

  clear() {
    this.feed = []
    this.counts.clear()
    this.received = 0
    this.lagged = 0
    this.rates = []
    this.buckets.clear()
    this.lastTraffic.clear()
    this.peers.clear()
    this.departed = []
    this.picks = []
    this.discovery.clear()
    this.clients.clear()
    this.disconnects.clear()
    this.announces = []
    this.lifecycle = []
    this.dialsFailed = 0
    this.wasted = 0
    this.wastedBlocks = 0
  }
}
