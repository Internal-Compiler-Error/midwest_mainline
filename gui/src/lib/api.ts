// The Tauri commands in src-tauri/src/main.rs, typed. Everything the UI knows about a
// torrent comes through `torrents()`; everything it does goes through the other three.
import { invoke } from '@tauri-apps/api/core'

export type TorrentId = number

export interface Progress {
  /** 40 hex digits, how the event bus names the torrent */
  info_hash: string
  name: string
  root: string
  files: TorrentFile[]
  total_size: number
  downloaded: number
  wasted: number
  uploaded: number
  left: number
  verified_pieces: number
  total_pieces: number
  completed: boolean
  download_bps: number
  upload_bps: number
  peers: Peer[]
  /** pieces are fetched in order, for playing a file while it downloads */
  sequential: boolean
  super_seed: boolean
  /** the trackers and the DHT; empty while paused */
  trackers: Tracker[]
  /** BEP 46: the DHT key it updates through */
  feed: Feed | null
}

export interface Feed {
  /** the public key, 64 hex digits */
  key: string
  /** hex, empty for none */
  salt: string
  /** the item seq that named this torrent; null when it came from the magnet before the DHT had one */
  seq: number | null
  /** a newer version, at this seq, was added as a torrent of its own */
  superseded: number | null
}

export interface Tracker {
  /** the announce URL, or "DHT" */
  url: string
  state: 'pending' | 'working' | 'failed'
  /** what went wrong, for "failed" */
  error: string | null
  peers: number
  next_announce_secs: number | null
  /** the swarm as this tracker counts it, null where it hasn't said */
  seeders: number | null
  leechers: number | null
  /** times the torrent has been downloaded to completion */
  downloaded: number | null
}

export interface TorrentFile {
  path: string
  size: number
  selected: boolean
  /** BEP 47 padding: never on disk, not listed */
  pad: boolean
}

export interface Peer {
  addr: string
  client: string
  progress: number
  downloaded: number
  uploaded: number
  download_bps: number
  upload_bps: number
  choked_us: boolean
  choked_them: boolean
  interested_us: boolean
  interested_them: boolean
  encrypted: boolean
  utp: boolean
}

/** The usual client shorthand: `D`/`d` we download from it (`d`: want to, but choked),
 * `U`/`u` it downloads from us (`u`: wants to, but we choke it), `E` encrypted, `T` uTP. */
export function peerFlags(p: Peer): string {
  let flags = ''
  if (p.interested_them) flags += p.choked_us ? 'd' : 'D'
  if (p.interested_us) flags += p.choked_them ? 'u' : 'U'
  if (p.encrypted) flags += 'E'
  if (p.utp) flags += 'T'
  return flags
}

export function trackerStatus(t: Tracker): string {
  return t.state === 'failed' ? (t.error ?? 'failed') : t.state === 'working' ? 'working' : 'waiting'
}

export type TorrentRow = { id: TorrentId } & (
  | { kind: 'resolving'; source: string; elapsed_ms: number }
  | ({ kind: 'downloading' } & Progress)
  | ({ kind: 'paused' } & Progress)
  | ({ kind: 'queued' } & Progress)
  | { kind: 'checking'; name: string; checked_pieces: number; total_pieces: number }
  | { kind: 'failed'; source: string; error: string }
)

/** A row whose torrent is known: running, paused, or waiting its turn. */
export type Known = Extract<TorrentRow, { kind: 'downloading' | 'paused' | 'queued' }>

export function isKnown(row: TorrentRow): row is Known {
  return row.kind === 'downloading' || row.kind === 'paused' || row.kind === 'queued'
}

export interface Resumable {
  path: string
  name: string
  root: string
  verified_pieces: number
  total_pieces: number
  total_size: number
  paused: boolean
}

export interface Status {
  download_bps: number
  upload_bps: number
  dht_nodes: number | null
  listen_port: number
  port_mapping: 'off' | 'searching' | 'mapped' | 'unavailable'
  external_ip: string | null
}

export interface LogChunk {
  seen: number
  lines: string[]
}

export const torrents = () => invoke<TorrentRow[]>('torrents')
export const status = () => invoke<Status>('status')
export const addTorrent = (source: string, root: string) => invoke<TorrentId>('add_torrent', { source, root })
export const resumeTorrent = (path: string) => invoke<TorrentId>('resume_torrent', { path })
export const removeTorrent = (id: TorrentId, deleteFiles: boolean) =>
  invoke<void>('remove_torrent', { id, deleteFiles })
export const pauseTorrent = (id: TorrentId) => invoke<void>('pause_torrent', { id })
export const selectFiles = (id: TorrentId, selected: boolean[]) => invoke<void>('select_files', { id, selected })
export const unpauseTorrent = (id: TorrentId) => invoke<void>('unpause_torrent', { id })
export const recheckTorrent = (id: TorrentId) => invoke<void>('recheck_torrent', { id })
export const setSequential = (id: TorrentId, on: boolean) => invoke<void>('set_sequential', { id, on })
export const setSuperSeed = (id: TorrentId, on: boolean) => invoke<void>('set_super_seed', { id, on })
export const resumable = () => invoke<Resumable[]>('resumable')
export const logsSince = (seen: number) => invoke<LogChunk>('logs_since', { seen })
export const clearLogs = () => invoke<void>('clear_logs')

export interface Settings {
  listen_port: number
  download_dir: string
  dht: boolean
  /** BEP 43: the DHT node asks but never answers; for hosts that can't take inbound UDP */
  dht_read_only: boolean
  max_peers_per_torrent: number
  /** bytes per second, 0 for no limit */
  download_limit: number
  upload_limit: number
  /** stop seeding at uploaded / size, 0 to seed forever */
  seed_ratio_limit: number
  /** MSE protocol encryption; obfuscation against traffic shaping, not secrecy */
  encryption: 'disabled' | 'prefer' | 'require'
  /** dial and accept peers over uTP as well as TCP */
  utp: boolean
  /** ask the router to forward our ports (NAT-PMP, PCP, or UPnP) */
  port_mapping: boolean
  /** torrents downloading at once, 0 for no limit; seeding doesn't count */
  max_active_downloads: number
}

export const settings = () => invoke<Settings>('settings')
/** resolves to whether a restart is needed for everything to take effect */
export const updateSettings = (settings: Settings) => invoke<boolean>('update_settings', { settings })

export const isMagnetUri = (s: string) => s.trim().toLowerCase().startsWith('magnet:?')

/** a BEP 46 magnet, which names a DHT key the torrent updates through */
export const isFeedUri = (s: string) => isMagnetUri(s) && /[?&]xs=urn(:|%3A)btpk(:|%3A)/i.test(s)

/** One span of a torrent's work (see downloader::telemetry), open or finished. */
export interface TraceSpan {
  id: number
  parent: number | null
  /** dial, peer, piece, piece.check, tracker.announce, dht.lookup, metadata, metadata.peer, ... */
  name: string
  /** unix milliseconds */
  start_ms: number
  /** null while still open */
  end_ms: number | null
  fields: [string, string][]
  events: { at_ms: number; level: string; message: string }[]
}

export interface TraceSnapshot {
  /** pass back as `since` next time */
  seq: number
  finished: TraceSpan[]
  open: TraceSpan[]
}

export const traces = (infoHash: string, since: number) => invoke<TraceSnapshot>('traces', { infoHash, since })

/** The info hash a row's spans are tagged with: known once it's running, and for a magnet
 * that's still resolving, read off its `xt=urn:btih:` (hex form only). */
export function infoHashOf(row: TorrentRow): string | null {
  if ('info_hash' in row) return row.info_hash
  if (row.kind === 'resolving' || row.kind === 'failed') {
    const m = /urn:btih:([0-9a-f]{40})/i.exec(row.source)
    return m ? m[1].toLowerCase() : null
  }
  return null
}
