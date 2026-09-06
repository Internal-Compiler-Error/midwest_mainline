// The library's event bus (downloader/src/events.rs) as it arrives over Tauri's `events`
// channel: batches of stamped events, serialized straight from the Rust enum. Keep the
// union in step with `Event` there.
import { listen } from '@tauri-apps/api/event'

export type MappingState =
  | { state: 'off' }
  | { state: 'searching' }
  | { state: 'mapped'; external_ip: string | null }
  | { state: 'unavailable' }

export type PeerSource =
  | { via: 'tracker'; url: string }
  | { via: 'dht' }
  | { via: 'pex'; from: string }
  | { via: 'lsd' }
  | { via: 'metadata' }

export type Event =
  | { kind: 'listening'; transport: 'tcp' | 'utp'; port: number }
  | { kind: 'port_mapping'; state: MappingState }
  | { kind: 'dht_up'; port: number; nodes: number }
  | { kind: 'torrent_resolved'; info_hash: string; name: string; size: number; pieces: number; piece_size: number; files: number }
  | { kind: 'metadata_fetched'; info_hash: string; from: string; bytes: number; took_ms: number }
  | { kind: 'torrent_started'; info_hash: string; verified: number; pieces: number }
  | { kind: 'torrent_queued'; info_hash: string }
  | { kind: 'torrent_paused'; info_hash: string }
  | { kind: 'torrent_checked'; info_hash: string; good: number; pieces: number }
  | { kind: 'torrent_completed'; info_hash: string }
  | { kind: 'torrent_failed'; source: string; error: string }
  | { kind: 'torrent_removed'; info_hash: string; deleted_files: boolean }
  | { kind: 'peers_discovered'; info_hash: string; source: PeerSource; count: number }
  | { kind: 'dial_failed'; info_hash: string; addr: string }
  | { kind: 'peer_connected'; info_hash: string; addr: string; client: string; dialed: boolean; encrypted: boolean; utp: boolean }
  | { kind: 'peer_disconnected'; info_hash: string; addr: string; downloaded: number; uploaded: number; reason: string }
  | { kind: 'choke_changed'; info_hash: string; addr: string; choked: boolean; by_us: boolean }
  | { kind: 'peer_picked'; info_hash: string; addr: string; piece: number; exploit: number; explore: number | null; picked_count: number; total_picks: number }
  | { kind: 'peer_sample'; info_hash: string; addr: string; rx_bps: number; downloaded: number; uploaded: number; outstanding: number; choked_us: boolean; choked_them: boolean }
  | { kind: 'pieces_known'; info_hash: string; bitfield: string }
  | { kind: 'piece_verified'; info_hash: string; piece: number; len: number; peers: string[] }
  | { kind: 'piece_failed'; info_hash: string; piece: number; peers: string[] }
  | { kind: 'block_wasted'; info_hash: string; addr: string; len: number; why: string }
  | { kind: 'traffic'; info_hash: string; downloaded: number; uploaded: number; wasted: number; peers: number }
  | { kind: 'announced'; info_hash: string; url: string; peers: number; interval_secs: number }
  | { kind: 'announce_failed'; info_hash: string; url: string; error: string }
  | { kind: 'dht_lookup'; info_hash: string; peers: number; took_ms: number }
  | { kind: 'lagged'; missed: number }

export type Kind = Event['kind']

export type Stamped = { seq: number; at_ms: number } & Event

/** Calls `handler` with every batch the backend sends; returns the unsubscribe. */
export function subscribe(handler: (batch: Stamped[]) => void): Promise<() => void> {
  return listen<Stamped[]>('events', (e) => handler(e.payload))
}

export function sourceLabel(source: PeerSource): string {
  switch (source.via) {
    case 'tracker':
      return 'tracker'
    case 'dht':
      return 'DHT'
    case 'pex':
      return 'PEX'
    case 'lsd':
      return 'LSD'
    case 'metadata':
      return 'metadata fetch'
  }
}
