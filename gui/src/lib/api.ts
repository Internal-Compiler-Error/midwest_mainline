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
  /** the trackers and the DHT; empty while paused */
  trackers: Tracker[]
}

export interface Tracker {
  /** the announce URL, or "DHT" */
  url: string
  /** "waiting", "working", or what went wrong */
  status: string
  peers: number
  next_announce_secs: number | null
}

export interface TorrentFile {
  path: string
  size: number
  selected: boolean
}

export interface Peer {
  addr: string
  client: string
  progress: number
  downloaded: number
  uploaded: number
  download_bps: number
  upload_bps: number
  flags: string
}

export type TorrentRow = { id: TorrentId } & (
  | { kind: 'resolving'; source: string; elapsed_ms: number }
  | ({ kind: 'downloading' } & Progress)
  | ({ kind: 'paused' } & Progress)
  | ({ kind: 'queued' } & Progress)
  | { kind: 'checking'; name: string; checked_pieces: number; total_pieces: number }
  | { kind: 'failed'; source: string; error: string }
)

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
export const resumable = () => invoke<Resumable[]>('resumable')
export const logsSince = (seen: number) => invoke<LogChunk>('logs_since', { seen })
export const clearLogs = () => invoke<void>('clear_logs')
export const defaultDownloadDir = () => invoke<string>('default_download_dir')

export interface Settings {
  listen_port: number
  download_dir: string
  dht: boolean
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

export function fraction(verified: number, total: number): number {
  if (total === 0) return 0
  return Math.min(1, Math.max(0, verified / total))
}

const UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB']

export function humanBytes(bytes: number): string {
  return humanBytesLike(bytes, bytes)
}

/** `bytes` in the unit `humanBytes(like)` would pick. An animated figure formats its
 * in-between values like its destination, so only the digits move, never the unit. */
export function humanBytesLike(bytes: number, like: number): string {
  let scale = 1
  let unit = 0
  while (like / scale >= 1024 && unit < UNITS.length - 1) {
    scale *= 1024
    unit++
  }
  return unit === 0 ? `${Math.round(bytes)} B` : `${(bytes / scale).toFixed(1)} ${UNITS[unit]}`
}

export const perSecondLike = (bytes: number, like: number) => `${humanBytesLike(bytes, like)}/s`
