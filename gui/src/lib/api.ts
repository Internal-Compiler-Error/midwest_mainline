// The Tauri commands in src-tauri/src/main.rs, typed. Everything the UI knows about a
// torrent comes through `torrents()`; everything it does goes through the other three.
import { invoke } from '@tauri-apps/api/core'

export type TorrentId = number

export interface Progress {
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

export interface LogChunk {
  seen: number
  lines: string[]
}

export const torrents = () => invoke<TorrentRow[]>('torrents')
export const addTorrent = (source: string, root: string) => invoke<TorrentId>('add_torrent', { source, root })
export const resumeTorrent = (path: string) => invoke<TorrentId>('resume_torrent', { path })
export const removeTorrent = (id: TorrentId, deleteFiles: boolean) =>
  invoke<void>('remove_torrent', { id, deleteFiles })
export const pauseTorrent = (id: TorrentId) => invoke<void>('pause_torrent', { id })
export const selectFiles = (id: TorrentId, selected: boolean[]) => invoke<void>('select_files', { id, selected })
export const unpauseTorrent = (id: TorrentId) => invoke<void>('unpause_torrent', { id })
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
  let value = bytes
  let unit = 0
  while (value >= 1024 && unit < UNITS.length - 1) {
    value /= 1024
    unit++
  }
  return unit === 0 ? `${bytes} B` : `${value.toFixed(1)} ${UNITS[unit]}`
}
