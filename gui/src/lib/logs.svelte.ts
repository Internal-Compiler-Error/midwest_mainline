// The library's tracing output for the console pane, fetched a chunk at a time from the
// backend's ring buffer (see `logs_since` in src-tauri).
import { clearLogs, logsSince } from './api'

const LINES_KEPT = 5000

export class Logs {
  lines = $state<string[]>([])
  #seen = 0
  #cleared = 0

  async poll() {
    const cleared = this.#cleared
    const chunk = await logsSince(this.#seen)
    if (cleared !== this.#cleared || chunk.lines.length === 0) return
    this.#seen = chunk.seen
    this.lines = [...this.lines, ...chunk.lines].slice(-LINES_KEPT)
  }

  clear() {
    this.#cleared++
    this.lines = []
    void clearLogs()
  }
}
