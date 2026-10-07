// The library's tracing output for the console pane, fetched a chunk at a time from the
// backend's ring buffer (see `logs_since` in src-tauri).
import { clearLogs, logsSince } from './api'

/** as many as the backend keeps, so polling only while the pane is open loses nothing */
const LINES_KEPT = 5000

export class Logs {
  lines = $state.raw<string[]>([])
  /** the session-wide number of `lines[0]`, a stable key for each line as older ones drop off */
  first = $state(0)
  #seen = 0
  #cleared = 0

  async poll() {
    const cleared = this.#cleared
    const chunk = await logsSince(this.#seen)
    if (cleared !== this.#cleared || chunk.lines.length === 0) return
    this.#seen = chunk.seen
    const all = this.lines.concat(chunk.lines)
    const dropped = Math.max(0, all.length - LINES_KEPT)
    this.first += dropped
    this.lines = dropped ? all.slice(dropped) : all
  }

  clear() {
    this.#cleared++
    this.first += this.lines.length
    this.lines = []
    void clearLogs()
  }
}
