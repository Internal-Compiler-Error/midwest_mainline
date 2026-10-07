// The selected torrent's spans, polled from the trace recorder (see downloader::telemetry) and
// kept here so the timeline can redraw from them. Finished spans accumulate; open ones are
// replaced by each poll, since they're still changing.
import { traces, type TraceSpan } from './api'

/** How far back finished spans are kept on this side; the recorder keeps more. */
const KEEP_MS = 30 * 60 * 1000
const POLL_MS = 250

export class Traces {
  finished = $state.raw<TraceSpan[]>([])
  open = $state.raw<TraceSpan[]>([])
  /** wall clock of the last poll, what "now" is for spans still open */
  now = $state(Date.now())
  /** a peer address (as `peer` fields carry it) to narrow the view to, or null for all */
  focus = $state<string | null>(null)
  #infoHash: string | null = null
  #seq = 0
  #timer: ReturnType<typeof setInterval> | null = null

  /** Starts following `infoHash`'s spans, or stops with null. */
  follow(infoHash: string | null) {
    if (infoHash === this.#infoHash) return
    if (this.#infoHash !== null) this.focus = null
    this.#infoHash = infoHash
    this.#seq = 0
    this.finished = []
    this.open = []
    if (this.#timer) clearInterval(this.#timer)
    this.#timer = null
    if (infoHash === null) return
    void this.#poll()
    this.#timer = setInterval(() => void this.#poll(), POLL_MS)
  }

  async #poll() {
    const asked = this.#infoHash
    if (asked === null) return
    try {
      const snapshot = await traces(asked, this.#seq)
      if (asked !== this.#infoHash) return
      this.#seq = snapshot.seq
      this.now = Date.now()
      const cutoff = this.now - KEEP_MS
      const kept = this.finished.filter((s) => (s.end_ms ?? 0) >= cutoff)
      this.finished = snapshot.finished.length ? kept.concat(snapshot.finished) : kept
      this.open = snapshot.open
    } catch {
      // the backend is restarting or the torrent went away; the next poll tries again
    }
  }

  /** Every span, finished first, with `id` to look one up by. */
  byId(id: number): TraceSpan | undefined {
    return this.open.find((s) => s.id === id) ?? this.finished.find((s) => s.id === id)
  }

  children(id: number): TraceSpan[] {
    return [...this.finished, ...this.open].filter((s) => s.parent === id)
  }
}

export function field(span: TraceSpan, name: string): string | undefined {
  return span.fields.find(([n]) => n === name)?.[1]
}

/** Whether `span` is about `peer`: its own `peer` field, or an event inside it naming the peer
 * (a racer joining a piece, a claim released). */
export function involves(span: TraceSpan, peer: string): boolean {
  // a web seed is listed by its URL, and its spans name it by host
  if (/^https?:\/\//.test(peer)) {
    const host = new URL(peer).host
    return field(span, 'host') === host || field(span, 'peer') === host
  }
  return field(span, 'peer') === peer || span.events.some((e) => e.message.includes(`peer=${peer}`))
}
