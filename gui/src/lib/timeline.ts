// The Traces pane's layout: which lane a span goes in, how spans pack into a lane's rows, and
// what colour says about how one ended.
import type { TraceSpan } from './api'
import { field } from './spans.svelte'

interface Lane {
  title: string
  names: string[]
  /** the most rows the lane packs into before spans overlap */
  rows: number
  /** its share of the height */
  weight: number
}

const LANES: Lane[] = [
  { title: 'Metadata', names: ['metadata', 'metadata.peer'], rows: 8, weight: 1.1 },
  { title: 'Trackers', names: ['tracker.announce', 'tracker.scrape'], rows: 6, weight: 1.1 },
  { title: 'DHT', names: ['dht.lookup'], rows: 3, weight: 1.1 },
  { title: 'Web seeds', names: ['webseed'], rows: 8, weight: 1.1 },
  { title: 'Dials', names: ['dial'], rows: 48, weight: 2.4 },
  { title: 'Peers', names: ['peer'], rows: 64, weight: 3.2 },
  { title: 'Pieces', names: ['piece'], rows: 96, weight: 4 },
]

/** Each lane's band on the 0..100 y axis, top to bottom. */
export const BANDS = (() => {
  const total = LANES.reduce((sum, l) => sum + l.weight, 0)
  let y = 0
  return LANES.map((lane) => {
    const height = (lane.weight / total) * 100
    const band = { lane, y0: y, y1: y + height }
    y += height
    return band
  })
})()

const COLOURS = {
  inFlight: '#3b82f6',
  raced: '#8b5cf6',
  verified: '#10b981',
  released: '#f59e0b',
  failed: '#ef4444',
  tcp: '#38bdf8',
  utp: '#c084fc',
  dialling: '#a3a3a3',
  succeeded: '#22c55e',
  dialFailed: '#64748b',
  scraped: '#5eead4',
  scrapeFailed: '#f87171',
  announced: '#14b8a6',
  webseed: '#f59e0b',
  webseedOpen: '#fbbf24',
  metadata: '#6366f1',
  nothing: '#94a3b8',
}

export const LEGEND: [string, string][] = [
  ['in flight', COLOURS.inFlight],
  ['raced', COLOURS.raced],
  ['verified', COLOURS.verified],
  ['released', COLOURS.released],
  ['failed', COLOURS.failed],
  ['tcp', COLOURS.tcp],
  ['utp', COLOURS.utp],
  ['dial failed', COLOURS.dialFailed],
]

/** How a finished piece span ended: its data verified, its claim given up, or anything else. */
export function pieceOutcome(span: TraceSpan): 'verified' | 'released' | 'failed' {
  const outcome = field(span, 'outcome')
  return outcome === 'verified' || outcome === 'released' ? outcome : 'failed'
}

function colour(span: TraceSpan): string {
  const open = span.end_ms === null
  const failed = !!field(span, 'error')
  switch (span.name) {
    case 'piece':
      if (open) return Number(field(span, 'racers') ?? 1) > 1 ? COLOURS.raced : COLOURS.inFlight
      return COLOURS[pieceOutcome(span)]
    case 'peer':
      return field(span, 'transport') === 'utp' ? COLOURS.utp : COLOURS.tcp
    case 'dial':
      if (open) return COLOURS.dialling
      return failed ? COLOURS.dialFailed : COLOURS.succeeded
    case 'tracker.scrape':
      return failed ? COLOURS.scrapeFailed : COLOURS.scraped
    case 'tracker.announce':
      return failed ? COLOURS.failed : COLOURS.announced
    case 'webseed':
      if (open) return COLOURS.webseedOpen
      return failed ? COLOURS.failed : COLOURS.webseed
    case 'dht.lookup':
      return Number(field(span, 'peers') ?? 0) > 0 ? COLOURS.verified : COLOURS.nothing
    case 'metadata':
      return COLOURS.metadata
    case 'metadata.peer':
      return field(span, 'outcome')?.endsWith('bytes') ? COLOURS.succeeded : COLOURS.nothing
    default:
      return COLOURS.nothing
  }
}

export interface Bar {
  /** id, start, end, y, height, opacity: what the chart's `renderItem` reads */
  value: number[]
  itemStyle: { color: string; opacity: number }
  span: TraceSpan
}

/** The bars: per lane, first-fit into rows, and when every row is busy into the one that
 * frees soonest (a lane that's always full overlaps rather than growing). A lane uses only as
 * many rows as it needs, so a few spans (one peer in focus, early on) draw thick. Open spans
 * end at `now`. */
export function layout(spans: TraceSpan[], now: number, pinned: number | null): Bar[] {
  const out: Bar[] = []
  for (const { lane, y0, y1 } of BANDS) {
    const inLane = spans.filter((s) => lane.names.includes(s.name)).sort((a, b) => a.start_ms - b.start_ms)
    const rowEnds: number[] = []
    const placed: [TraceSpan, number][] = []
    for (const span of inLane) {
      let row = rowEnds.findIndex((e) => e <= span.start_ms)
      if (row < 0 && rowEnds.length < lane.rows) row = rowEnds.push(-Infinity) - 1
      if (row < 0) row = rowEnds.indexOf(Math.min(...rowEnds))
      rowEnds[row] = span.end_ms ?? now
      placed.push([span, row])
    }
    // at least a few rows' worth of height per row, so one span isn't a slab
    const rowHeight = (y1 - y0) / Math.max(rowEnds.length, Math.min(lane.rows, 4))
    for (const [span, row] of placed) {
      const opacity = span.id === pinned ? 1 : span.end_ms === null ? 0.45 : 0.85
      out.push({
        value: [span.id, span.start_ms, span.end_ms ?? now, y0 + row * rowHeight, rowHeight, opacity],
        itemStyle: { color: colour(span), opacity },
        span,
      })
    }
  }
  return out
}
