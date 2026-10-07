// How figures read across the UI. Animated figures (see Num.svelte) pass the value they're
// gliding towards as `like`, so the unit is the destination's and never flips mid-glide.

export function fraction(part: number, total: number): number {
  if (total === 0) return 0
  return Math.min(1, Math.max(0, part / total))
}

export const percent = (n: number) => `${Math.round(n)}%`

/** The binary unit `like` reads best in, and the divisor that goes with it. */
function binaryUnit(like: number, units: string[]): [number, string] {
  let scale = 1
  let unit = 0
  while (unit < units.length - 1 && Math.abs(like) >= scale * 1024) {
    scale *= 1024
    unit++
  }
  return [scale, units[unit]]
}

const BYTES = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
const RATES = ['B/s', 'KiB/s', 'MiB/s', 'GiB/s']

export function humanBytes(bytes: number, like: number = bytes): string {
  const [scale, unit] = binaryUnit(like, BYTES)
  return scale === 1 ? `${Math.round(bytes)} B` : `${(bytes / scale).toFixed(1)} ${unit}`
}

/** A transfer rate to about three significant figures, so only digits that mean something move. */
export function rate(bps: number, like: number = bps): string {
  const [scale, unit] = binaryUnit(like, RATES)
  const v = Math.max(bps, 0) / scale
  const digits = scale === 1 || v >= 100 ? 0 : v >= 10 ? 1 : 2
  return `${v.toFixed(digits)} ${unit}`
}

/** A span's length: milliseconds under a second, then seconds. */
export function duration(ms: number): string {
  return ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(ms < 10_000 ? 2 : 1)} s`
}

/** Wall-clock time of a unix-milliseconds instant, 24-hour, optionally to the millisecond. */
export function timeOfDay(ms: number, millis = false): string {
  const time = new Date(ms).toLocaleTimeString(undefined, { hour12: false })
  return millis ? `${time}.${String(Math.floor(ms % 1000)).padStart(3, '0')}` : time
}
