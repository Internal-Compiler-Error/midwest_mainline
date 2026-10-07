/** Runs `tick` now and then every `ms`, each run starting only once the last has settled: a
 * slow call delays the next instead of piling up behind it, and two runs never see the same
 * cursor. A failed run is logged once per streak and the beat goes on. Returns the stop. */
export function every(ms: number, tick: () => Promise<unknown>): () => void {
  let stopped = false
  let failing = false
  let timer: ReturnType<typeof setTimeout> | undefined
  const run = async () => {
    const started = performance.now()
    try {
      await tick()
      failing = false
    } catch (e) {
      if (!failing) console.warn('poll failed:', e)
      failing = true
    }
    if (!stopped) timer = setTimeout(run, Math.max(0, ms - (performance.now() - started)))
  }
  void run()
  return () => {
    stopped = true
    clearTimeout(timer)
  }
}
