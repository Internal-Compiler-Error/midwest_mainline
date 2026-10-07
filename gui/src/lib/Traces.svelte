<script lang="ts">
  // A live timeline of the selected torrent's spans (see spans.svelte.ts): one lane per kind
  // of work, spans packed into rows so concurrent ones stack, coloured by how they ended.
  // Open spans grow up to now. Clicking a span pins its fields and events on the right.
  import * as echarts from 'echarts'
  import { Button } from '$lib/components/ui/button'
  import { echart, base, type Interactive } from './echart'
  import { duration, humanBytes } from './format'
  import SpanDetail from './SpanDetail.svelte'
  import { field, fieldValue, involves, type Traces } from './spans.svelte'
  import { BANDS, LEGEND, layout, pieceOutcome } from './timeline'

  let { traces }: { traces: Traces } = $props()

  /** `fit` (0) spans from the torrent's first span to now, up to the longest fixed window. */
  const WINDOWS = [
    { label: 'fit', ms: 0 },
    { label: '30 s', ms: 30_000 },
    { label: '2 min', ms: 120_000 },
    { label: '10 min', ms: 600_000 },
    { label: '30 min', ms: 1_800_000 },
  ]

  let windowMs = $state(0)
  let following = $state(true)
  /// where the view ends while not following
  let frozenAt = $state(Date.now())
  let pinned = $state<number | null>(null)

  /// the live edge, advanced every frame (at ~15 fps) between polls so the view scrolls
  /// smoothly instead of stepping with each one
  let frameNow = $state(Date.now())
  $effect(() => {
    if (!following) return
    let raf = 0
    let last = 0
    const tick = (t: number) => {
      if (t - last >= 66) {
        frameNow = Date.now()
        last = t
      }
      raf = requestAnimationFrame(tick)
    }
    raf = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(raf)
  })
  let now = $derived(following ? frameNow : frozenAt)
  /** every span, or with a peer in focus only the ones about it */
  let pool = $derived.by(() => {
    const all = [...traces.finished, ...traces.open]
    const peer = traces.focus
    return peer === null ? all : all.filter((s) => involves(s, peer))
  })
  let firstStart = $derived(Math.min(...pool.map((s) => s.start_ms), now - 10_000))
  let viewStart = $derived(
    windowMs > 0 ? now - windowMs : Math.max(firstStart - (now - firstStart) * 0.02, now - 1_800_000),
  )

  let visible = $derived(pool.filter((s) => s.start_ms <= now && (s.end_ms ?? now) >= viewStart))

  /** The focused peer's story in numbers. Its connection span records bytes only when it
   * closes, so while it's open the pieces it delivered stand in. */
  let focusSummary = $derived.by(() => {
    if (traces.focus === null) return null
    const conns = pool.filter((s) => s.name === 'peer')
    const latest = conns.at(-1)
    const pieces = pool.filter((s) => s.name === 'piece' && s.end_ms !== null)
    const verified = pieces.filter((s) => pieceOutcome(s) === 'verified' && field(s, 'peer') === traces.focus)
    const released = pieces.filter((s) => pieceOutcome(s) === 'released').length
    const failed = pieces.filter((s) => pieceOutcome(s) === 'failed').length
    const times = verified.map((s) => (s.end_ms ?? 0) - s.start_ms).sort((a, b) => a - b)
    const bytes = verified.reduce((sum, s) => sum + Number(field(s, 'size') ?? 0), 0)
    return {
      client: latest ? field(latest, 'client') : undefined,
      transport: latest ? field(latest, 'transport') : undefined,
      connected: latest?.end_ms === null,
      connections: conns.length,
      dials: pool.filter((s) => s.name === 'dial').length,
      verified: verified.length,
      released,
      failed,
      bytes,
      median: times.length ? times[Math.floor(times.length / 2)] : null,
    }
  })

  let bars = $derived(layout(visible, now, pinned))

  let chart = $derived.by((): Interactive => ({
    option: base({
      animation: false,
      grid: { left: 70, right: 12, top: 8, bottom: 24 },
      tooltip: {
        confine: true,
        formatter: (p) => {
          const span = bars[(p as { dataIndex: number }).dataIndex]?.span
          if (!span) return ''
          const took = (span.end_ms ?? now) - span.start_ms
          const rows = span.fields.map(([n, v]) => `<div><span style="opacity:.6">${n}</span> ${fieldValue(n, v)}</div>`)
          return `<b>${span.name}</b> · ${duration(took)}${span.end_ms === null ? ' (open)' : ''}${rows.join('')}`
        },
      },
      xAxis: {
        type: 'time',
        min: viewStart,
        max: now,
        axisLabel: { formatter: '{HH}:{mm}:{ss}' },
        splitLine: { show: true, lineStyle: { opacity: 0.15 } },
      },
      yAxis: { type: 'value', min: 0, max: 100, inverse: true, show: false },
      series: [
        {
          type: 'custom',
          // drawn whole every frame: progressive rendering would restart at each redraw and
          // never reach the last lanes
          progressive: 0,
          renderItem: (params, api) => {
            const start = api.coord([api.value(1), api.value(3)])
            const end = api.coord([api.value(2), api.value(3)])
            const height = (api.size?.([0, api.value(4)]) as number[])[1]
            const sys = params.coordSys as unknown as { x: number; y: number; width: number; height: number }
            const shape = echarts.graphic.clipRectByRect(
              // a pixel short at the end, so back-to-back spans in a row read as separate
              { x: start[0], y: start[1] + height * 0.1, width: Math.max(end[0] - start[0] - 1, 1.5), height: Math.max(height * 0.8, 1) },
              { x: sys.x, y: sys.y, width: sys.width, height: sys.height },
            )
            return (
              shape && {
                type: 'rect',
                shape: { ...shape, r: Math.min(2, shape.height / 2) },
                style: { fill: api.visual('color') as string, opacity: api.value(5) as number },
              }
            )
          },
          encode: { x: [1, 2], y: 3 },
          data: bars,
          markArea: {
            silent: true,
            label: { position: 'insideLeft', offset: [-66, 0], fontSize: 10, color: 'inherit', opacity: 0.75 },
            data: BANDS.map(({ lane, y0, y1 }, i) => [
              { yAxis: y0, name: lane.title, itemStyle: { color: i % 2 ? 'rgba(127,127,127,0.06)' : 'rgba(127,127,127,0.02)' } },
              { yAxis: y1 },
            ]) as never,
          },
          markLine: following
            ? { silent: true, symbol: 'none', animation: false, lineStyle: { color: '#3b82f6', opacity: 0.5 }, label: { show: false }, data: [{ xAxis: now }] }
            : undefined,
        },
      ],
    }),
    onclick: (p) => {
      const span = bars[p.dataIndex]?.span
      if (span) pinned = span.id
    },
  }))

  let detail = $derived(pinned === null ? undefined : traces.byId(pinned))

  // counts for the header: what's going on right now
  let openCounts = $derived.by(() => {
    const count = (name: string) => traces.open.filter((s) => s.name === name).length
    return { peers: count('peer'), pieces: count('piece'), dials: count('dial') }
  })
  let done = $derived.by(() => {
    const counts = { verified: 0, released: 0, failed: 0 }
    for (const s of traces.finished) if (s.name === 'piece') counts[pieceOutcome(s)]++
    return counts
  })
</script>

<div class="flex h-full flex-col">
  <div class="flex flex-wrap items-center gap-x-4 gap-y-1 border-b px-3 py-1.5 text-xs">
    <span class="tabular-nums text-muted-foreground">
      <b class="text-foreground">{openCounts.peers}</b> peers ·
      <b class="text-foreground">{openCounts.pieces}</b> pieces in flight ·
      <b class="text-foreground">{openCounts.dials}</b> dialling ·
      <span class="text-emerald-500">{done.verified} verified</span>
      {#if done.released}· <span class="text-amber-500">{done.released} released</span>{/if}
      {#if done.failed}· <span class="text-red-500">{done.failed} failed</span>{/if}
    </span>
    <span class="flex flex-wrap items-center gap-2 text-muted-foreground">
      {#each LEGEND as [label, colour] (label)}
        <span class="flex items-center gap-1"><i class="inline-block size-2 rounded-sm" style="background:{colour}"></i>{label}</span>
      {/each}
    </span>
    <span class="ml-auto flex items-center gap-1">
      {#each WINDOWS as w (w.ms)}
        <Button variant={windowMs === w.ms ? 'secondary' : 'ghost'} size="xs" aria-pressed={windowMs === w.ms} onclick={() => (windowMs = w.ms)}>
          {w.label}
        </Button>
      {/each}
      <Button
        variant="ghost"
        size="xs"
        title={following ? 'freeze the view' : 'follow the live edge'}
        onclick={() => {
          frozenAt = now
          following = !following
        }}>{following ? '❚❚ live' : '▶ resume'}</Button
      >
    </span>
  </div>
  {#if traces.focus !== null && focusSummary}
    <div class="flex flex-wrap items-center gap-x-3 gap-y-1 border-b bg-muted/40 px-3 py-1 text-xs tabular-nums">
      <span class="font-mono font-medium">{traces.focus}</span>
      {#if focusSummary.client}<span>{focusSummary.client}</span>{/if}
      {#if focusSummary.transport}<span class="text-muted-foreground">{focusSummary.transport}</span>{/if}
      <span class={focusSummary.connected ? 'text-emerald-500' : 'text-muted-foreground'}>
        {focusSummary.connected ? 'connected' : 'not connected'}{focusSummary.connections > 1 ? ` (${focusSummary.connections} connections)` : ''}
      </span>
      <span class="text-muted-foreground">
        {focusSummary.dials} dial{focusSummary.dials === 1 ? '' : 's'} ·
        <span class="text-emerald-500">{focusSummary.verified} pieces verified</span> ({humanBytes(focusSummary.bytes)})
        {#if focusSummary.released}· <span class="text-amber-500">{focusSummary.released} released</span>{/if}
        {#if focusSummary.failed}· <span class="text-red-500">{focusSummary.failed} failed</span>{/if}
        {#if focusSummary.median !== null}· median piece {duration(focusSummary.median)}{/if}
      </span>
      <Button class="ml-auto" variant="ghost" size="xs" onclick={() => (traces.focus = null)}>✕ all peers</Button>
    </div>
  {/if}
  <div class="flex min-h-0 flex-1">
    <div class="min-w-0 flex-1" use:echart={chart}></div>
    {#if detail}
      <SpanDetail {traces} span={detail} {now} onpin={(id) => (pinned = id)} />
    {/if}
  </div>
</div>
