<script lang="ts">
  // A live timeline of the selected torrent's spans (see spans.svelte.ts): one lane per kind
  // of work, spans packed into rows so concurrent ones stack, coloured by how they ended.
  // Open spans grow up to now. Clicking a span pins its fields and events on the right.
  import * as echarts from 'echarts'
  import { Button } from '$lib/components/ui/button'
  import { humanBytes, type TraceSpan } from './api'
  import { echart, type Interactive } from './echart'
  import { field, involves, type Traces } from './spans.svelte'

  let { traces }: { traces: Traces } = $props()

  interface Lane {
    title: string
    names: string[]
    rows: number
    weight: number
  }
  const LANES: Lane[] = [
    { title: 'Metadata', names: ['metadata', 'metadata.peer'], rows: 8, weight: 1.1 },
    { title: 'Trackers', names: ['tracker.announce'], rows: 6, weight: 1.1 },
    { title: 'DHT', names: ['dht.lookup'], rows: 3, weight: 1.1 },
    { title: 'Dials', names: ['dial'], rows: 48, weight: 2.4 },
    { title: 'Peers', names: ['peer'], rows: 64, weight: 3.2 },
    { title: 'Pieces', names: ['piece'], rows: 96, weight: 4 },
  ]
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
  let viewEnd = $derived(now)
  /** every span, or with a peer in focus only the ones about it */
  let pool = $derived.by(() => {
    const all = [...traces.finished, ...traces.open]
    const peer = traces.focus
    return peer === null ? all : all.filter((s) => involves(s, peer))
  })
  let firstStart = $derived(Math.min(...pool.map((s) => s.start_ms), viewEnd - 10_000))
  let viewStart = $derived(
    windowMs > 0 ? viewEnd - windowMs : Math.max(firstStart - (viewEnd - firstStart) * 0.02, viewEnd - 1_800_000),
  )

  function colour(span: TraceSpan): string {
    const outcome = field(span, 'outcome')
    const open = span.end_ms === null
    switch (span.name) {
      case 'piece':
        if (open) return Number(field(span, 'racers') ?? 1) > 1 ? '#8b5cf6' : '#3b82f6'
        if (outcome === 'verified') return '#10b981'
        if (outcome === 'released') return '#f59e0b'
        return '#ef4444'
      case 'peer':
        return field(span, 'transport') === 'utp' ? '#c084fc' : '#38bdf8'
      case 'dial':
        if (open) return '#a3a3a3'
        return field(span, 'error') ? '#64748b' : '#22c55e'
      case 'tracker.announce':
        return field(span, 'error') ? '#ef4444' : '#14b8a6'
      case 'dht.lookup':
        return Number(field(span, 'peers') ?? 0) > 0 ? '#10b981' : '#94a3b8'
      case 'metadata':
        return '#6366f1'
      case 'metadata.peer':
        return outcome?.endsWith('bytes') ? '#22c55e' : '#94a3b8'
      default:
        return '#94a3b8'
    }
  }

  const ms = (v: number) => (v < 1000 ? `${Math.round(v)} ms` : `${(v / 1000).toFixed(v < 10_000 ? 2 : 1)} s`)
  const clock = (t: number) =>
    new Date(t).toLocaleTimeString(undefined, { hour12: false }) + '.' + String(Math.floor(t % 1000)).padStart(3, '0')
  const pretty = (name: string, value: string) =>
    (name === 'downloaded' || name === 'uploaded' || name === 'size') && /^\d+$/.test(value) ? humanBytes(Number(value)) : value

  /** Each lane's band on the 0..100 y axis, top to bottom. */
  const bands = (() => {
    const total = LANES.reduce((sum, l) => sum + l.weight, 0)
    let y = 0
    return LANES.map((lane) => {
      const height = (lane.weight / total) * 100
      const band = { lane, y0: y, y1: y + height, rowHeight: height / lane.rows }
      y += height
      return band
    })
  })()

  let visible = $derived(pool.filter((s) => s.start_ms <= viewEnd && (s.end_ms ?? now) >= viewStart))

  /** The focused peer's story in numbers. Its connection span records bytes only when it
   * closes, so while it's open the pieces it delivered stand in. */
  let focusSummary = $derived.by(() => {
    if (traces.focus === null) return null
    const conns = pool.filter((s) => s.name === 'peer')
    const latest = conns.at(-1)
    const pieces = pool.filter((s) => s.name === 'piece' && s.end_ms !== null)
    const verified = pieces.filter((s) => field(s, 'outcome') === 'verified' && field(s, 'peer') === traces.focus)
    const released = pieces.filter((s) => field(s, 'outcome') === 'released').length
    const failed = pieces.filter((s) => field(s, 'outcome') !== 'verified' && field(s, 'outcome') !== 'released').length
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

  /** The bars: per lane, first-fit into rows, and when every row is busy into the one
   * that frees soonest (a lane that's always full overlaps rather than growing). A lane uses
   * only as many rows as it needs, so a few spans (one peer in focus, early on) draw thick. */
  let bars = $derived.by(() => {
    const out: { value: number[]; itemStyle: { color: string; opacity: number }; span: TraceSpan }[] = []
    for (const { lane, y0, y1 } of bands) {
      const spans = visible.filter((s) => lane.names.includes(s.name)).sort((a, b) => a.start_ms - b.start_ms)
      const rowEnds: number[] = []
      const placed: [TraceSpan, number][] = []
      for (const span of spans) {
        const end = span.end_ms ?? now
        let row = rowEnds.findIndex((e) => e <= span.start_ms)
        if (row < 0 && rowEnds.length < lane.rows) row = rowEnds.push(-Infinity) - 1
        if (row < 0) row = rowEnds.indexOf(Math.min(...rowEnds))
        rowEnds[row] = end
        placed.push([span, row])
      }
      // at least a few rows' worth of height per row, so one span isn't a slab
      const rowHeight = (y1 - y0) / Math.max(rowEnds.length, Math.min(lane.rows, 4))
      for (const [span, row] of placed) {
        out.push({
          value: [span.id, span.start_ms, span.end_ms ?? now, y0 + row * rowHeight, rowHeight],
          itemStyle: { color: colour(span), opacity: span.id === pinned ? 1 : span.end_ms === null ? 0.45 : 0.85 },
          span,
        })
      }
    }
    return out
  })

  let chart = $derived.by((): Interactive => ({
    option: {
      animation: false,
      textStyle: { fontFamily: 'Inter Variable, system-ui, sans-serif', fontSize: 11 },
      grid: { left: 70, right: 12, top: 8, bottom: 24 },
      tooltip: {
        confine: true,
        formatter: (p) => {
          const span = bars[(p as { dataIndex: number }).dataIndex]?.span
          if (!span) return ''
          const took = (span.end_ms ?? now) - span.start_ms
          const rows = span.fields.map(([n, v]) => `<div><span style="opacity:.6">${n}</span> ${pretty(n, v)}</div>`)
          return `<b>${span.name}</b> · ${ms(took)}${span.end_ms === null ? ' (open)' : ''}${rows.join('')}`
        },
      },
      xAxis: {
        type: 'time',
        min: viewStart,
        max: viewEnd,
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
            return shape && { type: 'rect', shape: { ...shape, r: Math.min(2, shape.height / 2) }, style: api.style() }
          },
          encode: { x: [1, 2], y: 3 },
          data: bars,
          markArea: {
            silent: true,
            label: { position: 'insideLeft', offset: [-66, 0], fontSize: 10, color: 'inherit', opacity: 0.75 },
            data: bands.map(({ lane, y0, y1 }, i) => [
              { yAxis: y0, name: lane.title, itemStyle: { color: i % 2 ? 'rgba(127,127,127,0.06)' : 'rgba(127,127,127,0.02)' } },
              { yAxis: y1 },
            ]) as never,
          },
          markLine: following
            ? { silent: true, symbol: 'none', animation: false, lineStyle: { color: '#3b82f6', opacity: 0.5 }, label: { show: false }, data: [{ xAxis: viewEnd }] }
            : undefined,
        },
      ],
    },
    onclick: (p) => {
      const span = bars[p.dataIndex]?.span
      if (span) pinned = span.id
    },
  }))

  let detail = $derived(pinned === null ? undefined : traces.byId(pinned))
  let detailChildren = $derived(pinned === null ? [] : traces.children(pinned))

  // counts for the header: what's going on right now
  let openCounts = $derived.by(() => {
    const count = (name: string) => traces.open.filter((s) => s.name === name).length
    return { peers: count('peer'), pieces: count('piece'), dials: count('dial') }
  })
  let done = $derived.by(() => {
    let verified = 0
    let failed = 0
    let released = 0
    for (const s of traces.finished) {
      if (s.name !== 'piece') continue
      const outcome = field(s, 'outcome')
      if (outcome === 'verified') verified++
      else if (outcome === 'released') released++
      else failed++
    }
    return { verified, failed, released }
  })

  const legend = [
    ['in flight', '#3b82f6'],
    ['raced', '#8b5cf6'],
    ['verified', '#10b981'],
    ['released', '#f59e0b'],
    ['failed', '#ef4444'],
    ['tcp', '#38bdf8'],
    ['utp', '#c084fc'],
    ['dial failed', '#64748b'],
  ]
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
      {#each legend as [label, colour] (label)}
        <span class="flex items-center gap-1"><i class="inline-block size-2 rounded-sm" style="background:{colour}"></i>{label}</span>
      {/each}
    </span>
    <span class="ml-auto flex items-center gap-1">
      {#each WINDOWS as w (w.ms)}
        <Button variant={windowMs === w.ms ? 'secondary' : 'ghost'} size="xs" onclick={() => (windowMs = w.ms)}>{w.label}</Button>
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
        {#if focusSummary.median !== null}· median piece {ms(focusSummary.median)}{/if}
      </span>
      <Button class="ml-auto" variant="ghost" size="xs" onclick={() => (traces.focus = null)}>✕ all peers</Button>
    </div>
  {/if}
  <div class="flex min-h-0 flex-1">
    <div class="min-w-0 flex-1" use:echart={chart}></div>
    {#if detail}
      <aside class="w-72 shrink-0 overflow-y-auto border-l px-3 py-2 text-xs">
        <div class="mb-1 flex items-center justify-between">
          <b class="text-sm">{detail.name}</b>
          <Button variant="ghost" size="xs" onclick={() => (pinned = null)}>✕</Button>
        </div>
        <div class="mb-2 text-muted-foreground tabular-nums">
          {clock(detail.start_ms)} · {ms((detail.end_ms ?? now) - detail.start_ms)}{detail.end_ms === null ? ', open' : ''}
        </div>
        <dl class="grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5">
          {#each detail.fields as [name, value] (name)}
            <dt class="text-muted-foreground">{name}</dt>
            <dd class="break-all">{pretty(name, value)}</dd>
          {/each}
        </dl>
        {#if detailChildren.length}
          <h4 class="mt-3 mb-1 font-medium">inside</h4>
          {#each detailChildren as child (child.id)}
            <button class="block w-full text-left hover:underline" onclick={() => (pinned = child.id)}>
              {child.name} · {ms((child.end_ms ?? now) - child.start_ms)}
            </button>
          {/each}
        {/if}
        {#if detail.events.length}
          <h4 class="mt-3 mb-1 font-medium">events</h4>
          <ol class="space-y-0.5">
            {#each detail.events as event, i (i)}
              <li><span class="text-muted-foreground tabular-nums">+{ms(event.at_ms - detail.start_ms)}</span> {event.message}</li>
            {/each}
          </ol>
        {/if}
        {#if field(detail, 'peer') && field(detail, 'peer') !== traces.focus}
          <Button class="mt-3 mr-1" variant="outline" size="xs" onclick={() => (traces.focus = field(detail, 'peer') ?? null)}>
            focus this peer
          </Button>
        {/if}
        {#if detail.parent !== null && traces.byId(detail.parent)}
          <Button class="mt-3" variant="outline" size="xs" onclick={() => (pinned = detail.parent)}>↑ parent</Button>
        {/if}
      </aside>
    {/if}
  </div>
</div>
