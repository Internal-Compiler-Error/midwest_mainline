<script lang="ts">
  // Live charts over the event bus (see bus.svelte.ts for what is accumulated). Every
  // chart is an ECharts option derived from the store; ECharts animates the changes.
  import { Button } from '$lib/components/ui/button'
  import { humanBytes } from './api'
  import { echart, base, type Option } from './echart'
  import type { Insights, PeerRecord } from './bus.svelte'
  import type { Kind, Stamped } from './events'

  let { insights }: { insights: Insights } = $props()
  let filter = $state<Kind | null>(null)
  let pieceTorrent = $state<string | null>(null)

  const clock = (ms: number) => new Date(ms).toLocaleTimeString(undefined, { hour12: false })
  const short = (addr: string) => addr.replace(/^\[?([^\]]+)\]?:(\d+)$/, '$1:$2')

  let connected = $derived([...insights.peers.values()].filter((p) => p.left_at === null))
  let byRate = $derived([...connected].sort((a, b) => b.rx_bps - a.rx_bps))

  // -- throughput, five minutes of one-second points
  let throughput = $derived.by((): Option => {
    const t = insights.rates.map((p) => p.t * 1000)
    return base({
      tooltip: { trigger: 'axis', valueFormatter: (v) => `${humanBytes(Number(v))}/s` },
      legend: { top: 0, right: 0, icon: 'circle' },
      xAxis: { type: 'time', axisLabel: { formatter: '{HH}:{mm}:{ss}' }, splitLine: { show: false } },
      yAxis: { type: 'value', axisLabel: { formatter: (v: number) => `${humanBytes(v)}/s` }, splitNumber: 3 },
      series: [
        {
          name: 'down',
          type: 'line',
          smooth: true,
          showSymbol: false,
          areaStyle: { opacity: 0.25 },
          data: insights.rates.map((p, i) => [t[i], p.down]),
        },
        {
          name: 'up',
          type: 'line',
          smooth: true,
          showSymbol: false,
          areaStyle: { opacity: 0.25 },
          data: insights.rates.map((p, i) => [t[i], p.up]),
        },
      ],
    })
  })

  // -- UCB: what each peer's last pick scored, split into its two halves
  let ucb = $derived.by((): Option => {
    const picked = connected.filter((p) => p.picks > 0).sort((a, b) => a.exploit + a.explore - (b.exploit + b.explore)).slice(-14)
    return base({
      tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
      legend: { top: 0, right: 0, icon: 'circle' },
      xAxis: { type: 'value', splitNumber: 3 },
      yAxis: { type: 'category', data: picked.map((p) => short(p.addr)), axisLabel: { fontSize: 10 } },
      series: [
        { name: 'exploit (rate)', type: 'bar', stack: 'ucb', data: picked.map((p) => p.exploit), barMaxWidth: 14 },
        {
          name: 'explore (bonus)',
          type: 'bar',
          stack: 'ucb',
          data: picked.map((p) => p.explore),
          barMaxWidth: 14,
          label: { show: true, position: 'right', formatter: (o) => `${picked[o.dataIndex as number].picks} picks`, fontSize: 9 },
        },
      ],
    })
  })

  // -- the last picks as points: far right is a proven peer, high up an unexplored one
  let scatter = $derived.by((): Option => {
    const addrs = [...new Set(insights.picks.map((p) => p.addr))]
    return base({
      tooltip: { formatter: (o) => `${(o as { data: unknown[] }).data[2]}` },
      xAxis: { name: 'exploit', nameGap: 4, splitNumber: 3, max: 1 },
      yAxis: { name: 'explore', nameGap: 4, splitNumber: 3 },
      series: [
        {
          type: 'scatter',
          symbolSize: 7,
          data: insights.picks.map((p) => [p.exploit, p.explore, short(p.addr)]),
          itemStyle: {
            color: (o) => `hsl(${(addrs.indexOf(((o.data ?? []) as unknown[])[2] as string) * 47) % 360} 70% 55%)`,
            opacity: 0.7,
          },
        },
      ],
    })
  })

  // -- peer rates as a bar race
  let race = $derived.by((): Option => {
    const top = byRate.slice(0, 8).reverse()
    return base({
      tooltip: { trigger: 'axis', valueFormatter: (v) => `${humanBytes(Number(v))}/s` },
      xAxis: { type: 'value', axisLabel: { formatter: (v: number) => `${humanBytes(v)}/s` }, splitNumber: 3 },
      yAxis: { type: 'category', data: top.map((p) => `${short(p.addr)} · ${p.client}`), axisLabel: { fontSize: 10, interval: 0 }, animationDuration: 300, animationDurationUpdate: 300 },
      series: [
        {
          type: 'bar',
          realtimeSort: true,
          barMaxWidth: 14,
          data: top.map((p) => p.rx_bps),
          itemStyle: { color: (o) => (top[o.dataIndex].encrypted ? '#7c3aed' : '#0ea5e9') },
          label: { show: true, position: 'right', formatter: (o) => `${top[o.dataIndex as number].pieces} pcs`, fontSize: 9 },
        },
      ],
    })
  })

  // -- the fastest peers' rate over the last minute
  let history = $derived.by((): Option => {
    const top = byRate.slice(0, 8)
    return base({
      tooltip: { trigger: 'axis', valueFormatter: (v) => `${humanBytes(Number(v))}/s` },
      legend: { top: 0, right: 0, icon: 'circle', textStyle: { fontSize: 9 } },
      xAxis: { type: 'category', data: [...Array(60).keys()].map((i) => `${i - 59}s`), axisLabel: { interval: 19 } },
      yAxis: { type: 'value', axisLabel: { formatter: (v: number) => `${humanBytes(v)}/s` }, splitNumber: 3 },
      series: top.map((p) => ({
        name: short(p.addr),
        type: 'line',
        smooth: true,
        showSymbol: false,
        data: [...Array(60 - p.history.length).fill(null), ...p.history],
      })),
    })
  })

  // -- how peers were found, and who they turned out to be
  let discovery = $derived.by((): Option => {
    const entries = [...insights.discovery.entries()].sort((a, b) => a[1] - b[1])
    return base({
      tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
      xAxis: { type: 'value', splitNumber: 3 },
      yAxis: { type: 'category', data: entries.map((e) => e[0]) },
      series: [{ type: 'bar', barMaxWidth: 16, data: entries.map((e) => e[1]), label: { show: true, position: 'right', fontSize: 9 } }],
    })
  })

  let clients = $derived.by((): Option => {
    const entries = [...insights.clients.entries()].sort((a, b) => b[1] - a[1])
    const shown = entries.slice(0, 7)
    const rest = entries.slice(7).reduce((n, e) => n + e[1], 0)
    if (rest > 0) shown.push(['others', rest])
    return base({
      tooltip: { trigger: 'item' },
      series: [
        {
          type: 'pie',
          roseType: 'radius',
          radius: ['18%', '75%'],
          center: ['50%', '55%'],
          itemStyle: { borderRadius: 4 },
          label: { fontSize: 9 },
          data: shown.map(([name, value]) => ({ name, value })),
        },
      ],
    })
  })

  let transport = $derived.by((): Option => {
    const all = [...insights.peers.values()]
    const count = (f: (p: PeerRecord) => boolean) => all.filter(f).length
    return base({
      tooltip: { trigger: 'item' },
      legend: { bottom: 0, icon: 'circle', textStyle: { fontSize: 9 } },
      series: [
        {
          type: 'pie',
          radius: ['35%', '60%'],
          center: ['50%', '45%'],
          itemStyle: { borderRadius: 3 },
          label: { show: false },
          data: [
            { name: 'TCP plain', value: count((p) => !p.utp && !p.encrypted) },
            { name: 'TCP encrypted', value: count((p) => !p.utp && p.encrypted) },
            { name: 'uTP plain', value: count((p) => p.utp && !p.encrypted) },
            { name: 'uTP encrypted', value: count((p) => p.utp && p.encrypted) },
          ],
        },
        {
          type: 'pie',
          radius: ['65%', '80%'],
          center: ['50%', '45%'],
          label: { show: false },
          data: [
            { name: 'dialed', value: count((p) => p.dialed) },
            { name: 'inbound', value: count((p) => !p.dialed) },
          ],
        },
      ],
    })
  })

  let goodbyes = $derived.by((): Option => {
    const entries = [...insights.disconnects.entries()].sort((a, b) => a[1] - b[1])
    return base({
      tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
      xAxis: { type: 'value', splitNumber: 3 },
      yAxis: { type: 'category', data: entries.map((e) => e[0]) },
      series: [{ type: 'bar', barMaxWidth: 16, data: entries.map((e) => e[1]), itemStyle: { color: '#f59e0b' }, label: { show: true, position: 'right', fontSize: 9 } }],
    })
  })

  // -- the piece map: one cell per piece (or per few pieces for a big torrent), coloured by
  // when it arrived
  let pieceMaps = $derived([...insights.pieces.values()])
  let shownPieces = $derived(pieceMaps.find((m) => m.info_hash === pieceTorrent) ?? pieceMaps[0])
  let pieceMap = $derived.by((): Option => {
    const map = shownPieces
    if (!map) return base({})
    const per = Math.max(1, Math.ceil(map.total / 6000))
    const cells = Math.ceil(map.total / per)
    const cols = Math.ceil(Math.sqrt(cells * 3))
    const rows = Math.ceil(cells / cols)
    const data: [number, number, number][] = []
    for (let c = 0; c < cells; c++) {
      let state = 0
      let order = 0
      let arrived = 0
      for (let i = c * per; i < Math.min(map.total, (c + 1) * per); i++) {
        state = Math.max(state, map.state[i])
        if (map.state[i] === 2) {
          order += map.order[i]
          arrived += 1
        }
      }
      // -2 failed, -1 had at start, 0..1 arrival order this session; missing stays blank
      const value = state === 3 ? -2 : state === 1 ? -1 : state === 2 ? order / arrived / Math.max(1, map.arrived) : null
      if (value !== null) data.push([c % cols, rows - 1 - Math.floor(c / cols), value])
    }
    return base({
      grid: { left: 2, right: 2, top: 2, bottom: 2 },
      tooltip: { formatter: (o) => `cell ${(o as { dataIndex: number }).dataIndex}` },
      xAxis: { type: 'category', show: false, data: [...Array(cols).keys()] },
      yAxis: { type: 'category', show: false, data: [...Array(rows).keys()] },
      visualMap: {
        show: false,
        type: 'piecewise',
        pieces: [
          { value: -2, color: '#ef4444' },
          { value: -1, color: '#64748b' },
          { min: 0, max: 0.2, color: '#22d3ee' },
          { min: 0.2, max: 0.4, color: '#38bdf8' },
          { min: 0.4, max: 0.6, color: '#3b82f6' },
          { min: 0.6, max: 0.8, color: '#8b5cf6' },
          { min: 0.8, max: 1.01, color: '#a855f7' },
        ],
      },
      series: [
        {
          type: 'heatmap',
          data,
          itemStyle: { borderWidth: 1, borderColor: 'transparent' },
          progressive: 4000,
          animation: false,
        },
      ],
    })
  })

  let kinds = $derived([...insights.counts.entries()].sort((a, b) => b[1] - a[1]))
  let shownFeed = $derived((filter ? insights.feed.filter((e) => e.kind === filter) : insights.feed).slice(-200).reverse())

  function describe(e: Stamped): string {
    const { seq: _s, at_ms: _a, kind: _k, ...rest } = e
    return Object.entries(rest)
      .map(([k, v]) => `${k}=${typeof v === 'object' && v !== null ? JSON.stringify(v) : typeof v === 'number' && !Number.isInteger(v) ? v.toFixed(3) : String(v)}`)
      .join('  ')
  }
</script>

<div class="h-full overflow-auto bg-card p-3 text-xs">
  <div class="mb-3 flex flex-wrap items-center gap-x-4 gap-y-1 text-muted-foreground tabular-nums">
    <span><b class="text-foreground">{insights.received}</b> events</span>
    <span><b class="text-foreground">{connected.length}</b> peers connected</span>
    <span><b class="text-foreground">{insights.dialsFailed}</b> dials failed</span>
    <span><b class="text-foreground">{humanBytes(insights.wasted)}</b> wasted in {insights.wastedBlocks} blocks</span>
    {#if insights.lagged > 0}<span class="text-amber-600 dark:text-amber-400">{insights.lagged} events missed (the panel fell behind)</span>{/if}
    <Button variant="ghost" size="xs" class="ml-auto" onclick={() => insights.clear()}>reset</Button>
  </div>

  <div class="grid grid-cols-2 gap-3 xl:grid-cols-3">
    <section class="rounded-md border p-2 xl:col-span-2">
      <h3 class="font-medium">Throughput</h3>
      <div class="h-52" use:echart={throughput}></div>
    </section>
    <section class="rounded-md border p-2">
      <h3 class="font-medium">Peers by rate <span class="font-normal text-muted-foreground">(purple: encrypted)</span></h3>
      <div class="h-52" use:echart={race}></div>
    </section>

    <section class="rounded-md border p-2">
      <h3 class="font-medium">UCB: the last pick per peer</h3>
      <div class="h-52" use:echart={ucb}></div>
    </section>
    <section class="rounded-md border p-2">
      <h3 class="font-medium">Recent picks <span class="font-normal text-muted-foreground">exploit vs explore</span></h3>
      <div class="h-52" use:echart={scatter}></div>
    </section>
    <section class="rounded-md border p-2">
      <h3 class="font-medium">Fastest peers, last minute</h3>
      <div class="h-52" use:echart={history}></div>
    </section>

    <section class="rounded-md border p-2 xl:col-span-2">
      <div class="flex items-center gap-2">
        <h3 class="font-medium">Piece map</h3>
        {#if pieceMaps.length > 1}
          <select class="rounded border bg-background px-1 text-xs" bind:value={pieceTorrent}>
            {#each pieceMaps as m (m.info_hash)}<option value={m.info_hash}>{m.name}</option>{/each}
          </select>
        {:else if shownPieces}
          <span class="text-muted-foreground">{shownPieces.name}</span>
        {/if}
        {#if shownPieces}
          <span class="ml-auto text-muted-foreground tabular-nums">
            {shownPieces.arrived} arrived this session · {shownPieces.failed} failed · {shownPieces.total} pieces
          </span>
        {/if}
      </div>
      <div class="h-44" use:echart={pieceMap}></div>
      <div class="flex gap-3 text-muted-foreground">
        <span><i class="inline-block size-2 rounded-sm bg-slate-500"></i> had at start</span>
        <span><i class="inline-block size-2 rounded-sm bg-cyan-400"></i> early <i class="inline-block size-2 rounded-sm bg-purple-500"></i> late</span>
        <span><i class="inline-block size-2 rounded-sm bg-red-500"></i> failed a hash check</span>
      </div>
    </section>
    <section class="rounded-md border p-2">
      <h3 class="font-medium">Peers found, by source</h3>
      <div class="h-44" use:echart={discovery}></div>
    </section>

    <section class="rounded-md border p-2">
      <h3 class="font-medium">Clients</h3>
      <div class="h-44" use:echart={clients}></div>
    </section>
    <section class="rounded-md border p-2">
      <h3 class="font-medium">Transport <span class="font-normal text-muted-foreground">(outer ring: who called whom)</span></h3>
      <div class="h-44" use:echart={transport}></div>
    </section>
    <section class="rounded-md border p-2">
      <h3 class="font-medium">Why peers left</h3>
      <div class="h-44" use:echart={goodbyes}></div>
    </section>

    <section class="rounded-md border p-2">
      <h3 class="font-medium">Announces</h3>
      <ul class="mt-1 max-h-44 overflow-auto font-mono text-[11px] leading-4">
        {#each [...insights.announces].reverse() as a (a.at + a.url)}
          <li class="truncate" class:text-destructive={!a.ok}>
            {clock(a.at)} {a.url.replace(/^\w+:\/\//, '').slice(0, 32)} → {a.ok ? `${a.peers} peers, ${a.detail}` : a.detail}
          </li>
        {:else}
          <li class="text-muted-foreground">nothing yet</li>
        {/each}
      </ul>
    </section>
    <section class="rounded-md border p-2 xl:col-span-2">
      <h3 class="font-medium">Lifecycle</h3>
      <ul class="mt-1 max-h-44 overflow-auto font-mono text-[11px] leading-4">
        {#each [...insights.lifecycle].reverse() as e (e.seq)}
          <li class="truncate">{clock(e.at_ms)} <b>{e.kind}</b> {describe(e)}</li>
        {:else}
          <li class="text-muted-foreground">nothing yet</li>
        {/each}
      </ul>
    </section>

    <section class="col-span-2 rounded-md border p-2 xl:col-span-3">
      <div class="flex flex-wrap items-center gap-1">
        <h3 class="mr-2 font-medium">Feed</h3>
        <Button variant={filter === null ? 'secondary' : 'ghost'} size="xs" onclick={() => (filter = null)}>all</Button>
        {#each kinds as [kind, n] (kind)}
          <Button variant={filter === kind ? 'secondary' : 'ghost'} size="xs" onclick={() => (filter = filter === kind ? null : kind)}>
            {kind} <span class="text-muted-foreground">{n}</span>
          </Button>
        {/each}
      </div>
      <ul class="mt-1 max-h-56 overflow-auto font-mono text-[11px] leading-4 select-text">
        {#each shownFeed as e (e.seq + '-' + e.at_ms)}
          <li class="truncate"><span class="text-muted-foreground">{clock(e.at_ms)}</span> <b>{e.kind}</b> {describe(e)}</li>
        {/each}
      </ul>
    </section>
  </div>
</div>
