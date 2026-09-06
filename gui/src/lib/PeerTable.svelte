<script lang="ts">
  // The connected peers: click a header to sort by it (again to flip), drag a header's right
  // edge to resize the column. Rows are sampled on the shared clock, like the figures in
  // them, so a sort by rate doesn't reshuffle the list on every poll.
  import { untrack } from 'svelte'
  import * as Table from '$lib/components/ui/table'
  import type { Peer } from './api'
  import { humanBytes, kibPerSecond, peerFlags } from './api'
  import { clock } from './clock.svelte'
  import Num from './Num.svelte'

  let { peers: live }: { peers: Peer[] } = $props()

  let peers = $state<Peer[]>(untrack(() => live))
  $effect(() => {
    clock.tick
    peers = untrack(() => live)
  })

  type Column = {
    key: string
    label: string
    width: number
    right?: boolean
    title?: string
    value: (p: Peer) => string | number
  }
  let columns = $state<Column[]>([
    { key: 'addr', label: 'Address', width: 200, value: (p) => p.addr },
    { key: 'client', label: 'Client', width: 150, value: (p) => p.client },
    { key: 'has', label: 'Has', width: 56, right: true, value: (p) => p.progress },
    { key: 'down', label: 'Down', width: 110, right: true, value: (p) => p.download_bps },
    { key: 'up', label: 'Up', width: 110, right: true, value: (p) => p.upload_bps },
    {
      key: 'flags',
      label: 'Flags',
      width: 64,
      title: 'D/d: we download from it (d: choked). U/u: it downloads from us (u: we choke it). E: encrypted. T: uTP',
      value: (p) => peerFlags(p),
    },
  ])
  const MIN_WIDTH = 40

  let sortKey = $state<string | null>(null)
  let descending = $state(true)

  function sortBy(col: Column) {
    if (sortKey === col.key) {
      descending = !descending
    } else {
      sortKey = col.key
      // numbers are more interesting from the top, names from A
      descending = peers.some((p) => typeof col.value(p) === 'number')
    }
  }

  let rows = $derived.by(() => {
    const col = columns.find((c) => c.key === sortKey)
    if (!col) return peers
    const sorted = [...peers].sort((a, b) => {
      const x = col.value(a)
      const y = col.value(b)
      return typeof x === 'number' && typeof y === 'number' ? x - y : String(x).localeCompare(String(y))
    })
    return descending ? sorted.reverse() : sorted
  })

  function resize(e: PointerEvent, col: Column) {
    e.preventDefault()
    e.stopPropagation()
    const startX = e.clientX
    const startWidth = col.width
    const move = (m: PointerEvent) => (col.width = Math.max(MIN_WIDTH, startWidth + m.clientX - startX))
    const stop = () => {
      window.removeEventListener('pointermove', move)
      window.removeEventListener('pointerup', stop)
    }
    window.addEventListener('pointermove', move)
    window.addEventListener('pointerup', stop)
  }

  let width = $derived(columns.reduce((sum, c) => sum + c.width, 0))
</script>

<!-- fixed layout with explicit widths: the columns are exactly as wide as set, and values
     coming and going never shift the rest of the row -->
<Table.Root class="table-fixed text-xs" style="width: {width}px; min-width: 100%">
  <Table.Header>
    <Table.Row>
      {#each columns as col (col.key)}
        <Table.Head class={['relative select-none', col.right && 'text-right']} style="width: {col.width}px" title={col.title}>
          <button type="button" class="cursor-pointer" onclick={() => sortBy(col)}>
            {col.label}
            {#if sortKey === col.key}<span class="text-muted-foreground">{descending ? '▼' : '▲'}</span>{/if}
          </button>
          <!-- svelte-ignore a11y_no_static_element_interactions -->
          <span
            class="absolute top-0 right-0 h-full w-1.5 cursor-col-resize hover:bg-primary/40"
            onpointerdown={(e) => resize(e, col)}
          ></span>
        </Table.Head>
      {/each}
    </Table.Row>
  </Table.Header>
  <Table.Body>
    {#each rows as peer (peer.addr)}
      <Table.Row>
        <Table.Cell class="truncate font-mono select-text" title={peer.addr}>{peer.addr}</Table.Cell>
        <Table.Cell class="truncate" title={peer.client}>{peer.client}</Table.Cell>
        <Table.Cell class="text-right tabular-nums"><Num value={peer.progress * 100} format={(n) => `${Math.round(n)}%`} /></Table.Cell>
        <Table.Cell class="text-right tabular-nums" title="{humanBytes(peer.downloaded)} in total">
          <Num value={peer.download_bps} format={kibPerSecond} />
        </Table.Cell>
        <Table.Cell class="text-right tabular-nums" title="{humanBytes(peer.uploaded)} in total">
          <Num value={peer.upload_bps} format={kibPerSecond} />
        </Table.Cell>
        <Table.Cell class="font-mono">{peerFlags(peer)}</Table.Cell>
      </Table.Row>
    {/each}
  </Table.Body>
</Table.Root>
