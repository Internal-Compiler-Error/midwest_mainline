<script lang="ts">
  // The connected peers: click a header to sort by it (again to flip), drag a header's right
  // edge to resize the column. The figures are live; the order is only re-sorted on the
  // shared slow beat (see clock.svelte.ts), so a sort by rate doesn't reshuffle the list
  // under the pointer on every poll.
  import { untrack } from 'svelte'
  import * as Table from '$lib/components/ui/table'
  import type { Peer } from './api'
  import { peerFlags } from './api'
  import { humanBytes, percent, rate } from './format'
  import { clock } from './clock.svelte'
  import Num from './Num.svelte'

  let { peers, onpeer }: { peers: Peer[]; onpeer?: (addr: string) => void } = $props()

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

  /// row order by address, re-sorted on the beat or when the sort changes
  let order = $state<string[]>([])
  $effect(() => {
    clock.tick
    const col = columns.find((c) => c.key === sortKey)
    const current = untrack(() => peers)
    if (!col) {
      order = current.map((p) => p.addr)
      return
    }
    const sorted = [...current].sort((a, b) => {
      const x = col.value(a)
      const y = col.value(b)
      return typeof x === 'number' && typeof y === 'number' ? x - y : String(x).localeCompare(String(y))
    })
    order = (descending ? sorted.reverse() : sorted).map((p) => p.addr)
  })

  /// live peers in the beat's order; ones that connected since go at the end until the next
  let rows = $derived.by(() => {
    const byAddr = new Map(peers.map((p) => [p.addr, p]))
    const placed = order.flatMap((addr) => byAddr.get(addr) ?? [])
    const placedSet = new Set(order)
    return placed.concat(peers.filter((p) => !placedSet.has(p.addr)))
  })

  let resizing = $state<string | null>(null)

  function resize(e: PointerEvent, col: Column) {
    e.preventDefault()
    e.stopPropagation()
    resizing = col.key
    const startX = e.clientX
    const startWidth = col.width
    const move = (m: PointerEvent) => (col.width = Math.max(MIN_WIDTH, startWidth + m.clientX - startX))
    const stop = () => {
      resizing = null
      window.removeEventListener('pointermove', move)
      window.removeEventListener('pointerup', stop)
    }
    window.addEventListener('pointermove', move)
    window.addEventListener('pointerup', stop)
  }

  function nudge(e: KeyboardEvent, col: Column) {
    const by = e.key === 'ArrowLeft' ? -1 : e.key === 'ArrowRight' ? 1 : 0
    if (!by) return
    e.preventDefault()
    col.width = Math.max(MIN_WIDTH, col.width + by * (e.shiftKey ? 40 : 10))
  }

  let width = $derived(columns.reduce((sum, c) => sum + c.width, 0))
</script>

<!-- fixed layout with explicit widths: the columns are exactly as wide as set, and values
     coming and going never shift the rest of the row -->
<Table.Root class="table-fixed text-xs" style="width: {width}px; min-width: 100%">
  <Table.Header>
    <Table.Row>
      {#each columns as col (col.key)}
        <Table.Head
          class={['relative select-none', col.right && 'text-right']}
          style="width: {col.width}px"
          title={col.title}
          aria-sort={sortKey !== col.key ? undefined : descending ? 'descending' : 'ascending'}
        >
          <button type="button" class="cursor-pointer" onclick={() => sortBy(col)}>
            {col.label}
            {#if sortKey === col.key}<span class="text-muted-foreground">{descending ? '▼' : '▲'}</span>{/if}
          </button>
          <!-- a visible divider, wider than it looks so it's easy to grab; the arrow keys move
               it too. A focusable separator is a widget (ARIA's window splitter), which
               Svelte's checks don't know -->
          <!-- svelte-ignore a11y_no_noninteractive_tabindex, a11y_no_noninteractive_element_interactions -->
          <span
            role="separator"
            aria-orientation="vertical"
            aria-label="{col.label} column width"
            aria-valuenow={col.width}
            aria-valuemin={MIN_WIDTH}
            tabindex={0}
            class={[
              'absolute top-1 right-0 bottom-1 w-2 cursor-col-resize border-r-2 outline-none hover:border-primary focus-visible:border-primary',
              resizing === col.key ? 'border-primary' : 'border-border',
            ]}
            onpointerdown={(e) => resize(e, col)}
            onkeydown={(e) => nudge(e, col)}
          ></span>
        </Table.Head>
      {/each}
    </Table.Row>
  </Table.Header>
  <Table.Body>
    {#each rows as peer (peer.addr)}
      <Table.Row
        class={onpeer && 'cursor-pointer'}
        title={onpeer && 'show this peer in Traces'}
        onclick={() => onpeer?.(peer.addr)}
      >
        <Table.Cell class="truncate font-mono select-text" title={peer.addr}>{peer.addr}</Table.Cell>
        <Table.Cell class="truncate" title={peer.client}>{peer.client}</Table.Cell>
        <Table.Cell class="text-right tabular-nums"><Num value={peer.progress * 100} format={percent} /></Table.Cell>
        <Table.Cell class="text-right tabular-nums" title="{humanBytes(peer.downloaded)} in total">
          <Num value={peer.download_bps} format={rate} />
        </Table.Cell>
        <Table.Cell class="text-right tabular-nums" title="{humanBytes(peer.uploaded)} in total">
          <Num value={peer.upload_bps} format={rate} />
        </Table.Cell>
        <Table.Cell class="font-mono">{peerFlags(peer)}</Table.Cell>
      </Table.Row>
    {/each}
  </Table.Body>
</Table.Root>
