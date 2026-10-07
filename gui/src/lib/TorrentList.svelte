<script lang="ts">
  import EllipsisVertical from '@lucide/svelte/icons/ellipsis-vertical'
  import Pause from '@lucide/svelte/icons/pause'
  import Play from '@lucide/svelte/icons/play'
  import { Button } from '$lib/components/ui/button'
  import * as DropdownMenu from '$lib/components/ui/dropdown-menu'
  import * as Table from '$lib/components/ui/table'
  import { isKnown, type TorrentId, type TorrentRow } from './api'
  import { fraction, rate } from './format'
  import Flip from './Flip.svelte'
  import Num from './Num.svelte'
  import ProgressBar from './ProgressBar.svelte'

  let {
    torrents,
    selected,
    onselect,
    onpause,
    onunpause,
    onrecheck,
    onreveal,
    onremove,
  }: {
    torrents: TorrentRow[]
    selected: TorrentId | null
    onselect: (id: TorrentId) => void
    onpause: (id: TorrentId) => void
    onunpause: (id: TorrentId) => void
    onrecheck: (id: TorrentId) => void
    onreveal: (id: TorrentId) => void
    onremove: (id: TorrentId, deleteFiles: boolean) => void
  } = $props()

  function name(t: TorrentRow): string {
    return t.kind === 'resolving' || t.kind === 'failed' ? t.source : t.name
  }

  // the state and the rates are separate fixed-width columns, so a longer word or a wider
  // number in one row doesn't shift the bar in every other
  function state(t: TorrentRow): string {
    switch (t.kind) {
      case 'resolving':
        return 'resolving…'
      case 'failed':
        return '⚠ failed'
      case 'checking':
        return `checking ${Math.floor(fraction(t.checked_pieces, t.total_pieces) * 100)}%`
      case 'queued':
        return 'queued'
      case 'paused':
        return t.completed ? 'paused, done' : 'paused'
      case 'downloading':
        return t.completed ? 'seeding' : 'downloading'
    }
  }

  /** the one row in the tab order: the selected one, arrow keys move between rows */
  let tabStop = $derived(torrents.some((t) => t.id === selected) ? selected : torrents[0]?.id)

  const STEPS: Record<string, (at: number) => number> = {
    ArrowDown: (at) => at + 1,
    ArrowUp: (at) => at - 1,
    Home: () => 0,
    End: () => torrents.length - 1,
  }

  function onkeydown(e: KeyboardEvent & { currentTarget: HTMLElement }, at: number) {
    if (e.target !== e.currentTarget) return
    if (e.key === 'Enter') return onselect(torrents[at].id)
    const step = STEPS[e.key]
    if (!step) return
    e.preventDefault()
    const to = Math.max(0, Math.min(torrents.length - 1, step(at)))
    onselect(torrents[to].id)
    ;(e.currentTarget.parentElement?.children[to] as HTMLElement | undefined)?.focus()
  }
</script>

<Table.Root>
  <Table.Body>
    {#each torrents as t, i (t.id)}
      <Table.Row
        data-state={t.id === selected ? 'selected' : undefined}
        tabindex={t.id === tabStop ? 0 : -1}
        onclick={() => onselect(t.id)}
        onkeydown={(e) => onkeydown(e, i)}
      >
        <Table.Cell class="w-full max-w-0 truncate" title={name(t)}>{name(t)}</Table.Cell>
        <Table.Cell class="w-44 min-w-44">
          {#if isKnown(t)}
            <ProgressBar fraction={fraction(t.verified_pieces, t.total_pieces)} done={t.completed} />
          {:else if t.kind === 'checking'}
            <ProgressBar fraction={fraction(t.checked_pieces, t.total_pieces)} done={false} />
          {/if}
        </Table.Cell>
        <Table.Cell class="w-28 min-w-28 whitespace-nowrap text-muted-foreground"><Flip text={state(t)} /></Table.Cell>
        <Table.Cell class="w-44 min-w-44 whitespace-nowrap text-right text-muted-foreground tabular-nums">
          {#if t.kind === 'downloading'}
            {#if !t.completed}↓ <Num value={t.download_bps} format={rate} />&nbsp;&nbsp;{/if}↑ <Num value={t.upload_bps} format={rate} />
          {/if}
        </Table.Cell>
        <Table.Cell class="w-16 pr-1 whitespace-nowrap">
          {#if t.kind === 'downloading' || t.kind === 'queued'}
            <Button
              variant="ghost"
              size="icon-xs"
              title="pause"
              onclick={(e) => {
                e.stopPropagation()
                onpause(t.id)
              }}><Pause /></Button
            >
          {:else if t.kind === 'paused'}
            <Button
              variant="ghost"
              size="icon-xs"
              title="resume"
              onclick={(e) => {
                e.stopPropagation()
                onunpause(t.id)
              }}><Play /></Button
            >
          {/if}
          <DropdownMenu.Root>
            <DropdownMenu.Trigger onclick={(e) => e.stopPropagation()}>
              {#snippet child({ props })}
                <Button {...props} variant="ghost" size="icon-xs" title="more"><EllipsisVertical /></Button>
              {/snippet}
            </DropdownMenu.Trigger>
            <DropdownMenu.Content align="end">
              {#if isKnown(t)}
                <DropdownMenu.Item onclick={() => onreveal(t.id)}>Show in Finder</DropdownMenu.Item>
                <DropdownMenu.Item onclick={() => onrecheck(t.id)}>Force recheck</DropdownMenu.Item>
                <DropdownMenu.Separator />
              {/if}
              <DropdownMenu.Item onclick={() => onremove(t.id, false)}>Remove, keep files</DropdownMenu.Item>
              <DropdownMenu.Item variant="destructive" onclick={() => onremove(t.id, true)}>
                Remove and delete files
              </DropdownMenu.Item>
            </DropdownMenu.Content>
          </DropdownMenu.Root>
        </Table.Cell>
      </Table.Row>
    {/each}
  </Table.Body>
</Table.Root>
