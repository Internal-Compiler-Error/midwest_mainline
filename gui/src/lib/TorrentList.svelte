<script lang="ts">
  import EllipsisVertical from '@lucide/svelte/icons/ellipsis-vertical'
  import Pause from '@lucide/svelte/icons/pause'
  import Play from '@lucide/svelte/icons/play'
  import { Button } from '$lib/components/ui/button'
  import * as DropdownMenu from '$lib/components/ui/dropdown-menu'
  import * as Table from '$lib/components/ui/table'
  import type { TorrentId, TorrentRow } from './api'
  import { fraction, humanBytes } from './api'
  import ProgressBar from './ProgressBar.svelte'

  let {
    torrents,
    selected,
    onselect,
    onpause,
    onunpause,
    onrecheck,
    onremove,
  }: {
    torrents: TorrentRow[]
    selected: TorrentId | null
    onselect: (id: TorrentId) => void
    onpause: (id: TorrentId) => void
    onunpause: (id: TorrentId) => void
    onrecheck: (id: TorrentId) => void
    onremove: (id: TorrentId, deleteFiles: boolean) => void
  } = $props()

  function name(t: TorrentRow): string {
    return t.kind === 'resolving' || t.kind === 'failed' ? t.source : t.name
  }

  function status(t: TorrentRow): string {
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
        return t.completed ? 'paused, complete' : 'paused'
      case 'downloading':
        return t.completed
          ? `seeding  ↑ ${humanBytes(t.upload_bps)}/s`
          : `↓ ${humanBytes(t.download_bps)}/s  ↑ ${humanBytes(t.upload_bps)}/s`
    }
  }
</script>

<Table.Root>
  <Table.Body>
    {#each torrents as t (t.id)}
      <Table.Row data-state={t.id === selected ? 'selected' : undefined} onclick={() => onselect(t.id)}>
        <Table.Cell class="w-full max-w-0 truncate" title={name(t)}>{name(t)}</Table.Cell>
        <Table.Cell class="w-44 min-w-44">
          {#if t.kind === 'downloading' || t.kind === 'paused' || t.kind === 'queued'}
            <ProgressBar fraction={fraction(t.verified_pieces, t.total_pieces)} done={t.completed} />
          {:else if t.kind === 'checking'}
            <ProgressBar fraction={fraction(t.checked_pieces, t.total_pieces)} done={false} />
          {/if}
        </Table.Cell>
        <Table.Cell class="whitespace-nowrap text-muted-foreground tabular-nums">{status(t)}</Table.Cell>
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
              {#if t.kind === 'downloading' || t.kind === 'paused' || t.kind === 'queued'}
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
