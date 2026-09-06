<script lang="ts">
  import X from '@lucide/svelte/icons/x'
  import { Button } from '$lib/components/ui/button'
  import * as Table from '$lib/components/ui/table'
  import type { TorrentId, TorrentRow } from './api'
  import { fraction, humanBytes } from './api'
  import ProgressBar from './ProgressBar.svelte'

  let {
    torrents,
    selected,
    onselect,
    onremove,
  }: {
    torrents: TorrentRow[]
    selected: TorrentId | null
    onselect: (id: TorrentId) => void
    onremove: (id: TorrentId) => void
  } = $props()

  function name(t: TorrentRow): string {
    return t.kind === 'downloading' ? t.name : t.source
  }

  function status(t: TorrentRow): string {
    switch (t.kind) {
      case 'resolving':
        return 'resolving…'
      case 'failed':
        return '⚠ failed'
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
          {#if t.kind === 'downloading'}
            <ProgressBar fraction={fraction(t.verified_pieces, t.total_pieces)} done={t.completed} />
          {/if}
        </Table.Cell>
        <Table.Cell class="whitespace-nowrap text-muted-foreground tabular-nums">{status(t)}</Table.Cell>
        <Table.Cell class="w-8 pr-1">
          <Button
            variant="ghost"
            size="icon-xs"
            title="remove, deleting its files"
            onclick={(e) => {
              e.stopPropagation()
              onremove(t.id)
            }}><X /></Button
          >
        </Table.Cell>
      </Table.Row>
    {/each}
  </Table.Body>
</Table.Root>
