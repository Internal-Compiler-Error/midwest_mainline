<script lang="ts">
  import RefreshCw from '@lucide/svelte/icons/refresh-cw'
  import { Button } from '$lib/components/ui/button'
  import * as Table from '$lib/components/ui/table'
  import type { Resumable } from './api'
  import { fraction } from './api'
  import ProgressBar from './ProgressBar.svelte'

  let {
    entries,
    onresume,
    onrescan,
  }: { entries: Resumable[]; onresume: (path: string) => void; onrescan: () => void } = $props()
</script>

<div class="mb-2 flex items-center gap-2">
  <span class="font-medium">Resume an earlier download</span>
  <Button variant="ghost" size="icon-xs" title="rescan ./resume" onclick={onrescan}><RefreshCw /></Button>
</div>
{#if entries.length === 0}
  <div class="text-muted-foreground">(nothing in ./resume)</div>
{:else}
  <Table.Root>
    <Table.Body>
      {#each entries as entry (entry.path)}
        <Table.Row>
          <Table.Cell class="w-full max-w-0 truncate" title={entry.root}>{entry.name}</Table.Cell>
          <Table.Cell class="w-44 min-w-44">
            <ProgressBar fraction={fraction(entry.verified_pieces, entry.total_pieces)} />
          </Table.Cell>
          <Table.Cell class="w-px"><Button size="xs" variant="outline" onclick={() => onresume(entry.path)}>Resume</Button></Table.Cell>
        </Table.Row>
      {/each}
    </Table.Body>
  </Table.Root>
{/if}
