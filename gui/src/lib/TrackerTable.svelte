<script lang="ts">
  // A torrent's peer sources: its trackers and the DHT, how each is doing, and the swarm as
  // each counts it.
  import * as Table from '$lib/components/ui/table'
  import type { Tracker } from './api'

  let { trackers }: { trackers: Tracker[] } = $props()

  function status(t: Tracker): string {
    return t.state === 'failed' ? (t.error ?? 'failed') : t.state === 'working' ? 'working' : 'waiting'
  }

  function countdown(secs: number | null): string {
    return secs === null ? '' : `next in ${Math.floor(secs / 60)}m ${secs % 60}s`
  }
</script>

<Table.Root class="table-fixed">
  <Table.Body>
    {#each trackers as tracker (tracker.url)}
      {@const estimated = tracker.url.startsWith('DHT')}
      <Table.Row>
        <Table.Cell class="truncate select-text" title={tracker.url}>{tracker.url}</Table.Cell>
        <Table.Cell
          class={[
            'w-64 truncate',
            tracker.state === 'pending' && 'text-muted-foreground',
            tracker.state === 'failed' && 'text-destructive',
          ]}
          title={status(tracker)}
        >
          {status(tracker)}
        </Table.Cell>
        <Table.Cell class="w-24 whitespace-nowrap tabular-nums">{tracker.peers} peers</Table.Cell>
        <Table.Cell
          class="w-48 whitespace-nowrap text-muted-foreground tabular-nums"
          title={estimated
            ? 'estimated from the bloom filters of the DHT nodes nearest the hash (BEP 33)'
            : 'the swarm as this tracker counts it'}
        >
          {#if tracker.seeders !== null}{estimated ? '~' : ''}{tracker.seeders} seeds · {tracker.leechers ?? '?'} leechers{/if}
          {#if tracker.downloaded !== null}· {tracker.downloaded} done{/if}
        </Table.Cell>
        <Table.Cell class="w-36 whitespace-nowrap text-muted-foreground tabular-nums">
          {countdown(tracker.next_announce_secs)}
        </Table.Cell>
      </Table.Row>
    {/each}
  </Table.Body>
</Table.Root>
