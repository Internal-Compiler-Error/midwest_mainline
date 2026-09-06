<script lang="ts">
  import LoaderCircle from '@lucide/svelte/icons/loader-circle'
  import { Checkbox } from '$lib/components/ui/checkbox'
  import { ScrollArea } from '$lib/components/ui/scroll-area'
  import { Switch } from '$lib/components/ui/switch'
  import { Label } from '$lib/components/ui/label'
  import * as Table from '$lib/components/ui/table'
  import type { TorrentRow } from './api'
  import { fraction, humanBytes, isMagnetUri } from './api'
  import ProgressBar from './ProgressBar.svelte'

  let {
    torrent,
    onselectfiles,
    onsequential,
  }: { torrent: TorrentRow; onselectfiles: (selected: boolean[]) => void; onsequential: (on: boolean) => void } = $props()

  function toggleFile(index: number, checked: boolean) {
    if (torrent.kind !== 'downloading' && torrent.kind !== 'paused' && torrent.kind !== 'queued') return
    const selected = torrent.files.map((f) => f.selected)
    selected[index] = checked
    onselectfiles(selected)
  }
</script>

<div class="flex flex-col gap-3">
  {#if torrent.kind === 'resolving'}
    <div class="flex items-center gap-2">
      <LoaderCircle class="size-4 animate-spin text-primary" />
      <span>{isMagnetUri(torrent.source) ? 'Fetching metadata from peers…' : 'Loading…'}</span>
      <span class="text-muted-foreground">{Math.round(torrent.elapsed_ms / 1000)}s</span>
    </div>
    {#if isMagnetUri(torrent.source) && torrent.elapsed_ms > 20_000}
      <!-- with no DHT, a magnet whose trackers are all dead has no fallback -->
      <div class="text-muted-foreground">(still looking — needs a peer from one of the magnet's trackers)</div>
    {/if}
  {:else if torrent.kind === 'failed'}
    <div class="text-destructive">⚠ {torrent.error}</div>
  {:else if torrent.kind === 'checking'}
    <h2 class="text-base font-semibold">Checking files</h2>
    <ProgressBar fraction={fraction(torrent.checked_pieces, torrent.total_pieces)} done={false} tall />
    <div class="text-muted-foreground">{torrent.checked_pieces} / {torrent.total_pieces} pieces hashed</div>
  {:else}
    <div class="flex items-center gap-3">
      <h2 class="text-base font-semibold">
        {torrent.kind === 'paused' ? 'Paused' : torrent.kind === 'queued' ? 'Queued' : torrent.completed ? 'Seeding' : 'Downloading'}
      </h2>
      {#if torrent.completed}<span class="text-emerald-600 dark:text-emerald-400">✔ complete</span>{/if}
    </div>
    <ProgressBar fraction={fraction(torrent.verified_pieces, torrent.total_pieces)} done={torrent.completed} tall />
    <div class="text-muted-foreground">{torrent.verified_pieces} / {torrent.total_pieces} pieces verified</div>

    <dl class="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 select-text">
      {#each [
        ['Location', torrent.root],
        ['Downloaded', `${humanBytes(torrent.downloaded)}  (${humanBytes(torrent.download_bps)}/s)`],
        ['Uploaded', `${humanBytes(torrent.uploaded)}  (${humanBytes(torrent.upload_bps)}/s)`],
        ['Ratio', (torrent.total_size ? torrent.uploaded / torrent.total_size : 0).toFixed(2)],
        ['Wasted', humanBytes(torrent.wasted)],
        ['Remaining', humanBytes(torrent.left)],
        ['Total size', humanBytes(torrent.total_size)],
      ] as [label, value] (label)}
        <dt class="font-medium">{label}</dt>
        <dd class="tabular-nums">{value}</dd>
      {/each}
    </dl>

    <div class="flex items-center gap-2">
      <Switch id="sequential" checked={torrent.sequential} onCheckedChange={(on) => onsequential(on)} />
      <Label for="sequential">Sequential download</Label>
      <span class="text-muted-foreground">(pieces in order, for playing while it downloads)</span>
    </div>

    <div class="font-medium">Files ({torrent.files.length})</div>
    <ScrollArea class="max-h-40 rounded-md border">
      <ul class="p-2">
        {#each torrent.files as file, i (file.path)}
          <li class="flex items-center gap-2">
            <Checkbox
              checked={file.selected}
              onCheckedChange={(checked) => toggleFile(i, checked === true)}
              title={file.selected ? 'skip this file' : 'download this file'}
            />
            <span class="truncate select-text" title={file.path}>{file.path}</span>
            <span class="ml-auto text-muted-foreground tabular-nums">{humanBytes(file.size)}</span>
          </li>
        {/each}
      </ul>
    </ScrollArea>

    {#if torrent.kind === 'downloading'}
      <div class="font-medium">Trackers ({torrent.trackers.length})</div>
      <ScrollArea class="max-h-32 rounded-md border">
        <Table.Root>
          <Table.Body>
            {#each torrent.trackers as tracker (tracker.url)}
              <Table.Row>
                <Table.Cell class="max-w-0 truncate select-text" title={tracker.url}>{tracker.url}</Table.Cell>
                <Table.Cell
                  class="max-w-64 truncate {tracker.status === 'working' ? '' : tracker.status === 'waiting' ? 'text-muted-foreground' : 'text-destructive'}"
                  title={tracker.status}>{tracker.status}</Table.Cell
                >
                <Table.Cell class="whitespace-nowrap tabular-nums">{tracker.peers} peers</Table.Cell>
                <Table.Cell class="whitespace-nowrap text-muted-foreground tabular-nums">
                  {tracker.next_announce_secs === null ? '' : `next in ${Math.floor(tracker.next_announce_secs / 60)}m ${tracker.next_announce_secs % 60}s`}
                </Table.Cell>
              </Table.Row>
            {/each}
          </Table.Body>
        </Table.Root>
      </ScrollArea>

      <div class="font-medium">Peers ({torrent.peers.length})</div>
      <ScrollArea class="max-h-64 rounded-md border">
        <Table.Root class="text-xs">
          <Table.Header>
            <Table.Row>
              <Table.Head>Address</Table.Head>
              <Table.Head>Client</Table.Head>
              <Table.Head class="text-right">Has</Table.Head>
              <Table.Head class="text-right">Down</Table.Head>
              <Table.Head class="text-right">Up</Table.Head>
              <Table.Head title="D/d: we download from it (d: choked). U/u: it downloads from us (u: we choke it)">
                Flags
              </Table.Head>
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {#each torrent.peers as peer (peer.addr)}
              <Table.Row>
                <Table.Cell class="font-mono select-text">{peer.addr}</Table.Cell>
                <Table.Cell class="max-w-40 truncate" title={peer.client}>{peer.client}</Table.Cell>
                <Table.Cell class="text-right tabular-nums">{Math.round(peer.progress * 100)}%</Table.Cell>
                <Table.Cell class="text-right tabular-nums" title="{humanBytes(peer.downloaded)} in total">
                  {humanBytes(peer.download_bps)}/s
                </Table.Cell>
                <Table.Cell class="text-right tabular-nums" title="{humanBytes(peer.uploaded)} in total">
                  {humanBytes(peer.upload_bps)}/s
                </Table.Cell>
                <Table.Cell class="font-mono">{peer.flags}</Table.Cell>
              </Table.Row>
            {/each}
          </Table.Body>
        </Table.Root>
      </ScrollArea>
    {/if}
  {/if}
</div>
