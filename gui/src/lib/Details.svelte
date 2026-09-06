<script lang="ts">
  import LoaderCircle from '@lucide/svelte/icons/loader-circle'
  import { Checkbox } from '$lib/components/ui/checkbox'
  import { Switch } from '$lib/components/ui/switch'
  import { Label } from '$lib/components/ui/label'
  import * as Table from '$lib/components/ui/table'
  import * as Tabs from '$lib/components/ui/tabs'
  import type { TorrentRow } from './api'
  import { fraction, humanBytes, humanBytesLike, isMagnetUri, kibPerSecond, trackerStatus } from './api'
  import Flip from './Flip.svelte'
  import Num from './Num.svelte'
  import PeerTable from './PeerTable.svelte'
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
    <div class="text-muted-foreground"><Num value={torrent.checked_pieces} /> / {torrent.total_pieces} pieces hashed</div>
  {:else}
    <div class="flex items-center gap-3">
      <h2 class="text-base font-semibold">
        <Flip text={torrent.kind === 'paused' ? 'Paused' : torrent.kind === 'queued' ? 'Queued' : torrent.completed ? 'Seeding' : 'Downloading'} />
      </h2>
      {#if torrent.completed}<span class="text-emerald-600 dark:text-emerald-400">✔ complete</span>{/if}
    </div>
    <ProgressBar fraction={fraction(torrent.verified_pieces, torrent.total_pieces)} done={torrent.completed} tall />
    <div class="text-muted-foreground"><Num value={torrent.verified_pieces} /> / {torrent.total_pieces} pieces verified</div>

    <dl class="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 select-text">
      <dt class="font-medium">Location</dt>
      <dd>{torrent.root}</dd>
      <dt class="font-medium">Downloaded</dt>
      <dd class="tabular-nums"><Num value={torrent.downloaded} format={humanBytesLike} />&nbsp;&nbsp;(<Num value={torrent.download_bps} format={kibPerSecond} />)</dd>
      <dt class="font-medium">Uploaded</dt>
      <dd class="tabular-nums"><Num value={torrent.uploaded} format={humanBytesLike} />&nbsp;&nbsp;(<Num value={torrent.upload_bps} format={kibPerSecond} />)</dd>
      <dt class="font-medium">Ratio</dt>
      <dd class="tabular-nums"><Num value={torrent.total_size ? torrent.uploaded / torrent.total_size : 0} format={(n) => n.toFixed(2)} /></dd>
      <dt class="font-medium">Wasted</dt>
      <dd class="tabular-nums"><Num value={torrent.wasted} format={humanBytesLike} /></dd>
      <dt class="font-medium">Remaining</dt>
      <dd class="tabular-nums"><Num value={torrent.left} format={humanBytesLike} /></dd>
      <dt class="font-medium">Total size</dt>
      <dd class="tabular-nums">{humanBytes(torrent.total_size)}</dd>
    </dl>

    <div class="flex items-center gap-2">
      <Switch id="sequential" checked={torrent.sequential} onCheckedChange={(on) => onsequential(on)} />
      <Label for="sequential">Sequential download</Label>
      <span class="text-muted-foreground">(pieces in order, for playing while it downloads)</span>
    </div>

    <div class="font-medium">Files ({torrent.files.length})</div>
    <div class="max-h-40 overflow-auto rounded-md border">
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
    </div>

    {#if torrent.kind === 'downloading'}
      <Tabs.Root value="peers">
        <Tabs.List>
          <Tabs.Trigger value="peers">Peers ({torrent.peers.length})</Tabs.Trigger>
          <Tabs.Trigger value="trackers">Trackers ({torrent.trackers.length})</Tabs.Trigger>
        </Tabs.List>
        <Tabs.Content value="trackers" class="max-h-64 overflow-auto rounded-md border">
        <Table.Root class="table-fixed">
          <Table.Body>
            {#each torrent.trackers as tracker (tracker.url)}
              <Table.Row>
                <Table.Cell class="truncate select-text" title={tracker.url}>{tracker.url}</Table.Cell>
                <Table.Cell
                  class="w-64 truncate {tracker.state === 'working' ? '' : tracker.state === 'pending' ? 'text-muted-foreground' : 'text-destructive'}"
                  title={trackerStatus(tracker)}>{trackerStatus(tracker)}</Table.Cell
                >
                <Table.Cell class="w-24 whitespace-nowrap tabular-nums">{tracker.peers} peers</Table.Cell>
                <Table.Cell class="w-36 whitespace-nowrap text-muted-foreground tabular-nums">
                  {tracker.next_announce_secs === null ? '' : `next in ${Math.floor(tracker.next_announce_secs / 60)}m ${tracker.next_announce_secs % 60}s`}
                </Table.Cell>
              </Table.Row>
            {/each}
          </Table.Body>
        </Table.Root>
        </Tabs.Content>
        <Tabs.Content value="peers" class="max-h-64 overflow-auto rounded-md border">
          <PeerTable peers={torrent.peers} />
        </Tabs.Content>
      </Tabs.Root>
    {/if}
  {/if}
</div>
