<script lang="ts">
  import LoaderCircle from '@lucide/svelte/icons/loader-circle'
  import { ScrollArea } from '$lib/components/ui/scroll-area'
  import type { TorrentRow } from './api'
  import { fraction, humanBytes, isMagnetUri } from './api'
  import ProgressBar from './ProgressBar.svelte'

  let { torrent }: { torrent: TorrentRow } = $props()
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
  {:else}
    <div class="flex items-center gap-3">
      <h2 class="text-base font-semibold">
        {torrent.kind === 'paused' ? 'Paused' : torrent.completed ? 'Seeding' : 'Downloading'}
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
        ['Wasted', humanBytes(torrent.wasted)],
        ['Remaining', humanBytes(torrent.left)],
        ['Total size', humanBytes(torrent.total_size)],
      ] as [label, value] (label)}
        <dt class="font-medium">{label}</dt>
        <dd class="tabular-nums">{value}</dd>
      {/each}
    </dl>

    <div class="font-medium">Files ({torrent.files.length})</div>
    <ScrollArea class="max-h-40 rounded-md border">
      <ul class="p-2 select-text">
        {#each torrent.files as file (file)}
          <li class="truncate" title={file}>{file}</li>
        {/each}
      </ul>
    </ScrollArea>
  {/if}
</div>
