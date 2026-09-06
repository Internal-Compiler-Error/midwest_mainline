<script lang="ts">
  import type { TorrentRow } from './api'
  import { fraction, humanBytes, isMagnetUri } from './api'
  import ProgressBar from './ProgressBar.svelte'

  let { torrent }: { torrent: TorrentRow } = $props()
</script>

<div class="details">
  {#if torrent.kind === 'resolving'}
    <div class="row">
      <span class="spinner"></span>
      <span>{isMagnetUri(torrent.source) ? 'Fetching metadata from peers…' : 'Loading…'}</span>
      <span class="muted">{Math.round(torrent.elapsed_ms / 1000)}s</span>
    </div>
    {#if isMagnetUri(torrent.source) && torrent.elapsed_ms > 20_000}
      <!-- with no DHT, a magnet whose trackers are all dead has no fallback -->
      <div class="muted">(still looking — needs a peer from one of the magnet's trackers)</div>
    {/if}
  {:else if torrent.kind === 'failed'}
    <div class="error">⚠ {torrent.error}</div>
  {:else}
    <div class="row">
      <h2>{torrent.completed ? 'Seeding' : 'Downloading'}</h2>
      {#if torrent.completed}<span class="ok">✔ complete</span>{/if}
    </div>
    <ProgressBar fraction={fraction(torrent.verified_pieces, torrent.total_pieces)} done={torrent.completed} height={20} />
    <div class="muted">{torrent.verified_pieces} / {torrent.total_pieces} pieces verified</div>

    <table class="transfer">
      <tbody>
        {#each [
          ['Location', torrent.root],
          ['Downloaded', `${humanBytes(torrent.downloaded)}  (${humanBytes(torrent.download_bps)}/s)`],
          ['Uploaded', `${humanBytes(torrent.uploaded)}  (${humanBytes(torrent.upload_bps)}/s)`],
          ['Wasted', humanBytes(torrent.wasted)],
          ['Remaining', humanBytes(torrent.left)],
          ['Total size', humanBytes(torrent.total_size)],
        ] as [label, value] (label)}
          <tr><th>{label}</th><td>{value}</td></tr>
        {/each}
      </tbody>
    </table>

    <strong>Files ({torrent.files.length})</strong>
    <ul class="files">
      {#each torrent.files as file (file)}
        <li>{file}</li>
      {/each}
    </ul>
  {/if}
</div>

<style>
  .details {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .row {
    display: flex;
    align-items: center;
    gap: 10px;
  }
  h2 {
    margin: 0;
    font-size: 16px;
  }
  .transfer th {
    text-align: left;
    padding: 2px 16px 2px 0;
  }
  .transfer td {
    padding: 2px 0;
    user-select: text;
  }
  .files {
    margin: 0;
    padding-left: 18px;
    max-height: 160px;
    overflow: auto;
    user-select: text;
  }
  .spinner {
    width: 12px;
    height: 12px;
    border: 2px solid var(--border);
    border-top-color: var(--accent);
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
  }
  @keyframes spin {
    to {
      transform: rotate(360deg);
    }
  }
</style>
