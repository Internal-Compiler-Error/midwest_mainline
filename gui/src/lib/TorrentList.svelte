<script lang="ts">
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

<table>
  <tbody>
    {#each torrents as t (t.id)}
      <tr class:selected={t.id === selected} onclick={() => onselect(t.id)}>
        <td class="name" title={name(t)}>{name(t)}</td>
        <td class="progress">
          {#if t.kind === 'downloading'}
            <ProgressBar fraction={fraction(t.verified_pieces, t.total_pieces)} done={t.completed} />
          {/if}
        </td>
        <td class="status">{status(t)}</td>
        <td class="remove">
          <button
            class="small"
            title="remove, deleting its files"
            onclick={(e) => {
              e.stopPropagation()
              onremove(t.id)
            }}>✕</button
          >
        </td>
      </tr>
    {/each}
  </tbody>
</table>

<style>
  table {
    width: 100%;
    border-collapse: collapse;
  }
  tr {
    cursor: default;
  }
  tr:nth-child(even) {
    background: var(--stripe);
  }
  tr.selected {
    background: var(--selected);
  }
  td {
    padding: 4px 8px;
    vertical-align: middle;
    white-space: nowrap;
  }
  .name {
    max-width: 0;
    width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .progress {
    width: 140px;
    min-width: 140px;
  }
  .remove {
    width: 1px;
  }
</style>
