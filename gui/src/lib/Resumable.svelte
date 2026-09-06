<script lang="ts">
  import type { Resumable } from './api'
  import { fraction } from './api'
  import ProgressBar from './ProgressBar.svelte'

  let {
    entries,
    onresume,
    onrescan,
  }: { entries: Resumable[]; onresume: (path: string) => void; onrescan: () => void } = $props()
</script>

<div class="head">
  <strong>Resume an earlier download</strong>
  <button class="small" title="rescan ./resume" onclick={onrescan}>⟳</button>
</div>
{#if entries.length === 0}
  <div class="muted">(nothing in ./resume)</div>
{:else}
  <table>
    <tbody>
      {#each entries as entry (entry.path)}
        <tr>
          <td class="name" title={entry.root}>{entry.name}</td>
          <td class="progress"><ProgressBar fraction={fraction(entry.verified_pieces, entry.total_pieces)} /></td>
          <td><button onclick={() => onresume(entry.path)}>Resume</button></td>
        </tr>
      {/each}
    </tbody>
  </table>
{/if}

<style>
  .head {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 6px;
  }
  table {
    border-collapse: collapse;
    width: 100%;
  }
  td {
    padding: 3px 8px 3px 0;
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
</style>
