<script lang="ts">
  // Layout and state only: what's shown comes from `torrents()` every 250 ms, and every
  // user action is one command to the Tauri side (see lib/api.ts).
  import { open } from '@tauri-apps/plugin-dialog'
  import * as api from './lib/api'
  import type { Resumable, TorrentId, TorrentRow } from './lib/api'
  import Console from './lib/Console.svelte'
  import Details from './lib/Details.svelte'
  import ResumableList from './lib/Resumable.svelte'
  import Toolbar from './lib/Toolbar.svelte'
  import TorrentList from './lib/TorrentList.svelte'

  let torrents = $state<TorrentRow[]>([])
  let selected = $state<TorrentId | null>(null)
  let selectedTorrent = $derived(torrents.find((t) => t.id === selected))
  /// where the next torrent goes; the folder picker starts here and updates it
  let downloadDir = $state('')
  let resumable = $state<Resumable[]>([])
  let showConsole = $state(true)
  let logLines = $state<string[]>([])
  let logSeen = 0

  async function refresh() {
    torrents = await api.torrents()
    const chunk = await api.logsSince(logSeen)
    if (chunk.lines.length) {
      logSeen = chunk.seen
      logLines = [...logLines, ...chunk.lines].slice(-5000)
    }
  }

  async function rescan() {
    resumable = await api.resumable()
  }

  /// A removal or a resume changes the resume dir in the background; look again shortly.
  function rescanSoon() {
    setTimeout(rescan, 1000)
  }

  $effect(() => {
    api.defaultDownloadDir().then((dir) => (downloadDir = dir))
    rescan()
    refresh()
    const timer = setInterval(refresh, 250)
    return () => clearInterval(timer)
  })

  /// Asks where this torrent should go, then adds it. Cancelling the picker cancels the add.
  async function add(source: string) {
    const dir = await open({ title: 'Download into…', directory: true, defaultPath: downloadDir })
    if (typeof dir !== 'string') return
    downloadDir = dir
    selected = await api.addTorrent(source, dir)
  }

  async function remove(id: TorrentId) {
    await api.removeTorrent(id)
    if (selected === id) selected = null
    rescanSoon()
  }

  async function resume(path: string) {
    selected = await api.resumeTorrent(path)
    rescanSoon()
  }

  function clearLogs() {
    logLines = []
    api.clearLogs()
  }
</script>

<div class="app">
  <Toolbar onadd={add} />
  <main>
    {#if torrents.length === 0}
      <p>Open a .torrent file, or paste a magnet link above.</p>
    {:else}
      <TorrentList {torrents} {selected} onselect={(id) => (selected = id)} onremove={remove} />
    {/if}

    {#if selectedTorrent}
      <hr />
      <Details torrent={selectedTorrent} />
    {/if}

    {#if resumable.length > 0 || torrents.length === 0}
      <hr />
      <ResumableList entries={resumable} onresume={resume} onrescan={rescan} />
    {/if}
  </main>
  <Console lines={logLines} bind:open={showConsole} onclear={clearLogs} />
</div>

<style>
  .app {
    display: flex;
    flex-direction: column;
    height: 100%;
  }
  main {
    flex: 1;
    overflow: auto;
    padding: 10px;
  }
  hr {
    border: none;
    border-top: 1px solid var(--border);
    margin: 12px 0;
  }
</style>
