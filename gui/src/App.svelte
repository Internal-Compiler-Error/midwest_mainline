<script lang="ts">
  // Layout and state only: what's shown comes from `torrents()` every 250 ms, and every
  // user action is one command to the Tauri side (see lib/api.ts).
  import { open } from '@tauri-apps/plugin-dialog'
  import { ModeWatcher } from 'mode-watcher'
  import { Button } from '$lib/components/ui/button'
  import * as Resizable from '$lib/components/ui/resizable'
  import { Separator } from '$lib/components/ui/separator'
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

<ModeWatcher />

<div class="flex h-screen flex-col text-sm select-none">
  <Toolbar onadd={add} />

  <Resizable.PaneGroup direction="vertical" class="flex-1">
    <Resizable.Pane defaultSize={70} minSize={25}>
      <main class="h-full overflow-auto p-3">
        {#if torrents.length === 0}
          <p class="text-muted-foreground">Open a .torrent file, or paste a magnet link above.</p>
        {:else}
          <TorrentList {torrents} {selected} onselect={(id) => (selected = id)} onremove={remove} />
        {/if}

        {#if selectedTorrent}
          <Separator class="my-3" />
          <Details torrent={selectedTorrent} />
        {/if}

        {#if resumable.length > 0 || torrents.length === 0}
          <Separator class="my-3" />
          <ResumableList entries={resumable} onresume={resume} onrescan={rescan} />
        {/if}
      </main>
    </Resizable.Pane>
    {#if showConsole}
      <Resizable.Handle withHandle />
      <Resizable.Pane defaultSize={30} minSize={10}>
        <Console lines={logLines} />
      </Resizable.Pane>
    {/if}
  </Resizable.PaneGroup>

  <footer class="flex items-center gap-3 border-t bg-card px-3 py-1">
    <Button variant={showConsole ? 'secondary' : 'ghost'} size="xs" onclick={() => (showConsole = !showConsole)}>
      Console
    </Button>
    <span class="text-xs text-muted-foreground">{logLines.length} lines</span>
    {#if showConsole}
      <Button variant="ghost" size="xs" onclick={clearLogs}>clear</Button>
    {/if}
  </footer>
</div>
