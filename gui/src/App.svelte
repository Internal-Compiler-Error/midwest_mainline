<script lang="ts">
  // Layout and state only: what's shown comes from `torrents()` every 250 ms, and every
  // user action is one command to the Tauri side (see lib/api.ts).
  import { open } from '@tauri-apps/plugin-dialog'
  import { ModeWatcher } from 'mode-watcher'
  import { Button } from '$lib/components/ui/button'
  import * as Resizable from '$lib/components/ui/resizable'
  import { Separator } from '$lib/components/ui/separator'
  import * as api from './lib/api'
  import { revealItemInDir } from '@tauri-apps/plugin-opener'
  import { isPermissionGranted, requestPermission, sendNotification } from '@tauri-apps/plugin-notification'
  import { getCurrentWebview } from '@tauri-apps/api/webview'
  import { perSecondLike } from './lib/api'
  import type { Resumable, TorrentId, TorrentRow, Status } from './lib/api'
  import Console from './lib/Console.svelte'
  import Insights from './lib/Insights.svelte'
  import { Insights as InsightsStore } from './lib/bus.svelte'
  import { subscribe as subscribeEvents } from './lib/events'
  import Details from './lib/Details.svelte'
  import Flip from './lib/Flip.svelte'
  import Num from './lib/Num.svelte'
  import ResumableList from './lib/Resumable.svelte'
  import SettingsDialog from './lib/SettingsDialog.svelte'
  import Toolbar from './lib/Toolbar.svelte'
  import TorrentList from './lib/TorrentList.svelte'

  let torrents = $state<TorrentRow[]>([])
  let selected = $state<TorrentId | null>(null)
  let selectedTorrent = $derived(torrents.find((t) => t.id === selected))
  /// where the next torrent goes; the folder picker starts here and updates it
  let downloadDir = $state('')
  let resumable = $state<Resumable[]>([])
  type Pane = 'console' | 'insights' | null
  /// what the bottom pane shows; the choice survives a restart
  let pane = $state<Pane>(rememberedPane())
  function rememberedPane(): Pane {
    try {
      const saved = localStorage.getItem('pane')
      if (saved === 'console' || saved === 'insights' || saved === 'none') return saved === 'none' ? null : saved
    } catch {}
    return 'insights'
  }
  $effect(() => {
    try {
      localStorage.setItem('pane', pane ?? 'none')
    } catch {}
  })
  /// the bottom pane of the splitter, collapsed when nothing is shown in it
  let bottom = $state<Resizable.Pane>()
  $effect(() => {
    if (!bottom) return
    if (pane === null) bottom.collapse()
    else if (bottom.isCollapsed()) bottom.expand()
  })
  const insights = new InsightsStore()
  let showSettings = $state(false)
  let logLines = $state<string[]>([])
  let logSeen = 0
  let status = $state<Status | null>(null)

  /// ids seen complete already, so finishing is announced once
  let announced = new Set<TorrentId>()
  let notifyReady = false

  async function notifyCompletions() {
    for (const t of torrents) {
      if (t.kind === 'downloading' && t.completed && !announced.has(t.id)) {
        announced.add(t.id)
        if (notifyReady) sendNotification({ title: 'Download complete', body: t.name })
      }
    }
  }

  function pauseAll() {
    for (const t of torrents) if (t.kind === 'downloading' || t.kind === 'queued') api.pauseTorrent(t.id)
  }

  function resumeAll() {
    for (const t of torrents) if (t.kind === 'paused') api.unpauseTorrent(t.id)
  }

  function reveal(id: TorrentId) {
    const t = torrents.find((t) => t.id === id)
    if (t && (t.kind === 'downloading' || t.kind === 'paused' || t.kind === 'queued')) {
      // a multi-file torrent is a directory named after it, a single-file one is the file
      revealItemInDir(`${t.root}/${t.files.length > 1 ? t.name : t.files[0]?.path ?? t.name}`)
    }
  }

  async function refresh() {
    torrents = await api.torrents()
    status = await api.status()
    notifyCompletions()
    // torrents that were there at startup (resumed, or given on the command line) get the
    // details panel too, without a click
    if (selected === null && torrents.length > 0) selected = torrents[0].id
    const chunk = await api.logsSince(logSeen)
    if (chunk.lines.length) {
      logSeen = chunk.seen
      logLines = [...logLines, ...chunk.lines].slice(-5000)
    }
  }

  function mappingLabel(s: Status): string {
    switch (s.port_mapping) {
      case 'off':
        return 'no mapping'
      case 'searching':
        return 'mapping…'
      case 'mapped':
        return 'mapped'
      case 'unavailable':
        return 'not mapped'
    }
  }

  function mappingTitle(s: Status): string {
    switch (s.port_mapping) {
      case 'off':
        return 'port mapping is off in the settings'
      case 'searching':
        return 'asking the router to forward the port'
      case 'mapped':
        return s.external_ip ? `the router forwards the port; external address ${s.external_ip}` : 'the router forwards the port'
      case 'unavailable':
        return 'the router answers neither NAT-PMP nor UPnP; forward the port by hand for inbound peers'
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
    // torrents complete at startup were complete before; only later ones get a notification
    api.torrents().then((initial) => {
      for (const t of initial) if (t.kind !== 'resolving' && t.kind !== 'failed' && t.kind !== 'checking' && t.completed) announced.add(t.id)
      isPermissionGranted()
        .then((granted) => (granted ? 'granted' : requestPermission()))
        .then((state) => (notifyReady = state === 'granted'))
    })
    refresh()
    const timer = setInterval(refresh, 250)
    // the library's event bus feeds the insights panel, whether or not it's showing
    const unlistenEvents = subscribeEvents((batch) => insights.ingest(batch))
    // .torrent files dropped on the window are added to the default download dir
    const unlisten = getCurrentWebview().onDragDropEvent((event) => {
      if (event.payload.type === 'drop') {
        for (const path of event.payload.paths) if (path.toLowerCase().endsWith('.torrent')) add(path)
      }
    })
    return () => {
      clearInterval(timer)
      unlisten.then((stop) => stop())
      unlistenEvents.then((stop) => stop())
    }
  })

  /// Asks where this torrent should go, then adds it. Cancelling the picker cancels the add.
  async function add(source: string) {
    const dir = await open({ title: 'Download into…', directory: true, defaultPath: downloadDir })
    if (typeof dir !== 'string') return
    downloadDir = dir
    selected = await api.addTorrent(source, dir)
  }

  async function remove(id: TorrentId, deleteFiles: boolean) {
    await api.removeTorrent(id, deleteFiles)
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
  <Toolbar onadd={add} onsettings={() => (showSettings = true)} onpauseall={pauseAll} onresumeall={resumeAll} />
  <SettingsDialog bind:open={showSettings} onsaved={(s) => (downloadDir = s.download_dir)} />

  <Resizable.PaneGroup direction="vertical" class="flex-1">
    <Resizable.Pane defaultSize={70} minSize={25}>
      <main class="h-full overflow-auto p-3">
        {#if torrents.length === 0}
          <p class="text-muted-foreground">Open a .torrent file, or paste a magnet link above.</p>
        {:else}
          <TorrentList
            {torrents}
            {selected}
            onselect={(id) => (selected = id)}
            onpause={api.pauseTorrent}
            onunpause={api.unpauseTorrent}
            onrecheck={api.recheckTorrent}
            onreveal={reveal}
            onremove={remove}
          />
        {/if}

        {#if selectedTorrent}
          <Separator class="my-3" />
          <Details
            torrent={selectedTorrent}
            onselectfiles={(files) => selected !== null && api.selectFiles(selected, files)}
            onsequential={(on) => selected !== null && api.setSequential(selected, on)}
          />
        {/if}

        {#if resumable.length > 0 || torrents.length === 0}
          <Separator class="my-3" />
          <ResumableList entries={resumable} onresume={resume} onrescan={rescan} />
        {/if}
      </main>
    </Resizable.Pane>
    <!-- always mounted: a pane added to the group later gets no size, so a closed pane is
         a collapsed one -->
    <Resizable.Handle withHandle class={pane === null ? 'hidden' : ''} />
    <Resizable.Pane bind:this={bottom} defaultSize={pane === null ? 0 : 35} minSize={10} collapsible collapsedSize={0}>
      {#if pane === 'console'}
        <Console lines={logLines} />
      {:else if pane === 'insights'}
        <Insights {insights} />
      {/if}
    </Resizable.Pane>
  </Resizable.PaneGroup>

  <footer class="flex items-center gap-3 border-t bg-card px-3 py-1">
    <Button variant={pane === 'console' ? 'secondary' : 'ghost'} size="xs" onclick={() => (pane = pane === 'console' ? null : 'console')}>
      Console
    </Button>
    <Button variant={pane === 'insights' ? 'secondary' : 'ghost'} size="xs" onclick={() => (pane = pane === 'insights' ? null : 'insights')}>
      Insights
    </Button>
    {#if pane === 'console'}
      <span class="text-xs text-muted-foreground">{logLines.length} lines</span>
      <Button variant="ghost" size="xs" onclick={clearLogs}>clear</Button>
    {:else if pane === 'insights'}
      <span class="text-xs text-muted-foreground">{insights.received} events</span>
    {/if}
    {#if status}
      <span class="ml-auto flex gap-4 text-xs text-muted-foreground tabular-nums">
        <span>↓ <Num value={status.download_bps} format={perSecondLike} /> ↑ <Num value={status.upload_bps} format={perSecondLike} /></span>
        <span title="nodes in the DHT routing table">DHT {#if status.dht_nodes === null}off{:else}<Num value={status.dht_nodes} /> nodes{/if}</span>
        <span title={mappingTitle(status)}>port {status.listen_port} · <Flip text={mappingLabel(status)} /></span>
      </span>
    {/if}
  </footer>
</div>
