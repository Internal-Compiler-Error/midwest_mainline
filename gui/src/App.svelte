<script lang="ts">
  // Layout and state only: what's shown comes from `torrents()` every 250 ms, and every
  // user action is one command to the Tauri side (see lib/api.ts).
  import { getCurrentWebview } from '@tauri-apps/api/webview'
  import { open } from '@tauri-apps/plugin-dialog'
  import { isPermissionGranted, requestPermission, sendNotification } from '@tauri-apps/plugin-notification'
  import { revealItemInDir } from '@tauri-apps/plugin-opener'
  import { ModeWatcher } from 'mode-watcher'
  import { Button } from '$lib/components/ui/button'
  import * as Resizable from '$lib/components/ui/resizable'
  import { Separator } from '$lib/components/ui/separator'
  import * as api from './lib/api'
  import { isKnown, type Resumable, type Status, type TorrentId, type TorrentRow } from './lib/api'
  import { Insights } from './lib/bus.svelte'
  import { subscribe as subscribeEvents } from './lib/events'
  import { Logs } from './lib/logs.svelte'
  import { Traces } from './lib/spans.svelte'
  import { every } from './lib/poll'
  import ConsolePane from './lib/Console.svelte'
  import Details from './lib/Details.svelte'
  import InsightsPane from './lib/Insights.svelte'
  import ResumableList from './lib/Resumable.svelte'
  import SettingsDialog from './lib/SettingsDialog.svelte'
  import StatusBar from './lib/StatusBar.svelte'
  import Toolbar from './lib/Toolbar.svelte'
  import TorrentList from './lib/TorrentList.svelte'
  import TracesPane from './lib/Traces.svelte'

  let torrents = $state<TorrentRow[]>([])
  let selected = $state<TorrentId | null>(null)
  let selectedTorrent = $derived(torrents.find((t) => t.id === selected))
  /// where the next torrent goes; the folder picker starts here and updates it
  let downloadDir = $state('')
  let resumable = $state<Resumable[]>([])
  const PANES = [
    ['console', 'Console'],
    ['insights', 'Insights'],
    ['traces', 'Traces'],
  ] as const
  type Pane = (typeof PANES)[number][0] | null
  /// what the bottom pane shows; the choice survives a restart
  let pane = $state<Pane>(rememberedPane())
  function rememberedPane(): Pane {
    try {
      const saved = localStorage.getItem('pane')
      if (saved === 'none') return null
      const known = PANES.find(([p]) => p === saved)
      if (known) return known[0]
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
  const insights = new Insights()
  const traces = new Traces()
  const logs = new Logs()
  // only polled while the pane is open, and only for the selected torrent
  $effect(() => {
    traces.follow(pane === 'traces' && selectedTorrent ? api.infoHashOf(selectedTorrent) : null)
  })
  let showSettings = $state(false)
  let status = $state<Status | null>(null)

  /// ids seen complete already, so finishing is announced once
  let announced = new Set<TorrentId>()
  let notifyReady = false

  function notifyCompletions() {
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
    if (t && isKnown(t)) {
      // a multi-file torrent is a directory named after it, a single-file one is the file
      revealItemInDir(`${t.root}/${t.files.length > 1 ? t.name : t.files[0]?.path ?? t.name}`)
    }
  }

  /// bumped whenever an add, resume or removal lands, so a poll that was already under way
  /// (and so doesn't show it yet) is dropped rather than undoing the new selection
  let changes = 0

  async function refresh() {
    const asked = changes
    const [rows, now] = await Promise.all([api.torrents(), api.status()])
    if (asked !== changes) return
    torrents = rows
    status = now
    notifyCompletions()
    // torrents that were there at startup (resumed, or given on the command line) get the
    // details panel too, without a click, and so does the next one when the selected one goes
    if (!torrents.some((t) => t.id === selected)) selected = torrents[0]?.id ?? null
    await logs.poll()
  }

  async function rescan() {
    resumable = await api.resumable()
  }

  /// A removal or a resume changes the resume dir in the background; look again shortly.
  function rescanSoon() {
    setTimeout(rescan, 1000)
  }

  $effect(() => {
    api.settings().then((s) => (downloadDir = s.download_dir))
    rescan()
    // torrents complete at startup were complete before; only later ones get a notification
    api.torrents().then((initial) => {
      for (const t of initial) if (isKnown(t) && t.completed) announced.add(t.id)
      isPermissionGranted()
        .then((granted) => (granted ? 'granted' : requestPermission()))
        .then((state) => (notifyReady = state === 'granted'))
    })
    const stopPolling = every(250, refresh)
    // the library's event bus feeds the insights panel, whether or not it's showing
    const unlistenEvents = subscribeEvents((batch) => insights.ingest(batch))
    // .torrent files dropped on the window are added to the default download dir
    const unlisten = getCurrentWebview().onDragDropEvent((event) => {
      if (event.payload.type === 'drop') {
        for (const path of event.payload.paths) if (path.toLowerCase().endsWith('.torrent')) add(path)
      }
    })
    return () => {
      stopPolling()
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
    changes++
  }

  async function remove(id: TorrentId, deleteFiles: boolean) {
    await api.removeTorrent(id, deleteFiles)
    changes++
    torrents = torrents.filter((t) => t.id !== id)
    if (selected === id) selected = torrents[0]?.id ?? null
    rescanSoon()
  }

  async function resume(path: string) {
    selected = await api.resumeTorrent(path)
    changes++
    rescanSoon()
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
          <!-- a fresh panel per torrent: its figures would otherwise glide over from the last one's -->
          {#key selectedTorrent.id}
            <Details
              torrent={selectedTorrent}
              onselectfiles={(files) => api.selectFiles(selectedTorrent.id, files)}
              onsequential={(on) => selected !== null && api.setSequential(selected, on)}
              onsuperseed={(on) => selected !== null && api.setSuperSeed(selected, on)}
              onpeer={(addr) => {
                traces.focus = addr
                pane = 'traces'
              }}
            />
          {/key}
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
        <ConsolePane lines={logs.lines} />
      {:else if pane === 'insights'}
        <InsightsPane {insights} />
      {:else if pane === 'traces'}
        {#if selectedTorrent}
          <TracesPane {traces} />
        {:else}
          <p class="p-3 text-sm text-muted-foreground">Select a torrent to see its traces.</p>
        {/if}
      {/if}
    </Resizable.Pane>
  </Resizable.PaneGroup>

  <footer class="flex items-center gap-3 border-t bg-card px-3 py-1">
    {#each PANES as [id, label] (id)}
      <Button variant={pane === id ? 'secondary' : 'ghost'} size="xs" aria-pressed={pane === id} onclick={() => (pane = pane === id ? null : id)}>
        {label}
      </Button>
    {/each}
    {#if pane === 'console'}
      <span class="text-xs text-muted-foreground">{logs.lines.length} lines</span>
      <Button variant="ghost" size="xs" onclick={() => logs.clear()}>clear</Button>
    {:else if pane === 'insights'}
      <span class="text-xs text-muted-foreground">{insights.received} events</span>
    {/if}
    {#if status}
      <StatusBar {status} />
    {/if}
  </footer>
</div>
