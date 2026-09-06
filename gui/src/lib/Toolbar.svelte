<script lang="ts">
  import { open } from '@tauri-apps/plugin-dialog'
  import FolderOpen from '@lucide/svelte/icons/folder-open'
  import SettingsIcon from '@lucide/svelte/icons/settings'
  import { Button } from '$lib/components/ui/button'
  import { Input } from '$lib/components/ui/input'
  import { Separator } from '$lib/components/ui/separator'
  import { isMagnetUri } from './api'

  // `onadd` gets a .torrent path or a magnet link; the parent asks where it should go
  let { onadd, onsettings }: { onadd: (source: string) => void; onsettings: () => void } = $props()
  let magnet = $state('')
  let ready = $derived(isMagnetUri(magnet))

  async function openTorrent() {
    const path = await open({ title: 'Open a .torrent file', filters: [{ name: 'torrent', extensions: ['torrent'] }] })
    if (typeof path === 'string') onadd(path)
  }

  function addMagnet() {
    if (!ready) return
    const source = magnet
    magnet = ''
    onadd(source)
  }
</script>

<div class="flex items-center gap-2 border-b bg-card px-3 py-2">
  <Button variant="outline" size="sm" onclick={openTorrent}><FolderOpen />Open .torrent…</Button>
  <Separator orientation="vertical" class="h-5!" />
  <Input
    type="text"
    class="h-8 flex-1"
    placeholder="or paste a magnet: link"
    bind:value={magnet}
    onkeydown={(e) => e.key === 'Enter' && addMagnet()}
  />
  <Button size="sm" disabled={!ready} onclick={addMagnet}>Add</Button>
  <Separator orientation="vertical" class="h-5!" />
  <Button variant="ghost" size="icon-sm" title="settings" onclick={onsettings}><SettingsIcon /></Button>
</div>
