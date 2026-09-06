<script lang="ts">
  import { open } from '@tauri-apps/plugin-dialog'
  import { isMagnetUri } from './api'

  // `onadd` gets a .torrent path or a magnet link; the parent asks where it should go
  let { onadd }: { onadd: (source: string) => void } = $props()
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

<div class="toolbar">
  <button onclick={openTorrent}>Open .torrent…</button>
  <span class="sep"></span>
  <input
    type="text"
    placeholder="or paste a magnet: link"
    bind:value={magnet}
    onkeydown={(e) => e.key === 'Enter' && addMagnet()}
  />
  <button disabled={!ready} onclick={addMagnet}>Add</button>
</div>

<style>
  .toolbar {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 10px;
    border-bottom: 1px solid var(--border);
    background: var(--panel);
  }
  .sep {
    width: 1px;
    height: 20px;
    background: var(--border);
  }
  input {
    flex: 1;
  }
</style>
