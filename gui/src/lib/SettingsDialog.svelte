<script lang="ts">
  // Edits a copy of the settings and saves on OK; the port and DHT switch only take effect
  // after a restart, which the dialog says when they changed.
  import { open } from '@tauri-apps/plugin-dialog'
  import { Button } from '$lib/components/ui/button'
  import * as Dialog from '$lib/components/ui/dialog'
  import { Input } from '$lib/components/ui/input'
  import { Label } from '$lib/components/ui/label'
  import { Switch } from '$lib/components/ui/switch'
  import * as api from './api'
  import type { Settings } from './api'

  let { open: isOpen = $bindable(), onsaved }: { open: boolean; onsaved: (settings: Settings) => void } = $props()
  let draft = $state<Settings | null>(null)
  let downloadKib = $state(0)
  let uploadKib = $state(0)
  let error = $state('')
  let notice = $state('')

  $effect(() => {
    if (isOpen) {
      error = ''
      notice = ''
      api.settings().then((s) => {
        draft = s
        downloadKib = Math.round(s.download_limit / 1024)
        uploadKib = Math.round(s.upload_limit / 1024)
      })
    }
  })

  async function pickDir() {
    if (!draft) return
    const dir = await open({ title: 'Download into…', directory: true, defaultPath: draft.download_dir })
    if (typeof dir === 'string') draft.download_dir = dir
  }

  async function save() {
    if (!draft) return
    const settings: Settings = {
      ...draft,
      download_limit: Math.max(0, downloadKib) * 1024,
      upload_limit: Math.max(0, uploadKib) * 1024,
    }
    try {
      const restart = await api.updateSettings(settings)
      onsaved(settings)
      if (restart) {
        notice = 'The listen port or DHT setting takes effect after a restart.'
      } else {
        isOpen = false
      }
    } catch (e) {
      error = String(e)
    }
  }
</script>

<Dialog.Root bind:open={isOpen}>
  <Dialog.Content class="sm:max-w-md">
    <Dialog.Header>
      <Dialog.Title>Settings</Dialog.Title>
      <Dialog.Description>Limits and the ratio at 0 mean no limit.</Dialog.Description>
    </Dialog.Header>
    {#if draft}
      <div class="grid grid-cols-[max-content_1fr] items-center gap-x-4 gap-y-3">
        <Label for="download-dir">Download to</Label>
        <div class="flex gap-2">
          <Input id="download-dir" bind:value={draft.download_dir} class="h-8" />
          <Button variant="outline" size="sm" onclick={pickDir}>Browse…</Button>
        </div>

        <Label for="listen-port">Listen port</Label>
        <Input id="listen-port" type="number" min="1" max="65535" bind:value={draft.listen_port} class="h-8 w-28" />

        <Label for="dht">DHT</Label>
        <Switch id="dht" bind:checked={draft.dht} />

        <Label for="max-peers">Peers per torrent</Label>
        <Input id="max-peers" type="number" min="1" bind:value={draft.max_peers_per_torrent} class="h-8 w-28" />

        <Label for="down-limit">Download limit (KiB/s)</Label>
        <Input id="down-limit" type="number" min="0" bind:value={downloadKib} class="h-8 w-28" />

        <Label for="up-limit">Upload limit (KiB/s)</Label>
        <Input id="up-limit" type="number" min="0" bind:value={uploadKib} class="h-8 w-28" />

        <Label for="ratio">Stop seeding at ratio</Label>
        <Input id="ratio" type="number" min="0" step="0.1" bind:value={draft.seed_ratio_limit} class="h-8 w-28" />
      </div>
      {#if error}<p class="text-destructive">{error}</p>{/if}
      {#if notice}<p class="text-muted-foreground">{notice}</p>{/if}
    {/if}
    <Dialog.Footer>
      <Button variant="outline" onclick={() => (isOpen = false)}>Cancel</Button>
      <Button onclick={save} disabled={!draft}>Save</Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>
