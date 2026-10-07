<script lang="ts">
  // The session at a glance, for the right of the footer: total rates, the DHT, and whether
  // the router forwards our port.
  import type { Status } from './api'
  import Flip from './Flip.svelte'
  import { rate } from './format'
  import Num from './Num.svelte'

  let { status }: { status: Status } = $props()

  const MAPPING: Record<Status['port_mapping'], [label: string, title: string]> = {
    off: ['no mapping', 'port mapping is off in the settings'],
    searching: ['mapping…', 'asking the router to forward the port'],
    mapped: ['mapped', 'the router forwards the port'],
    unavailable: ['not mapped', 'the router answers neither NAT-PMP nor UPnP; forward the port by hand for inbound peers'],
  }

  let mapping = $derived(MAPPING[status.port_mapping])
  let mappingTitle = $derived(
    status.port_mapping === 'mapped' && status.external_ip ? `${mapping[1]}; external address ${status.external_ip}` : mapping[1],
  )
</script>

<span class="ml-auto flex gap-4 text-xs text-muted-foreground tabular-nums">
  <span>↓ <Num value={status.download_bps} format={rate} /> ↑ <Num value={status.upload_bps} format={rate} /></span>
  <span title="nodes in the DHT routing table">DHT {#if status.dht_nodes === null}off{:else}<Num value={status.dht_nodes} /> nodes{/if}</span>
  <span title={mappingTitle}>port {status.listen_port} · <Flip text={mapping[0]} /></span>
</span>
