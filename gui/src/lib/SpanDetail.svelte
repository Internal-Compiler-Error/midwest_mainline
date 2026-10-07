<script lang="ts">
  // A pinned span in full: its fields and events, the spans inside it, and a way up to its
  // parent or over to its peer.
  import { Button } from '$lib/components/ui/button'
  import type { TraceSpan } from './api'
  import { duration, timeOfDay } from './format'
  import { field, fieldValue, type Traces } from './spans.svelte'

  let {
    traces,
    span,
    now,
    onpin,
  }: { traces: Traces; span: TraceSpan; now: number; onpin: (id: number | null) => void } = $props()

  let children = $derived(traces.children(span.id))
  let peer = $derived(field(span, 'peer'))

  const took = (s: TraceSpan) => duration((s.end_ms ?? now) - s.start_ms)
</script>

<aside class="w-72 shrink-0 overflow-y-auto border-l px-3 py-2 text-xs">
  <div class="mb-1 flex items-center justify-between">
    <b class="text-sm">{span.name}</b>
    <Button variant="ghost" size="xs" aria-label="close" onclick={() => onpin(null)}>✕</Button>
  </div>
  <div class="mb-2 text-muted-foreground tabular-nums">
    {timeOfDay(span.start_ms, true)} · {took(span)}{span.end_ms === null ? ', open' : ''}
  </div>
  <dl class="grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5">
    {#each span.fields as [name, value] (name)}
      <dt class="text-muted-foreground">{name}</dt>
      <dd class="break-all">{fieldValue(name, value)}</dd>
    {/each}
  </dl>
  {#if children.length}
    <h4 class="mt-3 mb-1 font-medium">inside</h4>
    {#each children as child (child.id)}
      <button class="block w-full text-left hover:underline" onclick={() => onpin(child.id)}>
        {child.name} · {took(child)}
      </button>
    {/each}
  {/if}
  {#if span.events.length}
    <h4 class="mt-3 mb-1 font-medium">events</h4>
    <ol class="space-y-0.5">
      {#each span.events as event, i (i)}
        <li><span class="text-muted-foreground tabular-nums">+{duration(event.at_ms - span.start_ms)}</span> {event.message}</li>
      {/each}
    </ol>
  {/if}
  {#if peer && peer !== traces.focus}
    <Button class="mt-3 mr-1" variant="outline" size="xs" onclick={() => (traces.focus = peer ?? null)}>focus this peer</Button>
  {/if}
  {#if span.parent !== null && traces.byId(span.parent)}
    <Button class="mt-3" variant="outline" size="xs" onclick={() => onpin(span.parent)}>↑ parent</Button>
  {/if}
</aside>
