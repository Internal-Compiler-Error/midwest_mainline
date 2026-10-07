<script lang="ts">
  // One card of the Insights grid: a title (or a `header` of its own), then a chart, or
  // whatever `children` draws.
  import type { Snippet } from 'svelte'
  import type { ClassValue } from 'svelte/elements'
  import { echart, type Chart, type Option } from './echart'

  let {
    title = '',
    note = '',
    chart,
    height = 'h-52',
    class: cls,
    header,
    children,
  }: {
    title?: string
    /** quieter words after the title */
    note?: string
    chart?: Option | Chart
    /** the chart's height, as a Tailwind class */
    height?: string
    class?: ClassValue
    header?: Snippet
    children?: Snippet
  } = $props()
</script>

<section class={['rounded-md border p-2', cls]}>
  {#if header}
    {@render header()}
  {:else}
    <h3 class="font-medium">{title}{#if note}&nbsp;<span class="font-normal text-muted-foreground">{note}</span>{/if}</h3>
  {/if}
  {#if chart}
    <div class={height} use:echart={chart}></div>
  {/if}
  {@render children?.()}
</section>
