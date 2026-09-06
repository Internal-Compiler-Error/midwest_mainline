<script lang="ts">
  // The library's tracing output, newest at the bottom, sticking there unless scrolled up.
  // Its height is whatever pane it's put in (see App.svelte).
  let { lines }: { lines: string[] } = $props()
  let scroller: HTMLDivElement | undefined = $state()
  let stick = $state(true)

  function onscroll() {
    if (!scroller) return
    stick = scroller.scrollTop + scroller.clientHeight >= scroller.scrollHeight - 4
  }

  $effect(() => {
    // reading `lines.length` makes this rerun on every new line
    if (lines.length && stick && scroller) scroller.scrollTop = scroller.scrollHeight
  })

  function level(line: string): string {
    if (line.includes(' ERROR ')) return 'text-destructive'
    if (line.includes('  WARN ')) return 'text-amber-600 dark:text-amber-400'
    return ''
  }
</script>

<div
  class="h-full overflow-auto bg-card px-3 py-1 font-mono text-[11.5px] leading-4 whitespace-pre select-text"
  bind:this={scroller}
  {onscroll}
>
  {#each lines as line, i (i)}
    <div class={level(line)}>{line}</div>
  {/each}
</div>
