<script lang="ts">
  // The library's tracing output, newest at the bottom, sticking there unless scrolled up.
  let { lines, open = $bindable(), onclear }: { lines: string[]; open: boolean; onclear: () => void } = $props()
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
    if (line.includes(' ERROR ')) return 'error'
    if (line.includes('  WARN ')) return 'warn'
    return ''
  }
</script>

<div class="console" class:open>
  <div class="head">
    <button class="small" class:active={open} onclick={() => (open = !open)}>Console</button>
    <span class="muted">{lines.length} lines</span>
    {#if open}<button class="small" onclick={onclear}>clear</button>{/if}
  </div>
  {#if open}
    <div class="lines" bind:this={scroller} {onscroll}>
      {#each lines as line, i (i)}
        <div class={level(line)}>{line}</div>
      {/each}
    </div>
  {/if}
</div>

<style>
  .console {
    border-top: 1px solid var(--border);
    background: var(--panel);
    display: flex;
    flex-direction: column;
    min-height: 0;
  }
  .console.open {
    height: 180px;
    resize: vertical;
    overflow: hidden;
  }
  .head {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 4px 10px;
  }
  .active {
    border-color: var(--accent);
  }
  .lines {
    flex: 1;
    overflow: auto;
    padding: 0 10px 6px;
    font-family: ui-monospace, Menlo, monospace;
    font-size: 11.5px;
    white-space: pre;
    user-select: text;
  }
  .warn {
    color: var(--warn);
  }
  .error {
    color: var(--error);
  }
</style>
