<script lang="ts">
  let { fraction, done = false, height = 14 }: { fraction: number; done?: boolean; height?: number } = $props()
  let percent = $derived(Math.round(fraction * 100))
</script>

<div class="bar" style:height="{height}px" title="{percent}%">
  <div class="fill" class:done style:width="{percent}%"></div>
  <span class="label">{percent}%</span>
</div>

<style>
  .bar {
    position: relative;
    width: 100%;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: var(--panel);
    overflow: hidden;
  }
  .fill {
    height: 100%;
    background: var(--accent);
    transition: width 250ms linear;
  }
  .fill.done {
    background: var(--ok);
  }
  .label {
    position: absolute;
    inset: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 11px;
    line-height: 1;
  }
</style>
