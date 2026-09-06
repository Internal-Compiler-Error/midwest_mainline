<script lang="ts">
  // A label that changes with a small vertical flip: the old text slides up and out while
  // the new one slides in from below. Both sit in the same grid cell meanwhile, so nothing
  // around them moves.
  import { fly } from 'svelte/transition'

  let { text, class: cls = '' }: { text: string; class?: string } = $props()
</script>

<span class={['inline-grid overflow-hidden align-bottom', cls]}>
  {#key text}
    <span class="col-start-1 row-start-1" in:fly={{ y: 10, duration: 220 }} out:fly={{ y: -10, duration: 220 }}>{text}</span>
  {/key}
</span>
