<script lang="ts">
  // A number that changes only on the shared 3-second tick, and rolls to its new value over
  // a second rather than jumping. `format` turns the in-between values into text and also
  // gets the destination, so a unit (KiB, MiB) can be taken from where the number is going
  // rather than flicker as the tween crosses a boundary.
  import { untrack } from 'svelte'
  import { Tween } from 'svelte/motion'
  import { cubicOut } from 'svelte/easing'
  import { clock } from './clock.svelte'

  let {
    value,
    format = (n: number) => String(Math.round(n)),
  }: { value: number; format?: (n: number, target: number) => string } = $props()

  let target = $state(untrack(() => value))
  $effect(() => {
    clock.tick
    target = untrack(() => value)
  })
  const tween = Tween.of(() => target, { duration: 1000, easing: cubicOut })
</script>

<span class="tabular-nums">{format(tween.current, target)}</span>
