<script lang="ts">
  // A number that rolls to its new value instead of jumping: the tween runs a little longer
  // than the polling interval (250 ms), so a counter that changes every poll moves without
  // pause. `format` turns the in-between values into text and also gets the destination,
  // so a unit (KiB, MiB) can be taken from where the number is going rather than flicker
  // as the tween crosses a boundary.
  import { Tween } from 'svelte/motion'
  import { cubicOut } from 'svelte/easing'

  let {
    value,
    format = (n: number) => String(Math.round(n)),
  }: { value: number; format?: (n: number, target: number) => string } = $props()

  const tween = Tween.of(() => value, { duration: 300, easing: cubicOut })
</script>

<span class="tabular-nums">{format(tween.current, value)}</span>
