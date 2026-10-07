<script lang="ts">
  // A number that follows its value continuously: each poll's value pulls a critically
  // damped spring, so the figure glides rather than jumps and settles without overshoot.
  // `format` gets the in-between value and the destination, so a unit (KiB, MiB) can be taken
  // from where the number is going rather than flicker as the spring crosses a boundary.
  import { Spring } from 'svelte/motion'

  let {
    value,
    format = (n: number) => String(Math.round(n)),
  }: { value: number; format?: (n: number, target: number) => string } = $props()

  const spring = Spring.of(() => value, { stiffness: 0.12, damping: 1 })
</script>

<span class="tabular-nums">{format(spring.current, value)}</span>
