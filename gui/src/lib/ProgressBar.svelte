<script lang="ts">
  import { Progress } from '$lib/components/ui/progress'
  import { percent } from './format'
  import Num from './Num.svelte'

  let { fraction, done = false, tall = false }: { fraction: number; done?: boolean; tall?: boolean } = $props()
</script>

<div class="flex items-center gap-2">
  <Progress value={fraction * 100} class={['[&>[data-slot=progress-indicator]]:duration-300 [&>[data-slot=progress-indicator]]:ease-linear', tall ? 'h-3' : 'h-2']} />
  <span class={['w-9 shrink-0 text-right text-xs tabular-nums', done && 'text-emerald-600 dark:text-emerald-400']}>
    <Num value={fraction * 100} format={percent} />
  </span>
</div>
