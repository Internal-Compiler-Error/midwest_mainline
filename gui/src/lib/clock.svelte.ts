// A slow shared beat for what mustn't move often: the order of sorted rows. Figures don't
// wait for it; they follow every poll smoothly (see Num.svelte).
export const REORDER_MS = 2000

export const clock = $state({ tick: 0 })

setInterval(() => clock.tick++, REORDER_MS)
