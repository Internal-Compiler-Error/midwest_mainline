// One shared 3-second tick: every figure on screen samples its value on it, so they all
// step at the same moment instead of each twitching with every poll.
export const REFRESH_MS = 3000

export const clock = $state({ tick: 0 })

setInterval(() => clock.tick++, REFRESH_MS)
