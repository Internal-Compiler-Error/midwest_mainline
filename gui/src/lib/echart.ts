// `use:echart={option}`: an ECharts instance on the element, re-fed the option whenever it
// changes (ECharts animates the difference itself), resized with the element, and re-themed
// when the `dark` class on <html> flips (mode-watcher toggles it).
import * as echarts from 'echarts'
import type { Action } from 'svelte/action'

export type Option = echarts.EChartsOption

function isDark(): boolean {
  return document.documentElement.classList.contains('dark')
}

/** Sensible defaults for a small card: tight margins, our font, no toolbox. */
export function base(option: Option): Option {
  return {
    animationDuration: 500,
    animationDurationUpdate: 400,
    animationEasingUpdate: 'cubicOut',
    textStyle: { fontFamily: 'Inter Variable, system-ui, sans-serif', fontSize: 11 },
    grid: { left: 8, right: 8, top: 24, bottom: 4, containLabel: true },
    ...option,
  }
}

export const echart: Action<HTMLElement, Option> = (node, option) => {
  let chart = echarts.init(node, isDark() ? 'dark' : undefined, { renderer: 'canvas' })
  let current = option
  const apply = () => chart.setOption({ backgroundColor: 'transparent', ...current })
  apply()

  const resize = new ResizeObserver(() => chart.resize())
  resize.observe(node)

  // the theme is baked in at init, so a switch means a fresh instance
  const theme = new MutationObserver(() => {
    chart.dispose()
    chart = echarts.init(node, isDark() ? 'dark' : undefined, { renderer: 'canvas' })
    apply()
  })
  theme.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] })

  return {
    update(next: Option) {
      current = next
      apply()
    },
    destroy() {
      resize.disconnect()
      theme.disconnect()
      chart.dispose()
    },
  }
}
