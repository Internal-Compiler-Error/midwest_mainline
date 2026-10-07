// `use:echart={option}`: an ECharts instance on the element, re-fed the option whenever it
// changes (ECharts animates the difference itself), resized with the element, and re-themed
// when the `dark` class on <html> flips (mode-watcher toggles it).
import * as echarts from 'echarts'
import type { Action } from 'svelte/action'

export type Option = echarts.EChartsOption

/** An option and how to apply it: a click handler for a chart whose items do something, and
 * `replace` for one whose series come and go. A plain merge keeps every series ever given;
 * with `replace`, series are matched by `id` (so a kept one still animates) and the rest dropped. */
export interface Chart {
  option: Option
  onclick?: (params: echarts.ECElementEvent) => void
  replace?: boolean
}

function split(arg: Option | Chart): Chart {
  return 'option' in arg ? (arg as Chart) : { option: arg as Option }
}

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

export const echart: Action<HTMLElement, Option | Chart> = (node, arg) => {
  let current = split(arg)
  let chart = echarts.init(node, isDark() ? 'dark' : undefined, { renderer: 'canvas' })
  const listen = () => chart.on('click', (params) => current.onclick?.(params))
  const apply = () =>
    chart.setOption({ backgroundColor: 'transparent', ...current.option }, current.replace ? { replaceMerge: ['series'] } : undefined)
  listen()
  apply()

  const resize = new ResizeObserver(() => chart.resize())
  resize.observe(node)

  // the theme is baked in at init, so a switch means a fresh instance
  const theme = new MutationObserver(() => {
    chart.dispose()
    chart = echarts.init(node, isDark() ? 'dark' : undefined, { renderer: 'canvas' })
    listen()
    apply()
  })
  theme.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] })

  return {
    update(next: Option | Chart) {
      current = split(next)
      apply()
    },
    destroy() {
      resize.disconnect()
      theme.disconnect()
      chart.dispose()
    },
  }
}
