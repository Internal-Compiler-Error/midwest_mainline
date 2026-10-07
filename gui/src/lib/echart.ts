// `use:echart={option}`: an ECharts instance on the element, re-fed the option whenever it
// changes (ECharts animates the difference itself), resized with the element, and re-themed
// when the `dark` class on <html> flips (mode-watcher toggles it).
import * as echarts from 'echarts'
import type { Action } from 'svelte/action'

export type Option = echarts.EChartsOption

/** An option plus a click handler, for a chart whose items do something when clicked. */
export interface Interactive {
  option: Option
  onclick: (params: echarts.ECElementEvent) => void
}

function split(arg: Option | Interactive): [Option, Interactive['onclick'] | undefined] {
  return 'onclick' in arg && 'option' in arg ? [arg.option as Option, arg.onclick as Interactive['onclick']] : [arg as Option, undefined]
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

export const echart: Action<HTMLElement, Option | Interactive> = (node, arg) => {
  let [current, onclick] = split(arg)
  let chart = echarts.init(node, isDark() ? 'dark' : undefined, { renderer: 'canvas' })
  const listen = () => chart.on('click', (params) => onclick?.(params))
  const apply = () => chart.setOption({ backgroundColor: 'transparent', ...current })
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
    update(next: Option | Interactive) {
      ;[current, onclick] = split(next)
      apply()
    },
    destroy() {
      resize.disconnect()
      theme.disconnect()
      chart.dispose()
    },
  }
}
