import { mount } from 'svelte'
import { invoke } from '@tauri-apps/api/core'
import './app.css'
import App from './App.svelte'

// the webview's console is invisible from outside; a front-end error goes to the backend's
// log instead, where the console panel and the run driver show it
function report(what: string) {
  console.error(what)
  invoke('report_error', { message: what }).catch(() => {})
}
window.addEventListener('error', (e) => report(`${e.message} at ${e.filename}:${e.lineno}\n${e.error?.stack ?? ''}`))
window.addEventListener('unhandledrejection', (e) => report(`unhandled rejection: ${e.reason?.stack ?? e.reason}`))

let app
try {
  app = mount(App, {
    target: document.getElementById('app')!,
  })
} catch (e) {
  report(`mount failed: ${(e as Error)?.stack ?? e}`)
  document.body.innerText = `mount failed: ${(e as Error)?.stack ?? e}`
  throw e
}

export default app
