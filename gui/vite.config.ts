import { svelte } from '@sveltejs/vite-plugin-svelte'
import { defineConfig } from 'vite'

// the Tauri shell expects the dev server exactly here (see src-tauri/tauri.conf.json)
export default defineConfig({
  plugins: [svelte()],
  clearScreen: false,
  server: { port: 5173, strictPort: true },
})
