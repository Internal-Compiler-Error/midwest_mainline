import { svelte } from '@sveltejs/vite-plugin-svelte'
import tailwindcss from '@tailwindcss/vite'
import { fileURLToPath, URL } from 'node:url'
import { defineConfig } from 'vite'

// the Tauri shell expects the dev server exactly here (see src-tauri/tauri.conf.json)
export default defineConfig({
  plugins: [tailwindcss(), svelte()],
  resolve: {
    // the SvelteKit-style alias that shadcn-svelte components import through
    alias: { $lib: fileURLToPath(new URL('./src/lib', import.meta.url)) },
  },
  clearScreen: false,
  server: { port: 5173, strictPort: true },
})
