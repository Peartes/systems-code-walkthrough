import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'node:path'

// Vite config for the Raft dashboard.
// - Tailwind v4 wired via the official Vite plugin (no postcss.config needed).
// - `@/` import alias for shadcn-style imports.
// - /api proxy so the browser hits the Vite dev server (5173) and Vite
//   forwards JSON to the Go dashboard backend on 8080. This avoids CORS
//   in dev and means the same fetch URL works in production once we
//   serve the built UI from the dashboard binary itself.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://localhost:8080',
    },
  },
})
