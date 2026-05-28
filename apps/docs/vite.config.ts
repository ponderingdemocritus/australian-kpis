import { defineConfig } from 'vite'

export default defineConfig({
  base: process.env.DOCS_BASE_PATH ?? '/',
  build: {
    chunkSizeWarningLimit: 3500,
    sourcemap: true,
  },
})
