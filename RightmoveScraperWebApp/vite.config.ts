import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api/rightmove': {
        target: 'https://los.rightmove.co.uk',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api\/rightmove/, ''),
        headers: {
          'Origin': 'https://www.rightmove.co.uk',
          'Referer': 'https://www.rightmove.co.uk/',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
      },
    },
  },
})
