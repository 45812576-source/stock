import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { fileURLToPath, URL } from 'node:url'

// 同域反代：把后端 API 路径代理到 FastAPI(8501)，浏览器视角同域，httponly cookie 正常携带
// 注意：用正则前缀 ^/xxx/ 精确匹配，避免 SPA 自身 base(/admin-app/) 被 /admin 前缀误代理
const backend = 'http://127.0.0.1:8501'
const proxyKeys = ['^/data/', '^/kg/', '^/settings/', '^/admin/', '^/auth/', '^/stock/', '^/hotspot/', '^/static/']

export default defineConfig({
  base: '/admin-app/',
  plugins: [vue()],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  server: {
    port: 5678,
    proxy: proxyKeys.reduce((acc, p) => {
      acc[p] = { target: backend, changeOrigin: true }
      return acc
    }, {}),
  },
})
