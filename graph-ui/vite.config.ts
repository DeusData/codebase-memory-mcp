/// <reference types="vitest" />
import { defineConfig, type ProxyOptions } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import path from "path";

const uiBackendOrigin = "http://127.0.0.1:9749";
const uiDevOrigins = new Set([
  "http://127.0.0.1:5173",
  "http://localhost:5173",
]);
const uiBackendProxy = (): ProxyOptions => ({
  target: uiBackendOrigin,
  changeOrigin: true,
  // The backend deliberately accepts only its own exact loopback authority.
  // Reject foreign browser origins before normalizing the trusted Vite origin.
  headers: { Origin: uiBackendOrigin },
  bypass(req, res) {
    const origin = req.headers.origin;
    if (origin !== undefined && !uiDevOrigins.has(origin)) {
      res.writeHead(403, { "Content-Type": "application/json" });
      res.end('{"error":"forbidden origin"}');
      return false;
    }
  },
});

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  test: {
    environment: "jsdom",
    globals: true,
  },
  build: {
    outDir: "dist",
    assetsDir: "assets",
    sourcemap: false,
    rollupOptions: {
      output: {
        manualChunks: undefined,
      },
    },
  },
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      "/rpc": uiBackendProxy(),
      "/api": uiBackendProxy(),
    },
  },
});
