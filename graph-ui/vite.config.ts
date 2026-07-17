/// <reference types="vitest" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { Buffer } from "node:buffer";
import path from "path";
import { assertChunkBudget, MAX_JS_CHUNK_BYTES } from "./src/lib/chunkBudget";

export default defineConfig({
  plugins: [
    react(),
    tailwindcss(),
    {
      name: "enforce-js-chunk-budget",
      generateBundle(_options, bundle) {
        const chunks = Object.values(bundle).flatMap((entry) =>
          entry.type === "chunk"
            ? [{ fileName: entry.fileName, bytes: Buffer.byteLength(entry.code, "utf8") }]
            : [],
        );
        assertChunkBudget(chunks);
      },
    },
  ],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  test: {
    environment: "jsdom",
    globals: true,
    coverage: {
      provider: "v8",
      reporter: ["text", "json-summary"],
      reportsDirectory: "coverage",
      include: ["src/**/*.{ts,tsx}"],
      exclude: ["src/**/*.test.{ts,tsx}", "src/vite-env.d.ts"],
      thresholds: {
        statements: 50,
        branches: 40,
        functions: 40,
        lines: 50,
      },
    },
  },
  build: {
    outDir: "dist",
    assetsDir: "assets",
    sourcemap: false,
    // The build plugin above turns this measured ceiling into a failing budget.
    chunkSizeWarningLimit: MAX_JS_CHUNK_BYTES / 1000,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes("node_modules/three/")) return "three-core";
          if (id.includes("node_modules/@react-three/")) return "three-react";
          if (id.includes("node_modules/react/") || id.includes("node_modules/react-dom/")) {
            return "react-core";
          }
        },
      },
    },
  },
  server: {
    port: 5173,
    proxy: {
      "/rpc": "http://127.0.0.1:9749",
      "/api": "http://127.0.0.1:9749",
    },
  },
});
