import { defineConfig } from "vite";
import react, { reactCompilerPreset } from "@vitejs/plugin-react";
import babel from "@rolldown/plugin-babel";
import tailwindcss from "@tailwindcss/vite";
import { fileURLToPath, URL } from "node:url";

export default defineConfig(({ command }) => ({
  root: "frontend",
  base: command === "build" ? "/static/" : "/",
  plugins: [
    react(),
    babel({ presets: [reactCompilerPreset({ compilationMode: "infer" })] }),
    tailwindcss(),
  ],
  resolve: {
    alias: { "@": fileURLToPath(new URL("./frontend/src", import.meta.url)) },
  },
  server: {
    port: 1313,
    proxy: {
      "/api": "http://localhost:1212",
      "/v1": "http://localhost:1212",
      "/ws": { target: "http://localhost:1212", ws: true },
      "/health": "http://localhost:1212",
    },
  },
  build: {
    outDir: "../proxen/dashboard",
    emptyOutDir: true,
    cssCodeSplit: false,
    rollupOptions: {
      output: {
        entryFileNames: "app.js",
        assetFileNames: "app.css",
      },
    },
  },
}));
