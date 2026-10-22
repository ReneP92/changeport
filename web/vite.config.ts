import path from "node:path";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// The console service (cmd/console) serves the built app and the API. In
// development, Vite serves the app with hot reload and passes /api on to the
// console running in kind, at http://localhost:8088.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: { "@": path.resolve(import.meta.dirname, "src") },
  },
  server: {
    port: 5173,
    strictPort: true,
    proxy: { "/api": "http://localhost:8088" },
  },
});
