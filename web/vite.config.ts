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
  build: {
    rolldownOptions: {
      output: {
        // Libraries change less often than the app, so they get chunks of
        // their own that browsers keep across releases. Clerk's bundled UI
        // (@clerk/ui) stays out of them, so that its components still load
        // only when shown.
        codeSplitting: {
          groups: [
            { name: "react", test: /node_modules[\\/](react|react-dom|scheduler)[\\/]/ },
            { name: "clerk", test: /node_modules[\\/]@clerk[\\/](react|shared)[\\/]/ },
            { name: "charts", test: /node_modules[\\/](recharts|d3-[^\\/]+|victory-vendor)[\\/]/ },
            { name: "vendor", test: /node_modules[\\/](?!@clerk[\\/]ui[\\/])/ },
          ],
        },
      },
    },
  },
  server: {
    port: 5173,
    strictPort: true,
    proxy: { "/api": "http://localhost:8088" },
  },
});
