import { Route, Routes } from "react-router";

/** The Changeport console: routes for customers and operators follow. */
export function App() {
  return (
    <Routes>
      <Route path="*" element={<Home />} />
    </Routes>
  );
}

function Home() {
  return (
    <main className="mx-auto flex min-h-svh max-w-3xl flex-col justify-center gap-4 p-8">
      <h1 className="text-3xl font-semibold tracking-tight">Changeport</h1>
      <p className="text-muted-foreground">Managed Kafka with observability built in.</p>
    </main>
  );
}
