import { afterEach, describe, expect, mock, test } from "bun:test";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { App } from "@/App";

const realFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = realFetch;
  cleanup();
});

/** Answers every request the way respond does. */
function stubFetch(respond: (url: string) => Response | Promise<Response>) {
  globalThis.fetch = mock((input: RequestInfo | URL) =>
    Promise.resolve(respond(input instanceof Request ? input.url : String(input))),
  ) as unknown as typeof fetch;
}

function renderApp() {
  const client = new QueryClient({ defaultOptions: { queries: { retryDelay: 0 } } });
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("App", () => {
  test("fails closed when the console is not configured", async () => {
    stubFetch(
      () =>
        new Response(JSON.stringify({ status: 503, title: "The console is not configured" }), {
          status: 503,
          headers: { "Content-Type": "application/problem+json" },
        }),
    );
    renderApp();
    expect(
      await screen.findByRole("heading", { name: "The console is not configured" }),
    ).toBeDefined();
  });

  test("says so when the console's server is unreachable", async () => {
    stubFetch(() => Promise.reject(new TypeError("Failed to fetch")));
    renderApp();
    expect(
      await screen.findByRole("heading", { name: "The console is unavailable" }),
    ).toBeDefined();
  });
});
