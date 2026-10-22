import { describe, expect, test } from "bun:test";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { App } from "@/App";

describe("App", () => {
  test("shows the product name", () => {
    render(
      <MemoryRouter>
        <App />
      </MemoryRouter>,
    );
    expect(screen.getByRole("heading", { name: "Changeport" })).toBeDefined();
  });
});
