import { afterEach, describe, expect, test } from "bun:test";
import { cleanup, render, screen } from "@testing-library/react";
import type { Customer } from "@/api/client";
import { CustomerSummary } from "@/components/CustomerSummary";

afterEach(cleanup);

const starting: Customer = {
  id: "acme",
  displayName: "Acme Corp",
  profile: "dev",
  ready: false,
  deletionProtection: false,
  createdAt: "2026-10-22T00:00:00Z",
  status: { reason: "KafkaPending", message: "Kafka customer-acme/acme: NotReady=True (Creating)" },
  conditions: [
    { type: "CertificateIssued", ready: true, reason: "Done", message: "Issued" },
    { type: "KafkaReady", ready: false, reason: "Pending", message: "NotReady=True (Creating)" },
    { type: "Ready", ready: false, reason: "KafkaPending", message: "Kafka customer-acme/acme" },
  ],
};

describe("CustomerSummary", () => {
  test("shows what a starting cluster waits for", () => {
    render(<CustomerSummary customer={starting} />);
    expect(screen.getByText("Starting")).toBeDefined();
    expect(screen.getByRole("status").textContent).toContain("NotReady=True (Creating)");
    expect(screen.getByText("Known once the cluster is created.")).toBeDefined();
    const progress = screen.getByRole("list", { name: "Progress" });
    expect(progress.textContent).toContain("Listener certificate");
    expect(progress.textContent).toContain("Kafka cluster");
    // Ready is the badge, not a step.
    expect(progress.textContent).not.toContain("Ready for clients");
  });

  test("shows how to reach a ready cluster", () => {
    render(
      <CustomerSummary
        customer={{
          ...starting,
          ready: true,
          bootstrapAddress: "bootstrap.acme.kafka.changeport.test:9094",
          status: { reason: "Done", message: "Clients can connect" },
        }}
      />,
    );
    expect(screen.getByText("Ready")).toBeDefined();
    expect(screen.getByText("bootstrap.acme.kafka.changeport.test:9094")).toBeDefined();
    expect(screen.queryByRole("status")).toBeNull();
  });

  test("shows a cluster that is being deleted", () => {
    render(<CustomerSummary customer={{ ...starting, deleting: true }} />);
    expect(screen.getByText("Being deleted")).toBeDefined();
  });
});
