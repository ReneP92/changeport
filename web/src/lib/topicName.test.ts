import { describe, expect, test } from "bun:test";
import type { TopicPolicy } from "@/api/client";
import { topicNameProblem } from "@/lib/topicName";

const policy: TopicPolicy = {
  maxNameLength: 200,
  reservedPrefixes: ["_", "changeport", "strimzi", "connect-"],
  partitions: { min: 1, max: 12, default: 3 },
  replicationFactor: 1,
  retentionPresets: [{ label: "1 hour", ms: 3_600_000 }],
  defaultRetentionMs: 3_600_000,
  cleanupPolicies: [{ name: "delete", tiered: true }],
};
const existing = ["orders", "payments.eu"];

describe("topicNameProblem", () => {
  test("accepts a good name", () => {
    expect(topicNameProblem("invoices.v2", policy, existing)).toBeNull();
  });
  test("refuses what the server refuses", () => {
    expect(topicNameProblem("", policy, existing)).toBe("Name the topic.");
    expect(topicNameProblem("a".repeat(201), policy, existing)).toBe("Use at most 200 characters.");
    expect(topicNameProblem("..", policy, existing)).toBe('".." is not a topic name.');
    expect(topicNameProblem("my topic", policy, existing)).toContain("Use only letters");
    expect(topicNameProblem("__consumer_offsets", policy, existing)).toContain('starting with "_"');
    expect(topicNameProblem("Changeport-audit", policy, existing)).toContain(
      'starting with "changeport"',
    );
    expect(topicNameProblem("orders", policy, existing)).toBe("Topic orders exists.");
    expect(topicNameProblem("payments_eu", policy, existing)).toContain(
      "collides with topic payments.eu",
    );
  });
});
