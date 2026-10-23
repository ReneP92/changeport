import { execFileSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { createClerkClient, type ClerkClient } from "@clerk/backend";

/**
 * One run's Clerk users and organizations, and the customers of those
 * organizations, which clerk.setup.ts creates and clerk.teardown.ts removes.
 */
export interface Fixtures {
  run: string;
  customers: { a: CustomerFixture; b: CustomerFixture };
  /**
   * Each test that signs in through the browser has users of its own, so
   * that no test's sign-out or session touches another's.
   */
  users: {
    /** Members of customer A's organization, for the form sign-in and the sign-out. */
    formA: UserFixture;
    signOutA: UserFixture;
    /** A member of customer A's organization. */
    memberA: UserFixture;
    /** An admin of customer A's organization. */
    adminA: UserFixture;
    /** A member of customer B's organization. */
    memberB: UserFixture;
    /** A member of the Changeport staff organization: an operator. */
    staff: UserFixture;
  };
  organizations: string[];
}

export interface CustomerFixture {
  id: string;
  displayName: string;
  organizationID: string;
}

export interface UserFixture {
  id: string;
  email: string;
}

/** Where the setup leaves the run's fixtures for the tests. */
const fixturesFile = path.join(
  import.meta.dirname,
  "..",
  "..",
  "playwright",
  ".clerk",
  "fixtures.json",
);

export function writeFixtures(fixtures: Fixtures): void {
  mkdirSync(path.dirname(fixturesFile), { recursive: true });
  writeFileSync(fixturesFile, JSON.stringify(fixtures, null, 2));
}

export function readFixtures(): Fixtures {
  return JSON.parse(readFileSync(fixturesFile, "utf8")) as Fixtures;
}

/** Fixtures older than this belong to runs that died; the janitor removes them. */
export const abandonedAfterMs = 60 * 60 * 1000;

/** Test users are Clerk test addresses, which take the fixed code 424242. */
export const testEmail = (run: string, role: string) => `${run}-${role}+clerk_test@example.com`;
export const testEmailPattern = /^e2e[a-z0-9]+-[a-z0-9-]+\+clerk_test@example\.com$/;
export const testOrganizationPattern = /^E2E e2e[a-z0-9]+ [A-Z]$/;
/** Labels the customers the tests create, by run. */
export const runLabel = "changeport.io/e2e-run";

/** Reads a setting from the environment, which make ui-e2e fills. */
export function env(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is not set; run the signed-in tests with make ui-e2e`);
  }
  return value;
}

export function clerkClient(): ClerkClient {
  return createClerkClient({ secretKey: env("CLERK_SECRET_KEY") });
}

/** Runs the pinned kubectl against the kind cluster, as hack/ui-e2e.sh sets it. */
export function kubectl(args: string[], input?: string): string {
  return execFileSync(env("KUBECTL"), ["--context", env("KUBE_CONTEXT"), ...args], {
    input,
    encoding: "utf8",
    stdio: ["pipe", "pipe", "inherit"],
  });
}
