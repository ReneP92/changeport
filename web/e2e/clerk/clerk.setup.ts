import { randomBytes } from "node:crypto";
import { clerkSetup } from "@clerk/testing/playwright";
import { test as setup } from "@playwright/test";
import {
  abandonedAfterMs,
  clerkClient,
  env,
  kubectl,
  runLabel,
  testEmail,
  testEmailPattern,
  testOrganizationPattern,
  writeFixtures,
  type CustomerFixture,
  type Fixtures,
} from "./fixtures";

// Creates this run's Clerk users and organizations, and a customer for each
// organization, after removing what runs that died left behind.
setup("create this run's users, organizations and customers", async () => {
  setup.setTimeout(120_000);
  const clerk = clerkClient();
  await removeAbandoned();

  // Clerk's testing token lets the browser past its bot protection.
  await clerkSetup({ publishableKey: env("CLERK_PUBLISHABLE_KEY") });

  const run = `e2e${Date.now().toString(36).slice(-5)}${randomBytes(2).toString("hex")}`;
  const user = async (role: string) => {
    const created = await clerk.users.createUser({
      emailAddress: [testEmail(run, role)],
      firstName: "E2E",
      lastName: role,
      skipPasswordRequirement: true,
    });
    return { id: created.id, email: testEmail(run, role) };
  };
  const users = {
    formA: await user("form-a"),
    signOutA: await user("sign-out-a"),
    memberA: await user("member-a"),
    adminA: await user("admin-a"),
    memberB: await user("member-b"),
    staff: await user("staff"),
  };
  const organizationA = await clerk.organizations.createOrganization({ name: `E2E ${run} A` });
  const organizationB = await clerk.organizations.createOrganization({ name: `E2E ${run} B` });
  const memberships: [string, string, "org:member" | "org:admin"][] = [
    [organizationA.id, users.formA.id, "org:member"],
    [organizationA.id, users.signOutA.id, "org:member"],
    [organizationA.id, users.memberA.id, "org:member"],
    [organizationA.id, users.adminA.id, "org:admin"],
    [organizationB.id, users.memberB.id, "org:member"],
    [env("CHANGEPORT_STAFF_ORG_ID"), users.staff.id, "org:member"],
  ];
  for (const [organizationId, userId, role] of memberships) {
    await clerk.organizations.createOrganizationMembership({ organizationId, userId, role });
  }

  const customer = (suffix: string, organizationID: string): CustomerFixture => ({
    id: `${run}-${suffix}`,
    displayName: `E2E ${run} ${suffix.toUpperCase()}`,
    organizationID,
  });
  const customers = { a: customer("a", organizationA.id), b: customer("b", organizationB.id) };
  for (const c of Object.values(customers)) {
    kubectl(
      ["apply", "--server-side", "--field-manager=changeport-e2e", "-f", "-"],
      JSON.stringify({
        apiVersion: "changeport.io/v1alpha1",
        kind: "Customer",
        metadata: { name: c.id, labels: { [runLabel]: run } },
        spec: { displayName: c.displayName, profile: "dev", clerkOrganizationID: c.organizationID },
      }),
    );
  }

  const fixtures: Fixtures = {
    run,
    customers,
    users,
    organizations: [organizationA.id, organizationB.id],
  };
  writeFixtures(fixtures);
});

/**
 * The janitor: removes the test users, organizations and customers of runs
 * that started over an hour ago and never cleaned up, so that a crashed run
 * costs nothing for long. Younger ones may belong to a run in progress.
 */
async function removeAbandoned(): Promise<void> {
  const clerk = clerkClient();
  const cutoff = Date.now() - abandonedAfterMs;
  const users = await clerk.users.getUserList({ query: "+clerk_test@example.com", limit: 100 });
  for (const u of users.data) {
    const email = u.primaryEmailAddress?.emailAddress ?? "";
    if (testEmailPattern.test(email) && u.createdAt < cutoff) {
      await clerk.users.deleteUser(u.id);
    }
  }
  const organizations = await clerk.organizations.getOrganizationList({
    query: "E2E e2e",
    limit: 100,
  });
  for (const o of organizations.data) {
    if (testOrganizationPattern.test(o.name) && o.createdAt < cutoff) {
      await clerk.organizations.deleteOrganization(o.id);
    }
  }
  const customers = JSON.parse(kubectl(["get", "customers", "-l", runLabel, "-o", "json"])) as {
    items: { metadata: { name: string; creationTimestamp: string } }[];
  };
  for (const c of customers.items) {
    if (Date.parse(c.metadata.creationTimestamp) < cutoff) {
      kubectl(["delete", "customer", c.metadata.name, "--wait=false"]);
    }
  }
}
