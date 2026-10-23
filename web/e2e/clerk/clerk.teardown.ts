import { test as teardown } from "@playwright/test";
import { clerkClient, kubectl, readFixtures, runLabel } from "./fixtures";

// Removes this run's users, organizations and customers. The provisioning
// controller deletes the customers' clusters in the background.
teardown("remove this run's users, organizations and customers", async () => {
  const fixtures = readFixtures();
  const clerk = clerkClient();
  for (const user of Object.values(fixtures.users)) {
    await clerk.users.deleteUser(user.id);
  }
  for (const organization of fixtures.organizations) {
    await clerk.organizations.deleteOrganization(organization);
  }
  kubectl(["delete", "customers", "-l", `${runLabel}=${fixtures.run}`, "--wait=false"]);
});
