import {
  ClerkFailed,
  ClerkLoading,
  OrganizationSwitcher,
  Show,
  SignIn,
  UserButton,
} from "@clerk/react";
import { Link, NavLink, Route, Routes } from "react-router";
import { ApiProvider } from "@/api/ApiProvider";
import { useMe } from "@/api/queries";
import { Notice } from "@/components/Notice";
import { Home } from "@/pages/Home";
import { OperatorCustomer } from "@/pages/operator/OperatorCustomer";
import { OperatorCustomers } from "@/pages/operator/OperatorCustomers";
import { NewSource } from "@/pages/sources/NewSource";
import { SourcePage } from "@/pages/sources/SourcePage";
import { Sources } from "@/pages/sources/Sources";
import { cn } from "@/lib/utils";

/** The console behind Clerk sign-in. */
export function Console() {
  return (
    <>
      <ClerkLoading>
        <Notice title="Changeport" busy />
      </ClerkLoading>
      {/* Clerk's script comes from Clerk; without it nobody can sign in. */}
      <ClerkFailed>
        <Notice title="Sign-in could not load">
          The console could not reach its sign-in service. Check your connection and reload the
          page.
        </Notice>
      </ClerkFailed>
      <Show when="signed-out">
        <main className="flex min-h-svh flex-col items-center justify-center gap-8 p-8">
          <h1 className="text-2xl font-semibold tracking-tight">Changeport</h1>
          <SignIn routing="hash" />
        </main>
      </Show>
      <Show when="signed-in">
        <ApiProvider>
          <SignedIn />
        </ApiProvider>
      </Show>
    </>
  );
}

function SignedIn() {
  return (
    <div className="min-h-svh">
      <header className="border-b">
        <div className="mx-auto flex max-w-5xl flex-wrap items-center gap-x-4 gap-y-2 px-6 py-3">
          <Link to="/" className="flex shrink-0 items-center gap-2 font-semibold tracking-tight">
            <img src="/favicon.svg" alt="" className="size-6" />
            {/* On phones the mark alone leaves room for the organization's name. */}
            <span className="max-sm:sr-only">Changeport</span>
          </Link>
          <CustomerNav />
          <div className="ml-auto flex min-w-0 items-center gap-3">
            <OrganizationSwitcher
              hidePersonal
              afterSelectOrganizationUrl="/"
              afterCreateOrganizationUrl="/"
            />
            <UserButton />
          </div>
        </div>
      </header>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/sources" element={<Sources />} />
        <Route path="/sources/new" element={<NewSource />} />
        <Route path="/sources/:name" element={<SourcePage />} />
        <Route path="/operator" element={<OperatorCustomers />} />
        <Route path="/operator/customers/:customerID" element={<OperatorCustomer />} />
        <Route path="*" element={<Notice title="There is no such page" />} />
      </Routes>
    </div>
  );
}

/** The pages of a customer's console, for its members. */
function CustomerNav() {
  const me = useMe();
  if (me.data?.access !== "customer") {
    return null;
  }
  const link = ({ isActive }: { isActive: boolean }) =>
    cn(
      "rounded-md px-2 py-1 text-sm transition-colors hover:text-foreground",
      isActive ? "font-medium text-foreground" : "text-muted-foreground",
    );
  return (
    // On phones the links get a row of their own, under the organization.
    <nav
      aria-label="Console"
      className="flex items-center gap-1 max-sm:order-last max-sm:-ml-2 max-sm:w-full"
    >
      <NavLink to="/" end className={link}>
        Cluster
      </NavLink>
      <NavLink to="/sources" className={link}>
        Sources
      </NavLink>
    </nav>
  );
}
