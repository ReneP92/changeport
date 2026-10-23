import { OrganizationSwitcher, Show, SignIn, UserButton } from "@clerk/react";
import { Link, Route, Routes } from "react-router";
import { ApiProvider } from "@/api/ApiProvider";
import { Notice } from "@/components/Notice";
import { Home } from "@/pages/Home";
import { OperatorCustomers } from "@/pages/OperatorCustomers";

/** The console behind Clerk sign-in. */
export function Console() {
  return (
    <>
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
        <div className="mx-auto flex max-w-5xl items-center gap-4 px-6 py-3">
          <Link to="/" className="flex items-center gap-2 font-semibold tracking-tight">
            <img src="/favicon.svg" alt="" className="size-6" />
            Changeport
          </Link>
          <div className="ml-auto flex items-center gap-3">
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
        <Route path="/operator" element={<OperatorCustomers />} />
        <Route path="*" element={<Notice title="There is no such page" />} />
      </Routes>
    </div>
  );
}
