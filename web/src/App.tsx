import { ClerkProvider } from "@clerk/react";
import { useQuery } from "@tanstack/react-query";
import { fetchConfig } from "@/api/config";
import { Console } from "@/Console";
import { Notice } from "@/components/Notice";

/**
 * The Changeport console. It learns the Clerk instance from the console's
 * API, so one build serves every environment, and fails closed: without a
 * configuration there is nothing to sign in to.
 */
export function App() {
  const config = useQuery({
    queryKey: ["config"],
    queryFn: fetchConfig,
    staleTime: Infinity,
    retry: 1,
  });
  if (config.isPending) {
    return <Notice title="Changeport" busy />;
  }
  if (config.isError) {
    return (
      <Notice title="The console is unavailable">
        It could not reach its server. Try again in a moment.
      </Notice>
    );
  }
  if (config.data.state === "not-configured") {
    return (
      <Notice title="The console is not configured">
        It has no sign-in configuration yet, so it refuses every request. Operators set it up with
        make console-config.
      </Notice>
    );
  }
  return (
    <ClerkProvider publishableKey={config.data.publishableKey} afterSignOutUrl="/">
      <Console />
    </ClerkProvider>
  );
}
