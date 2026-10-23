import { useAuth } from "@clerk/react";
import { useMemo, type ReactNode } from "react";
import { createApiClient } from "@/api/client";
import { ApiContext } from "@/api/queries";

/** Gives the signed-in part of the app a client that sends Clerk's token. */
export function ApiProvider({ children }: { children: ReactNode }) {
  const { getToken } = useAuth();
  const client = useMemo(() => createApiClient(() => getToken()), [getToken]);
  return <ApiContext value={client}>{children}</ApiContext>;
}
