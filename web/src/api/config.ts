import createClient from "openapi-fetch";
import { ApiError } from "@/api/client";
import type { paths } from "@/api/schema";

/** What the web app learns from the console before anyone signs in. */
export type ConsoleConfig =
  | { state: "ready"; publishableKey: string }
  // The console has no Clerk configuration, so it refuses everything.
  | { state: "not-configured" };

/** Fetches the console's public configuration. */
export async function fetchConfig(): Promise<ConsoleConfig> {
  const api = createClient<paths>({ baseUrl: window.location.origin });
  const { data, error, response } = await api.GET("/api/config");
  if (data) {
    return { state: "ready", publishableKey: data.clerkPublishableKey };
  }
  if (response.status === 503) {
    return { state: "not-configured" };
  }
  throw new ApiError(response.status, error);
}
