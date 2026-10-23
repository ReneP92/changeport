import { useAuth } from "@clerk/react";
import { useQuery } from "@tanstack/react-query";
import { createContext, use } from "react";
import { unwrap, type ApiClient } from "@/api/client";

/** The API client of the signed-in user, which ApiProvider provides. */
export const ApiContext = createContext<ApiClient | null>(null);

function useApi(): ApiClient {
  const client = use(ApiContext);
  if (!client) {
    throw new Error("useApi needs an ApiProvider");
  }
  return client;
}

/**
 * Queries are per active organization: switching organization switches
 * customer, so every query is keyed by it.
 */
function useOrganizationKey(): string {
  return useAuth().orgId ?? "none";
}

/** The signed-in user and what they may see. */
export function useMe() {
  const api = useApi();
  const organization = useOrganizationKey();
  return useQuery({
    queryKey: ["me", organization],
    queryFn: async () => unwrap(await api.GET("/api/me")),
  });
}

/** The customer of the active organization. */
export function useCustomer() {
  const api = useApi();
  const organization = useOrganizationKey();
  return useQuery({
    queryKey: ["customer", organization],
    queryFn: async () => unwrap(await api.GET("/api/customer")),
    refetchInterval: 10_000,
  });
}

/** Every customer, for operators. */
export function useCustomers() {
  const api = useApi();
  const organization = useOrganizationKey();
  return useQuery({
    queryKey: ["operator", "customers", organization],
    queryFn: async () => unwrap(await api.GET("/api/operator/customers")).customers,
    refetchInterval: 10_000,
  });
}
