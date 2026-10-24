import { useAuth } from "@clerk/react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createContext, use } from "react";
import {
  ApiError,
  unwrap,
  type ApiClient,
  type NewTopic,
  type ThroughputRange,
} from "@/api/client";

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

/** How the active organization's customer connects. */
export function useConnection() {
  const api = useApi();
  const organization = useOrganizationKey();
  return useQuery({
    queryKey: ["customer", "connection", organization],
    queryFn: async () => unwrap(await api.GET("/api/customer/connection")),
  });
}

/** The brokers and partitions of the active organization's cluster. */
export function useHealth() {
  const api = useApi();
  const organization = useOrganizationKey();
  return useQuery({
    queryKey: ["customer", "health", organization],
    queryFn: async () => unwrap(await api.GET("/api/customer/health")),
    refetchInterval: 15_000,
  });
}

/** The topics of the active organization's cluster. */
export function useTopics() {
  const api = useApi();
  const organization = useOrganizationKey();
  return useQuery({
    queryKey: ["customer", "topics", organization],
    queryFn: async () => unwrap(await api.GET("/api/customer/topics")).topics,
    refetchInterval: 15_000,
  });
}

/** The cluster's bytes in and out per second over range. */
export function useThroughput(range: ThroughputRange) {
  const api = useApi();
  const organization = useOrganizationKey();
  return useQuery({
    queryKey: ["customer", "throughput", range, organization],
    queryFn: async () =>
      unwrap(await api.GET("/api/customer/throughput", { params: { query: { range } } })),
    refetchInterval: 60_000,
  });
}

/** Downloads the admin user's client.properties as a file. */
export function useDownloadClientProperties() {
  const api = useApi();
  return async () => {
    const { data, error, response } = await api.GET("/api/customer/client-properties", {
      parseAs: "text",
    });
    if (data === undefined) {
      throw new ApiError(response.status, error);
    }
    const name =
      /filename="([^"]+)"/.exec(response.headers.get("Content-Disposition") ?? "")?.[1] ??
      "client.properties";
    const url = URL.createObjectURL(new Blob([data], { type: "text/plain" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = name;
    link.click();
    URL.revokeObjectURL(url);
  };
}

/** The guardrails on the active organization's topics. */
export function useTopicPolicy() {
  const api = useApi();
  const organization = useOrganizationKey();
  return useQuery({
    queryKey: ["customer", "topic-policy", organization],
    queryFn: async () => unwrap(await api.GET("/api/customer/topic-policy")),
    staleTime: Infinity,
  });
}

/** The consumer groups that read a topic, while enabled. */
export function useTopicConsumerGroups(topic: string, enabled: boolean) {
  const api = useApi();
  const organization = useOrganizationKey();
  return useQuery({
    queryKey: ["customer", "topic-consumer-groups", topic, organization],
    queryFn: async () =>
      unwrap(
        await api.GET("/api/customer/topics/{topic}/consumer-groups", {
          params: { path: { topic } },
        }),
      ).groups,
    enabled,
  });
}

/** Creates a topic, then refreshes the topics. */
export function useCreateTopic() {
  const api = useApi();
  const queries = useQueryClient();
  return useMutation({
    mutationFn: async (topic: NewTopic) =>
      unwrap(await api.POST("/api/customer/topics", { body: topic })),
    onSuccess: () => queries.invalidateQueries({ queryKey: ["customer", "topics"] }),
  });
}

/** Deletes a topic, then refreshes the topics. */
export function useDeleteTopic() {
  const api = useApi();
  const queries = useQueryClient();
  return useMutation({
    mutationFn: async (topic: string) => {
      const { error, response } = await api.DELETE("/api/customer/topics/{topic}", {
        params: { path: { topic } },
      });
      if (!response.ok) {
        throw new ApiError(response.status, error);
      }
    },
    onSuccess: () => queries.invalidateQueries({ queryKey: ["customer", "topics"] }),
  });
}
