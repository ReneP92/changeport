import { useAuth } from "@clerk/react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createContext, use } from "react";
import {
  ApiError,
  unwrap,
  type ApiClient,
  type NewCustomer,
  type NewSource,
  type NewTopic,
  type SetupSQLRequest,
  type SourceChange,
  type SourceCheckRequest,
  type SourceSQLRequest,
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

/**
 * One customer, for operators, refreshed often while its cluster is being
 * created or deleted. Once it is gone, the query fails with a 404.
 */
export function useOperatorCustomer(id: string) {
  const api = useApi();
  const organization = useOrganizationKey();
  return useQuery({
    queryKey: ["operator", "customers", organization, id],
    queryFn: async () =>
      unwrap(
        await api.GET("/api/operator/customers/{customerID}", {
          params: { path: { customerID: id } },
        }),
      ),
    refetchInterval: (query) => {
      const customer = query.state.data;
      if (query.state.error instanceof ApiError && query.state.error.status === 404) {
        return false;
      }
      return customer?.ready && !customer.deleting ? 10_000 : 2_000;
    },
    retry: (failures, error) =>
      !(error instanceof ApiError && error.status === 404) && failures < 3,
  });
}

/** Provisions a customer, then refreshes the customers. */
export function useProvisionCustomer() {
  const api = useApi();
  const queries = useQueryClient();
  return useMutation({
    mutationFn: async (customer: NewCustomer) =>
      unwrap(await api.POST("/api/operator/customers", { body: customer })),
    onSuccess: () => queries.invalidateQueries({ queryKey: ["operator", "customers"] }),
  });
}

/** Deprovisions a customer, then refreshes the customers. */
export function useDeprovisionCustomer() {
  const api = useApi();
  const queries = useQueryClient();
  return useMutation({
    mutationFn: async (id: string) => {
      const { error, response } = await api.DELETE("/api/operator/customers/{customerID}", {
        params: { path: { customerID: id } },
      });
      if (!response.ok) {
        throw new ApiError(response.status, error);
      }
    },
    onSuccess: () => queries.invalidateQueries({ queryKey: ["operator", "customers"] }),
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

/**
 * The consumer groups' lag per topic, most behind first, with the last hour
 * of it. The agent measures it every 30 seconds.
 */
export function useConsumerGroups() {
  const api = useApi();
  const organization = useOrganizationKey();
  return useQuery({
    queryKey: ["customer", "consumer-groups", organization],
    queryFn: async () => unwrap(await api.GET("/api/customer/consumer-groups")).groups,
    refetchInterval: 30_000,
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

/** What adding a Postgres source offers: providers, presets and the egress addresses. */
export function useSourceOptions() {
  const api = useApi();
  const organization = useOrganizationKey();
  return useQuery({
    queryKey: ["customer", "source-options", organization],
    queryFn: async () => unwrap(await api.GET("/api/customer/source-options")),
    staleTime: Infinity,
  });
}

/** The active organization's Postgres sources. */
export function useSources() {
  const api = useApi();
  const organization = useOrganizationKey();
  return useQuery({
    queryKey: ["customer", "sources", organization],
    queryFn: async () => unwrap(await api.GET("/api/customer/sources")).sources,
    refetchInterval: 10_000,
  });
}

/**
 * One Postgres source, refreshed often while it copies tables or is being
 * deleted. Once it is gone, the query fails with a 404.
 */
export function useSource(name: string) {
  const api = useApi();
  const organization = useOrganizationKey();
  return useQuery({
    queryKey: ["customer", "sources", organization, name],
    queryFn: async () =>
      unwrap(
        await api.GET("/api/customer/sources/{source}", { params: { path: { source: name } } }),
      ),
    refetchInterval: (query) => {
      if (query.state.error instanceof ApiError && query.state.error.status === 404) {
        return false;
      }
      const source = query.state.data;
      return source?.phase === "Streaming" || source?.phase === "Stopped" ? 10_000 : 3_000;
    },
    retry: (failures, error) =>
      !(error instanceof ApiError && error.status === 404) && failures < 3,
  });
}

/** The SQL of a new source's steps, which the first step's answer alone holds a password in. */
export function useSetupSQL() {
  const api = useApi();
  return useMutation({
    mutationFn: async (request: SetupSQLRequest) =>
      unwrap(await api.POST("/api/customer/sources/setup-sql", { body: request })),
  });
}

/** Checks a database for a source, keeping nothing. */
export function useCheckSource() {
  const api = useApi();
  return useMutation({
    mutationFn: async (request: SourceCheckRequest) =>
      unwrap(await api.POST("/api/customer/sources/checks", { body: request })),
  });
}

/** Lists a database's tables for the table picker of a new source. */
export function useListSourceTables() {
  const api = useApi();
  return useMutation({
    mutationFn: async (request: SourceCheckRequest) =>
      unwrap(await api.POST("/api/customer/sources/tables", { body: request })).tables,
  });
}

/** Creates a source, then refreshes the sources. */
export function useCreateSource() {
  const api = useApi();
  const queries = useQueryClient();
  return useMutation({
    mutationFn: async (source: NewSource) =>
      unwrap(await api.POST("/api/customer/sources", { body: source })),
    onSuccess: () => queries.invalidateQueries({ queryKey: ["customer", "sources"] }),
  });
}

/** Changes a source's tables, retention or state, then refreshes the sources and topics. */
export function useChangeSource(name: string) {
  const api = useApi();
  const queries = useQueryClient();
  return useMutation({
    mutationFn: async (change: SourceChange) =>
      unwrap(
        await api.PATCH("/api/customer/sources/{source}", {
          params: { path: { source: name } },
          body: change,
        }),
      ),
    onSuccess: async () => {
      await queries.invalidateQueries({ queryKey: ["customer", "sources"] });
      await queries.invalidateQueries({ queryKey: ["customer", "topics"] });
    },
  });
}

/** Deletes a source, with its name typed, then refreshes the sources. */
export function useDeleteSource(name: string) {
  const api = useApi();
  const queries = useQueryClient();
  return useMutation({
    mutationFn: async () => {
      const { error, response } = await api.DELETE("/api/customer/sources/{source}", {
        params: { path: { source: name }, query: { confirm: name } },
      });
      if (!response.ok) {
        throw new ApiError(response.status, error);
      }
    },
    onSuccess: () => queries.invalidateQueries({ queryKey: ["customer", "sources"] }),
  });
}

/** The SQL of a change to an existing source's database. */
export function useSourceSQL(name: string) {
  const api = useApi();
  return useMutation({
    mutationFn: async (request: SourceSQLRequest) =>
      unwrap(
        await api.POST("/api/customer/sources/{source}/sql", {
          params: { path: { source: name } },
          body: request,
        }),
      ),
  });
}

/** Has a source use a new password, once it logs in. */
export function useSetSourcePassword(name: string) {
  const api = useApi();
  const queries = useQueryClient();
  return useMutation({
    mutationFn: async (password: string) => {
      const { error, response } = await api.PUT("/api/customer/sources/{source}/password", {
        params: { path: { source: name } },
        body: { password },
      });
      if (!response.ok) {
        throw new ApiError(response.status, error);
      }
    },
    onSuccess: () => queries.invalidateQueries({ queryKey: ["customer", "sources"] }),
  });
}

/** Confirms that the customer dropped a deleted source's slot themselves. */
export function useConfirmSlotDropped(name: string) {
  const api = useApi();
  const queries = useQueryClient();
  return useMutation({
    mutationFn: async () => {
      const { error, response } = await api.POST("/api/customer/sources/{source}/slot-dropped", {
        params: { path: { source: name } },
      });
      if (!response.ok) {
        throw new ApiError(response.status, error);
      }
    },
    onSuccess: () => queries.invalidateQueries({ queryKey: ["customer", "sources"] }),
  });
}

/**
 * The tables of an existing source's database, which the source controller
 * lists on request: the console cannot log in to the database itself. While
 * enabled, it asks once, then polls until the answer is there.
 */
export function useSourceCatalog(name: string, enabled: boolean) {
  const api = useApi();
  const organization = useOrganizationKey();
  const queries = useQueryClient();
  const key = ["customer", "source-catalog", organization, name];
  const request = useQuery({
    queryKey: [...key, "request"],
    queryFn: async () =>
      unwrap(
        await api.POST("/api/customer/sources/{source}/catalog", {
          params: { path: { source: name } },
        }),
      ),
    enabled,
    staleTime: 0,
    gcTime: 0,
    retry: false,
  });
  const catalog = useQuery({
    queryKey: key,
    queryFn: async () =>
      unwrap(
        await api.GET("/api/customer/sources/{source}/catalog", {
          params: { path: { source: name } },
        }),
      ),
    enabled: enabled && request.isSuccess,
    gcTime: 0,
    refetchInterval: (query) => (query.state.data?.ready ? false : 1_000),
  });
  return {
    data: catalog.data?.ready ? catalog.data : undefined,
    error: request.error ?? catalog.error,
    retry: () => queries.resetQueries({ queryKey: key }),
  };
}
