import createClient, { type Client, type Middleware } from "openapi-fetch";
import type { components, paths } from "@/api/schema";

export type Customer = components["schemas"]["Customer"];
export type Me = components["schemas"]["Me"];
export type Problem = components["schemas"]["Problem"];
export type Connection = components["schemas"]["Connection"];
export type Health = components["schemas"]["Health"];
export type Topic = components["schemas"]["Topic"];
export type Throughput = components["schemas"]["Throughput"];
export type TopicPolicy = components["schemas"]["TopicPolicy"];
export type NewTopic = components["schemas"]["NewTopic"];
export type NewCustomer = components["schemas"]["NewCustomer"];
export type ConsumerGroupLag = components["schemas"]["ConsumerGroupLag"];
export type ThroughputRange = NonNullable<
  NonNullable<paths["/api/customer/throughput"]["get"]["parameters"]["query"]>["range"]
>;

/** An error the console's API answered, with its problem description. */
export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly problem?: Problem,
  ) {
    super(problem?.title ?? `The console answered ${String(status)}`);
    this.name = "ApiError";
  }
}

export type ApiClient = Client<paths>;

/** Creates a client of the console's API that sends the session token. */
export function createApiClient(getToken: () => Promise<string | null>): ApiClient {
  const client = createClient<paths>({ baseUrl: window.location.origin });
  const authenticate: Middleware = {
    async onRequest({ request }) {
      const token = await getToken();
      if (token) {
        request.headers.set("Authorization", `Bearer ${token}`);
      }
      return request;
    },
  };
  client.use(authenticate);
  return client;
}

/** Returns data, or throws the API's error. */
export function unwrap<T>(result: { data?: T; error?: unknown; response: Response }): T {
  if (result.data !== undefined) {
    return result.data;
  }
  throw new ApiError(result.response.status, result.error as Problem | undefined);
}
