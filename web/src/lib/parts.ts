import type { Customer } from "@/api/client";

/** The parts of a customer's cluster in the order it gets ready, before Ready itself. */
export const parts: Customer["conditions"][number]["type"][] = [
  "CertificateIssued",
  "KafkaReady",
  "UsersReady",
  "AgentAvailable",
];
