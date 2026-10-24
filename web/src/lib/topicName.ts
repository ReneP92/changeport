import type { TopicPolicy } from "@/api/client";

/**
 * Says what is wrong with a new topic's name under policy, or returns null.
 * The console's server applies the same rules (internal/topics); this lets
 * the form say so while the name is typed.
 */
export function topicNameProblem(
  name: string,
  policy: TopicPolicy,
  existing: string[],
): string | null {
  if (name === "") {
    return "Name the topic.";
  }
  if (name.length > policy.maxNameLength) {
    return `Use at most ${String(policy.maxNameLength)} characters.`;
  }
  if (name === "." || name === "..") {
    return `"${name}" is not a topic name.`;
  }
  if (!/^[a-zA-Z0-9._-]+$/.test(name)) {
    return "Use only letters, digits, dots, underscores and dashes.";
  }
  const reserved = policy.reservedPrefixes.find((prefix) => name.toLowerCase().startsWith(prefix));
  if (reserved !== undefined) {
    return `Names starting with "${reserved}" are reserved for the platform.`;
  }
  // Kafka's metrics replace dots with underscores, so such names collide.
  const normalized = name.replaceAll(".", "_");
  const other = existing.find((topic) => topic.replaceAll(".", "_") === normalized);
  if (other === name) {
    return `Topic ${name} exists.`;
  }
  if (other !== undefined) {
    return `It collides with topic ${other}, as dots and underscores count as equal.`;
  }
  return null;
}
