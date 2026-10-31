import type { Source } from "@/api/client";
import { Badge } from "@/components/ui/badge";

/** A source's phase, as a badge: Streaming, Copying rows, Paused, and so on. */
export function PhaseBadge({ source }: { source: Source }) {
  if (source.deleting) {
    return <Badge variant="destructive">Being deleted</Badge>;
  }
  switch (source.phase) {
    case "Streaming":
      return <Badge>Streaming</Badge>;
    case "Snapshotting":
      return <Badge>Copying rows</Badge>;
    case "Stopped":
      return <Badge variant="outline">Paused</Badge>;
    case "Failed":
      return <Badge variant="destructive">Failed</Badge>;
    default:
      return <Badge variant="secondary">Starting</Badge>;
  }
}
