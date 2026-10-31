import { CircleCheck, CircleMinus, CircleX, TriangleAlert } from "lucide-react";
import type { CheckReport, CheckResult } from "@/api/client";
import { cn } from "@/lib/utils";

/** What each of internal/cdc's checks looks at, in words. */
const checkNames: Record<string, string> = {
  reachable: "Reachable",
  tls: "TLS",
  certificate: "Certificate",
  authentication: "Login",
  version: "PostgreSQL version",
  wal_level: "Logical replication",
  replication_role: "Replication privilege",
  slots: "Replication slot",
  wal_senders: "WAL sender",
  wal_keep_size: "WAL kept for slots",
  replication_connection: "Replication connection",
  tables: "Tables",
  keys: "Primary keys",
  read_access: "Read access",
  publication: "Publication",
  published: "Published tables",
};

const statusIcons: Record<CheckResult["status"], typeof CircleCheck> = {
  pass: CircleCheck,
  warn: TriangleAlert,
  fail: CircleX,
  skipped: CircleMinus,
};

const statusNames: Record<CheckResult["status"], string> = {
  pass: "Passed",
  warn: "Warning",
  fail: "Failed",
  skipped: "Skipped",
};

/** The results of a database's checks, each with what was found or what to do. */
export function CheckList({ report, label }: { report: CheckReport; label: string }) {
  return (
    <ul aria-label={label} className="flex flex-col gap-3 text-sm">
      {report.results.map((result) => {
        const Icon = statusIcons[result.status];
        return (
          <li key={result.check} className="flex items-start gap-3">
            <Icon
              role="img"
              aria-label={statusNames[result.status]}
              className={cn(
                "mt-0.5 size-4 shrink-0",
                result.status === "pass" && "text-primary",
                result.status === "warn" && "text-amber-600 dark:text-amber-500",
                result.status === "fail" && "text-destructive",
                result.status === "skipped" && "text-muted-foreground",
              )}
            />
            <div className="flex min-w-0 flex-col gap-0.5">
              <span className="font-medium">{checkNames[result.check] ?? result.check}</span>
              <span
                className={cn(
                  "leading-snug break-words",
                  result.status === "fail" ? "text-destructive" : "text-muted-foreground",
                )}
              >
                {result.message}
              </span>
            </div>
          </li>
        );
      })}
    </ul>
  );
}
