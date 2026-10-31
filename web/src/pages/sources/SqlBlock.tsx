import { Check, Copy } from "lucide-react";
import { useState } from "react";
import type { SourceSQL } from "@/api/client";
import { Button } from "@/components/ui/button";

/**
 * What the customer does to their database for a step: the steps outside
 * SQL first, then the SQL, which they copy and run.
 */
export function SqlBlock({ sql, label }: { sql: SourceSQL; label: string }) {
  return (
    <div className="flex flex-col gap-4 text-sm">
      {sql.steps.length > 0 && (
        <ol className="flex list-decimal flex-col gap-2 pl-5 marker:text-muted-foreground">
          {sql.steps.map((step) => (
            <li key={step} className="pl-1 leading-relaxed">
              {step}
            </li>
          ))}
        </ol>
      )}
      {sql.sql.trim() !== "" && <CodeBlock code={sql.sql} label={label} />}
    </div>
  );
}

/** Code to copy, such as SQL, with a button that copies it. */
export function CodeBlock({ code, label }: { code: string; label: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <figure className="relative rounded-lg border bg-muted/40">
      <figcaption className="flex items-center justify-between gap-3 border-b px-3 py-1.5 text-xs text-muted-foreground">
        <span>{label}</span>
        <Button
          type="button"
          variant="ghost"
          size="xs"
          onClick={() => {
            void navigator.clipboard.writeText(code).then(() => {
              setCopied(true);
              setTimeout(() => {
                setCopied(false);
              }, 2_000);
            });
          }}
        >
          {copied ? <Check /> : <Copy />}
          {copied ? "Copied" : "Copy"}
        </Button>
      </figcaption>
      {/* Wrapped rather than scrolled sideways, so that a long line, such as
          one with a password, shows whole. */}
      <pre className="max-h-80 overflow-auto p-3 font-mono text-xs leading-relaxed whitespace-pre-wrap [overflow-wrap:anywhere]">
        <code>{code.trimEnd()}</code>
      </pre>
    </figure>
  );
}
