import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

/** A full-page message, such as a loading or error state. */
export function Notice({
  title,
  busy = false,
  children,
}: {
  title: string;
  busy?: boolean;
  children?: ReactNode;
}) {
  return (
    <main
      className="mx-auto flex min-h-svh max-w-xl flex-col justify-center gap-3 p-8"
      aria-busy={busy}
    >
      <h1 className={cn("text-2xl font-semibold tracking-tight", busy && "animate-pulse")}>
        {title}
      </h1>
      {children && <p className="text-muted-foreground">{children}</p>}
    </main>
  );
}
