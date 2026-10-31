import { useEffect, useState, type ReactNode } from "react";
import { Link, useNavigate } from "react-router";
import {
  errorText,
  ApiError,
  type CheckReport,
  type Problem,
  type Provider,
  type SourceCheckRequest,
  type SourceOptions,
  type SourceSQL,
  type SourceTable,
  type TableRef,
} from "@/api/client";
import {
  useCheckSource,
  useCreateSource,
  useListSourceTables,
  useSetupSQL,
  useSourceOptions,
} from "@/api/queries";
import { Notice } from "@/components/Notice";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { formatRetention } from "@/lib/format";
import { cn } from "@/lib/utils";
import { CustomerArea } from "@/pages/CustomerArea";
import { CheckList } from "@/pages/sources/CheckList";
import { EgressAddresses } from "@/pages/sources/EgressAddresses";
import { SqlBlock } from "@/pages/sources/SqlBlock";
import { tableKey, tableName } from "@/lib/tables";
import { TablePicker } from "@/pages/sources/TablePicker";

/** The wizard's steps, in order. */
const steps = [
  "Connect",
  "Create the role",
  "Choose tables",
  "Grant and publish",
  "Verify",
  "Start",
] as const;

/** The checks a connection passes before Changeport's role exists. */
const connectionChecks = new Set(["reachable", "tls", "certificate"]);

const sourceName = /^[a-z]([a-z0-9-]*[a-z0-9])?$/;

/** The wizard that adds a Postgres source, for admins. */
export function NewSource() {
  return (
    <CustomerArea>
      {(_, admin) =>
        admin ? (
          <Options />
        ) : (
          <Notice title="Admins only">
            Ask an admin of your organization to add a Postgres source.
          </Notice>
        )
      }
    </CustomerArea>
  );
}

function Options() {
  const options = useSourceOptions();
  if (options.isPending) {
    return <Notice title="Loading" busy />;
  }
  if (options.isError) {
    return <Notice title="The console is unavailable">{errorText(options.error)}</Notice>;
  }
  return <Wizard options={options.data} />;
}

interface Draft {
  name: string;
  provider: Provider;
  host: string;
  port: number;
  database: string;
  tlsMode: "verify-full" | "verify-ca";
  ca: string;
  /** The role's password: the one the console generated, or on Neon the customer's. */
  password: string;
  tables: TableRef[];
  retentionMs: number;
}

function Wizard({ options }: { options: SourceOptions }) {
  const navigate = useNavigate();
  const [step, setStep] = useState(0);
  const [draft, setDraft] = useState<Draft>({
    name: "",
    provider: "generic",
    host: "",
    port: 5432,
    database: "",
    tlsMode: "verify-full",
    ca: "",
    password: "",
    tables: [],
    retentionMs: options.defaultRetentionMs,
  });
  // The role's SQL, which chose the source's ID and password, for the name
  // and provider it was made for.
  const [role, setRole] = useState<{ sql: SourceSQL; for: string } | null>(null);
  const [grants, setGrants] = useState<SourceSQL | null>(null);
  const [report, setReport] = useState<CheckReport | null>(null);
  const [tables, setTables] = useState<SourceTable[] | null>(null);
  const [problem, setProblem] = useState<Problem | null>(null);

  const setupSQL = useSetupSQL();
  const check = useCheckSource();
  const listTables = useListSourceTables();
  const create = useCreateSource();
  const busy = setupSQL.isPending || check.isPending || listTables.isPending || create.isPending;
  const provider = options.providers.find((p) => p.id === draft.provider);
  const created = create.isSuccess;

  // Past the first step, the role's password exists only on this page.
  useEffect(() => {
    if (step === 0 || created) {
      return;
    }
    const warn = (event: BeforeUnloadEvent) => {
      event.preventDefault();
    };
    window.addEventListener("beforeunload", warn);
    return () => {
      window.removeEventListener("beforeunload", warn);
    };
  }, [step, created]);

  const request = (sql: SourceSQL, withTables: boolean): SourceCheckRequest => ({
    name: draft.name,
    id: sql.id,
    provider: draft.provider,
    connection: {
      host: draft.host.trim(),
      port: draft.port,
      database: draft.database,
      tlsMode: draft.tlsMode,
      ...(draft.ca.trim() === "" ? {} : { ca: draft.ca.trim() }),
      // Before the role exists, any password shows how far a connection gets.
      password: draft.password || "-",
    },
    ...(withTables ? { tables: draft.tables } : {}),
  });

  const fail = (error: unknown) => {
    setProblem(
      error instanceof ApiError
        ? (error.problem ?? { title: error.message, status: error.status })
        : { title: error instanceof Error ? error.message : String(error), status: 0 },
    );
  };
  // Each step's action: what it asks of the console before the next step.
  const next = async () => {
    setProblem(null);
    try {
      switch (step) {
        case 0: {
          const key = `${draft.name}\n${draft.provider}`;
          let sql = role?.for === key ? role.sql : null;
          if (!sql) {
            sql = await setupSQL.mutateAsync({
              step: "role",
              name: draft.name,
              provider: draft.provider,
            });
            setRole({ sql, for: key });
            setGrants(null);
            setDraft((d) => ({ ...d, password: sql?.password ?? "" }));
          }
          // The draft has the new role's password only from the next render.
          const connect = request(sql, false);
          const result = await check.mutateAsync({
            ...connect,
            connection: {
              ...connect.connection,
              password: sql.password ?? connect.connection.password,
            },
          });
          const connection = {
            ok: result.results.every(
              (r) => !connectionChecks.has(r.check) || r.status === "pass" || r.status === "warn",
            ),
            results: result.results.filter((r) => connectionChecks.has(r.check)),
          };
          setReport(connection);
          if (connection.ok) {
            setReport(null);
            setStep(1);
          }
          return;
        }
        case 1: {
          if (!role) return;
          const result = await check.mutateAsync(request(role.sql, false));
          setReport(result);
          if (result.ok) {
            setReport(null);
            setStep(2);
            setTables(await listTables.mutateAsync(request(role.sql, false)));
          }
          return;
        }
        case 2: {
          if (!role) return;
          const sql = await setupSQL.mutateAsync({
            step: "tables",
            name: draft.name,
            provider: draft.provider,
            id: role.sql.id,
            tables: draft.tables,
          });
          setGrants(sql);
          setStep(3);
          return;
        }
        case 3: {
          if (!role) return;
          setStep(4);
          setReport(null);
          setReport(await check.mutateAsync(request(role.sql, true)));
          return;
        }
        case 4:
          setStep(5);
          return;
        case 5: {
          if (!role) return;
          const r = request(role.sql, true);
          const source = await create.mutateAsync({
            name: draft.name,
            id: role.sql.id,
            provider: draft.provider,
            connection: { ...r.connection, password: draft.password },
            tables: draft.tables,
            retentionMs: draft.retentionMs,
          });
          void navigate(`/sources/${source.name}`);
          return;
        }
      }
    } catch (error) {
      fail(error);
    }
  };

  const fieldProblem = (field: string) =>
    problem?.field === field ? (problem.detail ?? problem.title) : null;
  const connectProblems = {
    name:
      draft.name === ""
        ? null
        : draft.name.length > options.maxNameLength
          ? `Use at most ${String(options.maxNameLength)} characters.`
          : sourceName.test(draft.name)
            ? null
            : "Use lowercase letters, digits and dashes, starting with a letter and ending with a letter or digit.",
    port:
      Number.isInteger(draft.port) && draft.port >= 1 && draft.port <= 65535
        ? null
        : "Choose a port from 1 to 65535.",
    ca:
      draft.tlsMode === "verify-ca" && draft.ca.trim() === ""
        ? "verify-ca checks the certificate against this CA, so it needs one."
        : null,
  };
  const canContinue = [
    draft.name !== "" &&
      draft.host.trim() !== "" &&
      draft.database !== "" &&
      Object.values(connectProblems).every((p) => p === null),
    draft.password !== "",
    draft.tables.length > 0,
    true,
    report?.ok === true,
    true,
  ][step];
  const first = draft.tables[0];
  const exampleTopic =
    (first && tables?.find((t) => tableKey(t) === tableKey(first))?.topic) ?? `${draft.name}.…`;
  const generalProblem = problem && !problem.field ? (problem.detail ?? problem.title) : null;

  return (
    <main className="mx-auto flex max-w-5xl flex-col gap-6 p-6 [&>*]:max-w-3xl">
      <Link
        to="/sources"
        className="text-sm text-muted-foreground underline-offset-4 hover:underline"
      >
        ← Sources
      </Link>
      <header className="flex flex-col gap-3">
        <h1 className="text-2xl font-semibold tracking-tight">Add a Postgres source</h1>
        <p className="text-sm text-muted-foreground sm:hidden">
          Step {step + 1} of {steps.length}: {steps[step]}
        </p>
        <ol aria-label="Steps" className="flex gap-2 text-sm max-sm:hidden">
          {steps.map((name, i) => (
            <li
              key={name}
              aria-current={i === step ? "step" : undefined}
              className={cn(
                "flex items-center gap-2",
                i === step ? "font-medium text-foreground" : "text-muted-foreground",
              )}
            >
              <span
                className={cn(
                  "flex size-5 items-center justify-center rounded-full border text-xs tabular-nums",
                  i < step && "border-primary bg-primary text-primary-foreground",
                  i === step && "border-foreground",
                )}
              >
                {i + 1}
              </span>
              {name}
              {i < steps.length - 1 && (
                <span aria-hidden className="text-muted-foreground/60">
                  ›
                </span>
              )}
            </li>
          ))}
        </ol>
      </header>

      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (canContinue && !busy) {
            void next();
          }
        }}
        noValidate
      >
        <Card>
          {step === 0 && (
            <StepCard
              title="Connect"
              description="Where Changeport finds your database. It always connects over TLS, and checks the database's certificate."
            >
              <EgressAddresses addresses={options.egressAddresses} />
              <Field
                id="source-name"
                label="Source name"
                hint={`Its tables' topics start with it, such as ${draft.name || "shop"}.public.orders.`}
                problem={connectProblems.name ?? fieldProblem("name")}
              >
                <Input
                  id="source-name"
                  autoComplete="off"
                  value={draft.name}
                  onChange={(event) => {
                    setDraft({ ...draft, name: event.target.value });
                  }}
                />
              </Field>
              <Field id="source-provider" label="Where it runs" problem={fieldProblem("provider")}>
                <Select
                  value={draft.provider}
                  onValueChange={(value) => {
                    setDraft({ ...draft, provider: value as Provider });
                  }}
                >
                  <SelectTrigger id="source-provider" className="w-full sm:w-72">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {options.providers.map((p) => (
                      <SelectItem key={p.id} value={p.id}>
                        {p.title}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </Field>
              <div className="grid gap-5 sm:grid-cols-[1fr_8rem]">
                <Field id="source-host" label="Host" problem={fieldProblem("connection")}>
                  <Input
                    id="source-host"
                    autoComplete="off"
                    placeholder="db.example.com"
                    value={draft.host}
                    onChange={(event) => {
                      setDraft({ ...draft, host: event.target.value });
                    }}
                  />
                </Field>
                <Field id="source-port" label="Port" problem={connectProblems.port}>
                  <Input
                    id="source-port"
                    type="number"
                    inputMode="numeric"
                    min={1}
                    max={65535}
                    value={Number.isNaN(draft.port) ? "" : draft.port}
                    onChange={(event) => {
                      setDraft({ ...draft, port: event.target.valueAsNumber });
                    }}
                  />
                </Field>
              </div>
              <Field id="source-database" label="Database" problem={null}>
                <Input
                  id="source-database"
                  autoComplete="off"
                  value={draft.database}
                  onChange={(event) => {
                    setDraft({ ...draft, database: event.target.value });
                  }}
                />
              </Field>
              <fieldset className="flex flex-col gap-2">
                <legend className="mb-2 text-sm font-medium">Certificate check</legend>
                <RadioGroup
                  value={draft.tlsMode}
                  onValueChange={(value) => {
                    setDraft({ ...draft, tlsMode: value as Draft["tlsMode"] });
                  }}
                >
                  <Choice name="tls" value="verify-full" title="Authority and host (verify-full)">
                    A trusted authority signed the certificate, for this host.
                  </Choice>
                  <Choice name="tls" value="verify-ca" title="Authority only (verify-ca)">
                    The CA below signed the certificate, whatever host it names.
                  </Choice>
                </RadioGroup>
              </fieldset>
              <Field
                id="source-ca"
                label={
                  draft.tlsMode === "verify-ca" ? "CA certificate" : "CA certificate (optional)"
                }
                hint="PEM. Leave it empty when a public authority signs the database's certificate."
                problem={connectProblems.ca}
              >
                <Textarea
                  id="source-ca"
                  spellCheck={false}
                  placeholder="-----BEGIN CERTIFICATE-----"
                  className="max-h-48 font-mono text-xs md:text-xs"
                  value={draft.ca}
                  onChange={(event) => {
                    setDraft({ ...draft, ca: event.target.value });
                  }}
                />
              </Field>
            </StepCard>
          )}

          {step === 1 && role && (
            <StepCard
              title="Create the role"
              description={`Changeport logs in as ${role.sql.role}, a role that may replicate and nothing else yet. Run this as an administrator of ${draft.database}.`}
            >
              <EgressAddresses addresses={options.egressAddresses} />
              <SqlBlock sql={role.sql} label="SQL" />
              {provider?.passwordFromCustomer ? (
                <Field
                  id="source-password"
                  label={`${role.sql.role}'s password`}
                  hint={`The password ${provider.title} showed when it created the role.`}
                  problem={null}
                >
                  <Input
                    id="source-password"
                    type="password"
                    autoComplete="new-password"
                    value={draft.password}
                    onChange={(event) => {
                      setDraft({ ...draft, password: event.target.value });
                    }}
                  />
                </Field>
              ) : (
                <p className="text-sm text-muted-foreground">
                  The SQL holds the role&apos;s password, which Changeport chose and shows only
                  here. Changeport keeps it where nobody can read it.
                </p>
              )}
            </StepCard>
          )}

          {step === 2 && (
            <StepCard
              title="Choose tables"
              description="Each table streams into a topic of its own. Tables need a primary key of integer, numeric, text, uuid or date columns."
            >
              {listTables.isPending && (
                <p className="text-sm text-muted-foreground">Listing the tables</p>
              )}
              {tables && (
                <TablePicker
                  tables={tables}
                  selected={draft.tables}
                  max={options.maxTables}
                  onChange={(chosen) => {
                    setDraft({ ...draft, tables: chosen });
                  }}
                />
              )}
              {!tables && !listTables.isPending && role && (
                <Button
                  type="button"
                  variant="outline"
                  className="self-start"
                  onClick={() => {
                    setProblem(null);
                    listTables.mutateAsync(request(role.sql, false)).then(setTables, fail);
                  }}
                >
                  List the tables again
                </Button>
              )}
              <Field
                id="source-retention"
                label="Topics keep messages for"
                problem={fieldProblem("retentionMs")}
              >
                <Select
                  value={String(draft.retentionMs)}
                  onValueChange={(value) => {
                    setDraft({ ...draft, retentionMs: Number(value) });
                  }}
                >
                  <SelectTrigger id="source-retention" className="w-40">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {options.retentionPresets.map((preset) => (
                      <SelectItem key={preset.ms} value={String(preset.ms)}>
                        {preset.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </Field>
            </StepCard>
          )}

          {step === 3 && grants && (
            <StepCard
              title="Grant and publish"
              description={`Lets ${grants.role} read the tables you chose, and publishes their changes. Run this as the owner of the tables.`}
            >
              <SqlBlock sql={grants} label="SQL" />
            </StepCard>
          )}

          {step === 4 && (
            <StepCard
              title="Verify"
              description="Changeport checks the database, the role and every table, as the source will use them."
            >
              {check.isPending && (
                <p className="text-sm text-muted-foreground" role="status">
                  Checking the database
                </p>
              )}
              {report && !check.isPending && (
                <>
                  <CheckList report={report} label="Checks" />
                  <Button
                    type="button"
                    variant="outline"
                    className="self-start"
                    onClick={() => {
                      if (role) {
                        setProblem(null);
                        check.mutateAsync(request(role.sql, true)).then(setReport, fail);
                      }
                    }}
                  >
                    Check again
                  </Button>
                </>
              )}
            </StepCard>
          )}

          {step === 5 && role && (
            <StepCard
              title="Start"
              description="The source copies each table's existing rows, then streams every change as it happens."
            >
              <dl className="grid grid-cols-1 gap-y-1 text-sm sm:grid-cols-[max-content_1fr] sm:gap-x-6 sm:gap-y-2 [&>dd]:min-w-0 [&>dd]:break-words max-sm:[&>dd]:mb-2">
                <dt className="text-muted-foreground">Source</dt>
                <dd className="font-medium">{draft.name}</dd>
                <dt className="text-muted-foreground">Database</dt>
                <dd>
                  {draft.database} on {draft.host.trim()}:{draft.port}
                </dd>
                <dt className="text-muted-foreground">Role</dt>
                <dd>
                  <code>{role.sql.role}</code>
                </dd>
                <dt className="text-muted-foreground">Tables</dt>
                <dd>{draft.tables.map(tableName).join(", ")}</dd>
                <dt className="text-muted-foreground">Topics</dt>
                <dd>
                  One per table, such as <code className="break-all">{exampleTopic}</code>, keeping
                  messages for {formatRetention(draft.retentionMs).toLowerCase()}
                </dd>
              </dl>
            </StepCard>
          )}

          {/* Every step's problems, from the console or the checks. */}
          {(report && !report.ok && step < 4) ||
          generalProblem ||
          stepFieldProblem(problem, step) ? (
            <CardContent className="flex flex-col gap-4 pt-0">
              {report && !report.ok && step < 4 && (
                <CheckList
                  report={report}
                  label={step === 0 ? "Connection checks" : "Server checks"}
                />
              )}
              {(generalProblem ?? stepFieldProblem(problem, step)) && (
                <p role="alert" className="text-sm text-destructive">
                  {generalProblem ?? stepFieldProblem(problem, step)}
                </p>
              )}
            </CardContent>
          ) : null}

          <CardContent className="flex flex-wrap items-center justify-between gap-3 border-t pt-4">
            {step === 0 ? (
              <Button type="button" variant="outline" asChild>
                <Link to="/sources">Cancel</Link>
              </Button>
            ) : (
              <Button
                type="button"
                variant="outline"
                disabled={busy}
                onClick={() => {
                  setProblem(null);
                  setReport(null);
                  setStep(step - 1);
                }}
              >
                Back
              </Button>
            )}
            <Button type="submit" disabled={!canContinue || busy}>
              {busy ? busyLabel(step) : nextLabel(step)}
            </Button>
          </CardContent>
        </Card>
      </form>
    </main>
  );
}

/** A field problem the current step has no field for, shown under it. */
function stepFieldProblem(problem: Problem | null, step: number): string | null {
  if (!problem?.field) {
    return null;
  }
  const shown: Record<number, string[]> = {
    0: ["name", "provider", "connection"],
    2: ["retentionMs"],
  };
  if (shown[step]?.includes(problem.field)) {
    return null;
  }
  return problem.detail ?? problem.title;
}

function nextLabel(step: number): string {
  return (
    ["Check the connection", "I ran it", "Continue", "I ran it", "Continue", "Start source"][
      step
    ] ?? "Continue"
  );
}

function busyLabel(step: number): string {
  return (
    ["Checking", "Checking", "Preparing", "Checking", "Checking", "Starting"][step] ?? "Working"
  );
}

function StepCard({
  title,
  description,
  children,
}: {
  title: string;
  description: string;
  children: ReactNode;
}) {
  return (
    <>
      <CardHeader>
        <CardTitle>
          <h2>{title}</h2>
        </CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-5">{children}</CardContent>
    </>
  );
}

function Field({
  id,
  label,
  hint,
  problem,
  children,
}: {
  id: string;
  label: string;
  hint?: string;
  problem: string | null;
  children: ReactNode;
}) {
  return (
    <div className="flex flex-col gap-2">
      <Label htmlFor={id}>{label}</Label>
      {children}
      {hint && !problem && <p className="text-sm text-muted-foreground">{hint}</p>}
      {problem && (
        <p id={`${id}-problem`} className="text-sm text-destructive">
          {problem}
        </p>
      )}
    </div>
  );
}

function Choice({
  name,
  value,
  title,
  children,
}: {
  name: string;
  value: string;
  title: string;
  children: ReactNode;
}) {
  const id = `${name}-${value}`;
  return (
    <div className="flex items-start gap-3">
      <RadioGroupItem value={value} id={id} className="mt-0.5" />
      <Label htmlFor={id} className="flex flex-col items-start gap-1 font-normal">
        <span className="font-medium">{title}</span>
        <span className="leading-snug text-muted-foreground">{children}</span>
      </Label>
    </div>
  );
}
