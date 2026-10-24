import { useState } from "react";
import { ApiError, type NewTopic, type TopicPolicy } from "@/api/client";
import { useCreateTopic, useTopicPolicy } from "@/api/queries";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
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
import { topicNameProblem } from "@/lib/topicName";

/** A button that opens the dialog to create a topic within the guardrails. */
export function CreateTopicDialog({ existing }: { existing: string[] }) {
  const policy = useTopicPolicy();
  const [open, setOpen] = useState(false);
  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button size="sm" disabled={!policy.data}>
          Create topic
        </Button>
      </DialogTrigger>
      <DialogContent>
        {policy.data && (
          // Mounted per opening, so that every opening starts afresh.
          <CreateTopicForm
            policy={policy.data}
            existing={existing}
            onCreated={() => {
              setOpen(false);
            }}
          />
        )}
      </DialogContent>
    </Dialog>
  );
}

function CreateTopicForm({
  policy,
  existing,
  onCreated,
}: {
  policy: TopicPolicy;
  existing: string[];
  onCreated: () => void;
}) {
  const create = useCreateTopic();
  const [topic, setTopic] = useState<NewTopic>({
    name: "",
    partitions: policy.partitions.default,
    retentionMs: policy.defaultRetentionMs,
    cleanupPolicy: "delete",
  });
  const [touched, setTouched] = useState(false);
  const nameProblem = topicNameProblem(topic.name, policy, existing);
  const partitionsProblem =
    Number.isInteger(topic.partitions) &&
    topic.partitions >= policy.partitions.min &&
    topic.partitions <= policy.partitions.max
      ? null
      : `Choose ${String(policy.partitions.min)} to ${String(policy.partitions.max)} partitions.`;
  // The server has the last word; its problem is shown by the field it is about.
  const serverProblem = create.error instanceof ApiError ? create.error.problem : undefined;
  const fieldProblem = (field: string) =>
    serverProblem?.field === field ? (serverProblem.detail ?? serverProblem.title) : null;

  const submit = (event: { preventDefault(): void }) => {
    event.preventDefault();
    setTouched(true);
    if (nameProblem || partitionsProblem) {
      return;
    }
    create.mutate(topic, { onSuccess: onCreated });
  };

  return (
    <form onSubmit={submit} className="flex flex-col gap-5" noValidate>
      <DialogHeader>
        <DialogTitle>Create a topic</DialogTitle>
        <DialogDescription>
          Your cluster&apos;s plan keeps{" "}
          {policy.replicationFactor === 1
            ? "one copy"
            : `${String(policy.replicationFactor)} copies`}{" "}
          of every partition.
        </DialogDescription>
      </DialogHeader>

      <Field
        id="topic-name"
        label="Name"
        problem={(touched || topic.name !== "" ? nameProblem : null) ?? fieldProblem("name")}
      >
        <Input
          id="topic-name"
          autoComplete="off"
          value={topic.name}
          onChange={(event) => {
            setTopic({ ...topic, name: event.target.value });
          }}
        />
      </Field>

      <Field
        id="topic-partitions"
        label="Partitions"
        problem={partitionsProblem ?? fieldProblem("partitions")}
      >
        <Input
          id="topic-partitions"
          type="number"
          inputMode="numeric"
          min={policy.partitions.min}
          max={policy.partitions.max}
          value={Number.isNaN(topic.partitions) ? "" : topic.partitions}
          onChange={(event) => {
            setTopic({ ...topic, partitions: event.target.valueAsNumber });
          }}
          className="w-28"
        />
      </Field>

      <Field id="topic-retention" label="Retention" problem={fieldProblem("retention")}>
        <Select
          value={String(topic.retentionMs)}
          onValueChange={(value) => {
            setTopic({ ...topic, retentionMs: Number(value) });
          }}
        >
          <SelectTrigger id="topic-retention" className="w-40">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {policy.retentionPresets.map((preset) => (
              <SelectItem key={preset.ms} value={String(preset.ms)}>
                {preset.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </Field>

      <fieldset className="flex flex-col gap-2">
        <legend className="mb-2 text-sm font-medium">Old messages</legend>
        <RadioGroup
          value={topic.cleanupPolicy}
          onValueChange={(value) => {
            setTopic({ ...topic, cleanupPolicy: value as NewTopic["cleanupPolicy"] });
          }}
        >
          <Choice value="delete" title="Delete after the retention">
            Tiered: the brokers keep recent messages and offload the rest to object storage.
          </Choice>
          <Choice value="compact" title="Keep the latest message per key">
            Compacted, for tables such as a customer&apos;s current address. Not tiered: tiered
            storage does not support compaction.
          </Choice>
        </RadioGroup>
      </fieldset>

      {serverProblem && !serverProblem.field && (
        <p role="alert" className="text-sm text-destructive">
          {serverProblem.detail ?? serverProblem.title}
        </p>
      )}
      {create.error && !(create.error instanceof ApiError) && (
        <p role="alert" className="text-sm text-destructive">
          {create.error.message}
        </p>
      )}
      <DialogFooter>
        <DialogClose asChild>
          <Button type="button" variant="outline">
            Cancel
          </Button>
        </DialogClose>
        <Button type="submit" disabled={create.isPending}>
          {create.isPending ? "Creating" : "Create topic"}
        </Button>
      </DialogFooter>
    </form>
  );
}

function Field({
  id,
  label,
  problem,
  children,
}: {
  id: string;
  label: string;
  problem: string | null;
  children: React.ReactNode;
}) {
  return (
    <div className="flex flex-col gap-2">
      <Label htmlFor={id}>{label}</Label>
      {children}
      {problem && (
        <p id={`${id}-problem`} className="text-sm text-destructive">
          {problem}
        </p>
      )}
    </div>
  );
}

function Choice({
  value,
  title,
  children,
}: {
  value: string;
  title: string;
  children: React.ReactNode;
}) {
  const id = `cleanup-${value}`;
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
