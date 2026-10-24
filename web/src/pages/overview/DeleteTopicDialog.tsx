import { Trash2 } from "lucide-react";
import { useState } from "react";
import { useDeleteTopic, useTopicConsumerGroups } from "@/api/queries";
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

/**
 * A button that opens the dialog to delete a topic: it says which consumer
 * groups still read it, and asks for the topic's name, since its messages go
 * for good.
 */
export function DeleteTopicDialog({ topic }: { topic: string }) {
  const [open, setOpen] = useState(false);
  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="ghost" size="icon" aria-label={`Delete ${topic}`}>
          <Trash2 />
        </Button>
      </DialogTrigger>
      <DialogContent>
        {open && (
          <DeleteTopicForm
            topic={topic}
            onDeleted={() => {
              setOpen(false);
            }}
          />
        )}
      </DialogContent>
    </Dialog>
  );
}

function DeleteTopicForm({ topic, onDeleted }: { topic: string; onDeleted: () => void }) {
  const groups = useTopicConsumerGroups(topic, true);
  const remove = useDeleteTopic();
  const [typed, setTyped] = useState("");
  const submit = (event: { preventDefault(): void }) => {
    event.preventDefault();
    if (typed === topic) {
      remove.mutate(topic, { onSuccess: onDeleted });
    }
  };
  return (
    <form onSubmit={submit} className="flex flex-col gap-5">
      <DialogHeader>
        <DialogTitle>Delete {topic}?</DialogTitle>
        <DialogDescription>
          Its messages are deleted for good, from the brokers and from tiered storage.
        </DialogDescription>
      </DialogHeader>
      <div role="status" className="text-sm">
        {groups.isPending && <span className="text-muted-foreground">Checking who reads it</span>}
        {groups.isError && <span className="text-destructive">{groups.error.message}</span>}
        {groups.data?.length === 0 && <span>No consumer group reads it.</span>}
        {groups.data && groups.data.length > 0 && (
          <span className="text-destructive">
            {groups.data.length === 1
              ? "1 consumer group still reads it: "
              : `${String(groups.data.length)} consumer groups still read it: `}
            {groups.data.join(", ")}.
          </span>
        )}
      </div>
      <div className="flex flex-col gap-2">
        <Label htmlFor="confirm-topic">
          Type <span className="font-mono">{topic}</span> to confirm
        </Label>
        <Input
          id="confirm-topic"
          autoComplete="off"
          value={typed}
          onChange={(event) => {
            setTyped(event.target.value);
          }}
        />
      </div>
      {remove.error && (
        <p role="alert" className="text-sm text-destructive">
          {remove.error.message}
        </p>
      )}
      <DialogFooter>
        <DialogClose asChild>
          <Button type="button" variant="outline">
            Cancel
          </Button>
        </DialogClose>
        <Button type="submit" variant="destructive" disabled={typed !== topic || remove.isPending}>
          {remove.isPending ? "Deleting" : "Delete topic"}
        </Button>
      </DialogFooter>
    </form>
  );
}
