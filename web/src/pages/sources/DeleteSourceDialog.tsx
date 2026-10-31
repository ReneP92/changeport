import { useEffect, useState } from "react";
import { errorText, type Source } from "@/api/client";
import { useDeleteSource, useSourceSQL } from "@/api/queries";
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
import { wideDialog } from "@/pages/sources/AddTablesDialog";
import { CodeBlock } from "@/pages/sources/SqlBlock";

/**
 * A button that opens the dialog to delete a source, with its name typed.
 * It shows the SQL that removes Changeport's role and publication from the
 * database afterwards, as the source is gone by then.
 */
export function DeleteSourceDialog({ source }: { source: Source }) {
  const [open, setOpen] = useState(false);
  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="destructive">Delete source</Button>
      </DialogTrigger>
      <DialogContent className={wideDialog}>
        {open && (
          <DeleteSource
            source={source}
            onDeleted={() => {
              setOpen(false);
            }}
          />
        )}
      </DialogContent>
    </Dialog>
  );
}

function DeleteSource({ source, onDeleted }: { source: Source; onDeleted: () => void }) {
  const cleanup = useSourceSQL(source.name);
  const remove = useDeleteSource(source.name);
  const [typed, setTyped] = useState("");
  const { mutate } = cleanup;
  useEffect(() => {
    mutate({ step: "cleanup" });
  }, [mutate]);
  return (
    <form
      className="flex flex-col gap-5"
      onSubmit={(event) => {
        event.preventDefault();
        if (typed === source.name) {
          remove.mutate(undefined, { onSuccess: onDeleted });
        }
      }}
    >
      <DialogHeader>
        <DialogTitle>Delete {source.name}?</DialogTitle>
        <DialogDescription>
          The source stops, and Changeport drops its replication slot. Its topics stay, as ordinary
          topics.
        </DialogDescription>
      </DialogHeader>
      {cleanup.data && cleanup.data.sql.trim() !== "" && (
        <div className="flex flex-col gap-2 text-sm">
          <p>Once it is gone, remove Changeport&apos;s role and publication with this SQL:</p>
          <CodeBlock code={cleanup.data.sql} label="SQL" />
        </div>
      )}
      <div className="flex flex-col gap-2">
        <Label htmlFor="confirm-source">
          Type <span className="font-mono">{source.name}</span> to confirm
        </Label>
        <Input
          id="confirm-source"
          autoComplete="off"
          value={typed}
          onChange={(event) => {
            setTyped(event.target.value);
          }}
        />
      </div>
      {remove.error && (
        <p role="alert" className="text-sm text-destructive">
          {errorText(remove.error)}
        </p>
      )}
      <DialogFooter>
        <DialogClose asChild>
          <Button type="button" variant="outline">
            Cancel
          </Button>
        </DialogClose>
        <Button
          type="submit"
          variant="destructive"
          disabled={typed !== source.name || remove.isPending}
        >
          {remove.isPending ? "Deleting" : "Delete source"}
        </Button>
      </DialogFooter>
    </form>
  );
}
