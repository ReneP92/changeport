import { useEffect, useState } from "react";
import { errorText, type Source } from "@/api/client";
import { useSetSourcePassword, useSourceSQL } from "@/api/queries";
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
import { SqlBlock } from "@/pages/sources/SqlBlock";

/**
 * A button that opens the dialog to give a source's role a new password:
 * Changeport chooses it, or on some providers the customer does, the
 * customer sets it in the database, and once it logs in the source uses it.
 */
export function PasswordDialog({
  source,
  passwordFromCustomer,
}: {
  source: Source;
  passwordFromCustomer: boolean;
}) {
  const [open, setOpen] = useState(false);
  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="outline">New password</Button>
      </DialogTrigger>
      <DialogContent className={wideDialog}>
        {open && (
          <NewPassword
            source={source}
            passwordFromCustomer={passwordFromCustomer}
            onChanged={() => {
              setOpen(false);
            }}
          />
        )}
      </DialogContent>
    </Dialog>
  );
}

function NewPassword({
  source,
  passwordFromCustomer,
  onChanged,
}: {
  source: Source;
  passwordFromCustomer: boolean;
  onChanged: () => void;
}) {
  const sql = useSourceSQL(source.name);
  const set = useSetSourcePassword(source.name);
  const [typed, setTyped] = useState("");
  const { mutate } = sql;
  // Each opening chooses a new password.
  useEffect(() => {
    mutate({ step: "password" });
  }, [mutate]);
  const password = sql.data?.password ?? typed;
  return (
    <form
      className="flex flex-col gap-5"
      onSubmit={(event) => {
        event.preventDefault();
        if (password !== "") {
          set.mutate(password, { onSuccess: onChanged });
        }
      }}
    >
      <DialogHeader>
        <DialogTitle>A new password for {source.role}</DialogTitle>
        <DialogDescription>
          {passwordFromCustomer
            ? "Reset the role's password, then enter it here. Once it logs in, the source uses it."
            : "Run this as an administrator of the database. Once the new password logs in, the source uses it; the old one stops working at once."}
        </DialogDescription>
      </DialogHeader>
      {sql.isPending && (
        <p role="status" className="text-sm text-muted-foreground">
          Preparing the SQL
        </p>
      )}
      {sql.error && (
        <p role="alert" className="text-sm text-destructive">
          {errorText(sql.error)}
        </p>
      )}
      {sql.data && <SqlBlock sql={sql.data} label="SQL" />}
      {sql.data && !sql.data.password && (
        <div className="flex flex-col gap-2">
          <Label htmlFor="new-source-password">New password</Label>
          <Input
            id="new-source-password"
            type="password"
            autoComplete="new-password"
            value={typed}
            onChange={(event) => {
              setTyped(event.target.value);
            }}
          />
        </div>
      )}
      {set.error && (
        <p role="alert" className="text-sm text-destructive">
          {errorText(set.error)}
        </p>
      )}
      <DialogFooter>
        <DialogClose asChild>
          <Button type="button" variant="outline">
            Cancel
          </Button>
        </DialogClose>
        <Button type="submit" disabled={!sql.data || password === "" || set.isPending}>
          {set.isPending ? "Checking" : sql.data?.password ? "I ran it" : "Use it"}
        </Button>
      </DialogFooter>
    </form>
  );
}
