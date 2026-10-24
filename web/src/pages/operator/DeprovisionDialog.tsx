import { useState } from "react";
import type { Customer } from "@/api/client";
import { useDeprovisionCustomer } from "@/api/queries";
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
 * A button that opens the dialog to deprovision a customer, which asks for
 * the customer ID typed. Protected customers cannot be deprovisioned.
 */
export function DeprovisionDialog({ customer }: { customer: Customer }) {
  const [open, setOpen] = useState(false);
  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="destructive" disabled={customer.deletionProtection}>
          Deprovision {customer.id}
        </Button>
      </DialogTrigger>
      <DialogContent>
        {open && (
          <DeprovisionForm
            customer={customer}
            onDone={() => {
              setOpen(false);
            }}
          />
        )}
      </DialogContent>
    </Dialog>
  );
}

function DeprovisionForm({ customer, onDone }: { customer: Customer; onDone: () => void }) {
  const deprovision = useDeprovisionCustomer();
  const [typed, setTyped] = useState("");
  const submit = (event: { preventDefault(): void }) => {
    event.preventDefault();
    if (typed === customer.id) {
      deprovision.mutate(customer.id, { onSuccess: onDone });
    }
  };
  return (
    <form onSubmit={submit} className="flex flex-col gap-5">
      <DialogHeader>
        <DialogTitle>Deprovision {customer.displayName}?</DialogTitle>
        <DialogDescription>
          Its Kafka cluster, topics and messages are deleted for good. Its Clerk organization is
          kept, no longer linked to a cluster.
        </DialogDescription>
      </DialogHeader>
      <div className="flex flex-col gap-2">
        <Label htmlFor="confirm-customer">
          Type <span className="font-mono">{customer.id}</span> to confirm
        </Label>
        <Input
          id="confirm-customer"
          autoComplete="off"
          value={typed}
          onChange={(event) => {
            setTyped(event.target.value);
          }}
        />
      </div>
      {deprovision.error && (
        <p role="alert" className="text-sm text-destructive">
          {deprovision.error.message}
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
          disabled={typed !== customer.id || deprovision.isPending}
        >
          {deprovision.isPending ? "Deprovisioning" : "Deprovision"}
        </Button>
      </DialogFooter>
    </form>
  );
}
