import { useState } from "react";
import { useNavigate } from "react-router";
import { ApiError, type NewCustomer } from "@/api/client";
import { useProvisionCustomer } from "@/api/queries";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
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

type Profile = NewCustomer["profile"];

/** The profiles, as operators choose among them. */
const profiles: { name: Profile; description: string }[] = [
  { name: "dev", description: "One broker, for trying Kafka out" },
  { name: "dev-ha", description: "Three brokers in three zones, small" },
  { name: "production", description: "Three brokers in three zones, sized for production" },
];

/**
 * A button that opens the dialog to provision a customer: a new Clerk
 * organization, an optional invitation, and the customer's cluster, whose
 * progress the customer's page then shows.
 */
export function ProvisionCustomerDialog() {
  const [open, setOpen] = useState(false);
  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button size="sm">Provision customer</Button>
      </DialogTrigger>
      <DialogContent>
        {/* Mounted per opening, so that every opening starts afresh. */}
        {open && <ProvisionCustomerForm />}
      </DialogContent>
    </Dialog>
  );
}

function ProvisionCustomerForm() {
  const provision = useProvisionCustomer();
  const navigate = useNavigate();
  const [customer, setCustomer] = useState<NewCustomer>({
    displayName: "",
    profile: "dev",
    deletionProtection: false,
    inviteEmail: "",
  });
  // Protection follows the profile, on for production, until chosen.
  const [protectionChosen, setProtectionChosen] = useState(false);
  const [touched, setTouched] = useState(false);
  const nameProblem =
    customer.displayName.trim() === ""
      ? "Name the customer."
      : customer.displayName.trim().length > 100
        ? "Use at most 100 characters."
        : null;
  const serverProblem = provision.error instanceof ApiError ? provision.error.problem : undefined;
  const fieldProblem = (field: string) =>
    serverProblem?.field === field ? (serverProblem.detail ?? serverProblem.title) : null;

  const submit = (event: { preventDefault(): void }) => {
    event.preventDefault();
    setTouched(true);
    if (nameProblem) {
      return;
    }
    const { inviteEmail, ...rest } = customer;
    const email = inviteEmail?.trim() ?? "";
    provision.mutate(email === "" ? rest : { ...rest, inviteEmail: email }, {
      onSuccess: (created) => {
        void navigate(`/operator/customers/${created.id}`);
      },
    });
  };

  return (
    <form onSubmit={submit} className="flex flex-col gap-5" noValidate>
      <DialogHeader>
        <DialogTitle>Provision a customer</DialogTitle>
        <DialogDescription>
          The customer gets a Clerk organization of the same name and a dedicated Kafka cluster,
          which takes a few minutes.
        </DialogDescription>
      </DialogHeader>

      <div className="flex flex-col gap-2">
        <Label htmlFor="customer-name">Name</Label>
        <Input
          id="customer-name"
          autoComplete="off"
          value={customer.displayName}
          onChange={(event) => {
            setCustomer({ ...customer, displayName: event.target.value });
          }}
        />
        <Problem
          text={
            (touched || customer.displayName !== "" ? nameProblem : null) ??
            fieldProblem("displayName")
          }
        />
      </div>

      <fieldset className="flex flex-col gap-2">
        <legend className="mb-2 text-sm font-medium">Profile</legend>
        <RadioGroup
          value={customer.profile}
          onValueChange={(value) => {
            const profile = value as Profile;
            setCustomer({
              ...customer,
              profile,
              deletionProtection: protectionChosen
                ? customer.deletionProtection
                : profile === "production",
            });
          }}
        >
          {profiles.map((profile) => (
            <div key={profile.name} className="flex items-start gap-3">
              <RadioGroupItem
                value={profile.name}
                id={`profile-${profile.name}`}
                className="mt-0.5"
              />
              <Label
                htmlFor={`profile-${profile.name}`}
                className="flex flex-col items-start gap-1 font-normal"
              >
                <span className="font-medium">{profile.name}</span>
                <span className="leading-snug text-muted-foreground">{profile.description}</span>
              </Label>
            </div>
          ))}
        </RadioGroup>
        <Problem text={fieldProblem("profile")} />
      </fieldset>

      <div className="flex items-start gap-3">
        <Checkbox
          id="customer-protection"
          className="mt-0.5"
          checked={customer.deletionProtection}
          onCheckedChange={(checked) => {
            setProtectionChosen(true);
            setCustomer({ ...customer, deletionProtection: checked === true });
          }}
        />
        <Label
          htmlFor="customer-protection"
          className="flex flex-col items-start gap-1 font-normal"
        >
          <span className="font-medium">Deletion protection</span>
          <span className="leading-snug text-muted-foreground">
            Deprovisioning is refused until it is turned off.
          </span>
        </Label>
      </div>

      <div className="flex flex-col gap-2">
        <Label htmlFor="customer-invite">Invite an admin (optional)</Label>
        <Input
          id="customer-invite"
          type="email"
          autoComplete="off"
          placeholder="ada@example.com"
          value={customer.inviteEmail ?? ""}
          onChange={(event) => {
            setCustomer({ ...customer, inviteEmail: event.target.value });
          }}
        />
        <Problem text={fieldProblem("inviteEmail")} />
      </div>

      {provision.error && !serverProblem?.field && (
        <p role="alert" className="text-sm text-destructive">
          {serverProblem?.detail ?? provision.error.message}
        </p>
      )}
      <DialogFooter>
        <DialogClose asChild>
          <Button type="button" variant="outline">
            Cancel
          </Button>
        </DialogClose>
        <Button type="submit" disabled={provision.isPending}>
          {provision.isPending ? "Provisioning" : "Provision"}
        </Button>
      </DialogFooter>
    </form>
  );
}

function Problem({ text }: { text: string | null }) {
  return text && <p className="text-sm text-destructive">{text}</p>;
}
