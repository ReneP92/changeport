# Runbook

Procedures for operators of the Changeport platform. Every command runs
against the platform's Kubernetes cluster with the pinned kubectl and the
provisioner CLI (`go run ./cmd/provisioner`), as an operator with cluster
access.

## Deletion protection

A customer with deletion protection (`spec.deletionProtection` on its
Customer) cannot be deleted by anyone: the API server's admission policies
(`deploy/base/provisioner/deletion-protection.yaml`) refuse deleting the
Customer and deleting its namespace, which carries the
`changeport.io/deletion-protection` label, and the provisioner CLI and the
console refuse deprovisioning it. New production customers are protected by
default, from the console and from `provisioner provision`.

Protection is turned on and off with the provisioner CLI only; the console
can create and delete customers but not change them. Turning it off takes
the customer ID typed again:

```bash
provisioner protect -customer c-7f3k2q
provisioner unprotect -customer c-7f3k2q -confirm c-7f3k2q
```

Turning protection off takes the label off the customer's namespace once
the provisioning controller has reconciled the Customer, within seconds.

## Deprovisioning a customer

Operators deprovision a customer in the operator console, typing its ID, or
with the CLI:

```bash
provisioner deprovision -customer c-7f3k2q -wait
```

The provisioning controller then deletes the customer's namespace, with its
Kafka cluster, and on EKS its dedicated nodes. The customer's Clerk
organization is kept, no longer linked to a cluster. The customer's data is
not deleted at once:

- **Volumes.** The production storage class retains the brokers' EBS volumes
  when their claims go. Before deleting the namespace, the controller labels
  each volume of the customer's claims with `changeport.io/tenant=<id>` and
  annotates it with `changeport.io/deprovisioned-at`, the time of the
  deprovisioning. Other environments delete volumes with the namespace.
- **Tiered storage.** Topic history in S3, under the customer's prefix
  `customer-<id>/` in the platform's tiered storage bucket, stays.

The data is kept for 30 days, so that a customer deprovisioned by mistake can
be restored, and then purged. Do not reuse a deprovisioned customer's ID
before the purge: a new cluster of the same ID would share its prefix.

## Purging a deprovisioned customer's data

After 30 days, purge the customer's data. Check first that the customer is
gone and has been for 30 days:

```bash
kubectl get customer c-7f3k2q   # NotFound
kubectl get pv -l changeport.io/tenant=c-7f3k2q \
  -o custom-columns='NAME:.metadata.name,DEPROVISIONED:.metadata.annotations.changeport\.io/deprovisioned-at,VOLUME:.spec.csi.volumeHandle,PHASE:.status.phase'
```

On EKS, delete each retained volume in AWS, then its PersistentVolume, and
the customer's tiered storage prefix (the bucket is
`changeport-<environment>-tiered-storage`):

```bash
aws ec2 delete-volume --volume-id <VOLUME>
kubectl delete pv <NAME>
aws s3 rm s3://changeport-prod-tiered-storage/customer-c-7f3k2q/ --recursive
```

On kind, the volumes went with the namespace, and the object store is
SeaweedFS; the deletion protection test (`make deletion-protection-test`)
runs this purge:

```bash
kubectl -n object-store exec deploy/seaweedfs -- \
  sh -c "echo 'fs.rm -r /buckets/changeport-tiered-storage/customer-c-7f3k2q' | weed shell"
```
