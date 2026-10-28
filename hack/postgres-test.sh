#!/usr/bin/env bash
# Tests the Postgres on kind that stands in for a customer's database for the
# Postgres CDC source (deploy/base/source-postgres): its tables are seeded,
# logical replication is on, and it takes clients over TLS only, with a
# certificate that verify-full accepts for its name and refuses for its
# address. A role made for the test opens a replication connection the way
# Debezium does (IDENTIFY_SYSTEM).
# Usage: hack/postgres-test.sh
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

namespace=source-postgres
host=postgres.$namespace.svc
role="changeport_check_$RANDOM"
password="check-$RANDOM$RANDOM"

ensure_kubectl
kubectl -n "$namespace" rollout status deployment/postgres --timeout=3m >/dev/null
# Runs SQL as the database's owner over its local socket.
admin() { kubectl -n "$namespace" exec deploy/postgres -- psql -U postgres -d app -v ON_ERROR_STOP=1 -tAq -c "$1"; }
# Connects to the database the way a client outside it does, over TCP and TLS,
# and prints what psql prints, errors included.
client() {
  kubectl -n "$namespace" exec deploy/postgres -- env PGPASSWORD="$password" PGCONNECT_TIMEOUT=10 \
    psql "$1 port=5432 dbname=app user=$role sslrootcert=/etc/postgres/tls/ca.crt" -tAq -c "$2" 2>&1 || true
}
trap 'admin "DROP ROLE IF EXISTS $role" >/dev/null 2>&1 || true' EXIT

log "Postgres test: the seed and logical replication"
[[ $(admin "SHOW wal_level") == logical ]] || die "wal_level is not logical"
counts=$(admin "SELECT (SELECT count(*) FROM customers) || ' ' || (SELECT count(*) FROM orders) || ' ' || (SELECT count(*) FROM notes) || ' ' || (SELECT count(*) FROM events)")
[[ $counts == "100 1000 10 400" ]] || die "the seed has $counts rows in customers, orders, notes and events, want 100 1000 10 400"

admin "CREATE ROLE $role LOGIN REPLICATION PASSWORD '$password'" >/dev/null
log "A replication connection over TLS, verifying the certificate and its name"
identify=$(client "host=$host sslmode=verify-full replication=database" "IDENTIFY_SYSTEM")
grep -qE '^[0-9]+\|[0-9]+\|[0-9A-F]+/[0-9A-F]+\|app$' <<<"$identify" ||
  die "IDENTIFY_SYSTEM over verify-full did not answer:"$'\n'"$identify"

log "Refused: a client without TLS, and verify-full by address instead of name"
plaintext=$(client "host=$host sslmode=disable" "SELECT 1")
grep -q "no encryption" <<<"$plaintext" || die "a connection without TLS was not refused:"$'\n'"$plaintext"
address=$(kubectl -n "$namespace" get service postgres -o jsonpath='{.spec.clusterIP}')
by_address=$(client "host=$address sslmode=verify-full" "SELECT 1")
grep -q "does not match host name" <<<"$by_address" ||
  die "verify-full accepted the certificate for $address:"$'\n'"$by_address"
log "Postgres test passed: $host replicates over TLS only, with a certificate for its name"
