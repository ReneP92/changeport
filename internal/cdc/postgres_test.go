package cdc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The tests against real Postgres servers run in Docker when
// CHANGEPORT_POSTGRES_TESTS is 1, as hack/go-test.sh sets it: the oldest
// version Changeport supports, the newest, and one too old.
const (
	tooOld          = "postgres:13.23"
	oldestSupported = "postgres:14.24"
	newest          = "postgres:18.6"
)

func requirePostgresTests(t *testing.T) {
	t.Helper()
	if os.Getenv("CHANGEPORT_POSTGRES_TESTS") != "1" {
		t.Skip("set CHANGEPORT_POSTGRES_TESTS=1 to run the tests against Postgres in Docker, as hack/go-test.sh does")
	}
}

// pki is a CA and a server certificate it signed for localhost.
type pki struct {
	caPEM, certPEM, keyPEM string
}

func newPKI(t *testing.T) pki {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	server := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, server, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	encode := func(kind string, der []byte) string {
		return string(pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}))
	}
	return pki{caPEM: encode("CERTIFICATE", caDER), certPEM: encode("CERTIFICATE", certDER), keyPEM: encode("PRIVATE KEY", keyDER)}
}

// startPostgres runs a Postgres server in Docker with TLS and the given
// settings, with the database app, and returns its port on localhost once
// it takes connections. The key is written inside the container, as Postgres
// insists that its user owns it.
func startPostgres(t *testing.T, image string, certs pki, settings ...string) int {
	t.Helper()
	args := []string{"-c", "ssl=on", "-c", "ssl_cert_file=/tls/server.crt", "-c", "ssl_key_file=/tls/server.key"}
	for _, setting := range settings {
		args = append(args, "-c", setting)
	}
	script := `mkdir -p /tls && printf '%s' "$TLS_CERT" >/tls/server.crt && printf '%s' "$TLS_KEY" >/tls/server.key &&
chown -R postgres /tls && chmod 600 /tls/server.key && exec docker-entrypoint.sh postgres "$@"`
	run := append([]string{
		"run", "-d", "-p", "127.0.0.1::5432", "-e", "POSTGRES_PASSWORD=admin", "-e", "POSTGRES_DB=app",
		"-e", "TLS_CERT=" + certs.certPEM, "-e", "TLS_KEY=" + certs.keyPEM, "--entrypoint", "sh", image, "-c", script, "postgres",
	}, args...)
	// Only the container's ID goes to standard output; pulling the image
	// reports to standard error.
	var stderr strings.Builder
	start := exec.CommandContext(t.Context(), "docker", run...)
	start.Stderr = &stderr
	out, err := start.Output()
	if err != nil {
		t.Fatalf("starting %s: %v\n%s", image, err, stderr.String())
	}
	container := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", container).Run() })
	// Docker publishes the port a moment after the container starts.
	deadline := time.Now().Add(90 * time.Second)
	var portText string
	for {
		out, _ := exec.CommandContext(t.Context(), "docker", "port", container, "5432/tcp").Output()
		if _, portText, err = net.SplitHostPort(strings.TrimSpace(strings.Split(string(out), "\n")[0])); err == nil {
			break
		}
		if time.Now().After(deadline) {
			logs, _ := exec.Command("docker", "logs", container).CombinedOutput()
			t.Fatalf("%s published no port: %v\n%s", image, err, logs)
		}
		time.Sleep(200 * time.Millisecond)
	}
	port, _ := strconv.Atoi(portText)

	// The image's entrypoint runs a server for its setup first, on its
	// socket only, then the server for good.
	for {
		conn, err := adminConnect(t.Context(), port, certs)
		if err == nil {
			_ = conn.Close(t.Context())
			return port
		}
		if time.Now().After(deadline) {
			logs, _ := exec.Command("docker", "logs", container).CombinedOutput()
			t.Fatalf("%s did not take connections: %v\n%s", image, err, logs)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func adminConnection(port int, certs pki) Connection {
	return Connection{Host: "localhost", Port: port, Database: "app", User: "postgres", Password: "admin", TLSMode: VerifyFull, CA: certs.caPEM}
}

func adminConnect(ctx context.Context, port int, certs pki) (*pgx.Conn, error) {
	var trace connectTrace
	config, err := local.config(adminConnection(port, certs), false, &trace)
	if err != nil {
		return nil, err
	}
	return pgx.ConnectConfig(ctx, config)
}

// admin runs SQL on the database as its superuser, as a customer runs the
// SQL the console shows.
func admin(t *testing.T, port int, certs pki, sql string) {
	t.Helper()
	conn, err := adminConnect(t.Context(), port, certs)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(t.Context()) }()
	// The simple protocol runs several statements at once, as psql does.
	if _, err := conn.PgConn().Exec(t.Context(), sql).ReadAll(); err != nil {
		t.Fatalf("running\n%s\nfailed: %v", sql, err)
	}
}

// local checks databases on this machine.
var local = Checker{Guard: Guard{AllowPrivate: true}}

// statuses is the report's statuses by check, for comparing.
func statuses(report Report) map[CheckID]Status {
	result := map[CheckID]Status{}
	for _, r := range report.Results {
		result[r.Check] = r.Status
	}
	return result
}

// expect fails the test unless the report has the given statuses, and every
// check not given passed.
func expect(t *testing.T, step string, report Report, want map[CheckID]Status) {
	t.Helper()
	for _, result := range report.Results {
		status, ok := want[result.Check]
		if !ok {
			status = Pass
		}
		if result.Status != status {
			t.Errorf("%s: %s is %s, want %s: %s", step, result.Check, result.Status, status, result.Message)
		}
	}
	for check := range want {
		if _, ran := report.Result(check); !ran {
			t.Errorf("%s: no result for %s in %v", step, check, statuses(report))
		}
	}
}

func message(t *testing.T, report Report, check CheckID) string {
	t.Helper()
	result, ok := report.Result(check)
	if !ok {
		t.Fatalf("no result for %s", check)
	}
	return result.Message
}

const seed = `
CREATE SCHEMA "Sales";
CREATE TABLE public.orders (id bigint PRIMARY KEY, amount numeric(10, 2));
CREATE TABLE "Sales"."Order Lines" (id bigint PRIMARY KEY, sku text);
CREATE TABLE public.notes (body text);
CREATE TABLE public.codes (code text NOT NULL, label text);
CREATE UNIQUE INDEX codes_code ON public.codes (code);
ALTER TABLE public.codes REPLICA IDENTITY USING INDEX codes_code;
CREATE TABLE public.events (id bigint, happened_at date, PRIMARY KEY (id, happened_at)) PARTITION BY RANGE (happened_at);
CREATE TABLE public.events_2026 PARTITION OF public.events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE public.events_2027 PARTITION OF public.events FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
INSERT INTO public.orders SELECT g, g FROM generate_series(1, 1000) g;
INSERT INTO public.events SELECT g, date '2026-06-01' + (g % 400) FROM generate_series(1, 2000) g;
ANALYZE;
`

func TestAgainstPostgres(t *testing.T) {
	requirePostgresTests(t)
	certs := newPKI(t)
	for _, run := range []struct {
		image    string
		settings []string
		keepSize Status
	}{
		{oldestSupported, []string{"wal_level=logical"}, Warn},
		{newest, []string{"wal_level=logical", "max_slot_wal_keep_size=1GB"}, Pass},
	} {
		t.Run(run.image, func(t *testing.T) {
			t.Parallel()
			port := startPostgres(t, run.image, certs, run.settings...)
			admin(t, port, certs, seed)
			sourceLifecycle(t, port, certs, run.keepSize)
			certificates(t, port, certs)
		})
	}
	t.Run(tooOld, func(t *testing.T) {
		t.Parallel()
		port := startPostgres(t, tooOld, certs, "wal_level=logical")
		report := local.Check(t.Context(), Target{Provider: Generic, Connection: adminConnection(port, certs), Names: testNames(t)})
		expect(t, "PostgreSQL 13", report, map[CheckID]Status{
			Version: Fail, WALLevel: Skipped, ReplicationRole: Skipped, Slots: Skipped, WALSenders: Skipped,
			WALKeepSize: Skipped, ReplicationConnection: Skipped,
		})
		if got := message(t, report, Version); !strings.HasPrefix(got, "The server runs PostgreSQL 13.23; Changeport needs 14 or newer.") {
			t.Errorf("version message = %q", got)
		}
	})
	t.Run("wal_level=replica", func(t *testing.T) {
		t.Parallel()
		port := startPostgres(t, newest, certs, "max_slot_wal_keep_size=1GB")
		report := local.Check(t.Context(), Target{Provider: RDS, Connection: adminConnection(port, certs), Names: testNames(t)})
		expect(t, "wal_level=replica", report, map[CheckID]Status{WALLevel: Fail, ReplicationRole: Warn, ReplicationConnection: Skipped})
		if got := message(t, report, WALLevel); !strings.Contains(got, "wal_level is replica). Set rds.logical_replication = 1") {
			t.Errorf("wal_level message = %q, want RDS's remedy", got)
		}
	})
}

// sourceLifecycle takes a source through its life on a database, running the
// SQL the console generates as the customer would, and checking after each
// step.
func sourceLifecycle(t *testing.T, port int, certs pki, keepSize Status) {
	names, err := NamesFor("app", NewID())
	if err != nil {
		t.Fatal(err)
	}
	password := GeneratePassword()
	connection := Connection{Host: "localhost", Port: port, Database: "app", User: names.Role, Password: password, TLSMode: VerifyFull, CA: certs.caPEM}
	target := Target{Provider: Generic, Connection: connection, Names: names}

	// Before the first step, the role does not exist.
	expect(t, "before the role", local.Check(t.Context(), target), map[CheckID]Status{
		Authentication: Fail, Version: Skipped, WALLevel: Skipped, ReplicationRole: Skipped, Slots: Skipped,
		WALSenders: Skipped, WALKeepSize: Skipped, ReplicationConnection: Skipped,
	})

	role, err := RoleSQL(Generic, names, password)
	if err != nil {
		t.Fatal(err)
	}
	admin(t, port, certs, role.SQL)
	report := local.Check(t.Context(), target)
	expect(t, "after the role", report, map[CheckID]Status{WALKeepSize: keepSize})
	if got := message(t, report, ReplicationConnection); !strings.HasPrefix(got, "A replication connection identified system ") {
		t.Errorf("replication connection message = %q", got)
	}

	// The table picker lists the tables, without the partitions.
	tables, err := local.ListTables(t.Context(), connection)
	if err != nil {
		t.Fatal(err)
	}
	listed := map[Table]TableInfo{}
	for _, info := range tables {
		listed[info.Table] = info
	}
	orders, lines, notes, codes, events := Table{"public", "orders"}, Table{"Sales", "Order Lines"}, Table{"public", "notes"}, Table{"public", "codes"}, Table{"public", "events"}
	if len(tables) != 5 || !listed[orders].Keyed || !listed[lines].Keyed || !listed[codes].Keyed || listed[notes].Keyed ||
		!listed[events].Keyed || !listed[events].Partitioned || listed[orders].Readable {
		t.Errorf("ListTables() = %+v, want 5 tables, all but notes keyed, events partitioned, none readable yet", tables)
	}
	if listed[orders].EstimatedRows != 1000 || listed[events].EstimatedRows != 2000 {
		t.Errorf("estimated rows of orders and events = %d and %d, want 1000 and 2000", listed[orders].EstimatedRows, listed[events].EstimatedRows)
	}
	if !strings.HasPrefix(listed[notes].Reason, "It has no primary key.") {
		t.Errorf("notes's reason = %q", listed[notes].Reason)
	}

	// Choosing tables before the second step.
	target.Tables = []Table{orders, lines, events}
	expect(t, "before the tables' SQL", local.Check(t.Context(), target), map[CheckID]Status{
		WALKeepSize: keepSize, ReadAccess: Fail, Publication: Fail, Published: Skipped,
	})
	withMissing := target
	withMissing.Tables = []Table{orders, {"public", "events_2026"}, notes}
	report = local.Check(t.Context(), withMissing)
	expect(t, "a partition and a table without a key", report, map[CheckID]Status{
		WALKeepSize: keepSize, Tables: Fail, Keys: Skipped, ReadAccess: Skipped, Publication: Skipped, Published: Skipped,
	})
	if got := message(t, report, Tables); !strings.HasPrefix(got, "Not found: public.events_2026.") {
		t.Errorf("tables message = %q", got)
	}
	withMissing.Tables = []Table{orders, notes}
	expect(t, "a table without a key", local.Check(t.Context(), withMissing), map[CheckID]Status{
		WALKeepSize: keepSize, Keys: Fail, ReadAccess: Fail, Publication: Fail, Published: Skipped,
	})

	tablesSQL, err := TablesSQL(names, target.Tables)
	if err != nil {
		t.Fatal(err)
	}
	admin(t, port, certs, tablesSQL.SQL)
	expect(t, "after the tables' SQL", local.Check(t.Context(), target), map[CheckID]Status{WALKeepSize: keepSize})

	// Adding and removing tables.
	add, err := AddTablesSQL(names, []Table{codes})
	if err != nil {
		t.Fatal(err)
	}
	admin(t, port, certs, add.SQL)
	target.Tables = append(target.Tables, codes)
	expect(t, "after adding a table", local.Check(t.Context(), target), map[CheckID]Status{WALKeepSize: keepSize})
	remove, err := RemoveTablesSQL(names, []Table{lines}, []Table{orders, events, codes})
	if err != nil {
		t.Fatal(err)
	}
	admin(t, port, certs, remove.SQL)
	report = local.Check(t.Context(), target)
	expect(t, "checking a removed table", report, map[CheckID]Status{WALKeepSize: keepSize, ReadAccess: Fail, Published: Fail})
	target.Tables = []Table{orders, events, codes}
	expect(t, "after removing a table", local.Check(t.Context(), target), map[CheckID]Status{WALKeepSize: keepSize})

	// The connector creates the slot, which the checks then find, and
	// DropSlotSQL drops.
	createSlot(t, connection, names.Slot)
	report = local.Check(t.Context(), target)
	if got := message(t, report, Slots); got != "The source's replication slot "+names.Slot+" exists." {
		t.Errorf("slots message = %q", got)
	}
	admin(t, port, certs, DropSlotSQL(names).SQL)
	if got := message(t, local.Check(t.Context(), target), Slots); strings.Contains(got, "exists") {
		t.Errorf("after DropSlotSQL, slots message = %q", got)
	}

	// A new password replaces the old one.
	newPassword := GeneratePassword()
	rotate, err := PasswordSQL(Generic, names, newPassword)
	if err != nil {
		t.Fatal(err)
	}
	admin(t, port, certs, rotate.SQL)
	if result, _ := local.Check(t.Context(), target).Result(Authentication); result.Status != Fail {
		t.Errorf("the old password still logs in: %+v", result)
	}
	target.Connection.Password = newPassword
	expect(t, "after a new password", local.Check(t.Context(), target), map[CheckID]Status{WALKeepSize: keepSize})

	// A role that may not replicate.
	plain := target
	plain.Connection.User, plain.Connection.Password = "plain", "plain-password"
	admin(t, port, certs, `CREATE ROLE plain LOGIN PASSWORD 'plain-password'`)
	report = local.Check(t.Context(), plain)
	if result, _ := report.Result(ReplicationRole); result.Status != Fail || !strings.Contains(result.Message, "Run ALTER ROLE plain WITH REPLICATION as a superuser.") {
		t.Errorf("a plain role: %+v", result)
	}

	// The superuser passes with a warning.
	superuser := target
	superuser.Connection = adminConnection(port, certs)
	if result, _ := local.Check(t.Context(), superuser).Result(ReplicationRole); result.Status != Warn {
		t.Errorf("the superuser: %+v", result)
	}

	// Cleaning up removes the role, the publication and the slot.
	createSlot(t, target.Connection, names.Slot)
	cleanup, err := CleanupSQL(Generic, names, target.Tables)
	if err != nil {
		t.Fatal(err)
	}
	admin(t, port, certs, cleanup.SQL)
	if result, _ := local.Check(t.Context(), target).Result(Authentication); result.Status != Fail {
		t.Errorf("the role logs in after the cleanup: %+v", result)
	}
}

// createSlot creates a logical replication slot over a replication
// connection, as Debezium does.
func createSlot(t *testing.T, connection Connection, slot string) {
	t.Helper()
	var trace connectTrace
	config, err := local.config(connection, true, &trace)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgconn.ConnectConfig(t.Context(), &config.Config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(t.Context()) }()
	if _, err := conn.Exec(t.Context(), "CREATE_REPLICATION_SLOT "+identifier(slot)+" LOGICAL pgoutput").ReadAll(); err != nil {
		t.Fatalf("creating slot %s: %v", slot, err)
	}
}

// certificates checks the certificate checks against a real server.
func certificates(t *testing.T, port int, certs pki) {
	byAddress := adminConnection(port, certs)
	byAddress.Host = "127.0.0.1"
	report := local.Check(t.Context(), Target{Provider: Generic, Connection: byAddress, Names: testNames(t)})
	if got := message(t, report, Certificate); got != "The server's certificate is not for 127.0.0.1: it names localhost. Connect to a name it has, or choose verify-ca with the database's CA." {
		t.Errorf("verify-full by address: %q", got)
	}

	byAddress.TLSMode = VerifyCA
	if result, _ := local.Check(t.Context(), Target{Provider: Generic, Connection: byAddress, Names: testNames(t)}).Result(Certificate); result.Status != Pass {
		t.Errorf("verify-ca by address: %+v", result)
	}

	otherCA := adminConnection(port, certs)
	otherCA.CA = newPKI(t).caPEM
	if got := message(t, local.Check(t.Context(), Target{Provider: Generic, Connection: otherCA, Names: testNames(t)}), Certificate); !strings.HasPrefix(got, "The server's certificate is not signed by the given CA.") {
		t.Errorf("another CA: %q", got)
	}
	otherCA.TLSMode = VerifyCA
	if got := message(t, local.Check(t.Context(), Target{Provider: Generic, Connection: otherCA, Names: testNames(t)}), Certificate); !strings.HasPrefix(got, "The server's certificate is not signed by the given CA.") {
		t.Errorf("another CA with verify-ca: %q", got)
	}
	noCA := adminConnection(port, certs)
	noCA.CA = ""
	// macOS verifies against the system's authorities itself, and says only
	// that the certificate is not trusted; the console runs on Linux.
	got := message(t, local.Check(t.Context(), Target{Provider: Generic, Connection: noCA, Names: testNames(t)}), Certificate)
	if !strings.HasPrefix(got, "The server's certificate is not signed by a public certificate authority.") &&
		(runtime.GOOS != "darwin" || !strings.HasPrefix(got, "The server's certificate is not trusted")) {
		t.Errorf("no CA: %q", got)
	}

	wrongPassword := adminConnection(port, certs)
	wrongPassword.Password = "wrong"
	if got := message(t, local.Check(t.Context(), Target{Provider: Generic, Connection: wrongPassword, Names: testNames(t)}), Authentication); got != "The password of role postgres is wrong, or the role does not exist." {
		t.Errorf("a wrong password: %q", got)
	}
	noDatabase := adminConnection(port, certs)
	noDatabase.Database = "missing"
	if got := message(t, local.Check(t.Context(), Target{Provider: Generic, Connection: noDatabase, Names: testNames(t)}), Authentication); got != "The database missing does not exist." {
		t.Errorf("a missing database: %q", got)
	}

	// Without private addresses allowed, the guard refuses localhost.
	guarded := Checker{}
	if got := message(t, guarded.Check(t.Context(), Target{Provider: Generic, Connection: adminConnection(port, certs), Names: testNames(t)}), Reachable); !strings.HasPrefix(got, "The host localhost resolves to ") || !strings.Contains(got, "which is not public") {
		t.Errorf("guarded localhost: %q", got)
	}
}
