package cdc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TLSMode is how Changeport checks the database's certificate.
type TLSMode string

// The TLS modes. Both encrypt and check that a trusted authority signed the
// certificate.
const (
	// VerifyFull also checks that the certificate is for the host.
	VerifyFull TLSMode = "verify-full"
	// VerifyCA does not check the name, for databases whose certificate does
	// not name the host Changeport connects to, as on some Cloud SQL
	// instances. It needs the database's own CA.
	VerifyCA TLSMode = "verify-ca"
)

// Connection is how Changeport connects to a customer's database.
type Connection struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
	TLSMode  TLSMode
	// CA is the PEM certificates of the authorities that sign the
	// database's certificate. Without it, verify-full trusts the public
	// authorities.
	CA string
}

// Validate checks that the connection is complete.
func (c Connection) Validate() error {
	switch {
	case c.Host == "":
		return errors.New("enter the database's host")
	case strings.ContainsAny(c.Host, "/ ,") || strings.HasPrefix(c.Host, "["):
		return errors.New("enter one host name or address, without a port")
	case c.Port < 1 || c.Port > 65535:
		return errors.New("the port is a number from 1 to 65535")
	case c.Database == "":
		return errors.New("enter the database's name")
	case c.User == "":
		return errors.New("enter the role")
	case c.Password == "":
		return errors.New("enter the role's password")
	case c.TLSMode != VerifyFull && c.TLSMode != VerifyCA:
		return fmt.Errorf("choose %s or %s", VerifyFull, VerifyCA)
	case c.TLSMode == VerifyCA && c.CA == "":
		return fmt.Errorf("%s needs the database's CA certificate", VerifyCA)
	}
	if _, err := certPool(c.CA); err != nil {
		return err
	}
	return nil
}

func certPool(pem string) (*x509.CertPool, error) {
	if pem == "" {
		return nil, nil // the system's
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(pem)) {
		return nil, errors.New("the CA is not a PEM certificate")
	}
	return pool, nil
}

// CheckID names a check.
type CheckID string

// The checks, in the order they run.
const (
	// Reachable: the host resolves to public addresses, and its port answers.
	Reachable CheckID = "reachable"
	// TLS: the server accepts TLS.
	TLS CheckID = "tls"
	// Certificate: a trusted authority signed the server's certificate, for
	// the host with verify-full.
	Certificate CheckID = "certificate"
	// Authentication: the role logs in to the database.
	Authentication CheckID = "authentication"
	// Version: PostgreSQL 14 or newer.
	Version CheckID = "version"
	// WALLevel: logical replication is on.
	WALLevel CheckID = "wal_level"
	// ReplicationRole: the role may replicate.
	ReplicationRole CheckID = "replication_role"
	// Slots: a replication slot is free, or the source's exists.
	Slots CheckID = "slots"
	// WALSenders: a WAL sender is free, or the source's is running.
	WALSenders CheckID = "wal_senders"
	// WALKeepSize: max_slot_wal_keep_size caps the WAL a slot keeps. Only
	// warns.
	WALKeepSize CheckID = "wal_keep_size"
	// ReplicationConnection: a replication connection runs
	// IDENTIFY_SYSTEM, which needs pg_hba.conf to allow it.
	ReplicationConnection CheckID = "replication_connection"
	// Tables: the chosen tables exist.
	Tables CheckID = "tables"
	// Keys: each chosen table has a primary key or a unique replica
	// identity.
	Keys CheckID = "keys"
	// ReadAccess: the role may read each chosen table.
	ReadAccess CheckID = "read_access"
	// Publication: the source's publication exists and publishes inserts,
	// updates and deletes, partitioned tables as themselves.
	Publication CheckID = "publication"
	// Published: the publication has each chosen table.
	Published CheckID = "published"
)

var (
	connectionChecks = []CheckID{Reachable, TLS, Certificate, Authentication}
	serverChecks     = []CheckID{Version, WALLevel, ReplicationRole, Slots, WALSenders, WALKeepSize, ReplicationConnection}
	tableChecks      = []CheckID{Tables, Keys, ReadAccess, Publication, Published}
)

// Status is a check's outcome.
type Status string

// The outcomes.
const (
	Pass Status = "pass"
	// Warn passes, with something worth fixing.
	Warn Status = "warn"
	Fail Status = "fail"
	// Skipped did not run, because a check it needs failed.
	Skipped Status = "skipped"
)

// Result is one check's outcome, with a message for the customer that says
// what was found and, when it failed, what to do.
type Result struct {
	Check   CheckID `json:"check"`
	Status  Status  `json:"status"`
	Message string  `json:"message"`
}

// Report is the outcome of the checks, in the order they ran.
type Report struct {
	Results []Result `json:"results"`
}

// OK reports whether every check passed, warnings included.
func (r Report) OK() bool {
	return !slices.ContainsFunc(r.Results, func(result Result) bool { return result.Status == Fail || result.Status == Skipped })
}

// Result returns a check's result, if it ran.
func (r Report) Result(check CheckID) (Result, bool) {
	i := slices.IndexFunc(r.Results, func(result Result) bool { return result.Check == check })
	if i < 0 {
		return Result{}, false
	}
	return r.Results[i], true
}

func (r *Report) add(check CheckID, status Status, format string, args ...any) {
	r.Results = append(r.Results, Result{Check: check, Status: status, Message: fmt.Sprintf(format, args...)})
}

// skip marks the checks that have not run as skipped.
func (r *Report) skip(checks []CheckID, reason string) {
	for _, check := range checks {
		if _, ran := r.Result(check); !ran {
			r.add(check, Skipped, "%s", reason)
		}
	}
}

// Checker checks customers' databases. The console runs it, from the same
// addresses as the Connect worker, so that it finds what Debezium will.
type Checker struct {
	Guard Guard
	// ConnectTimeout bounds each connection attempt; zero means 10 seconds.
	ConnectTimeout time.Duration
}

// Target is what a source needs of a database.
type Target struct {
	Provider   Provider
	Connection Connection
	Names      Names
	// Tables are the chosen tables. Without them, the checks of the tables
	// and the publication do not run, as before the customer has chosen.
	Tables []Table
}

// minimumVersion is PostgreSQL 14, as server_version_num.
const minimumVersion = 140000

// Check runs the checks on a source's database, stopping at the first
// failure that the later checks depend on.
func (c Checker) Check(ctx context.Context, target Target) Report {
	var report Report
	checks := slices.Concat(connectionChecks, serverChecks)
	if len(target.Tables) > 0 {
		checks = append(checks, tableChecks...)
	}
	if err := target.Connection.Validate(); err != nil {
		report.add(Reachable, Fail, "%s.", capitalize(err.Error()))
		report.skip(checks, "Skipped until the connection is complete.")
		return report
	}
	conn, results := c.connect(ctx, target.Connection, false)
	report.Results = append(report.Results, results...)
	if conn == nil {
		report.skip(checks, "Skipped until Changeport can log in.")
		return report
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	if !c.checkServer(ctx, conn, target, &report) {
		report.skip(checks, "Skipped until the server meets the requirements above.")
		return report
	}
	if len(target.Tables) > 0 {
		c.checkTables(ctx, conn, target, &report)
	}
	return report
}

// connect logs in as the target's role, and reports how far it got.
func (c Checker) connect(ctx context.Context, connection Connection, replication bool) (*pgx.Conn, []Result) {
	var trace connectTrace
	config, err := c.config(connection, replication, &trace)
	if err != nil {
		return nil, []Result{{Check: Reachable, Status: Fail, Message: capitalize(err.Error()) + "."}}
	}
	conn, err := pgx.ConnectConfig(ctx, config)
	if err == nil {
		address := net.JoinHostPort(connection.Host, strconv.Itoa(connection.Port))
		certificate := "A trusted authority signed the server's certificate for " + connection.Host + "."
		if connection.TLSMode == VerifyCA {
			certificate = "The given CA signed the server's certificate."
		}
		return conn, []Result{
			{Check: Reachable, Status: Pass, Message: address + " answers."},
			{Check: TLS, Status: Pass, Message: "The connection is encrypted with TLS."},
			{Check: Certificate, Status: Pass, Message: certificate},
			{Check: Authentication, Status: Pass, Message: fmt.Sprintf("%s logs in to the database %s.", connection.User, connection.Database)},
		}
	}
	return nil, connectFailure(err, connection, &trace)
}

// connectTrace records how far a connection attempt got.
type connectTrace struct {
	dialed   atomic.Bool
	verified atomic.Bool
}

// config is the pgx configuration of a connection. It is built field by
// field rather than from a connection string, so that no environment
// variable or file of the console's own changes it.
func (c Checker) config(connection Connection, replication bool, trace *connectTrace) (*pgx.ConnConfig, error) {
	config, err := pgx.ParseConfig("sslmode=disable")
	if err != nil {
		return nil, err
	}
	roots, err := certPool(connection.CA)
	if err != nil {
		return nil, err
	}
	timeout := c.ConnectTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	config.Host = connection.Host
	config.Port = uint16(connection.Port) //nolint:gosec // G115: 1 to 65535, validated
	config.Database = connection.Database
	config.User = connection.User
	config.Password = connection.Password
	config.ConnectTimeout = timeout
	config.Fallbacks = nil
	config.ValidateConnect = nil
	config.AfterConnect = nil
	config.RuntimeParams = map[string]string{"application_name": "changeport", "statement_timeout": "10s"}
	if replication {
		config.RuntimeParams["replication"] = "database"
	}
	config.LookupFunc = c.Guard.lookup
	config.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := c.Guard.dial(ctx, network, address)
		if err == nil {
			trace.dialed.Store(true)
		}
		return conn, err
	}
	config.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: connection.Host}
	if connection.TLSMode == VerifyCA {
		// Go checks the name with the chain, so verify-ca checks the chain
		// itself.
		config.TLSConfig.InsecureSkipVerify = true
		config.TLSConfig.VerifyConnection = func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return errors.New("the server sent no certificate")
			}
			intermediates := x509.NewCertPool()
			for _, certificate := range state.PeerCertificates[1:] {
				intermediates.AddCert(certificate)
			}
			if _, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates}); err != nil {
				return &tls.CertificateVerificationError{UnverifiedCertificates: state.PeerCertificates, Err: err}
			}
			trace.verified.Store(true)
			return nil
		}
	} else {
		config.TLSConfig.VerifyConnection = func(tls.ConnectionState) error {
			trace.verified.Store(true)
			return nil
		}
	}
	// The checks run a few statements each once; preparing them first would
	// only add round trips.
	config.DefaultQueryExecMode = pgx.QueryExecModeExec
	return config, nil
}

// SQLSTATE codes the checks tell apart.
const (
	invalidPassword       = "28P01"
	invalidAuthorization  = "28000"
	invalidCatalogName    = "3D000"
	insufficientPrivilege = "42501"
	tooManyConnections    = "53300"
	cannotConnectNow      = "57P03"
)

// connectFailure explains a failed connection attempt: the furthest it got,
// and why it stopped there.
func connectFailure(err error, connection Connection, trace *connectTrace) []Result {
	address := net.JoinHostPort(connection.Host, strconv.Itoa(connection.Port))
	reachable := Result{Check: Reachable, Status: Pass, Message: address + " answers."}
	tlsOK := Result{Check: TLS, Status: Pass, Message: "The server accepts TLS."}
	certificateOK := Result{Check: Certificate, Status: Pass, Message: "The server's certificate is trusted."}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && trace.verified.Load() {
		var message string
		switch pgErr.Code {
		case invalidPassword:
			message = fmt.Sprintf("The password of role %s is wrong, or the role does not exist.", connection.User)
		case invalidAuthorization:
			message = fmt.Sprintf("The server refused role %s: %s. Let the role connect from Changeport's addresses over TLS, as in the first step's instructions.", connection.User, pgErr.Message)
		case invalidCatalogName:
			message = fmt.Sprintf("The database %s does not exist.", connection.Database)
		case tooManyConnections:
			message = "The server has no free connections: " + pgErr.Message + "."
		case cannotConnectNow:
			message = "The server is starting or shutting down. Try again in a minute."
		default:
			message = fmt.Sprintf("The server refused the login: %s.", pgErr.Message)
		}
		return []Result{reachable, tlsOK, certificateOK, {Check: Authentication, Status: Fail, Message: message}}
	}

	var verification *tls.CertificateVerificationError
	if errors.As(err, &verification) {
		return []Result{reachable, tlsOK, {Check: Certificate, Status: Fail, Message: certificateFailure(verification.Err, connection)}}
	}
	if trace.dialed.Load() && strings.Contains(err.Error(), "server refused TLS connection") {
		return []Result{reachable, {Check: TLS, Status: Fail, Message: "The server does not accept TLS connections. Turn TLS on (ssl = on): Changeport connects only over TLS."}}
	}
	if trace.dialed.Load() && trace.verified.Load() {
		return []Result{reachable, tlsOK, certificateOK, {Check: Authentication, Status: Fail, Message: "The login failed: " + rootCause(err) + "."}}
	}
	if trace.dialed.Load() {
		return []Result{reachable, {Check: TLS, Status: Fail, Message: "The TLS handshake failed: " + rootCause(err) + "."}}
	}

	var message string
	var dnsErr *net.DNSError
	var notPublic *NotPublicError
	switch {
	case errors.As(err, &notPublic) && notPublic.Host == notPublic.Addr.String():
		message = fmt.Sprintf("The address %s is not public. Changeport connects to databases only over the internet: enter the database's public host name.", notPublic.Host)
	case errors.As(err, &notPublic):
		message = fmt.Sprintf("The host %s resolves to %s, which is not public. Changeport connects to databases only over the internet: enter the database's public host name.", notPublic.Host, notPublic.Addr)
	case errors.As(err, &dnsErr) && dnsErr.IsNotFound:
		message = fmt.Sprintf("The host %s does not exist.", connection.Host)
	default:
		message = fmt.Sprintf("Changeport cannot reach %s: %s. Check that the database is publicly accessible and lets Changeport's addresses in.", address, rootCause(err))
	}
	return []Result{{Check: Reachable, Status: Fail, Message: message}}
}

// certificateFailure explains why a certificate is not trusted.
func certificateFailure(err error, connection Connection) string {
	var hostname x509.HostnameError
	var unknown x509.UnknownAuthorityError
	var invalid x509.CertificateInvalidError
	signers := "a public certificate authority"
	if connection.CA != "" {
		signers = "the given CA"
	}
	switch {
	case errors.As(err, &hostname):
		names := hostname.Certificate.DNSNames
		for _, ip := range hostname.Certificate.IPAddresses {
			names = append(names, ip.String())
		}
		if len(names) == 0 {
			names = []string{"no host"}
		}
		return fmt.Sprintf("The server's certificate is not for %s: it names %s. Connect to a name it has, or choose %s with the database's CA.",
			connection.Host, strings.Join(names, ", "), VerifyCA)
	case errors.As(err, &unknown):
		return fmt.Sprintf("The server's certificate is not signed by %s. Add the CA certificate of the database's provider.", signers)
	case errors.As(err, &invalid) && invalid.Reason == x509.Expired:
		return "The server's certificate has expired, or is not valid yet: " + invalid.Detail + "."
	default:
		return "The server's certificate is not trusted: " + err.Error() + "."
	}
}

// rootCause is the innermost reason in an error, such as "connection
// refused" or "i/o timeout", without pgx's description of the attempt.
func rootCause(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "the connection timed out"
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Err != nil {
		return opErr.Err.Error()
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.Err
	}
	for {
		var next error
		switch e := err.(type) { //nolint:errorlint // walks the chain one error at a time
		case interface{ Unwrap() error }:
			next = e.Unwrap()
		case interface{ Unwrap() []error }:
			if errs := e.Unwrap(); len(errs) > 0 {
				next = errs[0]
			}
		}
		if next == nil {
			return err.Error()
		}
		err = next
	}
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// serverState is what the server checks read, in one query.
type serverState struct {
	versionNumber  int
	version        string
	walLevel       string
	walKeepSize    string
	superuser      bool
	replication    bool
	rdsReplication bool
	maxSlots       int
	slots          int
	slotExists     bool
	slotActive     bool
	maxWALSenders  int
	walSenders     int
}

const serverQuery = `
SELECT current_setting('server_version'),
       current_setting('wal_level'),
       current_setting('max_slot_wal_keep_size'),
       r.rolsuper,
       r.rolreplication,
       EXISTS (SELECT 1 FROM pg_roles g WHERE g.rolname = 'rds_replication' AND pg_has_role(r.oid, g.oid, 'MEMBER')),
       current_setting('max_replication_slots')::int,
       (SELECT count(*) FROM pg_replication_slots)::int,
       EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1),
       EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1 AND active),
       current_setting('max_wal_senders')::int,
       (SELECT count(*) FROM pg_stat_replication)::int
FROM pg_roles r
WHERE r.rolname = current_user`

// checkServer runs the server's checks, and reports whether the tables'
// checks may follow.
func (c Checker) checkServer(ctx context.Context, conn *pgx.Conn, target Target, report *Report) bool {
	var s serverState
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&s.versionNumber); err != nil {
		report.add(Version, Fail, "Reading the server's version failed: %s.", rootCause(err))
		return false
	}
	if s.versionNumber < minimumVersion {
		report.add(Version, Fail, "The server runs PostgreSQL %d.%d; Changeport needs 14 or newer.", s.versionNumber/10000, s.versionNumber%10000)
		return false
	}
	err := conn.QueryRow(ctx, serverQuery, target.Names.Slot).Scan(&s.version, &s.walLevel, &s.walKeepSize, &s.superuser, &s.replication,
		&s.rdsReplication, &s.maxSlots, &s.slots, &s.slotExists, &s.slotActive, &s.maxWALSenders, &s.walSenders)
	if err != nil {
		report.add(Version, Fail, "Reading the server's settings failed: %s.", rootCause(err))
		return false
	}
	report.add(Version, Pass, "The server runs PostgreSQL %s.", strings.Fields(s.version)[0])
	info := providers[target.Provider]

	ok := true
	if s.walLevel == "logical" {
		report.add(WALLevel, Pass, "Logical replication is on.")
	} else {
		report.add(WALLevel, Fail, "Logical replication is off (wal_level is %s). %s", s.walLevel, info.walLevel)
		ok = false
	}

	switch {
	case s.superuser:
		report.add(ReplicationRole, Warn, "%s is a superuser. Changeport needs only the replication privilege: a role with less is safer.", target.Connection.User)
	case s.replication || s.rdsReplication:
		report.add(ReplicationRole, Pass, "%s may replicate.", target.Connection.User)
	default:
		report.add(ReplicationRole, Fail, "%s may not replicate. %s", target.Connection.User, strings.ReplaceAll(info.grantReplication, "<role>", target.Connection.User))
		ok = false
	}

	switch {
	case s.slotExists:
		report.add(Slots, Pass, "The source's replication slot %s exists.", target.Names.Slot)
	case s.slots < s.maxSlots:
		report.add(Slots, Pass, "%d of %d replication slots are free.", s.maxSlots-s.slots, s.maxSlots)
	default:
		report.add(Slots, Fail, "All %d replication slots are in use. Drop a slot nothing reads, or raise max_replication_slots and restart.", s.maxSlots)
		ok = false
	}

	switch {
	case s.slotActive:
		report.add(WALSenders, Pass, "The source is replicating.")
	case s.walSenders < s.maxWALSenders:
		report.add(WALSenders, Pass, "%d of %d WAL senders are free.", s.maxWALSenders-s.walSenders, s.maxWALSenders)
	default:
		report.add(WALSenders, Fail, "All %d WAL senders are in use. Raise max_wal_senders and restart.", s.maxWALSenders)
		ok = false
	}

	if s.walKeepSize == "-1" {
		report.add(WALKeepSize, Warn, "Replication slots may keep unlimited WAL: if the source stops reading, the database keeps WAL until its disk is full. Set max_slot_wal_keep_size to cap it.")
	} else {
		report.add(WALKeepSize, Pass, "A replication slot keeps at most %s of WAL.", s.walKeepSize)
	}

	if ok {
		c.checkReplicationConnection(ctx, target, report)
		if result, _ := report.Result(ReplicationConnection); result.Status == Fail {
			ok = false
		}
	}
	return ok
}

// checkReplicationConnection opens a replication connection, as Debezium
// does, and identifies the system through it.
func (c Checker) checkReplicationConnection(ctx context.Context, target Target, report *Report) {
	var trace connectTrace
	config, err := c.config(target.Connection, true, &trace)
	if err != nil {
		report.add(ReplicationConnection, Fail, "%s.", capitalize(err.Error()))
		return
	}
	conn, err := pgconn.ConnectConfig(ctx, &config.Config)
	if err != nil {
		var pgErr *pgconn.PgError
		switch {
		case errors.As(err, &pgErr) && pgErr.Code == insufficientPrivilege:
			report.add(ReplicationConnection, Fail, "The server refused a replication connection: %s. %s", pgErr.Message,
				strings.ReplaceAll(providers[target.Provider].grantReplication, "<role>", target.Connection.User))
		case errors.As(err, &pgErr) && pgErr.Code == invalidAuthorization:
			report.add(ReplicationConnection, Fail, "The server refused a replication connection: %s. Let the role replicate from Changeport's addresses over TLS in pg_hba.conf.", pgErr.Message)
		default:
			report.add(ReplicationConnection, Fail, "A replication connection failed: %s.", rootCause(err))
		}
		return
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	results, err := conn.Exec(ctx, "IDENTIFY_SYSTEM").ReadAll()
	if err != nil || len(results) != 1 || len(results[0].Rows) != 1 || len(results[0].Rows[0]) < 3 {
		message := "no answer"
		if err != nil {
			message = rootCause(err)
		}
		report.add(ReplicationConnection, Fail, "IDENTIFY_SYSTEM failed on a replication connection: %s.", message)
		return
	}
	row := results[0].Rows[0]
	report.add(ReplicationConnection, Pass, "A replication connection identified system %s at %s.", row[0], row[2])
}

// TableInfo is a table as the console's table picker shows it.
type TableInfo struct {
	Table
	// Partitioned tables sync with their partitions, through one topic.
	Partitioned bool `json:"partitioned"`
	// EstimatedRows is Postgres's estimate, or -1 before the table was
	// first analyzed.
	EstimatedRows int64 `json:"estimatedRows"`
	// Readable reports whether the role may read the table yet.
	Readable bool `json:"readable"`
	// Keyed reports whether the table has a primary key or a unique replica
	// identity, which v1 needs to key its messages.
	Keyed bool `json:"keyed"`
	// Reason says why a table is not keyed.
	Reason string `json:"reason,omitempty"`
}

// tablesQuery lists the tables a source can sync: ordinary and partitioned
// tables, without the partitions, which sync through their partitioned
// table, and without Postgres's own. With $1 and $2, only those tables.
const tablesQuery = `
SELECT n.nspname,
       c.relname,
       c.relkind = 'p',
       CASE WHEN c.relkind = 'p'
            THEN coalesce((SELECT sum(greatest(l.reltuples, 0)) FROM pg_partition_tree(c.oid) t
                           JOIN pg_class l ON l.oid = t.relid WHERE t.isleaf), 0)
            ELSE c.reltuples END::bigint,
       has_schema_privilege(n.oid, 'USAGE') AND has_table_privilege(c.oid, 'SELECT'),
       c.relreplident::text,
       EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')
  AND NOT c.relispartition
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname NOT LIKE 'pg\_toast%'
  AND n.nspname NOT LIKE 'pg\_temp\_%'
  AND ($1::text[] IS NULL OR (n.nspname, c.relname) IN (SELECT * FROM unnest($1::text[], $2::text[])))
ORDER BY n.nspname, c.relname`

// ListTables lists the database's tables for the table picker, with whether
// each can be synced. It needs the role, not the tables' grants.
func (c Checker) ListTables(ctx context.Context, connection Connection) ([]TableInfo, error) {
	conn, err := c.open(ctx, connection)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	return listTables(ctx, conn, nil)
}

func listTables(ctx context.Context, conn *pgx.Conn, only []Table) ([]TableInfo, error) {
	var schemas, names []string
	if only != nil {
		schemas, names = make([]string, len(only)), make([]string, len(only))
		for i, table := range only {
			schemas[i], names[i] = table.Schema, table.Name
		}
	}
	rows, err := conn.Query(ctx, tablesQuery, schemas, names)
	if err != nil {
		return nil, fmt.Errorf("listing the tables: %s", rootCause(err))
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (TableInfo, error) {
		var t TableInfo
		var identity string
		var primaryKey bool
		if err := row.Scan(&t.Schema, &t.Name, &t.Partitioned, &t.EstimatedRows, &t.Readable, &identity, &primaryKey); err != nil {
			return t, err
		}
		t.Keyed, t.Reason = keyed(identity, primaryKey)
		return t, nil
	})
}

// keyed reports whether a table with the given replica identity can key its
// messages, and why not. Debezium keys a table's messages by its primary key,
// or by its replica identity's index.
func keyed(identity string, primaryKey bool) (bool, string) {
	switch {
	case identity == "n":
		return false, "Its replica identity is NOTHING, so Postgres publishes none of its updates and deletes."
	case identity == "i", primaryKey:
		return true, ""
	default:
		return false, "It has no primary key. Add one, or make a unique index on columns that are NOT NULL its replica identity (ALTER TABLE ... REPLICA IDENTITY USING INDEX ...)."
	}
}

// publicationState is the source's publication.
type publicationState struct {
	insert, update, delete, viaRoot, allTables bool
}

// checkTables runs the checks of the chosen tables and the publication.
func (c Checker) checkTables(ctx context.Context, conn *pgx.Conn, target Target, report *Report) {
	found, err := listTables(ctx, conn, target.Tables)
	if err != nil {
		report.add(Tables, Fail, "%s.", capitalize(err.Error()))
		report.skip(tableChecks, "Skipped until the tables can be listed.")
		return
	}
	byTable := make(map[Table]TableInfo, len(found))
	for _, info := range found {
		byTable[info.Table] = info
	}
	var missing, unkeyed, unreadable []string
	partitioned := false
	for _, table := range target.Tables {
		info, ok := byTable[table]
		switch {
		case !ok:
			missing = append(missing, table.String())
			continue
		case !info.Keyed:
			unkeyed = append(unkeyed, table.String()+": "+info.Reason)
		}
		if !info.Readable {
			unreadable = append(unreadable, table.String())
		}
		partitioned = partitioned || info.Partitioned
	}
	if len(missing) > 0 {
		report.add(Tables, Fail, "Not found: %s. Partitions sync through their partitioned table.", strings.Join(missing, ", "))
		report.skip(tableChecks, "Skipped until every table is found.")
		return
	}
	report.add(Tables, Pass, "All %d tables exist.", len(target.Tables))
	if len(unkeyed) > 0 {
		report.add(Keys, Fail, "Changeport syncs only tables with a key. %s", strings.Join(unkeyed, " "))
	} else {
		report.add(Keys, Pass, "Every table has a primary key or a unique replica identity.")
	}
	if len(unreadable) > 0 {
		report.add(ReadAccess, Fail, "%s may not read %s. Run the second step's SQL.", target.Connection.User, strings.Join(unreadable, ", "))
	} else {
		report.add(ReadAccess, Pass, "%s may read every table.", target.Connection.User)
	}

	var pub publicationState
	err = conn.QueryRow(ctx, `SELECT pubinsert, pubupdate, pubdelete, pubviaroot, puballtables FROM pg_publication WHERE pubname = $1`,
		target.Names.Publication).Scan(&pub.insert, &pub.update, &pub.delete, &pub.viaRoot, &pub.allTables)
	if errors.Is(err, pgx.ErrNoRows) {
		report.add(Publication, Fail, "The publication %s does not exist. Run the second step's SQL.", target.Names.Publication)
		report.skip(tableChecks, "Skipped until the publication exists.")
		return
	}
	if err != nil {
		report.add(Publication, Fail, "Reading the publication failed: %s.", rootCause(err))
		report.skip(tableChecks, "Skipped until the publication can be read.")
		return
	}
	var unpublished []string
	for operation, published := range map[string]bool{"inserts": pub.insert, "updates": pub.update, "deletes": pub.delete} {
		if !published {
			unpublished = append(unpublished, operation)
		}
	}
	slices.Sort(unpublished)
	switch {
	case len(unpublished) > 0:
		report.add(Publication, Fail, "The publication %s does not publish %s. Run ALTER PUBLICATION %s SET (publish = 'insert, update, delete').",
			target.Names.Publication, strings.Join(unpublished, " and "), identifier(target.Names.Publication))
	case partitioned && !pub.viaRoot:
		report.add(Publication, Fail, "The publication %s publishes partitioned tables as their partitions. Run ALTER PUBLICATION %s SET (publish_via_partition_root = true).",
			target.Names.Publication, identifier(target.Names.Publication))
	default:
		report.add(Publication, Pass, "The publication %s publishes inserts, updates and deletes.", target.Names.Publication)
	}

	if pub.allTables {
		report.add(Published, Pass, "The publication %s publishes every table.", target.Names.Publication)
		return
	}
	rows, err := conn.Query(ctx, `SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = $1`, target.Names.Publication)
	if err == nil {
		var published []Table
		published, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (Table, error) {
			var t Table
			return t, row.Scan(&t.Schema, &t.Name)
		})
		if err == nil {
			reportPublished(report, target, published)
			return
		}
	}
	report.add(Published, Fail, "Reading the publication's tables failed: %s.", rootCause(err))
}

func reportPublished(report *Report, target Target, published []Table) {
	var missing, extra []string
	for _, table := range target.Tables {
		if !slices.Contains(published, table) {
			missing = append(missing, table.String())
		}
	}
	for _, table := range published {
		if !slices.Contains(target.Tables, table) {
			extra = append(extra, table.String())
		}
	}
	switch {
	case len(missing) > 0:
		report.add(Published, Fail, "The publication %s does not publish %s. Run the SQL that adds them.", target.Names.Publication, strings.Join(missing, ", "))
	case len(extra) > 0:
		report.add(Published, Warn, "The publication %s also publishes %s, which Changeport does not sync. Postgres decodes their changes anyway.",
			target.Names.Publication, strings.Join(extra, ", "))
	default:
		report.add(Published, Pass, "The publication %s publishes all %d tables.", target.Names.Publication, len(target.Tables))
	}
}
