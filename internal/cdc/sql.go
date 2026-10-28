package cdc

import (
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Provider is where the customer's Postgres runs. Each provider turns on
// logical replication, gives a role the right to replicate, and lets
// Changeport in its own way.
type Provider string

// The providers the console has instructions for.
const (
	Generic  Provider = "generic"
	RDS      Provider = "rds"
	CloudSQL Provider = "cloudsql"
	Azure    Provider = "azure"
	Supabase Provider = "supabase"
	Neon     Provider = "neon"
)

// Providers lists the providers in the order the console offers them.
var Providers = []Provider{Generic, RDS, CloudSQL, Azure, Supabase, Neon}

// providerInfo is what differs between providers.
type providerInfo struct {
	title string
	// setup is what the customer does outside SQL before creating the role:
	// turning logical replication on and letting Changeport in.
	setup []string
	// walLevel says how to turn logical replication on, for when a check
	// finds it off.
	walLevel string
	// createRole is the SQL creating the role, with %[1]s for the role and
	// %[2]s for the password; empty when the role is created outside SQL.
	createRole string
	// roleOutsideSQL says how to create the role and get its password when
	// createRole is empty.
	roleOutsideSQL string
	// grantReplication says how to let the role replicate, for when a check
	// finds it cannot.
	grantReplication string
}

var providers = map[Provider]providerInfo{
	Generic: {
		title: "PostgreSQL",
		setup: []string{
			"Set wal_level = logical in postgresql.conf, and max_slot_wal_keep_size to cap the WAL a stalled source can hold back (for example 50GB), then restart Postgres.",
			"Let Changeport's addresses reach the database's port through your firewall.",
			"Let the role connect over TLS: for each of Changeport's addresses, add a line hostssl <database> <role> <address>/32 scram-sha-256 to pg_hba.conf, then reload Postgres.",
		},
		walLevel:         "Set wal_level = logical in postgresql.conf, then restart Postgres.",
		createRole:       "CREATE ROLE %[1]s WITH LOGIN REPLICATION PASSWORD %[2]s;",
		grantReplication: "Run ALTER ROLE <role> WITH REPLICATION as a superuser.",
	},
	RDS: {
		title: "Amazon RDS and Aurora",
		setup: []string{
			"In the instance's DB parameter group (the DB cluster parameter group on Aurora), set rds.logical_replication = 1 and max_slot_wal_keep_size to cap the WAL a stalled source can hold back (for example 50000, in MB), then reboot.",
			"Make the instance publicly accessible, and let Changeport's addresses reach its port in its security group.",
			"Changeport verifies the database's certificate: add the RDS certificate bundle for the instance's region as its CA.",
		},
		walLevel:         "Set rds.logical_replication = 1 in the DB parameter group (the DB cluster parameter group on Aurora), then reboot.",
		createRole:       "CREATE ROLE %[1]s WITH LOGIN PASSWORD %[2]s;\nGRANT rds_replication TO %[1]s;",
		grantReplication: "Run GRANT rds_replication TO <role> as the master user.",
	},
	CloudSQL: {
		title: "Google Cloud SQL",
		setup: []string{
			"Set the database flag cloudsql.logical_decoding to on, then restart the instance.",
			"Give the instance a public IP address, and add Changeport's addresses to its authorized networks.",
			"Add the instance's server CA certificate as the CA, and choose verify-ca unless the instance's certificate names its DNS name.",
		},
		walLevel:         "Set the database flag cloudsql.logical_decoding to on, then restart the instance.",
		createRole:       "CREATE ROLE %[1]s WITH LOGIN REPLICATION PASSWORD %[2]s;",
		grantReplication: "Run ALTER ROLE <role> WITH REPLICATION as the postgres user.",
	},
	Azure: {
		title: "Azure Database for PostgreSQL",
		setup: []string{
			"Set the server parameter wal_level to logical, then restart the server.",
			"Allow public access, and add a firewall rule for each of Changeport's addresses.",
			"Run the SQL as the server's admin. On PostgreSQL 16 and newer the admin needs the replication privilege first: ALTER ROLE <admin> WITH REPLICATION.",
		},
		walLevel:         "Set the server parameter wal_level to logical, then restart the server.",
		createRole:       "CREATE ROLE %[1]s WITH LOGIN REPLICATION PASSWORD %[2]s;",
		grantReplication: "Run ALTER ROLE <role> WITH REPLICATION as the server's admin.",
	},
	Supabase: {
		title: "Supabase",
		setup: []string{
			"Turn on the IPv4 add-on: Changeport connects over IPv4, and without it the database's direct connection is IPv6 only.",
			"Connect Changeport to the direct connection, db.<project>.supabase.co on port 5432. The pooler does not support replication.",
			"If network restrictions are on, allow Changeport's addresses.",
			"Add the project's SSL certificate (Database settings, SSL configuration) as the CA.",
		},
		walLevel:         "Supabase turns logical replication on for every project; ask Supabase's support why it is off.",
		createRole:       "CREATE ROLE %[1]s WITH LOGIN REPLICATION PASSWORD %[2]s;",
		grantReplication: "Run ALTER ROLE <role> WITH REPLICATION as the postgres user.",
	},
	Neon: {
		title: "Neon",
		setup: []string{
			"Turn on logical replication in the project's settings (Settings, Logical replication). This restarts the project's computes.",
			"Connect Changeport to the direct connection: the host without -pooler. The pooler does not support replication.",
			"If IP Allow is on, allow Changeport's addresses.",
			"Neon removes replication slots that are inactive for about 40 hours: a source stopped for longer needs a new snapshot.",
		},
		walLevel:         "Turn on logical replication in the project's settings (Settings, Logical replication).",
		roleOutsideSQL:   "Create the role %s in the Neon Console (your branch, Roles & Databases, Add role), and enter the password Neon shows into Changeport. Roles created with SQL cannot replicate on Neon.",
		grantReplication: "Create the role in the Neon Console, not with SQL: only roles created there can replicate.",
	},
}

// Valid reports whether p is a provider the console knows.
func (p Provider) Valid() bool {
	_, ok := providers[p]
	return ok
}

// Title is the provider's name as the console shows it.
func (p Provider) Title() string { return providers[p].title }

// PasswordFromCustomer reports whether the customer brings the role's
// password, as on Neon, where the role is created in Neon's console. For the
// other providers, Changeport generates it.
func (p Provider) PasswordFromCustomer() bool { return providers[p].createRole == "" }

// GeneratePassword returns a new password for a source's role: 26 random
// letters and digits, 130 bits.
func GeneratePassword() string { return rand.Text() }

// Script is what the customer does to their database for a step: the steps
// outside SQL first, then the SQL. It is empty when there is nothing to do.
type Script struct {
	// Steps are done outside SQL, before the SQL.
	Steps []string `json:"steps"`
	// SQL is run in the source's database. It holds one statement per line
	// or group of lines, each ending with a semicolon.
	SQL string `json:"sql"`
}

func checkProvider(p Provider) error {
	if !p.Valid() {
		return fmt.Errorf("unknown provider %q", p)
	}
	return nil
}

// RoleSQL is the first step: turning logical replication on, letting
// Changeport in, and creating the role it logs in as. For a provider whose
// password comes from the customer, the password is ignored.
func RoleSQL(p Provider, names Names, password string) (Script, error) {
	if err := checkProvider(p); err != nil {
		return Script{}, err
	}
	info := providers[p]
	script := Script{Steps: slices.Clone(info.setup)}
	if info.createRole == "" {
		script.Steps = append(script.Steps, fmt.Sprintf(info.roleOutsideSQL, names.Role))
		return script, nil
	}
	if password == "" {
		return Script{}, errors.New("no password for the role")
	}
	script.SQL = fmt.Sprintf(info.createRole, identifier(names.Role), literal(password)) + "\n"
	return script, nil
}

// publish is what the publication publishes: every change to a row, but not
// truncations, which Debezium skips. Changes to a partitioned table's
// partitions are published as the partitioned table's, so that they reach
// its topic.
const publish = "WITH (publish = 'insert, update, delete', publish_via_partition_root = true)"

// TablesSQL is the second step: letting the role read the chosen tables and
// publishing them. It is run as the tables' owner, since only an owner may
// publish a table.
func TablesSQL(names Names, tables []Table) (Script, error) {
	if err := ValidateTables(tables); err != nil {
		return Script{}, err
	}
	var sql strings.Builder
	writeGrants(&sql, names, tables)
	fmt.Fprintf(&sql, "CREATE PUBLICATION %s FOR TABLE\n%s\n  %s;\n", identifier(names.Publication), tableList(tables), publish)
	return Script{Steps: []string{"Run the SQL as the owner of the tables."}, SQL: sql.String()}, nil
}

// AddTablesSQL adds tables to a running source.
func AddTablesSQL(names Names, added []Table) (Script, error) {
	if err := ValidateTables(added); err != nil {
		return Script{}, err
	}
	var sql strings.Builder
	writeGrants(&sql, names, added)
	fmt.Fprintf(&sql, "ALTER PUBLICATION %s ADD TABLE\n%s;\n", identifier(names.Publication), tableList(added))
	return Script{Steps: []string{"Run the SQL as the owner of the tables."}, SQL: sql.String()}, nil
}

// RemoveTablesSQL removes tables from a source, which keeps the remaining
// ones. It takes back the role's access to the schemas none of the remaining
// tables are in.
func RemoveTablesSQL(names Names, removed, remaining []Table) (Script, error) {
	if err := ValidateTables(removed); err != nil {
		return Script{}, err
	}
	var sql strings.Builder
	fmt.Fprintf(&sql, "ALTER PUBLICATION %s DROP TABLE\n%s;\n", identifier(names.Publication), tableList(removed))
	fmt.Fprintf(&sql, "REVOKE SELECT ON TABLE\n%s\nFROM %s;\n", tableList(removed), identifier(names.Role))
	for _, schema := range schemas(removed) {
		if !slices.ContainsFunc(remaining, func(t Table) bool { return t.Schema == schema }) {
			fmt.Fprintf(&sql, "REVOKE USAGE ON SCHEMA %s FROM %s;\n", identifier(schema), identifier(names.Role))
		}
	}
	return Script{Steps: []string{"Run the SQL as the owner of the tables."}, SQL: sql.String()}, nil
}

// PasswordSQL gives the role a new password. For a provider whose password
// comes from the customer, the password is ignored.
func PasswordSQL(p Provider, names Names, password string) (Script, error) {
	if err := checkProvider(p); err != nil {
		return Script{}, err
	}
	if p.PasswordFromCustomer() {
		return Script{Steps: []string{fmt.Sprintf("Reset the password of the role %s in the provider's console, and enter the new one into Changeport.", names.Role)}}, nil
	}
	if password == "" {
		return Script{}, errors.New("no password for the role")
	}
	return Script{SQL: fmt.Sprintf("ALTER ROLE %s WITH PASSWORD %s;\n", identifier(names.Role), literal(password))}, nil
}

// DropSlotSQL drops the source's replication slot, for when Changeport cannot
// reach the database to drop it. It does nothing if the slot is gone, and
// fails while the slot is in use.
func DropSlotSQL(names Names) Script {
	return Script{SQL: dropSlot(names)}
}

func dropSlot(names Names) string {
	return fmt.Sprintf("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s;\n", literal(names.Slot))
}

// CleanupSQL removes what a deleted source left in the customer's database:
// its slot, its publication, and its role with the role's access to the
// source's tables.
func CleanupSQL(p Provider, names Names, tables []Table) (Script, error) {
	if err := checkProvider(p); err != nil {
		return Script{}, err
	}
	if err := ValidateTables(tables); err != nil {
		return Script{}, err
	}
	var sql strings.Builder
	sql.WriteString(dropSlot(names))
	fmt.Fprintf(&sql, "DROP PUBLICATION IF EXISTS %s;\n", identifier(names.Publication))
	fmt.Fprintf(&sql, "REVOKE SELECT ON TABLE\n%s\nFROM %s;\n", tableList(tables), identifier(names.Role))
	for _, schema := range schemas(tables) {
		fmt.Fprintf(&sql, "REVOKE USAGE ON SCHEMA %s FROM %s;\n", identifier(schema), identifier(names.Role))
	}
	script := Script{Steps: []string{"Run the SQL as the owner of the tables."}, SQL: sql.String()}
	if p.PasswordFromCustomer() {
		script.Steps = append(script.Steps, fmt.Sprintf("Then delete the role %s in the provider's console.", names.Role))
	} else {
		fmt.Fprintf(&sql, "DROP ROLE IF EXISTS %s;\n", identifier(names.Role))
		script.SQL = sql.String()
	}
	return script, nil
}

// writeGrants lets the role use the tables' schemas and read the tables.
func writeGrants(sql *strings.Builder, names Names, tables []Table) {
	for _, schema := range schemas(tables) {
		fmt.Fprintf(sql, "GRANT USAGE ON SCHEMA %s TO %s;\n", identifier(schema), identifier(names.Role))
	}
	fmt.Fprintf(sql, "GRANT SELECT ON TABLE\n%s\nTO %s;\n", tableList(tables), identifier(names.Role))
}

// schemas returns the tables' schemas, each once, in the order they first
// appear.
func schemas(tables []Table) []string {
	var result []string
	for _, table := range tables {
		if !slices.Contains(result, table.Schema) {
			result = append(result, table.Schema)
		}
	}
	return result
}

// tableList lists tables one per line, indented, separated by commas.
func tableList(tables []Table) string {
	lines := make([]string, len(tables))
	for i, table := range tables {
		lines[i] = "  " + identifier(table.Schema) + "." + identifier(table.Name)
	}
	return strings.Join(lines, ",\n")
}

// identifier quotes a Postgres identifier. It always quotes, which keeps the
// case of names and works for names that are keywords.
func identifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// literal quotes a Postgres string literal, for servers with
// standard_conforming_strings on, the default since PostgreSQL 9.1.
func literal(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
