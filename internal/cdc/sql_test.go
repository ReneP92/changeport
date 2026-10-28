package cdc

import (
	"strings"
	"testing"
)

func testNames(t *testing.T) Names {
	t.Helper()
	names, err := NamesFor("app", "x1y2z3")
	if err != nil {
		t.Fatal(err)
	}
	return names
}

func TestRoleSQLPerProvider(t *testing.T) {
	names := testNames(t)
	for provider, want := range map[Provider]string{
		Generic:  `CREATE ROLE "changeport_app_x1y2z3" WITH LOGIN REPLICATION PASSWORD 'S3CRET';` + "\n",
		RDS:      `CREATE ROLE "changeport_app_x1y2z3" WITH LOGIN PASSWORD 'S3CRET';` + "\n" + `GRANT rds_replication TO "changeport_app_x1y2z3";` + "\n",
		CloudSQL: `CREATE ROLE "changeport_app_x1y2z3" WITH LOGIN REPLICATION PASSWORD 'S3CRET';` + "\n",
		Azure:    `CREATE ROLE "changeport_app_x1y2z3" WITH LOGIN REPLICATION PASSWORD 'S3CRET';` + "\n",
		Supabase: `CREATE ROLE "changeport_app_x1y2z3" WITH LOGIN REPLICATION PASSWORD 'S3CRET';` + "\n",
		Neon:     "",
	} {
		script, err := RoleSQL(provider, names, "S3CRET")
		if err != nil {
			t.Fatal(err)
		}
		if script.SQL != want {
			t.Errorf("RoleSQL(%s).SQL =\n%s\nwant\n%s", provider, script.SQL, want)
		}
		if len(script.Steps) == 0 {
			t.Errorf("RoleSQL(%s) has no steps", provider)
		}
		if provider.Title() == "" {
			t.Errorf("%s has no title", provider)
		}
	}
	if len(Providers) != len(providers) {
		t.Errorf("Providers lists %d of the %d providers", len(Providers), len(providers))
	}
}

func TestNeonRoleIsCreatedInNeonsConsole(t *testing.T) {
	names := testNames(t)
	if !Neon.PasswordFromCustomer() || Generic.PasswordFromCustomer() {
		t.Error("only Neon's password should come from the customer")
	}
	script, err := RoleSQL(Neon, names, "")
	if err != nil {
		t.Fatal(err)
	}
	if last := script.Steps[len(script.Steps)-1]; !strings.Contains(last, "Create the role changeport_app_x1y2z3 in the Neon Console") {
		t.Errorf("last step = %q, want creating the role in Neon's console", last)
	}
	rotate, err := PasswordSQL(Neon, names, "")
	if err != nil || rotate.SQL != "" || !strings.Contains(rotate.Steps[0], "Reset the password of the role changeport_app_x1y2z3") {
		t.Errorf("PasswordSQL(Neon) = %+v, %v", rotate, err)
	}
}

func TestRoleSQLRefusesBadInput(t *testing.T) {
	names := testNames(t)
	if _, err := RoleSQL("oracle", names, "S3CRET"); err == nil {
		t.Error("RoleSQL accepted an unknown provider")
	}
	if _, err := RoleSQL(Generic, names, ""); err == nil {
		t.Error("RoleSQL accepted no password")
	}
}

func TestQuoting(t *testing.T) {
	if got := identifier(`we"ird`); got != `"we""ird"` {
		t.Errorf("identifier = %s", got)
	}
	if got := literal(`it's`); got != `'it''s'` {
		t.Errorf("literal = %s", got)
	}
	password := GeneratePassword()
	if len(password) != 26 || strings.Trim(password, "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567") != "" {
		t.Errorf("GeneratePassword() = %q, want 26 letters and digits", password)
	}
}

var chosen = []Table{{"public", "orders"}, {"Sales", "Order Lines"}, {"public", "customers"}}

func TestTablesSQL(t *testing.T) {
	script, err := TablesSQL(testNames(t), chosen)
	if err != nil {
		t.Fatal(err)
	}
	want := `GRANT USAGE ON SCHEMA "public" TO "changeport_app_x1y2z3";
GRANT USAGE ON SCHEMA "Sales" TO "changeport_app_x1y2z3";
GRANT SELECT ON TABLE
  "public"."orders",
  "Sales"."Order Lines",
  "public"."customers"
TO "changeport_app_x1y2z3";
CREATE PUBLICATION "changeport_app_x1y2z3" FOR TABLE
  "public"."orders",
  "Sales"."Order Lines",
  "public"."customers"
  WITH (publish = 'insert, update, delete', publish_via_partition_root = true);
`
	if script.SQL != want {
		t.Errorf("TablesSQL() =\n%s\nwant\n%s", script.SQL, want)
	}
	if _, err := TablesSQL(testNames(t), nil); err == nil {
		t.Error("TablesSQL accepted no tables")
	}
}

func TestAddAndRemoveTablesSQL(t *testing.T) {
	names := testNames(t)
	add, err := AddTablesSQL(names, []Table{{"billing", "invoices"}})
	if err != nil {
		t.Fatal(err)
	}
	want := `GRANT USAGE ON SCHEMA "billing" TO "changeport_app_x1y2z3";
GRANT SELECT ON TABLE
  "billing"."invoices"
TO "changeport_app_x1y2z3";
ALTER PUBLICATION "changeport_app_x1y2z3" ADD TABLE
  "billing"."invoices";
`
	if add.SQL != want {
		t.Errorf("AddTablesSQL() =\n%s\nwant\n%s", add.SQL, want)
	}

	// Removing Sales's only table takes back the schema; public keeps one.
	remove, err := RemoveTablesSQL(names, []Table{{"Sales", "Order Lines"}, {"public", "orders"}}, []Table{{"public", "customers"}})
	if err != nil {
		t.Fatal(err)
	}
	want = `ALTER PUBLICATION "changeport_app_x1y2z3" DROP TABLE
  "Sales"."Order Lines",
  "public"."orders";
REVOKE SELECT ON TABLE
  "Sales"."Order Lines",
  "public"."orders"
FROM "changeport_app_x1y2z3";
REVOKE USAGE ON SCHEMA "Sales" FROM "changeport_app_x1y2z3";
`
	if remove.SQL != want {
		t.Errorf("RemoveTablesSQL() =\n%s\nwant\n%s", remove.SQL, want)
	}
}

func TestPasswordAndSlotSQL(t *testing.T) {
	names := testNames(t)
	rotate, err := PasswordSQL(RDS, names, "N3W")
	if err != nil || rotate.SQL != `ALTER ROLE "changeport_app_x1y2z3" WITH PASSWORD 'N3W';`+"\n" {
		t.Errorf("PasswordSQL() = %q, %v", rotate.SQL, err)
	}
	if got := DropSlotSQL(names).SQL; got != "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'changeport_app_x1y2z3';\n" {
		t.Errorf("DropSlotSQL() = %q", got)
	}
}

func TestCleanupSQL(t *testing.T) {
	names := testNames(t)
	script, err := CleanupSQL(Generic, names, []Table{{"public", "orders"}})
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'changeport_app_x1y2z3';
DROP PUBLICATION IF EXISTS "changeport_app_x1y2z3";
REVOKE SELECT ON TABLE
  "public"."orders"
FROM "changeport_app_x1y2z3";
REVOKE USAGE ON SCHEMA "public" FROM "changeport_app_x1y2z3";
DROP ROLE IF EXISTS "changeport_app_x1y2z3";
`
	if script.SQL != want {
		t.Errorf("CleanupSQL() =\n%s\nwant\n%s", script.SQL, want)
	}
	neon, err := CleanupSQL(Neon, names, []Table{{"public", "orders"}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(neon.SQL, "DROP ROLE") || !strings.Contains(neon.Steps[len(neon.Steps)-1], "delete the role changeport_app_x1y2z3") {
		t.Errorf("CleanupSQL(Neon) = %+v, want the role deleted in Neon's console", neon)
	}
}
