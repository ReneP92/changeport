package console

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/cdc"
	"github.com/ReneP92/changeport/internal/console/api"
	"github.com/ReneP92/changeport/internal/topics"
)

// fakeSources keeps sources in memory, by customer.
type fakeSources struct {
	sources map[string][]v1alpha1.PostgresSource
	// passwords are the passwords written, by customer/source, the latest
	// last.
	passwords map[string][]string
	// changed makes the next update fail, as if the source changed
	// meanwhile.
	changed bool
	// deleted and slotDropped record the sources deleted and the slots
	// confirmed dropped, as customer/source.
	deleted, slotDropped []string
}

func newFakeSources() *fakeSources {
	return &fakeSources{sources: map[string][]v1alpha1.PostgresSource{}, passwords: map[string][]string{}}
}

func (f *fakeSources) index(customerID, name string) int {
	return slices.IndexFunc(f.sources[customerID], func(s v1alpha1.PostgresSource) bool { return s.Name == name })
}

func (f *fakeSources) List(_ context.Context, customerID string) ([]v1alpha1.PostgresSource, error) {
	var list []v1alpha1.PostgresSource
	for _, s := range f.sources[customerID] {
		list = append(list, *s.DeepCopy())
	}
	return list, nil
}

func (f *fakeSources) Get(_ context.Context, customerID, name string) (*v1alpha1.PostgresSource, error) {
	if i := f.index(customerID, name); i >= 0 {
		return f.sources[customerID][i].DeepCopy(), nil
	}
	return nil, nil
}

func (f *fakeSources) Create(_ context.Context, customerID string, source *v1alpha1.PostgresSource, password string) error {
	if f.index(customerID, source.Name) >= 0 {
		return ErrSourceExists
	}
	source.Namespace = "customer-" + customerID
	source.UID = types.UID("uid-" + source.Name)
	source.Generation = 1
	source.CreationTimestamp = metav1.NewTime(time.Date(2026, 10, 30, 9, 0, 0, 0, time.UTC))
	source.Spec.Connection.PasswordSecret = passwordSecretPrefix + source.Name + "-000001"
	f.sources[customerID] = append(f.sources[customerID], *source.DeepCopy())
	f.passwords[customerID+"/"+source.Name] = []string{password}
	return nil
}

func (f *fakeSources) Update(_ context.Context, source *v1alpha1.PostgresSource) error {
	if f.changed {
		f.changed = false
		return ErrSourceChanged
	}
	customerID := strings.TrimPrefix(source.Namespace, "customer-")
	i := f.index(customerID, source.Name)
	if i < 0 {
		return errors.New("no such source")
	}
	f.sources[customerID][i] = *source.DeepCopy()
	return nil
}

func (f *fakeSources) SetPassword(ctx context.Context, source *v1alpha1.PostgresSource, password string) error {
	key := strings.TrimPrefix(source.Namespace, "customer-") + "/" + source.Name
	f.passwords[key] = append(f.passwords[key], password)
	source.Spec.Connection.PasswordSecret = passwordSecretPrefix + source.Name + "-000002"
	return f.Update(ctx, source)
}

func (f *fakeSources) Delete(_ context.Context, source *v1alpha1.PostgresSource) error {
	f.deleted = append(f.deleted, strings.TrimPrefix(source.Namespace, "customer-")+"/"+source.Name)
	return nil
}

func (f *fakeSources) ConfirmSlotDropped(_ context.Context, source *v1alpha1.PostgresSource) error {
	f.slotDropped = append(f.slotDropped, strings.TrimPrefix(source.Namespace, "customer-")+"/"+source.Name)
	return nil
}

func (f *fakeSources) RequestCatalog(_ context.Context, source *v1alpha1.PostgresSource) (string, error) {
	customerID := strings.TrimPrefix(source.Namespace, "customer-")
	i := f.index(customerID, source.Name)
	if i < 0 {
		return "", errors.New("no such source")
	}
	stored := &f.sources[customerID][i]
	requested := "request-" + strconv.Itoa(len(stored.Annotations[v1alpha1.CatalogRequestAnnotation])+1)
	stored.Annotations = map[string]string{v1alpha1.CatalogRequestAnnotation: requested}
	source.Annotations = stored.Annotations
	return requested, nil
}

// addApp puts the running source app of the tables, with ID k3x9q2, in acme.
func (f *fakeSources) addApp(tables ...string) *v1alpha1.PostgresSource {
	source := v1alpha1.PostgresSource{
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "customer-acme", UID: types.UID("uid-app"), Generation: 1},
		Spec: v1alpha1.PostgresSourceSpec{
			ID: "k3x9q2", Provider: "generic",
			Connection: v1alpha1.PostgresConnection{
				Host: "db.example.com", Port: 5432, Database: "shop", PasswordSecret: passwordSecretPrefix + "app-000001",
				TLS: v1alpha1.PostgresTLS{Mode: v1alpha1.TLSVerifyFull},
			},
			RetentionMs: 7 * 24 * 3600 * 1000, State: v1alpha1.StateRunning,
		},
	}
	for _, table := range tables {
		schema, table, _ := strings.Cut(table, ".")
		source.Spec.Tables = append(source.Spec.Tables, v1alpha1.PostgresTable{Schema: schema, Name: table})
	}
	f.sources["acme"] = append(f.sources["acme"], source)
	return &f.sources["acme"][len(f.sources["acme"])-1]
}

// fakeDatabases passes every check but the failing ones, and lists tables.
type fakeDatabases struct {
	// failing are the checks that fail, with their messages; the checks
	// after a failed one are skipped.
	failing map[cdc.CheckID]string
	tables  []cdc.TableInfo
	// unreachable makes listing tables fail.
	unreachable bool
	// targets are the databases checked.
	targets []cdc.Target
}

func (d *fakeDatabases) Check(_ context.Context, target cdc.Target) cdc.Report {
	d.targets = append(d.targets, target)
	var report cdc.Report
	failed := false
	for _, check := range []cdc.CheckID{cdc.Reachable, cdc.TLS, cdc.Certificate, cdc.Authentication, cdc.Version, cdc.Published} {
		result := cdc.Result{Check: check, Status: cdc.Pass, Message: "Fine."}
		if message, ok := d.failing[check]; ok {
			result.Status, result.Message = cdc.Fail, message
		}
		if failed {
			result.Status, result.Message = cdc.Skipped, "Skipped."
		}
		failed = failed || result.Status == cdc.Fail
		report.Results = append(report.Results, result)
	}
	return report
}

func (d *fakeDatabases) ListTables(context.Context, cdc.Connection) ([]cdc.TableInfo, error) {
	if d.unreachable {
		return nil, errors.New("the server db.example.com does not answer")
	}
	return d.tables, nil
}

const (
	connection = `{"host":" db.example.com ","port":5432,"database":"shop","tlsMode":"verify-full","password":"Old-p4ssword"}`
	newSource  = `{"name":"shop","id":"k3x9q2","provider":"generic","connection":` + connection +
		`,"tables":[{"schema":"public","name":"orders"},{"schema":"public","name":"customers"}],"retentionMs":604800000}`
)

func role(t *testing.T, name string) string {
	t.Helper()
	names, err := cdc.NamesFor(name, "k3x9q2")
	if err != nil {
		t.Fatal(err)
	}
	return names.Role
}

func decode[T any](t *testing.T, body string) T {
	t.Helper()
	var v T
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	return v
}

func TestMembersReadSourcesAndOnlyAdminsChangeThem(t *testing.T) {
	f := newFixture(t)
	f.sources.addApp("public.orders")
	member := f.signer.token(t, session{user: "user_2", org: "org_acme", role: "member"})
	for _, path := range []string{"/api/customer/source-options", "/api/customer/sources", "/api/customer/sources/app"} {
		if status, body, _ := get(t, f.handler, path, member); status != 200 {
			t.Errorf("a member reading %s = %d %s, want 200", path, status, body)
		}
	}
	staff := f.signer.token(t, session{user: "user_6", org: staffOrg, role: "admin"})
	if status, body, _ := get(t, f.handler, "/api/customer/sources", staff); status != 403 {
		t.Errorf("staff listing a customer's sources = %d %s, want 403", status, body)
	}
	for name, token := range map[string]string{"a member": member, "staff": staff} {
		for _, r := range []struct{ method, path, body string }{
			{http.MethodPost, "/api/customer/sources/setup-sql", `{"step":"role","name":"shop","provider":"generic"}`},
			{http.MethodPost, "/api/customer/sources/checks", `{"name":"shop","id":"k3x9q2","provider":"generic","connection":` + connection + `}`},
			{http.MethodPost, "/api/customer/sources/tables", `{"name":"shop","id":"k3x9q2","provider":"generic","connection":` + connection + `}`},
			{http.MethodPost, "/api/customer/sources", newSource},
			{http.MethodPatch, "/api/customer/sources/app", `{"state":"Stopped"}`},
			{http.MethodDelete, "/api/customer/sources/app?confirm=app", ""},
			{http.MethodPost, "/api/customer/sources/app/sql", `{"step":"password"}`},
			{http.MethodPut, "/api/customer/sources/app/password", `{"password":"New-p4ssword"}`},
			{http.MethodPost, "/api/customer/sources/app/slot-dropped", ""},
			{http.MethodPost, "/api/customer/sources/app/catalog", ""},
			{http.MethodGet, "/api/customer/sources/app/catalog", ""},
		} {
			if status, body := send(t, f.handler, r.method, r.path, token, r.body); status != 403 {
				t.Errorf("%s: %s %s = %d %s, want 403", name, r.method, r.path, status, body)
			}
		}
	}
	if len(f.databases.targets) != 0 || len(f.sources.deleted) != 0 || len(f.sources.sources["acme"]) != 1 ||
		f.sources.sources["acme"][0].Spec.State != v1alpha1.StateRunning {
		t.Errorf("refused requests reached a database or changed a source: %+v", f.sources)
	}
}

func TestSourceOptions(t *testing.T) {
	f := newFixture(t)
	member := f.signer.token(t, session{user: "user_2", org: "org_acme", role: "member"})
	status, body, _ := get(t, f.handler, "/api/customer/source-options", member)
	if status != 200 {
		t.Fatalf("GET /api/customer/source-options = %d %s", status, body)
	}
	options := decode[api.SourceOptions](t, body)
	if strings.Join(options.EgressAddresses, ",") != "203.0.113.10,203.0.113.11" {
		t.Errorf("egress addresses %v", options.EgressAddresses)
	}
	if options.RegistryURL == nil || *options.RegistryURL != "https://registry.acme.kafka.changeport.test" {
		t.Errorf("registry URL %v, want the gateway's next to the bootstrap address", options.RegistryURL)
	}
	if len(options.Providers) != len(cdc.Providers) || options.MaxTables != cdc.MaxTables || options.DefaultRetentionMs != 604800000 {
		t.Errorf("options %+v", options)
	}
	neon := slices.IndexFunc(options.Providers, func(p api.ProviderOption) bool { return p.Id == api.Neon })
	if neon < 0 || !options.Providers[neon].PasswordFromCustomer {
		t.Errorf("Neon should take the customer's password: %+v", options.Providers)
	}
}

func TestSetupSQLChoosesTheIDAndPasswordOnce(t *testing.T) {
	f := newFixture(t)
	admin := f.signer.token(t, session{user: "user_3", org: "org_acme", role: "admin"})
	status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources/setup-sql", admin, `{"step":"role","name":"shop","provider":"generic","id":"ignored"}`)
	if status != 200 {
		t.Fatalf("the role step = %d %s", status, body)
	}
	first := decode[api.SourceSQL](t, body)
	if cdc.ValidateID(first.Id) != nil || first.Id == "ignored" || first.Password == nil || len(*first.Password) < 20 ||
		!strings.Contains(first.Sql, `CREATE ROLE "`+first.Role+`"`) || !strings.Contains(first.Sql, *first.Password) {
		t.Fatalf("the role step chose %+v", first)
	}
	if strings.Contains(f.log.String(), *first.Password) {
		t.Error("the generated password was logged")
	}
	_, body = send(t, f.handler, http.MethodPost, "/api/customer/sources/setup-sql", admin, `{"step":"role","name":"shop","provider":"generic"}`)
	if second := decode[api.SourceSQL](t, body); second.Id == first.Id || *second.Password == *first.Password {
		t.Errorf("a second role step chose the same ID or password: %+v", second)
	}

	// Neon creates the role itself, with a password the customer enters.
	_, body = send(t, f.handler, http.MethodPost, "/api/customer/sources/setup-sql", admin, `{"step":"role","name":"shop","provider":"neon"}`)
	if neon := decode[api.SourceSQL](t, body); neon.Password != nil {
		t.Errorf("the Neon role step chose a password: %+v", neon)
	}

	status, body = send(t, f.handler, http.MethodPost, "/api/customer/sources/setup-sql", admin,
		`{"step":"tables","name":"shop","provider":"generic","id":"`+first.Id+`","tables":[{"schema":"public","name":"orders"}]}`)
	tables := decode[api.SourceSQL](t, body)
	if status != 200 || tables.Id != first.Id || tables.Password != nil || !strings.Contains(tables.Sql, "CREATE PUBLICATION") {
		t.Errorf("the tables step = %d %+v", status, tables)
	}

	f.sources.addApp("public.orders")
	for _, tt := range []struct{ name, body, field string }{
		{"no name", `{"step":"role","name":"","provider":"generic"}`, "name"},
		{"a reserved name", `{"step":"role","name":"changeport","provider":"generic"}`, "name"},
		{"another source's name", `{"step":"role","name":"app","provider":"generic"}`, "name"},
		{"the prefix of an existing topic", `{"step":"role","name":"acme","provider":"generic"}`, "name"},
		{"an unknown provider", `{"step":"role","name":"shop","provider":"oracle"}`, ""},
		{"the tables step without the ID", `{"step":"tables","name":"shop","provider":"generic","tables":[{"schema":"public","name":"orders"}]}`, "id"},
		{"the tables step without tables", `{"step":"tables","name":"shop","provider":"generic","id":"k3x9q2"}`, "tables"},
	} {
		status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources/setup-sql", admin, tt.body)
		if status != 400 || (tt.field != "" && !strings.Contains(body, `"field":"`+tt.field+`"`)) {
			t.Errorf("%s = %d %s, want 400 about %q", tt.name, status, body, tt.field)
		}
	}
}

func TestChecksLogInAsTheSourcesRole(t *testing.T) {
	f := newFixture(t)
	f.databases.failing = map[cdc.CheckID]string{cdc.Authentication: "The login failed: password authentication failed."}
	admin := f.signer.token(t, session{user: "user_3", org: "org_acme", role: "admin"})
	status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources/checks", admin,
		`{"name":"shop","id":"k3x9q2","provider":"generic","connection":`+connection+`,"tables":[{"schema":"public","name":"orders"}]}`)
	report := decode[api.CheckReport](t, body)
	if status != 200 || report.Ok || len(report.Results) != 6 || report.Results[3].Status != api.Fail ||
		report.Results[4].Status != api.Skipped {
		t.Errorf("checks = %d %s", status, body)
	}
	target := f.databases.targets[0]
	if target.Connection.User != role(t, "shop") || target.Connection.Host != "db.example.com" || target.Names.Publication == "" ||
		len(target.Tables) != 1 || target.Connection.TLSMode != cdc.VerifyFull {
		t.Errorf("checked %+v", target)
	}

	for _, tt := range []struct{ name, body, field string }{
		{"no ID", `{"name":"shop","id":"","provider":"generic","connection":` + connection + `}`, "name"},
		{"no host", `{"name":"shop","id":"k3x9q2","provider":"generic","connection":{"host":"","port":5432,"database":"shop","tlsMode":"verify-full","password":"x"}}`, "connection"},
	} {
		if status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources/checks", admin, tt.body); status != 400 || !strings.Contains(body, `"field":"`+tt.field+`"`) {
			t.Errorf("checks with %s = %d %s, want 400 about %s", tt.name, status, body, tt.field)
		}
	}
}

func TestTablePickerShowsEachTablesTopic(t *testing.T) {
	f := newFixture(t)
	f.clusters.topics["acme"] = []topics.Topic{{Name: "shop.public.invoices", Partitions: 1, ReplicationFactor: 1}}
	f.databases.tables = []cdc.TableInfo{
		{Table: cdc.Table{Schema: "public", Name: "orders"}, EstimatedRows: 1200, Readable: true, Keyed: true},
		{Table: cdc.Table{Schema: "public", Name: "events"}, EstimatedRows: -1, Readable: true, Reason: "Its primary key has a timestamp column."},
		{Table: cdc.Table{Schema: "public", Name: "invoices"}, Readable: false, Keyed: true},
	}
	admin := f.signer.token(t, session{user: "user_3", org: "org_acme", role: "admin"})
	request := `{"name":"shop","id":"k3x9q2","provider":"generic","connection":` + connection + `}`
	status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources/tables", admin, request)
	if status != 200 {
		t.Fatalf("the tables = %d %s", status, body)
	}
	list := decode[api.SourceTableList](t, body).Tables
	if len(list) != 3 || list[0].Topic != "shop.public.orders" || list[0].TopicProblem != nil || !list[0].Keyed || list[0].EstimatedRows != 1200 {
		t.Errorf("orders = %+v", list[0])
	}
	if list[1].Keyed || list[1].Reason == nil || !strings.Contains(*list[1].Reason, "timestamp") {
		t.Errorf("events = %+v, want it unkeyed with the reason", list[1])
	}
	if list[2].Readable || list[2].TopicProblem == nil || !strings.Contains(*list[2].TopicProblem, "exists already") {
		t.Errorf("invoices = %+v, want its topic's collision", list[2])
	}

	f.databases.unreachable = true
	if status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources/tables", admin, request); status != 409 || !strings.Contains(body, "does not answer") {
		t.Errorf("the tables of an unreachable database = %d %s, want 409", status, body)
	}
}

func TestAdminsCreateSourcesThatPassTheChecks(t *testing.T) {
	f := newFixture(t)
	admin := f.signer.token(t, session{user: "user_3", org: "org_acme", role: "admin"})
	status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources", admin, newSource)
	if status != 201 {
		t.Fatalf("creating a source = %d %s", status, body)
	}
	created := decode[api.Source](t, body)
	if created.Name != "shop" || created.Role != role(t, "shop") || created.Host != "db.example.com" || created.State != api.SourceStateRunning ||
		created.Phase != v1alpha1.PhasePending || len(created.Tables) != 2 || created.Tables[1].Topic != "shop.public.customers" ||
		created.Tables[1].Snapshot != api.SourceTableStatusSnapshot(v1alpha1.SnapshotPending) {
		t.Errorf("created %+v", created)
	}
	stored := f.sources.sources["acme"]
	if len(stored) != 1 || stored[0].Spec.ID != "k3x9q2" || stored[0].Spec.RetentionMs != 604800000 || stored[0].Spec.Connection.TLS.Mode != v1alpha1.TLSVerifyFull {
		t.Errorf("stored %+v", stored)
	}
	if got := f.sources.passwords["acme/shop"]; len(got) != 1 || got[0] != "Old-p4ssword" {
		t.Errorf("the password written = %v", got)
	}
	if len(f.databases.targets) != 1 || len(f.databases.targets[0].Tables) != 2 {
		t.Errorf("checked %+v, want the database with both tables", f.databases.targets)
	}
	if !strings.Contains(f.log.String(), `"msg":"source created","audit":true,"tenant":"acme","user":"user_3","organization":"org_acme","source":"shop"`) {
		t.Errorf("no audit line for the creation in %s", f.log.String())
	}
	if strings.Contains(f.log.String(), "Old-p4ssword") {
		t.Error("the password was logged")
	}

	with := func(old, new string) string { return strings.Replace(newSource, old, new, 1) }
	for _, tt := range []struct {
		name, body, field string
		status            int
	}{
		{"a taken name", newSource, "name", 400},
		{"the prefix of an existing topic", with(`"name":"shop"`, `"name":"acme"`), "name", 400},
		{"a retention that is no preset", with(`604800000`, `1234`), "retentionMs", 400},
		{"no tables", with(`[{"schema":"public","name":"orders"},{"schema":"public","name":"customers"}]`, `[]`), "tables", 400},
		{"a table twice", with(`{"schema":"public","name":"customers"}`, `{"schema":"public","name":"orders"}`), "tables", 400},
		{"tables whose topics collide", with(`{"schema":"public","name":"customers"}`, `{"schema":"public","name":"or.ders"},{"schema":"public.or","name":"ders"}`), "tables", 400},
	} {
		body := strings.Replace(tt.body, `"name":"shop"`, `"name":"other"`, 1)
		if tt.name == "a taken name" || tt.name == "the prefix of an existing topic" {
			body = tt.body
		}
		if status, got := send(t, f.handler, http.MethodPost, "/api/customer/sources", admin, body); status != tt.status || !strings.Contains(got, `"field":"`+tt.field+`"`) {
			t.Errorf("creating a source with %s = %d %s, want %d about %s", tt.name, status, got, tt.status, tt.field)
		}
	}
	if len(f.sources.sources["acme"]) != 1 || len(f.databases.targets) != 1 {
		t.Errorf("refused sources were checked or created: %d checks, %d sources", len(f.databases.targets), len(f.sources.sources["acme"]))
	}
}

func TestCreatingASourceNeedsAReadyDatabaseAndRoom(t *testing.T) {
	f := newFixture(t)
	admin := f.signer.token(t, session{user: "user_3", org: "org_acme", role: "admin"})

	f.databases.failing = map[cdc.CheckID]string{cdc.Published: "The publication does not publish public.customers. Run the SQL of step 2."}
	status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources", admin, newSource)
	if status != 409 || !strings.Contains(body, "does not publish public.customers") {
		t.Errorf("creating a source whose checks fail = %d %s, want 409 with the failure", status, body)
	}
	f.databases.failing = nil

	// 52 replicas and 946 more leave room for 2 of the 1000.
	f.clusters.topics["acme"] = []topics.Topic{{Name: "big", Partitions: 946, ReplicationFactor: 1}}
	three := strings.Replace(newSource, `{"schema":"public","name":"customers"}`, `{"schema":"public","name":"customers"},{"schema":"public","name":"items"}`, 1)
	if status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources", admin, three); status != 409 || !strings.Contains(body, "no room") {
		t.Errorf("creating a source beyond the replica budget = %d %s, want 409", status, body)
	}

	f.clusters.notReady["acme"] = true
	if status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources", admin, newSource); status != 409 || !strings.Contains(body, "not ready") {
		t.Errorf("creating a source before the cluster is ready = %d %s, want 409", status, body)
	}
	if len(f.sources.sources["acme"]) != 0 {
		t.Errorf("refused sources were created: %+v", f.sources.sources["acme"])
	}
	f.clusters.notReady["acme"] = false

	// Another source's table whose topic Connect has not created yet takes
	// room too.
	f.sources.addApp("public.orders")
	if status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources", admin, newSource); status != 409 || !strings.Contains(body, "3 tables without a topic") {
		t.Errorf("creating a source beyond the replica budget with another's pending = %d %s, want 409", status, body)
	}
	delete(f.sources.sources, "acme")
	if status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources", admin, newSource); status != 201 {
		t.Errorf("creating a source with room for its 2 tables = %d %s", status, body)
	}
}

func TestReadingASourcesStatus(t *testing.T) {
	f := newFixture(t)
	source := f.sources.addApp("public.orders", "public.customers")
	source.Status = v1alpha1.PostgresSourceStatus{
		Phase: v1alpha1.PhaseSnapshotting,
		Conditions: []metav1.Condition{{
			Type: v1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: "Snapshotting",
			Message: "Copying the tables' rows.", ObservedGeneration: 1,
		}},
		Tables: []v1alpha1.TableStatus{{Schema: "public", Name: "orders", Snapshot: v1alpha1.SnapshotRunning, RowsCopied: 400, RowsEstimated: 1200}},
		Slot:   &v1alpha1.SlotStatus{Name: "changeport_app_k3x9q2", Active: true, RetainedWALBytes: 4096, WALStatus: "reserved", CheckedAt: metav1.Now()},
	}
	member := f.signer.token(t, session{user: "user_2", org: "org_acme", role: "member"})
	_, body, _ := get(t, f.handler, "/api/customer/sources/app", member)
	got := decode[api.Source](t, body)
	if !got.Ready || got.Phase != v1alpha1.PhaseSnapshotting || got.Reason != "Snapshotting" || got.Slot == nil || got.Slot.RetainedWALBytes != 4096 ||
		got.Slot.WalStatus == nil || got.DropSlotSQL != nil {
		t.Errorf("source %s", body)
	}
	if orders := got.Tables[0]; orders.Snapshot != api.SourceTableStatusSnapshot(v1alpha1.SnapshotRunning) || *orders.RowsCopied != 400 || *orders.RowsEstimated != 1200 {
		t.Errorf("orders %+v", orders)
	}
	if customers := got.Tables[1]; customers.Snapshot != api.SourceTableStatusSnapshot(v1alpha1.SnapshotPending) || customers.RowsCopied != nil {
		t.Errorf("customers %+v, want it pending", customers)
	}

	// A deleted source whose database is unreachable shows the SQL to drop
	// its slot by hand.
	source.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	source.Status.Conditions[0] = metav1.Condition{
		Type: v1alpha1.ConditionReady, Status: metav1.ConditionFalse,
		Reason: v1alpha1.SourceReasonDatabaseUnreachable, Message: "Changeport cannot drop the slot.", ObservedGeneration: 1,
	}
	_, body, _ = get(t, f.handler, "/api/customer/sources", member)
	list := decode[api.SourceList](t, body).Sources
	if len(list) != 1 || list[0].Deleting == nil || !*list[0].Deleting || list[0].DropSlotSQL == nil ||
		!strings.Contains(*list[0].DropSlotSQL, "slot_name = 'changeport_app_k3x9q2'") {
		t.Errorf("sources %s", body)
	}

	if status, body, _ := get(t, f.handler, "/api/customer/sources/unknown", member); status != 404 {
		t.Errorf("GET an unknown source = %d %s, want 404", status, body)
	}
}

func TestAdminsChangeSources(t *testing.T) {
	f := newFixture(t)
	f.sources.addApp("public.orders")
	// The source's own topic is no collision, and counts against the budget
	// already: 52, 946 and its 1 leave room for 1 more table, whose topic
	// then counts until Connect creates it.
	f.clusters.topics["acme"] = []topics.Topic{{Name: "big", Partitions: 946, ReplicationFactor: 1}, {Name: "app.public.orders", Partitions: 1, ReplicationFactor: 1}}
	admin := f.signer.token(t, session{user: "user_3", org: "org_acme", role: "admin"})
	status, body := send(t, f.handler, http.MethodPatch, "/api/customer/sources/app", admin,
		`{"tables":[{"schema":"public","name":"orders"},{"schema":"public","name":"customers"}],"retentionMs":86400000,"state":"Stopped"}`)
	if status != 200 {
		t.Fatalf("changing a source = %d %s", status, body)
	}
	spec := f.sources.sources["acme"][0].Spec
	if len(spec.Tables) != 2 || spec.RetentionMs != 86400000 || spec.State != v1alpha1.StateStopped || spec.ID != "k3x9q2" {
		t.Errorf("changed to %+v", spec)
	}
	if !strings.Contains(f.log.String(), `"msg":"source changed","audit":true,"tenant":"acme","user":"user_3","organization":"org_acme","source":"app","tables":2,"retention_ms":86400000,"state":"Stopped"`) {
		t.Errorf("no audit line for the change in %s", f.log.String())
	}

	for _, tt := range []struct {
		name, body string
		status     int
	}{
		{"a table beyond the budget", `{"tables":[{"schema":"public","name":"orders"},{"schema":"public","name":"customers"},{"schema":"public","name":"items"}]}`, 409},
		{"a retention that is no preset", `{"retentionMs":5}`, 400},
		{"no tables", `{"tables":[]}`, 400},
		{"another state", `{"state":"Paused"}`, 400},
	} {
		if status, body := send(t, f.handler, http.MethodPatch, "/api/customer/sources/app", admin, tt.body); status != tt.status {
			t.Errorf("changing a source with %s = %d %s, want %d", tt.name, status, body, tt.status)
		}
	}
	if spec := f.sources.sources["acme"][0].Spec; len(spec.Tables) != 2 || spec.RetentionMs != 86400000 {
		t.Errorf("refused changes changed the source: %+v", spec)
	}

	f.sources.changed = true
	if status, body := send(t, f.handler, http.MethodPatch, "/api/customer/sources/app", admin, `{"state":"Running"}`); status != 409 || !strings.Contains(body, "changed meanwhile") {
		t.Errorf("changing a source changed meanwhile = %d %s, want 409", status, body)
	}
	f.sources.sources["acme"][0].DeletionTimestamp = &metav1.Time{Time: time.Now()}
	if status, body := send(t, f.handler, http.MethodPatch, "/api/customer/sources/app", admin, `{"state":"Running"}`); status != 409 || !strings.Contains(body, "being deleted") {
		t.Errorf("changing a deleted source = %d %s, want 409", status, body)
	}
	if status, body := send(t, f.handler, http.MethodPatch, "/api/customer/sources/unknown", admin, `{"state":"Running"}`); status != 404 {
		t.Errorf("changing an unknown source = %d %s, want 404", status, body)
	}
}

func TestDeletingASourceTakesItsName(t *testing.T) {
	f := newFixture(t)
	f.sources.addApp("public.orders")
	admin := f.signer.token(t, session{user: "user_3", org: "org_acme", role: "admin"})
	if status, body := send(t, f.handler, http.MethodDelete, "/api/customer/sources/app", admin, ""); status != 400 || !strings.Contains(body, "confirm is required") {
		t.Errorf("DELETE without the name = %d %s, want 400", status, body)
	}
	for _, path := range []string{"/api/customer/sources/app?confirm=ap", "/api/customer/sources/app?confirm=APP"} {
		if status, body := send(t, f.handler, http.MethodDelete, path, admin, ""); status != 400 || !strings.Contains(body, `"field":"confirm"`) {
			t.Errorf("DELETE %s = %d %s, want 400", path, status, body)
		}
	}
	if status, body := send(t, f.handler, http.MethodDelete, "/api/customer/sources/unknown?confirm=unknown", admin, ""); status != 404 {
		t.Errorf("deleting an unknown source = %d %s, want 404", status, body)
	}
	if status, body := send(t, f.handler, http.MethodDelete, "/api/customer/sources/app?confirm=app", admin, ""); status != 204 {
		t.Fatalf("deleting a source = %d %s", status, body)
	}
	if strings.Join(f.sources.deleted, ",") != "acme/app" {
		t.Errorf("deleted %v", f.sources.deleted)
	}
	if !strings.Contains(f.log.String(), `"msg":"source deleted","audit":true,"tenant":"acme","user":"user_3","organization":"org_acme","source":"app"`) {
		t.Errorf("no audit line for the deletion in %s", f.log.String())
	}
}

func TestSourceSQLSteps(t *testing.T) {
	f := newFixture(t)
	f.sources.addApp("public.orders", "public.customers")
	admin := f.signer.token(t, session{user: "user_3", org: "org_acme", role: "admin"})
	for _, tt := range []struct {
		step, tables, want string
		password           bool
	}{
		{"add", `[{"schema":"public","name":"items"}]`, "ALTER PUBLICATION \"changeport_app_k3x9q2\" ADD TABLE\n  \"public\".\"items\"", false},
		{"remove", `[{"schema":"public","name":"customers"}]`, "ALTER PUBLICATION \"changeport_app_k3x9q2\" DROP TABLE\n  \"public\".\"customers\"", false},
		{"password", ``, `ALTER ROLE "changeport_app_k3x9q2" WITH PASSWORD`, true},
		{"cleanup", ``, "DROP PUBLICATION", false},
		{"dropSlot", ``, "pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'changeport_app_k3x9q2'", false},
	} {
		request := `{"step":"` + tt.step + `"}`
		if tt.tables != "" {
			request = `{"step":"` + tt.step + `","tables":` + tt.tables + `}`
		}
		status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources/app/sql", admin, request)
		got := decode[api.SourceSQL](t, body)
		if status != 200 || !strings.Contains(got.Sql, tt.want) || (got.Password != nil) != tt.password || got.Id != "k3x9q2" {
			t.Errorf("the %s step = %d %s, want SQL with %q", tt.step, status, body, tt.want)
		}
	}
	if status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources/app/sql", admin, `{"step":"add"}`); status != 400 {
		t.Errorf("the add step without tables = %d %s, want 400", status, body)
	}
	if status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources/unknown/sql", admin, `{"step":"cleanup"}`); status != 404 {
		t.Errorf("the SQL of an unknown source = %d %s, want 404", status, body)
	}
}

func TestANewPasswordMustLogIn(t *testing.T) {
	f := newFixture(t)
	f.sources.addApp("public.orders")
	admin := f.signer.token(t, session{user: "user_3", org: "org_acme", role: "admin"})
	f.databases.failing = map[cdc.CheckID]string{cdc.Authentication: "The login failed: password authentication failed."}
	status, body := send(t, f.handler, http.MethodPut, "/api/customer/sources/app/password", admin, `{"password":"Wrong-p4ssword"}`)
	if status != 409 || !strings.Contains(body, "password authentication failed") {
		t.Errorf("a password that does not log in = %d %s, want 409", status, body)
	}
	if len(f.sources.passwords["acme/app"]) != 0 {
		t.Errorf("a refused password was written: %v", f.sources.passwords["acme/app"])
	}
	target := f.databases.targets[0]
	if target.Connection.User != role(t, "app") || target.Connection.Host != "db.example.com" || target.Connection.Password != "Wrong-p4ssword" {
		t.Errorf("checked %+v, want the source's database as its role", target.Connection)
	}

	f.databases.failing = nil
	if status, body := send(t, f.handler, http.MethodPut, "/api/customer/sources/app/password", admin, `{"password":"New-p4ssword"}`); status != 204 {
		t.Fatalf("a new password = %d %s", status, body)
	}
	if got := f.sources.passwords["acme/app"]; strings.Join(got, ",") != "New-p4ssword" {
		t.Errorf("passwords written %v", got)
	}
	if !strings.Contains(f.log.String(), `"msg":"source password changed","audit":true,"tenant":"acme","user":"user_3","organization":"org_acme","source":"app"`) {
		t.Errorf("no audit line for the password in %s", f.log.String())
	}
	if strings.Contains(f.log.String(), "p4ssword") {
		t.Error("a password was logged")
	}
	if status, body := send(t, f.handler, http.MethodPut, "/api/customer/sources/app/password", admin, `{"password":""}`); status != 400 {
		t.Errorf("an empty password = %d %s, want 400", status, body)
	}
}

func TestConfirmingADeletedSourcesSlotDropped(t *testing.T) {
	f := newFixture(t)
	source := f.sources.addApp("public.orders")
	admin := f.signer.token(t, session{user: "user_3", org: "org_acme", role: "admin"})
	if status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources/app/slot-dropped", admin, ""); status != 409 {
		t.Errorf("confirming the slot of a running source dropped = %d %s, want 409", status, body)
	}
	source.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	if status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources/app/slot-dropped", admin, ""); status != 204 {
		t.Fatalf("confirming the slot of a deleted source dropped = %d %s", status, body)
	}
	if strings.Join(f.sources.slotDropped, ",") != "acme/app" {
		t.Errorf("confirmed %v", f.sources.slotDropped)
	}
	if !strings.Contains(f.log.String(), `"msg":"source slot confirmed dropped","audit":true`) {
		t.Errorf("no audit line in %s", f.log.String())
	}
}

func TestSourcesTopicsStayTheirs(t *testing.T) {
	f := newFixture(t)
	f.sources.addApp("public.orders")
	f.clusters.topics["acme"] = []topics.Topic{
		{Name: "app.public.orders", Partitions: 1, ReplicationFactor: 1},
		{Name: "changeport.connect.offsets", Partitions: 25, ReplicationFactor: 1},
	}
	f.clusters.groups["app.public.orders"] = []string{"changeport-connect", "warehouse"}
	admin := f.signer.token(t, session{user: "user_3", org: "org_acme", role: "admin"})

	_, body, _ := get(t, f.handler, "/api/customer/topics", admin)
	list := decode[api.TopicList](t, body).Topics
	var names []string
	for _, topic := range list {
		names = append(names, topic.Name)
		if topic.Name == "app.public.orders" && (topic.Source == nil || *topic.Source != "app") {
			t.Errorf("topic %+v, want it marked as source app's", topic)
		}
		if topic.Name == "acme-orders" && topic.Source != nil {
			t.Errorf("topic %+v, want no source", topic)
		}
	}
	if strings.Join(names, ",") != "acme-orders,app.public.orders" {
		t.Errorf("topics %v, want the platform's left out", names)
	}

	if status, body := send(t, f.handler, http.MethodDelete, "/api/customer/topics/app.public.orders", admin, ""); status != 409 || !strings.Contains(body, "Postgres source app writes") {
		t.Errorf("deleting a source's topic = %d %s, want 409", status, body)
	}
	if status, body := send(t, f.handler, http.MethodPost, "/api/customer/topics", admin,
		`{"name":"app.public.items","partitions":1,"retentionMs":3600000,"cleanupPolicy":"delete"}`); status != 400 || !strings.Contains(body, "belong to the Postgres source app") {
		t.Errorf("creating a topic under a source's prefix = %d %s, want 400", status, body)
	}
	if len(f.clusters.deleted) != 0 || len(f.clusters.topics["acme"]) != 2 {
		t.Errorf("refused requests changed topics: %v %v", f.clusters.deleted, f.clusters.topics["acme"])
	}

	_, body, _ = get(t, f.handler, "/api/customer/topics/app.public.orders/consumer-groups", admin)
	if groups := decode[api.ConsumerGroupList](t, body).Groups; strings.Join(groups, ",") != "warehouse" {
		t.Errorf("groups of a source's topic %v, want the platform's left out", groups)
	}
	_, body, _ = get(t, f.handler, "/api/customer/consumer-groups", admin)
	if strings.Contains(body, "changeport") {
		t.Errorf("consumer groups %s, want the platform's left out", body)
	}
}

func TestAddingTablesListsThemThroughTheController(t *testing.T) {
	f := newFixture(t)
	source := f.sources.addApp("public.orders")
	// The source's own topic is no collision; another existing one is.
	f.clusters.topics["acme"] = []topics.Topic{
		{Name: "app.public.orders", Partitions: 1, ReplicationFactor: 1},
		{Name: "app.public.invoices", Partitions: 1, ReplicationFactor: 1},
	}
	admin := f.signer.token(t, session{user: "user_3", org: "org_acme", role: "admin"})
	status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources/app/catalog", admin, "")
	if catalog := decode[api.SourceCatalog](t, body); status != 202 || catalog.Ready || len(catalog.Tables) != 0 {
		t.Fatalf("asking for the tables = %d %s, want 202 and not ready", status, body)
	}
	requested := source.Annotations[v1alpha1.CatalogRequestAnnotation]
	if requested == "" {
		t.Fatal("the request did not reach the source")
	}

	// The controller answers an older request, then this one.
	source.Status.Catalog = &v1alpha1.TableCatalog{RequestedAt: "older", ListedAt: metav1.Now()}
	if _, body, _ := get(t, f.handler, "/api/customer/sources/app/catalog", admin); decode[api.SourceCatalog](t, body).Ready {
		t.Errorf("an answer to an older request is ready: %s", body)
	}
	source.Status.Catalog = &v1alpha1.TableCatalog{RequestedAt: requested, ListedAt: metav1.Now(), Truncated: true, Tables: []v1alpha1.CatalogTable{
		{Schema: "public", Name: "orders", EstimatedRows: 10, Readable: true, Keyed: true},
		{Schema: "public", Name: "invoices", EstimatedRows: 5, Keyed: true},
		{Schema: "public", Name: "events", EstimatedRows: -1, Reason: "Its primary key has a timestamp column."},
	}}
	_, body, _ = get(t, f.handler, "/api/customer/sources/app/catalog", admin)
	catalog := decode[api.SourceCatalog](t, body)
	if !catalog.Ready || !catalog.Truncated || catalog.ListedAt == nil || len(catalog.Tables) != 3 {
		t.Fatalf("catalog %s", body)
	}
	if orders := catalog.Tables[0]; orders.Topic != "app.public.orders" || orders.TopicProblem != nil {
		t.Errorf("orders %+v, want its own topic without a problem", orders)
	}
	if invoices := catalog.Tables[1]; invoices.TopicProblem == nil || !strings.Contains(*invoices.TopicProblem, "exists already") {
		t.Errorf("invoices %+v, want its topic's collision", invoices)
	}
	if events := catalog.Tables[2]; events.Keyed || events.Reason == nil {
		t.Errorf("events %+v, want it unkeyed with the reason", events)
	}

	source.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	if status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources/app/catalog", admin, ""); status != 409 {
		t.Errorf("asking for a deleted source's tables = %d %s, want 409", status, body)
	}
	if status, body := send(t, f.handler, http.MethodPost, "/api/customer/sources/unknown/catalog", admin, ""); status != 404 {
		t.Errorf("asking for an unknown source's tables = %d %s, want 404", status, body)
	}
}
