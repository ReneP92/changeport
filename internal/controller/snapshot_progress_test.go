package controller

import (
	"slices"
	"testing"

	"github.com/ReneP92/changeport/api/v1alpha1"
)

func note(kind string, data map[string]string) Notification {
	return Notification{Type: kind, AggregateType: incrementalSnapshot, Data: data}
}

// The notifications of a snapshot of two tables, as Debezium 3.7 sent them
// on kind; orders has three chunks here.
var snapshotOfTwoTables = []Notification{
	{Type: "SKIPPED", AggregateType: "Initial Snapshot"},
	note("STARTED", map[string]string{"data_collections": "public.customers,public.orders"}),
	note("DATA_COLLECTIONS_RESOLVED", map[string]string{"data_collections": "public.customers,public.orders"}),
	note("IN_PROGRESS", map[string]string{"current_collection_in_progress": "public.customers", "chunk_index": "1", "total_chunks": "1", "total_rows": "100"}),
	note("TABLE_SCAN_COMPLETED", map[string]string{"scanned_collection": "public.customers", "total_rows_scanned": "100", "status": "SUCCEEDED"}),
	note("IN_PROGRESS", map[string]string{"current_collection_in_progress": "public.orders", "chunk_index": "1", "total_chunks": "3", "total_rows": "3000"}),
	note("IN_PROGRESS", map[string]string{"current_collection_in_progress": "public.orders", "chunk_index": "2", "total_chunks": "3", "total_rows": "3000"}),
}

func pendingTables(names ...string) []v1alpha1.TableStatus {
	var tables []v1alpha1.TableStatus
	for _, name := range names {
		tables = append(tables, v1alpha1.TableStatus{Schema: "public", Name: name, Snapshot: v1alpha1.SnapshotPending})
	}
	return tables
}

func TestApplyNotifications(t *testing.T) {
	tables := pendingTables("customers", "orders", "notes")
	applyNotifications(tables, snapshotOfTwoTables)
	want := []v1alpha1.TableStatus{
		{Schema: "public", Name: "customers", Snapshot: v1alpha1.SnapshotDone, RowsCopied: 100, RowsEstimated: 100},
		{Schema: "public", Name: "orders", Snapshot: v1alpha1.SnapshotRunning, RowsCopied: 2048, RowsEstimated: 3000},
		{Schema: "public", Name: "notes", Snapshot: v1alpha1.SnapshotPending},
	}
	if !slices.Equal(tables, want) {
		t.Errorf("tables =\n%+v\nwant\n%+v", tables, want)
	}

	// Reading the notifications again changes nothing.
	applyNotifications(tables, snapshotOfTwoTables)
	if !slices.Equal(tables, want) {
		t.Errorf("after reading them again, tables =\n%+v", tables)
	}

	applyNotifications(tables, []Notification{
		note("TABLE_SCAN_COMPLETED", map[string]string{"scanned_collection": "public.orders", "total_rows_scanned": "3017", "status": "SUCCEEDED"}),
		note("COMPLETED", nil),
	})
	if tables[1].Snapshot != v1alpha1.SnapshotDone || tables[1].RowsCopied != 3017 || tables[1].RowsEstimated != 3017 {
		t.Errorf("orders = %+v, want done with its 3017 rows", tables[1])
	}

	// A chunk read twice, as read-only snapshots do while transactions
	// commit: the table's 1000 rows were copied, some twice.
	tables = pendingTables("orders")
	applyNotifications(tables, []Notification{
		note("IN_PROGRESS", map[string]string{"current_collection_in_progress": "public.orders", "chunk_index": "1", "total_rows": "1000"}),
		note("IN_PROGRESS", map[string]string{"current_collection_in_progress": "public.orders", "chunk_index": "1", "total_rows": "1000"}),
		note("TABLE_SCAN_COMPLETED", map[string]string{"scanned_collection": "public.orders", "total_rows": "1000", "total_rows_scanned": "2000", "status": "SUCCEEDED"}),
	})
	if tables[0].RowsCopied != 1000 || tables[0].RowsEstimated != 1000 {
		t.Errorf("orders = %+v, want its 1000 rows copied", tables[0])
	}
}

func TestApplyNotificationsOfFailures(t *testing.T) {
	tables := pendingTables("a", "b", "c")
	applyNotifications(tables, []Notification{
		note("STARTED", map[string]string{"data_collections": "public.a,public.b,public.c"}),
		note("TABLE_SCAN_COMPLETED", map[string]string{"scanned_collection": "public.a", "status": "EMPTY", "total_rows_scanned": "0"}),
		note("TABLE_SCAN_COMPLETED", map[string]string{"scanned_collection": "public.b", "status": "SQL_EXCEPTION"}),
		note("ABORTED", map[string]string{"data_collections": "public.c"}),
		// Another aggregate's notifications are ignored.
		{Type: "TABLE_SCAN_COMPLETED", AggregateType: "Initial Snapshot", Data: map[string]string{"scanned_collection": "public.c", "status": "SUCCEEDED"}},
	})
	for i, want := range []struct{ state, message string }{
		{v1alpha1.SnapshotDone, ""},
		{v1alpha1.SnapshotFailed, "The database refused to read the table. Check that the role may read it."},
		{v1alpha1.SnapshotFailed, "The snapshot was aborted."},
	} {
		if tables[i].Snapshot != want.state || tables[i].Message != want.message {
			t.Errorf("%s = %s %q, want %s %q", tables[i].Name, tables[i].Snapshot, tables[i].Message, want.state, want.message)
		}
	}
}

func TestSyncTables(t *testing.T) {
	source := newSource()
	source.Spec.Tables = []v1alpha1.PostgresTable{{Schema: "public", Name: "orders"}, {Schema: "public", Name: "invoices"}}
	status := &v1alpha1.PostgresSourceStatus{Tables: []v1alpha1.TableStatus{
		{Schema: "public", Name: "orders", Topic: "app.public.orders", Snapshot: v1alpha1.SnapshotDone, RowsCopied: 10},
		{Schema: "Sales", Name: "Order Lines", Topic: "app.Sales.Order_Lines", Snapshot: v1alpha1.SnapshotDone},
	}}
	syncTables(source, status)
	want := []v1alpha1.TableStatus{
		{Schema: "public", Name: "orders", Topic: "app.public.orders", Snapshot: v1alpha1.SnapshotDone, RowsCopied: 10},
		{Schema: "public", Name: "invoices", Topic: "app.public.invoices", Snapshot: v1alpha1.SnapshotPending},
	}
	if !slices.Equal(status.Tables, want) {
		t.Errorf("tables = %+v, want %+v", status.Tables, want)
	}
}
