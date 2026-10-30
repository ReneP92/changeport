package controller

import (
	"slices"
	"strconv"
	"strings"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/cdc"
	"github.com/ReneP92/changeport/internal/provisioning"
)

// incrementalSnapshot is the aggregate type of Debezium's notifications about
// incremental snapshots.
const incrementalSnapshot = "Incremental Snapshot"

// syncTables keeps the status's tables in step with the spec's: a table
// added to the spec waits for its snapshot, and a table removed from it
// leaves the status. Its topic stays, as an ordinary topic.
func syncTables(source *v1alpha1.PostgresSource, status *v1alpha1.PostgresSourceStatus) {
	var tables []v1alpha1.TableStatus
	for _, table := range source.Spec.Tables {
		i := slices.IndexFunc(status.Tables, func(t v1alpha1.TableStatus) bool { return t.Schema == table.Schema && t.Name == table.Name })
		if i >= 0 {
			tables = append(tables, status.Tables[i])
			continue
		}
		tables = append(tables, v1alpha1.TableStatus{
			Schema: table.Schema, Name: table.Name, Snapshot: v1alpha1.SnapshotPending,
			Topic: cdc.TopicName(source.Name, cdc.Table{Schema: table.Schema, Name: table.Name}),
		})
	}
	status.Tables = tables
}

// applyNotifications folds Debezium's notifications into the tables'
// snapshot states. Each notification sets a state rather than adding to it,
// so reading one again changes nothing.
func applyNotifications(tables []v1alpha1.TableStatus, notifications []Notification) {
	table := func(collection string) *v1alpha1.TableStatus {
		for i := range tables {
			if tables[i].Schema+"."+tables[i].Name == collection {
				return &tables[i]
			}
		}
		return nil
	}
	number := func(data map[string]string, key string) int64 {
		n, _ := strconv.ParseInt(data[key], 10, 64)
		return n
	}
	for _, n := range notifications {
		if n.AggregateType != incrementalSnapshot {
			continue
		}
		switch n.Type {
		case "STARTED":
			for collection := range strings.SplitSeq(n.Data["data_collections"], ",") {
				if t := table(collection); t != nil && t.Snapshot != v1alpha1.SnapshotDone {
					t.Snapshot, t.RowsCopied, t.Message = v1alpha1.SnapshotRunning, 0, ""
				}
			}
		case "IN_PROGRESS":
			if t := table(n.Data["current_collection_in_progress"]); t != nil {
				t.Snapshot, t.Message = v1alpha1.SnapshotRunning, ""
				t.RowsEstimated = number(n.Data, "total_rows")
				// A chunk holds up to SnapshotChunkSize rows; the last one is
				// counted when the table is done.
				t.RowsCopied = min(number(n.Data, "chunk_index")*provisioning.SnapshotChunkSize, t.RowsEstimated)
			}
		case "TABLE_SCAN_COMPLETED":
			t := table(n.Data["scanned_collection"])
			if t == nil {
				continue
			}
			switch status := n.Data["status"]; status {
			case "SUCCEEDED", "EMPTY":
				// A read-only snapshot reads a chunk again while transactions
				// commit during it, so it may scan a row more than once:
				// rows arrive at least once. The rows copied are the table's.
				t.Snapshot, t.Message = v1alpha1.SnapshotDone, ""
				t.RowsCopied = number(n.Data, "total_rows_scanned")
				if _, ok := n.Data["total_rows"]; ok {
					t.RowsEstimated = number(n.Data, "total_rows")
					t.RowsCopied = min(t.RowsCopied, t.RowsEstimated)
				}
				t.RowsEstimated = max(t.RowsEstimated, t.RowsCopied)
			default:
				t.Snapshot, t.Message = v1alpha1.SnapshotFailed, scanFailure(status)
			}
		case "ABORTED":
			for collection := range strings.SplitSeq(n.Data["data_collections"], ",") {
				if t := table(collection); t != nil && t.Snapshot == v1alpha1.SnapshotRunning {
					t.Snapshot, t.Message = v1alpha1.SnapshotFailed, "The snapshot was aborted."
				}
			}
		}
	}
}

// scanFailure explains why Debezium could not copy a table.
func scanFailure(status string) string {
	switch status {
	case "NO_PRIMARY_KEY":
		return "The table has no primary key, which the snapshot copies by."
	case "SQL_EXCEPTION":
		return "The database refused to read the table. Check that the role may read it."
	case "UNKNOWN_SCHEMA":
		return "The connector does not know the table. Check that it is in the publication."
	case "SKIPPED":
		return "The connector skipped the table."
	default:
		return "The snapshot failed: " + status + "."
	}
}

// snapshotting counts the tables whose snapshot is pending or running.
func snapshotting(tables []v1alpha1.TableStatus) int {
	count := 0
	for _, t := range tables {
		if t.Snapshot == v1alpha1.SnapshotPending || t.Snapshot == v1alpha1.SnapshotRunning {
			count++
		}
	}
	return count
}
