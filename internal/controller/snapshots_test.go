package controller

import (
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hamba/avro/v2"

	"github.com/ReneP92/changeport/internal/provisioning"
)

// notificationSchema is Debezium 3.7's, as Apicurio registered it on kind.
const notificationSchema = `{"type":"record","name":"Notification","namespace":"io.debezium.connector.common","fields":[{"name":"id","type":"string"},{"name":"type","type":"string"},{"name":"aggregate_type","type":"string"},{"name":"additional_data","type":["null",{"type":"map","values":"string"}],"default":null},{"name":"timestamp","type":"long"}],"connect.version":1,"connect.name":"io.debezium.connector.common.Notification"}`

// wire encodes a notification as the Apicurio converter does: Confluent's
// wire format with schema ID 96.
func wire(t *testing.T, record notificationRecord) []byte {
	t.Helper()
	encoded, err := avro.Marshal(avro.MustParse(notificationSchema), record)
	if err != nil {
		t.Fatal(err)
	}
	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header[1:], 96)
	return append(header, encoded...)
}

func TestDecodeNotifications(t *testing.T) {
	requests := 0
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/apis/ccompat/v7/schemas/ids/96" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": notificationSchema})
	}))
	defer registry.Close()
	snapshots := &KafkaSnapshots{RegistryURL: func(provisioning.Spec) string { return registry.URL }}
	source := newSource()

	data := map[string]string{"scanned_collection": "public.orders", "total_rows_scanned": "1000", "status": "SUCCEEDED"}
	got, err := snapshots.decode(t.Context(), source, wire(t, notificationRecord{
		ID: "1", Type: "TABLE_SCAN_COMPLETED", AggregateType: incrementalSnapshot, AdditionalData: &data, Timestamp: 1,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != "TABLE_SCAN_COMPLETED" || got.AggregateType != incrementalSnapshot || got.Data["total_rows_scanned"] != "1000" {
		t.Errorf("decode() = %+v", got)
	}

	// Without additional data, from the cached schema.
	got, err = snapshots.decode(t.Context(), source, wire(t, notificationRecord{ID: "2", Type: "COMPLETED", AggregateType: incrementalSnapshot}))
	if err != nil || got.Type != "COMPLETED" || got.Data != nil || requests != 1 {
		t.Errorf("decode() = %+v, %v after %d schema requests", got, err, requests)
	}

	// A tombstone, or a value in another format, is no notification.
	for _, value := range [][]byte{nil, []byte("{}")} {
		if got, err := snapshots.decode(t.Context(), source, value); got != nil || err != nil {
			t.Errorf("decode(%q) = %+v, %v", value, got, err)
		}
	}
}
