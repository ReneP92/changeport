package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/cdc"
	"github.com/ReneP92/changeport/internal/provisioning"
)

// fakeSlots reports the slot a test sets, and records what it was asked.
type fakeSlots struct {
	state   cdc.SlotState
	err     error
	dropErr error
	calls   []string
	// last is the connection of the last call.
	last cdc.Connection
}

func (f *fakeSlots) Slot(_ context.Context, connection cdc.Connection, slot string) (cdc.SlotState, error) {
	f.calls = append(f.calls, "read "+slot)
	f.last = connection
	return f.state, f.err
}

func (f *fakeSlots) DropSlot(_ context.Context, connection cdc.Connection, slot string) (bool, error) {
	f.calls = append(f.calls, "drop "+slot)
	f.last = connection
	return true, f.dropErr
}

const sourcesNamespaceName = "customer-acme"

func newSource() *v1alpha1.PostgresSource {
	source := validSource(sourcesNamespaceName, "app")
	source.Generation = 1
	source.Spec.Connection.Port = 5432
	source.Spec.Connection.TLS = v1alpha1.PostgresTLS{Mode: v1alpha1.TLSVerifyFull, CA: "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"}
	source.Spec.State = v1alpha1.StateRunning
	return source
}

func passwordSecret(name, password string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: sourcesNamespaceName, Name: name},
		Type:       v1alpha1.PostgresPasswordSecretType,
		Data:       map[string][]byte{v1alpha1.PostgresPasswordKey: []byte(password)},
	}
}

// connectorWith is the source's KafkaConnector as Strimzi reports it.
func connectorWith(connectorState string, tasks ...map[string]any) *unstructured.Unstructured {
	connector := &unstructured.Unstructured{}
	connector.SetGroupVersionKind(kafkaConnectorGVK)
	connector.SetNamespace(sourcesNamespaceName)
	connector.SetName("app-x1y2z3")
	taskList := make([]any, len(tasks))
	for i, task := range tasks {
		taskList[i] = task
	}
	connector.Object["status"] = map[string]any{"connectorStatus": map[string]any{
		"connector": map[string]any{"state": connectorState}, "tasks": taskList,
	}}
	return connector
}

var testNow = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

func newSourceReconciler(t *testing.T, slots *fakeSlots, objects ...client.Object) (*PostgresSourceReconciler, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithStatusSubresource(&v1alpha1.PostgresSource{}).Build()
	return &PostgresSourceReconciler{
		Client: c, Slots: slots, PollInterval: pollInterval, SlotInterval: time.Minute,
		Now: func() time.Time { return testNow },
	}, c
}

func reconcileSource(t *testing.T, r *PostgresSourceReconciler) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: sourcesNamespaceName, Name: "app"}})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func getSource(t *testing.T, c client.Client) *v1alpha1.PostgresSource {
	t.Helper()
	var source v1alpha1.PostgresSource
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: sourcesNamespaceName, Name: "app"}, &source); err != nil {
		t.Fatal(err)
	}
	return &source
}

func sourceReady(t *testing.T, source *v1alpha1.PostgresSource) metav1.Condition {
	t.Helper()
	condition := meta.FindStatusCondition(source.Status.Conditions, v1alpha1.ConditionReady)
	if condition == nil {
		t.Fatalf("no Ready condition: %+v", source.Status)
	}
	return *condition
}

func credentials(t *testing.T, c client.Client) map[string]string {
	t.Helper()
	var secret corev1.Secret
	err := c.Get(t.Context(), types.NamespacedName{Namespace: sourcesNamespaceName, Name: provisioning.ConnectSourcesSecret}, &secret)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	data := map[string]string{}
	for key, value := range secret.Data {
		data[key] = string(value)
	}
	return data
}

func TestSourceWaitsForItsPassword(t *testing.T) {
	r, c := newSourceReconciler(t, &fakeSlots{}, newSource())
	if result := reconcileSource(t, r); result.RequeueAfter != pollInterval {
		t.Errorf("requeue after %v, want %v", result.RequeueAfter, pollInterval)
	}
	source := getSource(t, c)
	ready := sourceReady(t, source)
	if source.Status.Phase != v1alpha1.PhasePending || ready.Reason != v1alpha1.SourceReasonWaitingForPassword || ready.Message != "Waiting for the password Secret postgres-password-app-1." {
		t.Errorf("status = %s, %s %q", source.Status.Phase, ready.Reason, ready.Message)
	}
	if !strings.Contains(strings.Join(source.Finalizers, ","), v1alpha1.SourceFinalizer) {
		t.Error("the source has no finalizer")
	}

	// A Secret of another type, which the console never writes, is left
	// alone.
	other := passwordSecret("postgres-password-app-1", "s3cret")
	other.Type = corev1.SecretTypeOpaque
	r, c = newSourceReconciler(t, &fakeSlots{}, newSource(), other)
	reconcileSource(t, r)
	if credentials(t, c) != nil || getSource(t, c).Status.PasswordSecret != "" {
		t.Error("an Opaque Secret was handed over")
	}
}

func TestSourceHandsItsPasswordOver(t *testing.T) {
	r, c := newSourceReconciler(t, &fakeSlots{}, newSource(), passwordSecret("postgres-password-app-1", "s3cret"))
	reconcileSource(t, r)
	got := credentials(t, c)
	if got["app"] != "s3cret" || !strings.HasPrefix(got["app.ca"], "-----BEGIN CERTIFICATE-----") {
		t.Errorf("the Connect worker's credentials = %v", got)
	}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: sourcesNamespaceName, Name: "postgres-password-app-1"}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Errorf("the console's password Secret: %v, want deleted", err)
	}
	source := getSource(t, c)
	if source.Status.PasswordSecret != "postgres-password-app-1" || sourceReady(t, source).Reason != v1alpha1.SourceReasonStarting {
		t.Errorf("status = %+v", source.Status)
	}

	// A new password replaces the old; dropping the CA removes it.
	source.Spec.Connection.PasswordSecret = "postgres-password-app-2"
	source.Spec.Connection.TLS.CA = ""
	if err := c.Update(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(t.Context(), passwordSecret("postgres-password-app-2", "n3w")); err != nil {
		t.Fatal(err)
	}
	reconcileSource(t, r)
	if got := credentials(t, c); got["app"] != "n3w" || got["app.ca"] != "" {
		t.Errorf("after a new password, the credentials = %v", got)
	}
	if got := getSource(t, c).Status.PasswordSecret; got != "postgres-password-app-2" {
		t.Errorf("password Secret = %q", got)
	}
}

func handedOver() *v1alpha1.PostgresSource {
	source := newSource()
	source.Finalizers = []string{v1alpha1.SourceFinalizer}
	source.Status.PasswordSecret = "postgres-password-app-1"
	return source
}

func sourcesSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: sourcesNamespaceName, Name: provisioning.ConnectSourcesSecret},
		Data:       map[string][]byte{"app": []byte("s3cret"), "app.ca": []byte("CA"), "billing": []byte("other")},
	}
}

func TestSourceReportsItsConnector(t *testing.T) {
	running := map[string]any{"id": int64(0), "state": "RUNNING"}
	for _, test := range []struct {
		name      string
		state     string
		connector *unstructured.Unstructured
		phase     string
		reason    string
		message   string
	}{
		{"streaming", v1alpha1.StateRunning, connectorWith("RUNNING", running), v1alpha1.PhaseStreaming, v1alpha1.SourceReasonStreaming, "Changes stream from 2 tables."},
		{"a task starting", v1alpha1.StateRunning, connectorWith("RUNNING", map[string]any{"id": int64(0), "state": "UNASSIGNED"}), v1alpha1.PhasePending, v1alpha1.SourceReasonStarting, "The connector is starting."},
		{"a failed task", v1alpha1.StateRunning, connectorWith("RUNNING", map[string]any{
			"id": int64(0), "state": "FAILED",
			"trace": "org.apache.kafka.connect.errors.ConnectException: Error\n\tat x\nCaused by: org.postgresql.util.PSQLException: FATAL: password authentication failed for user \"changeport_app_x1y2z3\"",
		}), v1alpha1.PhaseFailed, v1alpha1.SourceReasonFailed, "The database refused the password of role changeport_app_x1y2z3. Run the SQL for a new password, and give it to Changeport."},
		{"stopped", v1alpha1.StateStopped, connectorWith("STOPPED"), v1alpha1.PhaseStopped, v1alpha1.SourceReasonStopped, "The source is stopped. Its slot keeps the database's WAL until it runs again."},
		{"no connector yet", v1alpha1.StateRunning, nil, v1alpha1.PhasePending, v1alpha1.SourceReasonStarting, "Waiting for Kafka Connect to start the connector."},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := handedOver()
			source.Spec.State = test.state
			objects := []client.Object{source, sourcesSecret()}
			if test.connector != nil {
				objects = append(objects, test.connector)
			}
			r, c := newSourceReconciler(t, &fakeSlots{}, objects...)
			result := reconcileSource(t, r)
			got := getSource(t, c)
			ready := sourceReady(t, got)
			if got.Status.Phase != test.phase || ready.Reason != test.reason || ready.Message != test.message {
				t.Errorf("status = %s, %s %q; want %s, %s %q", got.Status.Phase, ready.Reason, ready.Message, test.phase, test.reason, test.message)
			}
			if (ready.Status == metav1.ConditionTrue) != (test.phase == v1alpha1.PhaseStreaming) {
				t.Errorf("Ready = %s in phase %s", ready.Status, got.Status.Phase)
			}
			wantRequeue := pollInterval
			if test.phase == v1alpha1.PhaseStreaming || test.phase == v1alpha1.PhaseStopped {
				wantRequeue = time.Minute
			}
			if result.RequeueAfter != wantRequeue {
				t.Errorf("requeue after %v, want %v", result.RequeueAfter, wantRequeue)
			}
		})
	}
}

func TestSourceReadsItsSlot(t *testing.T) {
	safe := int64(900_000_000)
	slots := &fakeSlots{state: cdc.SlotState{Exists: true, Active: true, RetainedBytes: 123_456, WALStatus: "reserved", SafeWALSize: &safe}}
	r, c := newSourceReconciler(t, slots, handedOver(), sourcesSecret(), connectorWith("RUNNING", map[string]any{"id": int64(0), "state": "RUNNING"}))
	reconcileSource(t, r)
	slot := getSource(t, c).Status.Slot
	if slot == nil || !slot.Active || slot.RetainedWALBytes != 123_456 || slot.WALStatus != "reserved" || *slot.SafeWALSizeBytes != safe || !slot.CheckedAt.Time.Equal(testNow) {
		t.Errorf("slot = %+v", slot)
	}
	if got := testutil.ToFloat64(retainedWAL.WithLabelValues("acme", "app")); got != 123_456 {
		t.Errorf("retained WAL metric = %v", got)
	}
	want := cdc.Connection{
		Host: "app.abc123.us-east-1.rds.amazonaws.com", Port: 5432, Database: "app", User: "changeport_app_x1y2z3",
		Password: "s3cret", TLSMode: cdc.VerifyFull, CA: "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n",
	}
	if slots.last != want {
		t.Errorf("connection = %+v, want %+v", slots.last, want)
	}

	// Within the slot interval, the slot is not read again.
	slots.calls = nil
	reconcileSource(t, r)
	if len(slots.calls) != 0 {
		t.Errorf("read the slot again within a minute: %v", slots.calls)
	}
	// A database it cannot reach leaves the last reading.
	r.Now = func() time.Time { return testNow.Add(2 * time.Minute) }
	slots.err = errors.New("cannot reach")
	reconcileSource(t, r)
	if got := getSource(t, c).Status.Slot; got == nil || got.RetainedWALBytes != 123_456 {
		t.Errorf("after a failed reading, slot = %+v", got)
	}
}

func deletedSource(source *v1alpha1.PostgresSource) *v1alpha1.PostgresSource {
	source.DeletionTimestamp = &metav1.Time{Time: testNow}
	return source
}

func TestDeletedSourceDropsItsSlot(t *testing.T) {
	slots := &fakeSlots{}
	inbox := passwordSecret("postgres-password-app-1", "unused")
	r, c := newSourceReconciler(t, slots, deletedSource(handedOver()), sourcesSecret(), connectorWith("RUNNING"), inbox)

	// The connector goes first.
	if result := reconcileSource(t, r); result.RequeueAfter != pollInterval {
		t.Errorf("requeue after %v while the connector runs", result.RequeueAfter)
	}
	if ready := sourceReady(t, getSource(t, c)); ready.Message != "Stopping the connector before dropping the replication slot changeport_app_x1y2z3." || len(slots.calls) != 0 {
		t.Errorf("Ready = %q, calls %v", ready.Message, slots.calls)
	}
	if err := c.Delete(t.Context(), connectorWith("")); err != nil {
		t.Fatal(err)
	}

	// While the connector still holds the slot, it waits.
	slots.dropErr = cdc.ErrSlotActive
	reconcileSource(t, r)
	if ready := sourceReady(t, getSource(t, c)); ready.Reason != v1alpha1.SourceReasonDeleting || !strings.Contains(ready.Message, "let go of the replication slot") {
		t.Errorf("Ready = %s %q", ready.Reason, ready.Message)
	}

	// Without the database, it shows the SQL and keeps trying.
	slots.dropErr = errors.New("Changeport cannot reach db:5432: i/o timeout.")
	if result := reconcileSource(t, r); result.RequeueAfter != time.Minute {
		t.Errorf("requeue after %v while the database is unreachable", result.RequeueAfter)
	}
	ready := sourceReady(t, getSource(t, c))
	if ready.Reason != v1alpha1.SourceReasonDatabaseUnreachable ||
		ready.Message != "Changeport cannot drop the replication slot changeport_app_x1y2z3: Changeport cannot reach db:5432: i/o timeout. It keeps trying. To drop it yourself, run: SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'changeport_app_x1y2z3';" {
		t.Errorf("Ready = %s %q", ready.Reason, ready.Message)
	}

	// Once dropped, the credentials go and so does the source.
	slots.dropErr = nil
	reconcileSource(t, r)
	if got := credentials(t, c); got["app"] != "" || got["app.ca"] != "" || got["billing"] != "other" {
		t.Errorf("credentials after the drop = %v, want only billing's", got)
	}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: sourcesNamespaceName, Name: "app"}, &v1alpha1.PostgresSource{}); !apierrors.IsNotFound(err) {
		t.Errorf("the source: %v, want gone", err)
	}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: sourcesNamespaceName, Name: inbox.Name}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Errorf("a password Secret not handed over: %v, want deleted", err)
	}
}

func TestDeletedSourceWhoseSlotTheCustomerDropped(t *testing.T) {
	source := deletedSource(handedOver())
	source.Annotations = map[string]string{v1alpha1.SlotDroppedAnnotation: "true"}
	slots := &fakeSlots{}
	r, c := newSourceReconciler(t, slots, source, sourcesSecret())
	reconcileSource(t, r)
	if len(slots.calls) != 0 {
		t.Errorf("calls = %v, want none once the customer dropped the slot", slots.calls)
	}
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: sourcesNamespaceName, Name: "app"}, &v1alpha1.PostgresSource{}); !apierrors.IsNotFound(err) {
		t.Errorf("the source: %v, want gone", err)
	}
}

func TestExplain(t *testing.T) {
	source := newSource()
	names, _ := cdc.NamesFor("app", "x1y2z3")
	for trace, want := range map[string]string{
		"PSQLException: FATAL: no pg_hba.conf entry for host \"1.2.3.4\", user \"x\", database \"app\", no encryption":    "The database refused the connection: no pg_hba.conf entry for host \"1.2.3.4\", user \"x\", database \"app\", no encryption. Let the role connect from Changeport's addresses over TLS.",
		"ERROR: replication slot \"changeport_app_x1y2z3\" can no longer get changes":                                     "The database lost the replication slot changeport_app_x1y2z3, probably because it held back more WAL than max_slot_wal_keep_size allows. The source needs a new slot and a new snapshot.",
		"ERROR: publication \"changeport_app_x1y2z3\" does not exist":                                                     "The publication changeport_app_x1y2z3 does not exist. Run the second step's SQL.",
		"ERROR: permission denied for table orders":                                                                       "The database refused the role access: permission denied for table orders. Run the SQL that grants it.",
		"javax.net.ssl.SSLHandshakeException: PKIX path building failed":                                                  "The certificate of app.abc123.us-east-1.rds.amazonaws.com is not trusted: SSLHandshakeException: PKIX path building failed.",
		"java.net.UnknownHostException: app.abc123.us-east-1.rds.amazonaws.com":                                           "Changeport cannot reach app.abc123.us-east-1.rds.amazonaws.com on port 5432. Check that the database is publicly accessible and lets Changeport's addresses in.",
		"java.io.IOException: Could not read /mnt/sources/app":                                                            "Kafka Connect has not received the source's credentials yet; it retries.",
		"org.apache.kafka.connect.errors.ConnectException: boom\n\tat a\nCaused by: java.lang.IllegalStateException: odd": "The connector failed: java.lang.IllegalStateException: odd",
	} {
		if got := Explain(trace, source, names); got != want {
			t.Errorf("Explain(%q) =\n%q\nwant\n%q", trace, got, want)
		}
	}
}

// The Customer controller renders a source's connector once its password
// reached the Connect worker, and drops it once the source is deleted.
func TestConnectorOfASource(t *testing.T) {
	source := newSource()
	if connector(source) != nil {
		t.Error("a source without its password handed over has a connector")
	}
	source.Status.PasswordSecret = "postgres-password-app-1"
	source.Spec.RetentionMs = 3600000
	source.Spec.State = v1alpha1.StateStopped
	got := connector(source)
	if got == nil {
		t.Fatal("no connector")
	}
	want := provisioning.Connector{
		Name: "app-x1y2z3", Host: "app.abc123.us-east-1.rds.amazonaws.com", Port: 5432, Database: "app",
		Role: "changeport_app_x1y2z3", Publication: "changeport_app_x1y2z3", Slot: "changeport_app_x1y2z3",
		TLSMode: "verify-full", CA: true, TableIncludeList: `public\.orders,Sales\.Order\ Lines`,
		RetentionMs: 3600000, Running: false, CredentialsVersion: got.CredentialsVersion,
	}
	if *got != want {
		t.Errorf("connector = %+v, want %+v", *got, want)
	}
	before := got.CredentialsVersion
	source.Spec.Connection.TLS.CA = "-----BEGIN CERTIFICATE-----\nOTHER\n-----END CERTIFICATE-----\n"
	if !strings.HasPrefix(before, "postgres-password-app-1/") || connector(source).CredentialsVersion == before {
		t.Errorf("credentials version %q does not change with the CA", before)
	}
	if connector(deletedSource(source)) != nil {
		t.Error("a deleted source has a connector")
	}
}

// A slot the connector has not created yet leaves no slot status, and is
// read again on the next pass.
func TestSourceWithoutItsSlotYet(t *testing.T) {
	slots := &fakeSlots{state: cdc.SlotState{}}
	r, c := newSourceReconciler(t, slots, handedOver(), sourcesSecret())
	reconcileSource(t, r)
	reconcileSource(t, r)
	if got := getSource(t, c).Status.Slot; got != nil {
		t.Errorf("slot = %+v, want none before it exists", got)
	}
	if len(slots.calls) != 2 {
		t.Errorf("calls = %v, want the slot read on each pass until it exists", slots.calls)
	}
}
