package controller

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/cdc"
	"github.com/ReneP92/changeport/internal/provisioning"
)

var kafkaConnectorGVK = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1", Kind: "KafkaConnector"}

// Database is what the controller does in sources' databases; cdc.Checker
// does it.
type Database interface {
	// Check checks the database for a source, before its tables' snapshots.
	Check(ctx context.Context, target cdc.Target) cdc.Report
	// Slot reads the source's replication slot.
	Slot(ctx context.Context, connection cdc.Connection, slot string) (cdc.SlotState, error)
	// DropSlot drops it.
	DropSlot(ctx context.Context, connection cdc.Connection, slot string) (bool, error)
	// ListTables lists the database's tables, for the console's table
	// picker.
	ListTables(ctx context.Context, connection cdc.Connection) ([]cdc.TableInfo, error)
}

var _ Database = cdc.Checker{}

// PostgresSourceReconciler takes every PostgresSource from created to
// streaming, and drops its slot once it is deleted (docs/cdc.md). The
// Customer controller renders each source's connector from the template;
// this controller does the rest:
//
//   - it moves the password from the console's write-once Secret into the
//     Connect worker's, with the source's CA;
//   - it reports the connector's state in the source's status, in plain
//     language;
//   - it asks for a snapshot of each table once, when the table is new and
//     the database checks pass, and follows its progress;
//   - it reads the slot every SlotInterval, and exports the WAL it keeps;
//   - it lists the database's tables when the console asks, as the console
//     cannot read the password;
//   - once the source is deleted and its connector gone, it drops the slot.
type PostgresSourceReconciler struct {
	Client    client.Client
	Database  Database
	Snapshots SnapshotChannel
	// PollInterval is how soon a source that is not streaming yet is
	// looked at again.
	PollInterval time.Duration
	// SlotInterval is how often a source's slot is read.
	SlotInterval time.Duration
	// Now is when the slot was read; time.Now by default.
	Now func() time.Time

	// signalled remembers the snapshots asked for in the last
	// signalMemory, by source UID and table, so that a status update that
	// failed after a signal never sends it again: Debezium copies a table
	// once per signal.
	mu        sync.Mutex
	signalled map[string]time.Time
}

// signalMemory is how long the controller remembers a snapshot it asked for.
const signalMemory = 10 * time.Minute

// SetupWithManager registers the reconciler with mgr.
func (r *PostgresSourceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.PostgresSource{}).
		Named("postgressource").
		WithOptions(controller.Options{MaxConcurrentReconciles: 4}).
		Complete(r)
}

// Reconcile implements reconcile.Reconciler.
func (r *PostgresSourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	result, err := r.reconcile(ctx, req)
	if apierrors.IsConflict(err) {
		// The source or the Connect worker's Secret changed since it was
		// read; the Secret's change triggers no reconciliation of its own.
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	return result, err
}

func (r *PostgresSourceReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var source v1alpha1.PostgresSource
	if err := r.Client.Get(ctx, req.NamespacedName, &source); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	names, err := cdc.NamesFor(source.Name, source.Spec.ID)
	if err != nil {
		// The CRD's validation keeps such sources out.
		return ctrl.Result{}, nil
	}
	if !source.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &source, names)
	}
	if !controllerutil.ContainsFinalizer(&source, v1alpha1.SourceFinalizer) {
		patch := client.MergeFromWithOptions(source.DeepCopy(), client.MergeFromWithOptimisticLock{})
		controllerutil.AddFinalizer(&source, v1alpha1.SourceFinalizer)
		if err := r.Client.Patch(ctx, &source, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("add the finalizer: %w", err)
		}
	}

	status := source.Status.DeepCopy()
	if err := r.handOver(ctx, &source, status); err != nil {
		return ctrl.Result{}, err
	}
	r.resnapshot(ctx, &source, status)
	ready, connector := r.connectorState(ctx, &source, names, status)
	if status.Phase == v1alpha1.PhaseStreaming {
		ready = r.followSnapshots(ctx, &source, names, status, connector, ready)
	}
	requeue := r.PollInterval
	// Snapshots are followed at the poll interval.
	if status.Phase == v1alpha1.PhaseStreaming || status.Phase == v1alpha1.PhaseStopped {
		requeue = r.SlotInterval
	}
	if status.PasswordSecret != "" && r.slotDue(status) {
		r.readSlot(ctx, &source, names, status)
	}
	if err := r.listCatalog(ctx, &source, names, status); err != nil {
		return ctrl.Result{}, err
	}
	setSourceCondition(status, source.Generation, ready)
	recordPhase(&source, status.Phase)
	return ctrl.Result{RequeueAfter: requeue}, r.updateStatus(ctx, &source, status)
}

// handOver moves the password from the Secret the console wrote into the
// Connect worker's Secret, with the source's CA, and deletes the console's.
// Once done, status.PasswordSecret names the Secret it came from.
func (r *PostgresSourceReconciler) handOver(ctx context.Context, source *v1alpha1.PostgresSource, status *v1alpha1.PostgresSourceStatus) error {
	key := client.ObjectKey{Namespace: source.Namespace, Name: source.Spec.Connection.PasswordSecret}
	var inbox corev1.Secret
	err := r.Client.Get(ctx, key, &inbox)
	switch {
	case apierrors.IsNotFound(err):
		// Already handed over, or not written yet; keep the CA current.
		if status.PasswordSecret == "" {
			return nil
		}
		return r.writeCredentials(ctx, source, nil)
	case err != nil:
		return fmt.Errorf("get the password Secret %s: %w", key.Name, err)
	}
	password := inbox.Data[v1alpha1.PostgresPasswordKey]
	if string(inbox.Type) != v1alpha1.PostgresPasswordSecretType || len(password) == 0 {
		// Not a Secret the console wrote; leave it alone.
		return nil
	}
	if err := r.writeCredentials(ctx, source, password); err != nil {
		return err
	}
	if err := r.Client.Delete(ctx, &inbox); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("delete the password Secret %s: %w", key.Name, err)
	}
	status.PasswordSecret = source.Spec.Connection.PasswordSecret
	return nil
}

// writeCredentials writes the source's password, unless nil, and CA into the
// Connect worker's Secret, as <source> and <source>.ca.
func (r *PostgresSourceReconciler) writeCredentials(ctx context.Context, source *v1alpha1.PostgresSource, password []byte) error {
	var secret corev1.Secret
	key := client.ObjectKey{Namespace: source.Namespace, Name: provisioning.ConnectSourcesSecret}
	err := r.Client.Get(ctx, key, &secret)
	if apierrors.IsNotFound(err) {
		secret = corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, Labels: map[string]string{
				"app.kubernetes.io/managed-by": provisioning.ManagedBy,
				provisioning.TenantLabel:       strings.TrimPrefix(source.Namespace, provisioning.NamespacePrefix),
			}},
			Data: map[string][]byte{},
		}
	} else if err != nil {
		return fmt.Errorf("get the Secret %s: %w", key.Name, err)
	}
	original := secret.DeepCopy()
	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}
	if password != nil {
		secret.Data[source.Name] = password
	}
	if ca := source.Spec.Connection.TLS.CA; ca != "" {
		secret.Data[source.Name+".ca"] = []byte(ca)
	} else {
		delete(secret.Data, source.Name+".ca")
	}
	switch {
	case secret.ResourceVersion == "":
		err = r.Client.Create(ctx, &secret)
	case !equality.Semantic.DeepEqual(original.Data, secret.Data):
		err = r.Client.Update(ctx, &secret)
	}
	if err != nil {
		return fmt.Errorf("write source %s's credentials into the Secret %s: %w", source.Name, key.Name, err)
	}
	return nil
}

// connectorState sets the source's phase from its connector's state, which
// Strimzi reports, and returns its Ready condition and the connector, if any.
func (r *PostgresSourceReconciler) connectorState(ctx context.Context, source *v1alpha1.PostgresSource, names cdc.Names, status *v1alpha1.PostgresSourceStatus) (metav1.Condition, *unstructured.Unstructured) {
	condition := func(phase string, ready metav1.ConditionStatus, reason, message string) metav1.Condition {
		status.Phase = phase
		return metav1.Condition{Type: v1alpha1.ConditionReady, Status: ready, Reason: reason, Message: message}
	}
	if status.PasswordSecret == "" {
		return condition(v1alpha1.PhasePending, metav1.ConditionFalse, v1alpha1.SourceReasonWaitingForPassword,
			fmt.Sprintf("Waiting for the password Secret %s.", source.Spec.Connection.PasswordSecret)), nil
	}
	connector := &unstructured.Unstructured{}
	connector.SetGroupVersionKind(kafkaConnectorGVK)
	err := r.Client.Get(ctx, client.ObjectKey{Namespace: source.Namespace, Name: names.Connector}, connector)
	if err != nil {
		return condition(v1alpha1.PhasePending, metav1.ConditionFalse, v1alpha1.SourceReasonStarting, "Waiting for Kafka Connect to start the connector."), nil
	}
	state, trace := connectorStatus(connector)
	stopped := source.Spec.State == v1alpha1.StateStopped
	switch {
	// Debezium does not fail on a lost slot: it tries to take it again for
	// half an hour, and its task reports nothing meanwhile. The slot's own
	// reading says so at once.
	case !stopped && status.Slot != nil && status.Slot.WALStatus == walStatusLost:
		return condition(v1alpha1.PhaseFailed, metav1.ConditionFalse, v1alpha1.SourceReasonFailed, lostSlot(names)), connector
	case stopped && state == "STOPPED":
		return condition(v1alpha1.PhaseStopped, metav1.ConditionFalse, v1alpha1.SourceReasonStopped,
			"The source is stopped. Its slot keeps the database's WAL until it runs again."), connector
	case stopped:
		return condition(v1alpha1.PhaseStopped, metav1.ConditionFalse, v1alpha1.SourceReasonStopped, "The source is stopping."), connector
	case state == "FAILED":
		return condition(v1alpha1.PhaseFailed, metav1.ConditionFalse, v1alpha1.SourceReasonFailed, Explain(trace, source, names)), connector
	case state == "RUNNING":
		return condition(v1alpha1.PhaseStreaming, metav1.ConditionTrue, v1alpha1.SourceReasonStreaming,
			fmt.Sprintf("Changes stream from %d tables.", len(source.Spec.Tables))), connector
	default:
		return condition(v1alpha1.PhasePending, metav1.ConditionFalse, v1alpha1.SourceReasonStarting, "The connector is starting."), connector
	}
}

// followSnapshots follows the snapshots of a streaming source's tables. It
// asks for a snapshot of each new table once the connector runs with it and
// the database checks pass, and reads the progress of those running. While
// any table's existing rows are being copied, the source is Snapshotting,
// and Ready says how far.
func (r *PostgresSourceReconciler) followSnapshots(ctx context.Context, source *v1alpha1.PostgresSource, names cdc.Names,
	status *v1alpha1.PostgresSourceStatus, connector *unstructured.Unstructured, ready metav1.Condition,
) metav1.Condition {
	log := ctrl.LoggerFrom(ctx)
	syncTables(source, status)
	if slices.ContainsFunc(status.Tables, func(t v1alpha1.TableStatus) bool { return t.Snapshot == v1alpha1.SnapshotRunning }) {
		notifications, next, err := r.Snapshots.Notifications(ctx, source, names, status.NotificationsOffset)
		applyNotifications(status.Tables, notifications)
		status.NotificationsOffset = next
		if err != nil {
			log.Info("could not read the snapshot notifications", "source", source.Name, "error", err.Error())
		}
	}
	r.askAgainUnanswered(ctx, source, status)
	// A table asked for recently runs, even if the status lost it.
	running := slices.ContainsFunc(status.Tables, func(t v1alpha1.TableStatus) bool {
		return t.Snapshot == v1alpha1.SnapshotRunning || t.Snapshot == v1alpha1.SnapshotPending && r.asked(source, t)
	})
	if pending := r.pendingTables(source, status); len(pending) > 0 && !running && connectorHasTables(connector, source) {
		r.requestSnapshot(ctx, source, names, status, pending)
	}

	copying := snapshotting(status.Tables)
	if copying == 0 {
		return ready
	}
	status.Phase = v1alpha1.PhaseSnapshotting
	message := fmt.Sprintf("Copying the existing rows of %d of %d tables, while changes stream.", copying, len(status.Tables))
	// The first table that waits says what for.
	if i := slices.IndexFunc(status.Tables, func(t v1alpha1.TableStatus) bool {
		return t.Snapshot == v1alpha1.SnapshotPending && t.Message != ""
	}); i >= 0 {
		t := status.Tables[i]
		message = fmt.Sprintf("%s %s.%s waits: %s", message, t.Schema, t.Name, t.Message)
	}
	return metav1.Condition{Type: v1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: v1alpha1.SourceReasonSnapshotting, Message: message}
}

// pendingTables are the tables whose snapshot has not been asked for, and
// not just been asked for either.
func (r *PostgresSourceReconciler) pendingTables(source *v1alpha1.PostgresSource, status *v1alpha1.PostgresSourceStatus) []cdc.Table {
	var pending []cdc.Table
	for _, t := range status.Tables {
		if t.Snapshot == v1alpha1.SnapshotPending && !r.asked(source, t) {
			pending = append(pending, cdc.Table{Schema: t.Schema, Name: t.Name})
		}
	}
	return pending
}

// asked reports whether the table's snapshot was asked for in the last
// signalMemory.
func (r *PostgresSourceReconciler) asked(source *v1alpha1.PostgresSource, t v1alpha1.TableStatus) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	asked, ok := r.signalled[string(source.UID)+"/"+t.Schema+"."+t.Name]
	return ok && r.now().Sub(asked) <= signalMemory
}

// requestSnapshot asks for the snapshot of the first pending table that
// passes the database checks, as the console runs them before a source
// starts. Tables are copied one at a time: Debezium drops the rest of a
// snapshot's tables when one fails. A table that fails a check stays
// pending, with what to do.
func (r *PostgresSourceReconciler) requestSnapshot(ctx context.Context, source *v1alpha1.PostgresSource, names cdc.Names, status *v1alpha1.PostgresSourceStatus, pending []cdc.Table) {
	tableStatus := func(table cdc.Table) *v1alpha1.TableStatus {
		i := slices.IndexFunc(status.Tables, func(t v1alpha1.TableStatus) bool { return t.Schema == table.Schema && t.Name == table.Name })
		return &status.Tables[i]
	}
	connection, err := r.connection(ctx, source, names)
	if err != nil {
		for _, table := range pending {
			tableStatus(table).Message = capitalizeFirst(err.Error()) + "."
		}
		return
	}
	for _, table := range pending {
		t := tableStatus(table)
		report := r.Database.Check(ctx, cdc.Target{Provider: cdc.Provider(source.Spec.Provider), Connection: connection, Names: names, Tables: []cdc.Table{table}})
		if !report.OK() {
			t.Message = "The database checks did not finish."
			for _, result := range report.Results {
				if result.Status == cdc.Fail {
					t.Message = result.Message
					break
				}
			}
			continue
		}
		if err := r.Snapshots.Signal(ctx, source, names, []cdc.Table{table}); err != nil {
			t.Message = "Asking for the snapshot failed, and is retried: " + err.Error() + "."
			return
		}
		r.remember(source, table)
		t.Snapshot, t.Message, t.RowsCopied, t.AskedAt = v1alpha1.SnapshotRunning, "", 0, ptr.To(metav1.NewTime(r.now()))
		return
	}
}

// unansweredAfter is how long the controller waits for Debezium to answer
// a snapshot's signal with a notification about the table. Debezium reads
// signals every 5 seconds and answers at once, so a signal unanswered this
// long was lost, such as by a connector that restarted as it read it.
const unansweredAfter = 5 * time.Minute

// askAgainUnanswered makes a table whose snapshot Debezium never answered
// pending again, so that it is asked for anew. Asking twice is safe: a
// table's rows arrive at least once.
func (r *PostgresSourceReconciler) askAgainUnanswered(ctx context.Context, source *v1alpha1.PostgresSource, status *v1alpha1.PostgresSourceStatus) {
	for i := range status.Tables {
		t := &status.Tables[i]
		if t.Snapshot != v1alpha1.SnapshotRunning || t.AskedAt == nil || r.now().Sub(t.AskedAt.Time) < unansweredAfter {
			continue
		}
		ctrl.LoggerFrom(ctx).Info("asking again for a snapshot Debezium did not answer", "source", source.Name, "table", t.Schema+"."+t.Name)
		t.Snapshot, t.AskedAt = v1alpha1.SnapshotPending, nil
		t.Message = "Debezium did not start copying the table within 5 minutes, so Changeport asks again."
		r.forget(source, cdc.Table{Schema: t.Schema, Name: t.Name})
	}
}

// resnapshot copies every table's existing rows again when
// ResnapshotAnnotation asks for it with a value it has not acted on, such
// as after the database lost the source's slot (docs/runbook.md).
func (r *PostgresSourceReconciler) resnapshot(ctx context.Context, source *v1alpha1.PostgresSource, status *v1alpha1.PostgresSourceStatus) {
	requested := source.Annotations[v1alpha1.ResnapshotAnnotation]
	if requested == "" || requested == status.Resnapshot {
		return
	}
	ctrl.LoggerFrom(ctx).Info("copying every table again", "source", source.Name, "request", requested)
	syncTables(source, status)
	for i := range status.Tables {
		t := &status.Tables[i]
		t.Snapshot, t.RowsCopied, t.RowsEstimated, t.AskedAt = v1alpha1.SnapshotPending, 0, 0, nil
		t.Message = "Copying the table again, as asked."
		r.forget(source, cdc.Table{Schema: t.Schema, Name: t.Name})
	}
	status.Resnapshot = requested
}

// forget forgets that a table's snapshot was asked for.
func (r *PostgresSourceReconciler) forget(source *v1alpha1.PostgresSource, table cdc.Table) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.signalled, string(source.UID)+"/"+table.String())
}

// remember notes that a table's snapshot was asked for, and forgets those
// asked for longer than signalMemory ago.
func (r *PostgresSourceReconciler) remember(source *v1alpha1.PostgresSource, table cdc.Table) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.signalled == nil {
		r.signalled = map[string]time.Time{}
	}
	r.signalled[string(source.UID)+"/"+table.String()] = r.now()
	for key, asked := range r.signalled {
		if r.now().Sub(asked) > signalMemory {
			delete(r.signalled, key)
		}
	}
}

// connectorHasTables reports whether Kafka Connect runs the connector with
// all of the source's tables, so that a snapshot of a new table is not asked
// for before the connector would stream it.
func connectorHasTables(connector *unstructured.Unstructured, source *v1alpha1.PostgresSource) bool {
	if connector == nil {
		return false
	}
	observed, _, _ := unstructured.NestedInt64(connector.Object, "status", "observedGeneration")
	included, _, _ := unstructured.NestedString(connector.Object, "spec", "config", "table.include.list")
	tables := make([]cdc.Table, len(source.Spec.Tables))
	for i, t := range source.Spec.Tables {
		tables[i] = cdc.Table{Schema: t.Schema, Name: t.Name}
	}
	return observed >= connector.GetGeneration() && included == cdc.TableIncludeList(tables)
}

func capitalizeFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// connectorStatus is the state of a connector and its task, as Strimzi
// reports them from Kafka Connect: FAILED with the first failure's trace,
// RUNNING once the connector and every task run, or the connector's state.
func connectorStatus(connector *unstructured.Unstructured) (state, trace string) {
	connectorState, _, _ := unstructured.NestedString(connector.Object, "status", "connectorStatus", "connector", "state")
	if connectorState == "FAILED" {
		trace, _, _ = unstructured.NestedString(connector.Object, "status", "connectorStatus", "connector", "trace")
		return "FAILED", trace
	}
	tasks, _, _ := unstructured.NestedSlice(connector.Object, "status", "connectorStatus", "tasks")
	for _, t := range tasks {
		task, _ := t.(map[string]any)
		if task["state"] == "FAILED" {
			trace, _ := task["trace"].(string)
			return "FAILED", trace
		}
		if connectorState == "RUNNING" && task["state"] != "RUNNING" {
			return "STARTING", ""
		}
	}
	if connectorState == "RUNNING" && len(tasks) == 0 {
		return "STARTING", ""
	}
	return connectorState, ""
}

// explanations turn the failures customers can fix into plain language, in
// the order they are tried on the connector's trace.
var explanations = []struct {
	pattern *regexp.Regexp
	explain func(source *v1alpha1.PostgresSource, names cdc.Names, detail string) string
}{
	{regexp.MustCompile(`password authentication failed|invalid_password|28P01`), func(_ *v1alpha1.PostgresSource, names cdc.Names, _ string) string {
		return fmt.Sprintf("The database refused the password of role %s. Run the SQL for a new password, and give it to Changeport.", names.Role)
	}},
	{regexp.MustCompile(`no pg_hba\.conf entry[^\n]*`), func(_ *v1alpha1.PostgresSource, _ cdc.Names, detail string) string {
		return "The database refused the connection: " + detail + ". Let the role connect from Changeport's addresses over TLS."
	}},
	{regexp.MustCompile(`(?:replication slot[^\n]*(?:does not exist|has been invalidated|can no longer get changes|lost))[^\n]*`), func(_ *v1alpha1.PostgresSource, names cdc.Names, _ string) string {
		return lostSlot(names)
	}},
	{regexp.MustCompile(`publication "?[^"\s]*"? does not exist`), func(_ *v1alpha1.PostgresSource, names cdc.Names, _ string) string {
		return fmt.Sprintf("The publication %s does not exist. Run the second step's SQL.", names.Publication)
	}},
	{regexp.MustCompile(`must be superuser or (?:have the )?replication|permission denied to start WAL sender|must have the REPLICATION`), func(_ *v1alpha1.PostgresSource, names cdc.Names, _ string) string {
		return fmt.Sprintf("Role %s may not replicate. Run the first step's SQL again.", names.Role)
	}},
	{regexp.MustCompile(`permission denied[^\n]*`), func(_ *v1alpha1.PostgresSource, _ cdc.Names, detail string) string {
		return "The database refused the role access: " + detail + ". Run the SQL that grants it."
	}},
	{regexp.MustCompile(`(?:PKIX path|SSLHandshakeException|certificate|hostname[^\n]*verif)[^\n]*`), func(source *v1alpha1.PostgresSource, _ cdc.Names, detail string) string {
		return fmt.Sprintf("The certificate of %s is not trusted: %s.", source.Spec.Connection.Host, detail)
	}},
	{regexp.MustCompile(`server does not support SSL|The server does not support SSL`), func(_ *v1alpha1.PostgresSource, _ cdc.Names, _ string) string {
		return "The database does not accept TLS connections. Changeport connects only over TLS."
	}},
	{regexp.MustCompile(`UnknownHostException|Connection refused|connect timed out|Connection attempt timed out|NoRouteToHost`), func(source *v1alpha1.PostgresSource, _ cdc.Names, _ string) string {
		return fmt.Sprintf("Changeport cannot reach %s on port %d. Check that the database is publicly accessible and lets Changeport's addresses in.",
			source.Spec.Connection.Host, source.Spec.Connection.Port)
	}},
	{regexp.MustCompile(`/mnt/sources/[^\s]*`), func(_ *v1alpha1.PostgresSource, _ cdc.Names, _ string) string {
		return "Kafka Connect has not received the source's credentials yet; it retries."
	}},
}

// Explain turns a connector's trace into what the customer reads: a known
// failure in plain language, or the trace's innermost cause.
func Explain(trace string, source *v1alpha1.PostgresSource, names cdc.Names) string {
	for _, e := range explanations {
		if match := e.pattern.FindString(trace); match != "" {
			return e.explain(source, names, strings.TrimSpace(strings.TrimRight(match, ".")))
		}
	}
	return "The connector failed: " + innermostCause(trace)
}

// innermostCause is the message of a Java trace's last "Caused by", or its
// first line.
func innermostCause(trace string) string {
	cause := strings.TrimSpace(strings.SplitN(trace, "\n", 2)[0])
	for line := range strings.SplitSeq(trace, "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "Caused by: "); ok {
			cause = rest
		}
	}
	if cause == "" {
		return "no reason given"
	}
	const limit = 300
	if len(cause) > limit {
		cause = cause[:limit] + "..."
	}
	return cause
}

// slotDue reports whether the slot was last read SlotInterval ago or more.
// A lost slot is read on every pass, so that the source says so only until
// it has a new one.
func (r *PostgresSourceReconciler) slotDue(status *v1alpha1.PostgresSourceStatus) bool {
	return status.Slot == nil || status.Slot.WALStatus == walStatusLost || r.now().Sub(status.Slot.CheckedAt.Time) >= r.SlotInterval
}

// walStatusLost is Postgres's wal_status of a slot it invalidated.
const walStatusLost = "lost"

// lostSlot says that the database lost the source's slot, and what it takes.
func lostSlot(names cdc.Names) string {
	return fmt.Sprintf("The database lost the replication slot %s, probably because it held back more WAL than max_slot_wal_keep_size allows. "+
		"The source needs a new slot and a new copy of its tables, which Changeport's operators set up with you.", names.Slot)
}

func (r *PostgresSourceReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// readSlot reads the source's slot into its status and the retained WAL
// metric. A database that cannot be reached leaves the last reading.
func (r *PostgresSourceReconciler) readSlot(ctx context.Context, source *v1alpha1.PostgresSource, names cdc.Names, status *v1alpha1.PostgresSourceStatus) {
	connection, err := r.connection(ctx, source, names)
	if err != nil {
		return
	}
	state, err := r.Database.Slot(ctx, connection, names.Slot)
	if err != nil {
		ctrl.LoggerFrom(ctx).Info("could not read the slot", "source", source.Name, "error", err.Error())
		return
	}
	recordSlot(source, state, r.now())
	if !state.Exists {
		// Not created yet, or dropped: it is read again on the next pass.
		status.Slot = nil
		return
	}
	status.Slot = &v1alpha1.SlotStatus{
		Name: names.Slot, Active: state.Active, RetainedWALBytes: state.RetainedBytes,
		WALStatus: state.WALStatus, SafeWALSizeBytes: state.SafeWALSize, CheckedAt: metav1.NewTime(r.now()),
	}
}

// catalogLifetime is how long a source's catalog stays in its status, which
// it would otherwise make larger with every update.
const catalogLifetime = 15 * time.Minute

// listCatalog lists the database's tables into the status when
// CatalogRequestAnnotation asks for them, and forgets them, with the
// request, catalogLifetime later.
func (r *PostgresSourceReconciler) listCatalog(ctx context.Context, source *v1alpha1.PostgresSource, names cdc.Names, status *v1alpha1.PostgresSourceStatus) error {
	requested := source.Annotations[v1alpha1.CatalogRequestAnnotation]
	switch {
	case requested == "":
		status.Catalog = nil
		return nil
	case status.Catalog != nil && status.Catalog.RequestedAt == requested:
		if r.now().Sub(status.Catalog.ListedAt.Time) < catalogLifetime {
			return nil
		}
		patch := client.MergeFromWithOptions(source.DeepCopy(), client.MergeFromWithOptimisticLock{})
		delete(source.Annotations, v1alpha1.CatalogRequestAnnotation)
		if err := r.Client.Patch(ctx, source, patch); err != nil {
			return fmt.Errorf("forget the catalog's request: %w", err)
		}
		status.Catalog = nil
		return nil
	}
	catalog := &v1alpha1.TableCatalog{RequestedAt: requested, ListedAt: metav1.NewTime(r.now())}
	status.Catalog = catalog
	connection, err := r.connection(ctx, source, names)
	if err != nil {
		catalog.Message = "Changeport cannot log in to the database yet: " + err.Error() + "."
		return nil
	}
	tables, err := r.Database.ListTables(ctx, connection)
	if err != nil {
		catalog.Message = capitalizeFirst(err.Error()) + "."
		return nil
	}
	if len(tables) > v1alpha1.MaxCatalogTables {
		tables, catalog.Truncated = tables[:v1alpha1.MaxCatalogTables], true
	}
	catalog.Tables = make([]v1alpha1.CatalogTable, len(tables))
	for i, t := range tables {
		catalog.Tables[i] = v1alpha1.CatalogTable{
			Schema: t.Schema, Name: t.Name, Partitioned: t.Partitioned, EstimatedRows: t.EstimatedRows,
			Readable: t.Readable, Keyed: t.Keyed, Reason: t.Reason,
		}
	}
	return nil
}

// connection is how the controller logs in to the source's database: as its
// role, with the password from the Connect worker's Secret.
func (r *PostgresSourceReconciler) connection(ctx context.Context, source *v1alpha1.PostgresSource, names cdc.Names) (cdc.Connection, error) {
	var secret corev1.Secret
	if err := r.Client.Get(ctx, client.ObjectKey{Namespace: source.Namespace, Name: provisioning.ConnectSourcesSecret}, &secret); err != nil {
		return cdc.Connection{}, fmt.Errorf("get the source's credentials: %w", err)
	}
	password := secret.Data[source.Name]
	if len(password) == 0 {
		return cdc.Connection{}, errors.New("the source has no password in the Connect worker's Secret")
	}
	c := source.Spec.Connection
	return cdc.Connection{
		Host: c.Host, Port: int(c.Port), Database: c.Database, User: names.Role, Password: string(bytes.Clone(password)),
		TLSMode: cdc.TLSMode(cmp.Or(c.TLS.Mode, v1alpha1.TLSVerifyFull)), CA: c.TLS.CA,
	}, nil
}

// finalize drops a deleted source's slot once its connector is gone, then
// removes its credentials and lets the source go. When the database cannot
// be reached, it waits, with the SQL for the customer in the status, until
// it can or the customer confirms with SlotDroppedAnnotation.
func (r *PostgresSourceReconciler) finalize(ctx context.Context, source *v1alpha1.PostgresSource, names cdc.Names) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(source, v1alpha1.SourceFinalizer) {
		return ctrl.Result{}, nil
	}
	status := source.Status.DeepCopy()
	status.Phase = v1alpha1.PhaseDeleting
	recordPhase(source, status.Phase)
	waiting := func(reason, message string, after time.Duration) (ctrl.Result, error) {
		setSourceCondition(status, source.Generation, metav1.Condition{Type: v1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: message})
		return ctrl.Result{RequeueAfter: after}, r.updateStatus(ctx, source, status)
	}

	connector := &unstructured.Unstructured{}
	connector.SetGroupVersionKind(kafkaConnectorGVK)
	err := r.Client.Get(ctx, client.ObjectKey{Namespace: source.Namespace, Name: names.Connector}, connector)
	if err == nil {
		return waiting(v1alpha1.SourceReasonDeleting, "Stopping the connector before dropping the replication slot "+names.Slot+".", r.PollInterval)
	}
	if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("get the connector %s: %w", names.Connector, err)
	}

	confirmed := source.Annotations[v1alpha1.SlotDroppedAnnotation] == "true"
	if status.PasswordSecret != "" && !confirmed {
		connection, err := r.connection(ctx, source, names)
		if err == nil {
			_, err = r.Database.DropSlot(ctx, connection, names.Slot)
		}
		switch {
		case errors.Is(err, cdc.ErrSlotActive):
			return waiting(v1alpha1.SourceReasonDeleting, "Waiting for the connector to let go of the replication slot "+names.Slot+".", r.PollInterval)
		case err != nil:
			return waiting(v1alpha1.SourceReasonDatabaseUnreachable, fmt.Sprintf(
				"Changeport cannot drop the replication slot %s: %s. It keeps trying. To drop it yourself, run: %s",
				names.Slot, strings.TrimSuffix(err.Error(), "."), strings.TrimSpace(cdc.DropSlotSQL(names).SQL)), r.SlotInterval)
		}
	}

	if err := r.removeCredentials(ctx, source); err != nil {
		return ctrl.Result{}, err
	}
	forgetSourceMetrics(source)
	patch := client.MergeFromWithOptions(source.DeepCopy(), client.MergeFromWithOptimisticLock{})
	controllerutil.RemoveFinalizer(source, v1alpha1.SourceFinalizer)
	if err := r.Client.Patch(ctx, source, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove the finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

// removeCredentials deletes the source's password and CA from the Connect
// worker's Secret, and a password Secret of the console's not handed over.
func (r *PostgresSourceReconciler) removeCredentials(ctx context.Context, source *v1alpha1.PostgresSource) error {
	var secret corev1.Secret
	err := r.Client.Get(ctx, client.ObjectKey{Namespace: source.Namespace, Name: provisioning.ConnectSourcesSecret}, &secret)
	if client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("get the Secret %s: %w", provisioning.ConnectSourcesSecret, err)
	}
	if err == nil {
		_, hasPassword := secret.Data[source.Name]
		_, hasCA := secret.Data[source.Name+".ca"]
		if hasPassword || hasCA {
			delete(secret.Data, source.Name)
			delete(secret.Data, source.Name+".ca")
			if err := r.Client.Update(ctx, &secret); err != nil {
				return fmt.Errorf("remove source %s's credentials: %w", source.Name, err)
			}
		}
	}
	var inbox corev1.Secret
	err = r.Client.Get(ctx, client.ObjectKey{Namespace: source.Namespace, Name: source.Spec.Connection.PasswordSecret}, &inbox)
	if err == nil && string(inbox.Type) == v1alpha1.PostgresPasswordSecretType {
		if err := r.Client.Delete(ctx, &inbox); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete the password Secret %s: %w", inbox.Name, err)
		}
	}
	return nil
}

func setSourceCondition(status *v1alpha1.PostgresSourceStatus, generation int64, condition metav1.Condition) {
	condition.ObservedGeneration = generation
	meta.SetStatusCondition(&status.Conditions, condition)
}

// updateStatus writes status unless it is what the source already has.
func (r *PostgresSourceReconciler) updateStatus(ctx context.Context, source *v1alpha1.PostgresSource, status *v1alpha1.PostgresSourceStatus) error {
	status.ObservedGeneration = source.Generation
	if equality.Semantic.DeepEqual(source.Status, *status) {
		return nil
	}
	source.Status = *status
	if err := r.Client.Status().Update(ctx, source); err != nil {
		return fmt.Errorf("update the status: %w", err)
	}
	return nil
}
