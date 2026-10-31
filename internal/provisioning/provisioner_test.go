package provisioning

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// fakeCluster is an in-memory Cluster. Like cert-manager, it issues every
// Certificate as it is first applied unless holdCertificates is set.
type fakeCluster struct {
	mu      sync.Mutex
	objects map[string]*unstructured.Unstructured
	applied []string
	deleted []string
	// gone holds each deleted object as it was when deleted.
	gone             map[string]*unstructured.Unstructured
	applyErr         error
	holdCertificates bool
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{objects: map[string]*unstructured.Unstructured{}, gone: map[string]*unstructured.Unstructured{}}
}

// setStatus gives a stored object conditions, observed for its current
// generation (always 0 in the fake).
func (f *fakeCluster) setStatus(gvk schema.GroupVersionKind, namespace, name string, conditions ...map[string]any) {
	obj := f.objects[fakeKey(gvk, namespace, name)]
	condList := make([]any, len(conditions))
	for i, c := range conditions {
		condList[i] = c
	}
	obj.Object["status"] = map[string]any{"observedGeneration": int64(0), "conditions": condList}
}

var readyTrue = map[string]any{"type": "Ready", "status": "True"}

// setUsersReady marks the template's KafkaUsers ready and the agent available.
func (f *fakeCluster) setUsersReady(spec Spec) {
	for _, user := range []string{AdminUser, AgentUser, ConsoleUser} {
		f.setStatus(kafkaUserGVK, spec.Namespace(), user, readyTrue)
	}
	agent := f.objects[fakeKey(deploymentGVK, spec.Namespace(), "changeport-agent")]
	agent.Object["status"] = map[string]any{"observedGeneration": int64(0), "updatedReplicas": int64(1), "availableReplicas": int64(1)}
}

func fakeKey(gvk schema.GroupVersionKind, namespace, name string) string {
	return gvk.Kind + ":" + namespace + "/" + name
}

func (f *fakeCluster) Apply(_ context.Context, obj *unstructured.Unstructured) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.applyErr != nil {
		return f.applyErr
	}
	key := fakeKey(obj.GroupVersionKind(), obj.GetNamespace(), obj.GetName())
	applied := obj.DeepCopy()
	// Like server-side apply, applying the spec leaves the status alone.
	if old, ok := f.objects[key]; ok {
		if status, ok := old.Object["status"]; ok {
			applied.Object["status"] = status
		}
	}
	f.objects[key] = applied
	f.applied = append(f.applied, key)
	if obj.GroupVersionKind() == certificateGVK && !f.holdCertificates && applied.Object["status"] == nil {
		f.objects[key].Object["status"] = map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "True", "observedGeneration": int64(0)},
		}}
	}
	return nil
}

func (f *fakeCluster) Get(_ context.Context, gvk schema.GroupVersionKind, namespace, name string) (*unstructured.Unstructured, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[fakeKey(gvk, namespace, name)]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, name)
	}
	return obj.DeepCopy(), nil
}

func (f *fakeCluster) Delete(_ context.Context, gvk schema.GroupVersionKind, namespace, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fakeKey(gvk, namespace, name)
	f.deleted = append(f.deleted, key)
	obj, ok := f.objects[key]
	if !ok {
		return nil
	}
	// Like the API server, it keeps an object with finalizers, marked as
	// deleted, until they are gone.
	if len(obj.GetFinalizers()) > 0 {
		if obj.GetDeletionTimestamp() == nil {
			obj.SetDeletionTimestamp(&metav1.Time{Time: time.Now()})
		}
		return nil
	}
	f.gone[key] = obj
	delete(f.objects, key)
	return nil
}

// finalize takes the finalizers off a deleted object, which the fake then
// deletes, as the API server does.
func (f *fakeCluster) finalize(gvk schema.GroupVersionKind, namespace, name string) {
	f.mu.Lock()
	key := fakeKey(gvk, namespace, name)
	obj := f.objects[key]
	obj.SetFinalizers(nil)
	f.mu.Unlock()
	if obj.GetDeletionTimestamp() != nil {
		_ = f.Delete(context.Background(), gvk, namespace, name)
	}
}

// List supports label selectors of comma-separated requirements, each
// "key=value" or "key" for objects with the label; "" selects every object.
func (f *fakeCluster) List(_ context.Context, gvk schema.GroupVersionKind, namespace, labelSelector string) ([]unstructured.Unstructured, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	matches := func(labels map[string]string) bool {
		if labelSelector == "" {
			return true
		}
		for requirement := range strings.SplitSeq(labelSelector, ",") {
			key, value, hasValue := strings.Cut(requirement, "=")
			label, hasLabel := labels[key]
			if !hasLabel || hasValue && label != value {
				return false
			}
		}
		return true
	}
	var items []unstructured.Unstructured
	for _, obj := range f.objects {
		if obj.GroupVersionKind() == gvk && obj.GetNamespace() == namespace && matches(obj.GetLabels()) {
			items = append(items, *obj.DeepCopy())
		}
	}
	return items, nil
}

// fakePodIdentities records Pod Identity calls.
type fakePodIdentities struct{ calls []string }

func (f *fakePodIdentities) Associate(_ context.Context, cluster, namespace, serviceAccount, roleName string) error {
	f.calls = append(f.calls, "associate "+cluster+" "+namespace+"/"+serviceAccount+" "+roleName)
	return nil
}

func (f *fakePodIdentities) Disassociate(_ context.Context, cluster, namespace, serviceAccount string) error {
	f.calls = append(f.calls, "disassociate "+cluster+" "+namespace+"/"+serviceAccount)
	return nil
}

func testProvisioner(cluster Cluster) *Provisioner {
	return New(cluster, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// provision applies the whole template for spec, as the controller does once
// the listener certificate is issued, which the fake cluster does at once.
func provision(t *testing.T, p *Provisioner, spec Spec) {
	t.Helper()
	progress, err := p.Reconcile(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if !progress.Certificate.Ready {
		t.Fatalf("Reconcile() stopped at the certificate: %v", progress.Certificate)
	}
}

var acme = Spec{CustomerID: "acme", Profile: DevProfile, Platform: KindPlatform}

func TestReconcileAppliesTemplateInOrder(t *testing.T) {
	cluster := newFakeCluster()
	provision(t, testProvisioner(cluster), acme)
	want := []string{
		"Namespace:/customer-acme",
		"Secret:customer-acme/tiered-storage-credentials",
		"Certificate:customer-acme/external-listener",
		"KafkaNodePool:customer-acme/dual-role",
		"Kafka:customer-acme/acme",
		"KafkaUser:customer-acme/admin",
		"KafkaUser:customer-acme/changeport-agent",
		"KafkaUser:customer-acme/changeport-console",
		"Role:customer-acme/changeport-console",
		"RoleBinding:customer-acme/changeport-console",
		"Deployment:customer-acme/changeport-agent",
	}
	if strings.Join(cluster.applied, ",") != strings.Join(want, ",") {
		t.Errorf("applied %v, want %v", cluster.applied, want)
	}
}

func TestReconcileIsIdempotent(t *testing.T) {
	cluster := newFakeCluster()
	p := testProvisioner(cluster)
	for range 2 {
		provision(t, p, acme)
	}
	if len(cluster.objects) != 11 {
		t.Errorf("cluster holds %d objects after provisioning twice, want 11", len(cluster.objects))
	}
}

func TestReconcileReportsApplyErrors(t *testing.T) {
	cluster := newFakeCluster()
	cluster.applyErr = errors.New("forbidden")
	_, err := testProvisioner(cluster).Reconcile(t.Context(), acme)
	if err == nil || !strings.Contains(err.Error(), "Namespace customer-acme") {
		t.Fatalf("Reconcile() error = %v, want it to name the failing object", err)
	}
}

func TestReconcileRejectsInvalidSpec(t *testing.T) {
	cluster := newFakeCluster()
	if _, err := testProvisioner(cluster).Reconcile(t.Context(), Spec{CustomerID: "BAD", Profile: DevProfile, Platform: KindPlatform}); err == nil {
		t.Fatal("Reconcile() = nil, want a validation error")
	}
	if len(cluster.applied) != 0 {
		t.Errorf("applied %v for an invalid spec", cluster.applied)
	}
}

func TestStrimziReady(t *testing.T) {
	ready := map[string]any{"type": "Ready", "status": "True"}
	tests := []struct {
		name       string
		generation int64
		status     map[string]any
		want       bool
		message    string
	}{
		{"no status", 1, nil, false, "operator has not reconciled the latest spec"},
		{"stale generation", 2, map[string]any{"observedGeneration": int64(1), "conditions": []any{ready}}, false, "operator has not reconciled the latest spec; last status Ready"},
		{"creating", 1, map[string]any{"observedGeneration": int64(0), "conditions": []any{
			map[string]any{"type": "NotReady", "status": "True", "reason": "Creating"},
		}}, false, "operator has not reconciled the latest spec; last status NotReady=True (Creating)"},
		{"ready", 2, map[string]any{"observedGeneration": int64(2), "conditions": []any{ready}}, true, "Ready"},
		{"not ready", 1, map[string]any{"observedGeneration": int64(1), "conditions": []any{
			map[string]any{"type": "NotReady", "status": "True", "reason": "Creating"},
		}}, false, "NotReady=True (Creating)"},
		{"ready false", 1, map[string]any{"observedGeneration": int64(1), "conditions": []any{
			map[string]any{"type": "Ready", "status": "False"},
		}}, false, "Ready=False"},
		{"no conditions", 1, map[string]any{"observedGeneration": int64(1)}, false, "no Ready condition"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kafka := &unstructured.Unstructured{Object: map[string]any{}}
			kafka.SetGeneration(tt.generation)
			if tt.status != nil {
				kafka.Object["status"] = tt.status
			}
			got, message := strimziReady(kafka)
			if got != tt.want || message != tt.message {
				t.Errorf("strimziReady() = %v, %q; want %v, %q", got, message, tt.want, tt.message)
			}
		})
	}
}

func TestDeploymentReady(t *testing.T) {
	tests := []struct {
		name       string
		generation int64
		status     map[string]any
		want       bool
	}{
		{"no status", 1, nil, false},
		{"old generation", 2, map[string]any{"observedGeneration": int64(1), "updatedReplicas": int64(1), "availableReplicas": int64(1)}, false},
		{"not available", 1, map[string]any{"observedGeneration": int64(1), "updatedReplicas": int64(1), "availableReplicas": int64(0)}, false},
		{"available", 1, map[string]any{"observedGeneration": int64(1), "updatedReplicas": int64(1), "availableReplicas": int64(1)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"replicas": int64(1)}}}
			d.SetGeneration(tt.generation)
			if tt.status != nil {
				d.Object["status"] = tt.status
			}
			if got, message := deploymentReady(d); got != tt.want {
				t.Errorf("deploymentReady() = %v (%s), want %v", got, message, tt.want)
			}
		})
	}
}

func TestCertificateReady(t *testing.T) {
	tests := []struct {
		name       string
		generation int64
		conditions []any
		want       bool
		message    string
	}{
		{"no status", 1, nil, false, "no Ready condition"},
		{"issued", 2, []any{map[string]any{"type": "Ready", "status": "True", "observedGeneration": int64(2)}}, true, "Ready"},
		{"issued for an older spec", 2, []any{map[string]any{"type": "Ready", "status": "True", "observedGeneration": int64(1)}}, false, "Ready=True ()"},
		{"pending", 1, []any{
			map[string]any{"type": "Issuing", "status": "True"},
			map[string]any{"type": "Ready", "status": "False", "reason": "DoesNotExist", "observedGeneration": int64(1)},
		}, false, "Ready=False (DoesNotExist)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			certificate := &unstructured.Unstructured{Object: map[string]any{}}
			certificate.SetGeneration(tt.generation)
			if tt.conditions != nil {
				certificate.Object["status"] = map[string]any{"conditions": tt.conditions}
			}
			got, message := certificateReady(certificate)
			if got != tt.want || message != tt.message {
				t.Errorf("certificateReady() = %v, %q; want %v, %q", got, message, tt.want, tt.message)
			}
		})
	}
}

func TestStrimziWarnings(t *testing.T) {
	kafka := &unstructured.Unstructured{Object: map[string]any{"status": map[string]any{"conditions": []any{
		map[string]any{"type": "Warning", "status": "True", "reason": "KafkaStorage", "message": "data loss on restart"},
		map[string]any{"type": "Warning", "status": "False", "reason": "Resolved", "message": "ignored"},
		map[string]any{"type": "Ready", "status": "True"},
	}}}}
	got := strimziWarnings(kafka)
	if len(got) != 1 || got[0] != "KafkaStorage: data loss on restart" {
		t.Errorf("strimziWarnings() = %q, want the one active warning", got)
	}
	if got := strimziWarnings(&unstructured.Unstructured{Object: map[string]any{}}); len(got) != 0 {
		t.Errorf("strimziWarnings() without status = %q, want none", got)
	}
}

func TestDeprovisionDeletesTheNamespace(t *testing.T) {
	cluster := newFakeCluster()
	p := testProvisioner(cluster)
	provision(t, p, acme)
	if err := p.Deprovision(t.Context(), acme); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(cluster.deleted, ","); got != "Namespace:/customer-acme" {
		t.Errorf("deleted %s, want only the namespace on kind", got)
	}
}

// A customer's Postgres sources, their connectors and their KafkaTopics go
// before the namespace: their finalizers need what is in it.
func TestDeprovisionRetiresSourcesFirst(t *testing.T) {
	cluster := newFakeCluster()
	addKubernetesAPI(cluster, "172.19.0.5")
	p := testProvisioner(cluster)
	spec := acme
	spec.Sources = sources
	provision(t, p, spec)
	finalized := func(gvk schema.GroupVersionKind, name, finalizer string) {
		obj := &unstructured.Unstructured{}
		if stored, ok := cluster.objects[fakeKey(gvk, spec.Namespace(), name)]; ok {
			obj = stored
		} else {
			obj.SetGroupVersionKind(gvk)
			obj.SetNamespace(spec.Namespace())
			obj.SetName(name)
			cluster.objects[fakeKey(gvk, spec.Namespace(), name)] = obj
		}
		obj.SetFinalizers([]string{finalizer})
	}
	for _, source := range sources {
		finalized(sourceGVK, source.Name, "changeport.io/drop-slot")
		finalized(topicGVK, "changeport.cdc.signals."+source.Name, "strimzi.io/topic-operator")
	}
	app := cluster.objects[fakeKey(sourceGVK, spec.Namespace(), sources[0].Name)]
	app.Object["status"] = map[string]any{"conditions": []any{map[string]any{
		"type": "Ready", "status": "False", "reason": "Deleting", "message": "Waiting for the connector to let go of the replication slot.",
	}}}

	var retired []string
	for _, kind := range []schema.GroupVersionKind{sourceGVK, connectorGVK, topicGVK} {
		objects, _ := cluster.List(t.Context(), kind, spec.Namespace(), "")
		for _, obj := range objects {
			retired = append(retired, fakeKey(kind, obj.GetNamespace(), obj.GetName()))
		}
	}
	if len(retired) < 6 {
		t.Fatalf("the sources, connector and topics to retire are %v", retired)
	}

	if err := p.Deprovision(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	namespace := fakeKey(schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}, "", spec.Namespace())
	if slices.Contains(cluster.deleted, namespace) {
		t.Fatal("deleted the namespace before the sources were gone")
	}
	for _, key := range retired {
		if !slices.Contains(cluster.deleted, key) {
			t.Errorf("did not delete %s; deleted %v", key, cluster.deleted)
		}
	}
	deleted, status, err := p.Deleted(t.Context(), spec)
	if err != nil || deleted || !strings.Contains(status, "PostgresSource customer-acme/"+sources[0].Name+" is being deleted: Waiting for the connector to let go of the replication slot.") {
		t.Errorf("Deleted() = %v, %q, %v", deleted, status, err)
	}

	// Calling it again deletes nothing twice.
	cluster.deleted = nil
	if err := p.Deprovision(t.Context(), spec); err != nil || len(cluster.deleted) != 0 {
		t.Errorf("deprovisioning again deleted %v, %v", cluster.deleted, err)
	}

	// Once the source controller and the Topic Operator let them go, the
	// namespace goes.
	for _, source := range sources {
		cluster.finalize(sourceGVK, spec.Namespace(), source.Name)
		cluster.finalize(topicGVK, spec.Namespace(), "changeport.cdc.signals."+source.Name)
	}
	if err := p.Deprovision(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(cluster.deleted, namespace) {
		t.Errorf("deleted %v, want the namespace once the sources are gone", cluster.deleted)
	}
}

func TestDeprovisionRefusesProtectedCustomers(t *testing.T) {
	cluster := newFakeCluster()
	p := testProvisioner(cluster)
	protected := acme
	protected.DeletionProtection = true
	provision(t, p, protected)
	if err := p.Deprovision(t.Context(), protected); err == nil || len(cluster.deleted) != 0 {
		t.Errorf("Deprovision() of a protected customer = %v, deleted %v; want an error and nothing deleted", err, cluster.deleted)
	}
}

func TestDeprovisionReleasesTheNamespace(t *testing.T) {
	cluster := newFakeCluster()
	p := testProvisioner(cluster)
	p.Now = func() time.Time { return time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC) }
	protected := acme
	protected.DeletionProtection = true
	provision(t, p, protected)
	namespace := fakeKey(schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}, "", "customer-acme")
	if cluster.objects[namespace].GetLabels()[DeletionProtectionLabel] != "true" {
		t.Fatalf("the protected customer's namespace has labels %v", cluster.objects[namespace].GetLabels())
	}
	// A broker's volume claim, bound to a volume.
	claim := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"volumeName": "pvc-1a2b"}}}
	claim.SetGroupVersionKind(pvcGVK)
	claim.SetNamespace("customer-acme")
	claim.SetName("data-0-acme-dual-role-0")
	cluster.objects[fakeKey(pvcGVK, "customer-acme", claim.GetName())] = claim
	pending := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}}
	pending.SetGroupVersionKind(pvcGVK)
	pending.SetNamespace("customer-acme")
	pending.SetName("unbound")
	cluster.objects[fakeKey(pvcGVK, "customer-acme", pending.GetName())] = pending

	// Turned off, protection lets the namespace go, without its label.
	if err := p.Deprovision(t.Context(), acme); err != nil {
		t.Fatal(err)
	}
	gone := cluster.gone[namespace]
	if gone == nil {
		t.Fatal("the namespace was not deleted")
	}
	if _, ok := gone.GetLabels()[DeletionProtectionLabel]; ok {
		t.Errorf("the namespace was deleted with labels %v, want the protection label off", gone.GetLabels())
	}
	volume := cluster.objects[fakeKey(pvGVK, "", "pvc-1a2b")]
	if volume == nil || volume.GetLabels()[TenantLabel] != "acme" || volume.GetAnnotations()[DeprovisionedAtAnnotation] != "2026-10-24T12:00:00Z" {
		t.Errorf("volume pvc-1a2b = %v, want it marked with the customer and the time", volume)
	}
}

func TestReconcileAssociatesTheBrokersOnEKS(t *testing.T) {
	spec := Spec{CustomerID: "acme", Profile: ProductionProfile, Platform: EKSProdPlatform}
	cluster := newFakeCluster()
	p := testProvisioner(cluster)
	if _, err := p.Reconcile(t.Context(), spec); err == nil || len(cluster.applied) != 0 {
		t.Fatalf("Reconcile() without AWS access = %v after applying %v; want an error before applying anything", err, cluster.applied)
	}
	identities := &fakePodIdentities{}
	p.PodIdentities = identities
	provision(t, p, spec)
	want := "associate changeport-prod customer-acme/acme-kafka changeport-prod-kafka-broker"
	if got := strings.Join(identities.calls, ","); got != want {
		t.Errorf("Pod Identity calls = %s, want %s", got, want)
	}
	if err := p.Deprovision(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	if got := identities.calls[len(identities.calls)-1]; got != "disassociate changeport-prod customer-acme/acme-kafka" {
		t.Errorf("last Pod Identity call = %s, want the disassociation", got)
	}
}

func TestReconcileLeavesPodIdentityAloneOnKind(t *testing.T) {
	identities := &fakePodIdentities{}
	p := testProvisioner(newFakeCluster())
	p.PodIdentities = identities
	provision(t, p, acme)
	if err := p.Deprovision(t.Context(), acme); err != nil {
		t.Fatal(err)
	}
	if len(identities.calls) != 0 {
		t.Errorf("Pod Identity calls on kind: %v", identities.calls)
	}
}

func TestDeprovisionRemovesDedicatedNodes(t *testing.T) {
	spec := Spec{CustomerID: "acme", Profile: ProductionProfile, Platform: EKSProdPlatform}
	cluster := newFakeCluster()
	p := testProvisioner(cluster)
	p.PodIdentities = &fakePodIdentities{}
	provision(t, p, spec)
	if err := p.Deprovision(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	want := "Namespace:/customer-acme,NodePool:/customer-acme,NodeClass:/customer-acme"
	if got := strings.Join(cluster.deleted, ","); got != want {
		t.Errorf("deleted %s, want %s", got, want)
	}

	// Karpenter removes the NodePool's nodes in the background.
	nodeClaim := &unstructured.Unstructured{Object: map[string]any{}}
	nodeClaim.SetGroupVersionKind(nodeClaimGVK)
	nodeClaim.SetName("customer-acme-x7k2p")
	nodeClaim.SetLabels(map[string]string{"karpenter.sh/nodepool": "customer-acme"})
	cluster.objects[fakeKey(nodeClaimGVK, "", nodeClaim.GetName())] = nodeClaim
	deleted, status, err := p.Deleted(t.Context(), spec)
	if err != nil || deleted || status != "1 nodes of NodePool customer-acme are being deleted" {
		t.Fatalf("Deleted() with a node left = %v, %q, %v", deleted, status, err)
	}
	delete(cluster.objects, fakeKey(nodeClaimGVK, "", nodeClaim.GetName()))
	if deleted, status, err := p.Deleted(t.Context(), spec); err != nil || !deleted {
		t.Fatalf("Deleted() once the nodes are gone = %v, %q, %v", deleted, status, err)
	}
}

func TestDeprovisionRejectsInvalidCustomerID(t *testing.T) {
	spec := Spec{CustomerID: "kube-system/../x", Profile: DevProfile, Platform: KindPlatform}
	if err := testProvisioner(newFakeCluster()).Deprovision(t.Context(), spec); err == nil {
		t.Fatal("Deprovision() = nil, want a validation error")
	}
}

func TestReconcileStopsAtTheUnissuedCertificate(t *testing.T) {
	cluster := newFakeCluster()
	cluster.holdCertificates = true
	progress, err := testProvisioner(cluster).Reconcile(t.Context(), acme)
	if err != nil {
		t.Fatal(err)
	}
	want := Readiness{Object: "Certificate customer-acme/external-listener", Status: "no Ready condition"}
	if progress.Certificate != want {
		t.Errorf("certificate = %+v, want %+v", progress.Certificate, want)
	}
	if progress.Ready() || progress.Kafka.Ready || progress.Kafka.Status != "waiting for the listener certificate" {
		t.Errorf("progress = %+v, want everything after the certificate waiting for it", progress)
	}
	if got := cluster.applied[len(cluster.applied)-1]; got != "Certificate:customer-acme/external-listener" {
		t.Errorf("last applied %s; nothing may be applied before the certificate is issued", got)
	}
}

func TestReconcileReportsEachPart(t *testing.T) {
	cluster := newFakeCluster()
	p := testProvisioner(cluster)
	steps := []struct {
		name   string
		before func()
		want   Progress
	}{
		{
			name:   "operator has not seen the cluster",
			before: func() {},
			want: Progress{
				Certificate: Readiness{Ready: true, Status: "Issued"},
				Kafka:       Readiness{Object: "Kafka customer-acme/acme", Status: "operator has not reconciled the latest spec"},
				Users:       Readiness{Object: "KafkaUser customer-acme/admin", Status: "operator has not reconciled the latest spec"},
				Agent:       Readiness{Object: "Deployment customer-acme/changeport-agent", Status: "0 of 1 replicas updated, 0 available"},
				CDC:         Readiness{Ready: true, Status: "No Postgres CDC sources"},
			},
		},
		{
			name: "everything ready",
			before: func() {
				cluster.setStatus(kafkaGVK, acme.Namespace(), acme.ClusterName(), readyTrue)
				cluster.setUsersReady(acme)
			},
			want: Progress{
				Certificate: Readiness{Ready: true, Status: "Issued"},
				Kafka:       Readiness{Ready: true, Status: "Ready"},
				Users:       Readiness{Ready: true, Status: "Ready"},
				Agent:       Readiness{Ready: true, Status: "Ready"},
				CDC:         Readiness{Ready: true, Status: "No Postgres CDC sources"},
			},
		},
	}
	for _, step := range steps {
		step.before()
		progress, err := p.Reconcile(t.Context(), acme)
		if err != nil {
			t.Fatal(err)
		}
		if progress != step.want {
			t.Errorf("%s: progress = %+v, want %+v", step.name, progress, step.want)
		}
		if progress.Ready() != (step.name == "everything ready") {
			t.Errorf("%s: Ready() = %v", step.name, progress.Ready())
		}
	}
}

func TestReconcileWithoutAnAgent(t *testing.T) {
	spec := acme
	spec.Platform.AgentImage = ""
	cluster := newFakeCluster()
	progress, err := testProvisioner(cluster).Reconcile(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if want := (Readiness{Ready: true, Status: "None on this platform"}); progress.Agent != want {
		t.Errorf("agent = %+v, want %+v", progress.Agent, want)
	}
}

func TestReconcileReportsGetErrors(t *testing.T) {
	p := testProvisioner(failingGets{newFakeCluster()})
	if _, err := p.Reconcile(t.Context(), acme); err == nil || !strings.Contains(err.Error(), "Certificate customer-acme/external-listener") {
		t.Fatalf("Reconcile() error = %v, want it to name the object it could not read", err)
	}
}

// failingGets is a Cluster whose reads fail with something other than not
// found, such as a lost connection.
type failingGets struct{ *fakeCluster }

func (failingGets) Get(context.Context, schema.GroupVersionKind, string, string) (*unstructured.Unstructured, error) {
	return nil, errors.New("connection refused")
}

func TestDeletedReportsWhatIsLeft(t *testing.T) {
	cluster := newFakeCluster()
	p := testProvisioner(cluster)
	provision(t, p, acme)
	deleted, status, err := p.Deleted(t.Context(), acme)
	if err != nil || deleted || status != "Namespace customer-acme is being deleted" {
		t.Fatalf("Deleted() before deprovisioning = %v, %q, %v", deleted, status, err)
	}
	if err := p.Deprovision(t.Context(), acme); err != nil {
		t.Fatal(err)
	}
	if deleted, status, err := p.Deleted(t.Context(), acme); err != nil || !deleted {
		t.Fatalf("Deleted() after deprovisioning = %v, %q, %v", deleted, status, err)
	}
}

func TestDeprovisionLeavesADeletionInProgressAlone(t *testing.T) {
	cluster := newFakeCluster()
	p := testProvisioner(cluster)
	provision(t, p, acme)
	// The namespace controller is still emptying the namespace.
	cluster.objects[fakeKey(schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}, "", acme.Namespace())].SetDeletionTimestamp(&metav1.Time{Time: time.Now()})
	if err := p.Deprovision(t.Context(), acme); err != nil {
		t.Fatal(err)
	}
	if len(cluster.deleted) != 0 {
		t.Errorf("deleted %v again while its deletion runs", cluster.deleted)
	}
}

func TestReconcileReportsStrimziWarnings(t *testing.T) {
	cluster := newFakeCluster()
	p := testProvisioner(cluster)
	provision(t, p, acme)
	cluster.setStatus(kafkaGVK, acme.Namespace(), acme.ClusterName(), readyTrue,
		map[string]any{"type": "Warning", "status": "True", "reason": "UnsafeStorage", "message": "the storage is not replicated"})
	progress, err := p.Reconcile(t.Context(), acme)
	if err != nil {
		t.Fatal(err)
	}
	want := Readiness{Ready: true, Status: "Ready, with warnings: UnsafeStorage: the storage is not replicated"}
	if progress.Kafka != want {
		t.Errorf("Kafka = %+v, want %+v", progress.Kafka, want)
	}
}
