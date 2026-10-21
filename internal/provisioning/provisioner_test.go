package provisioning

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// fakeCluster is an in-memory Cluster. Like cert-manager, it issues every
// Certificate as it is applied unless holdCertificates is set. onGet, when
// set, runs before every Get so tests can change objects while the
// provisioner polls.
type fakeCluster struct {
	mu               sync.Mutex
	objects          map[string]*unstructured.Unstructured
	applied          []string
	deleted          []string
	applyErr         error
	holdCertificates bool
	onGet            func(f *fakeCluster)
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{objects: map[string]*unstructured.Unstructured{}}
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

// setUsersReady marks the template's KafkaUsers ready.
func (f *fakeCluster) setUsersReady(spec Spec) {
	for _, user := range []string{AdminUser, AgentUser} {
		f.setStatus(kafkaUserGVK, spec.Namespace(), user, readyTrue)
	}
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
	f.objects[key] = obj.DeepCopy()
	f.applied = append(f.applied, key)
	if obj.GroupVersionKind() == certificateGVK && !f.holdCertificates {
		f.objects[key].Object["status"] = map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "True", "observedGeneration": int64(0)},
		}}
	}
	return nil
}

func (f *fakeCluster) Get(_ context.Context, gvk schema.GroupVersionKind, namespace, name string) (*unstructured.Unstructured, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onGet != nil {
		f.onGet(f)
	}
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
	delete(f.objects, key)
	f.deleted = append(f.deleted, key)
	return nil
}

// List supports label selectors of the form "key=value".
func (f *fakeCluster) List(_ context.Context, gvk schema.GroupVersionKind, namespace, labelSelector string) ([]unstructured.Unstructured, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onGet != nil {
		f.onGet(f)
	}
	key, value, _ := strings.Cut(labelSelector, "=")
	var items []unstructured.Unstructured
	for _, obj := range f.objects {
		if obj.GroupVersionKind() == gvk && obj.GetNamespace() == namespace && obj.GetLabels()[key] == value {
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
	p := New(cluster, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.PollInterval = time.Millisecond
	return p
}

var acme = Spec{CustomerID: "acme", Profile: DevProfile, Platform: KindPlatform}

func TestProvisionAppliesTemplateInOrder(t *testing.T) {
	cluster := newFakeCluster()
	if err := testProvisioner(cluster).Provision(t.Context(), acme); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"Namespace:/customer-acme",
		"Secret:customer-acme/tiered-storage-credentials",
		"Certificate:customer-acme/external-listener",
		"KafkaNodePool:customer-acme/dual-role",
		"Kafka:customer-acme/acme",
		"KafkaUser:customer-acme/admin",
		"KafkaUser:customer-acme/changeport-agent",
	}
	if strings.Join(cluster.applied, ",") != strings.Join(want, ",") {
		t.Errorf("applied %v, want %v", cluster.applied, want)
	}
}

func TestProvisionIsIdempotent(t *testing.T) {
	cluster := newFakeCluster()
	p := testProvisioner(cluster)
	for range 2 {
		if err := p.Provision(t.Context(), acme); err != nil {
			t.Fatal(err)
		}
	}
	if len(cluster.objects) != 7 {
		t.Errorf("cluster holds %d objects after provisioning twice, want 7", len(cluster.objects))
	}
}

func TestProvisionWaitsForTheListenerCertificate(t *testing.T) {
	cluster := newFakeCluster()
	cluster.holdCertificates = true
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	err := testProvisioner(cluster).Provision(ctx, acme)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "Certificate customer-acme/external-listener") {
		t.Fatalf("Provision() error = %v, want a deadline error naming the certificate", err)
	}
	if got := cluster.applied[len(cluster.applied)-1]; got != "Certificate:customer-acme/external-listener" {
		t.Errorf("last applied %s; nothing may be applied before the certificate is issued", got)
	}
}

func TestProvisionReportsApplyErrors(t *testing.T) {
	cluster := newFakeCluster()
	cluster.applyErr = errors.New("forbidden")
	err := testProvisioner(cluster).Provision(t.Context(), acme)
	if err == nil || !strings.Contains(err.Error(), "Namespace customer-acme") {
		t.Fatalf("Provision() error = %v, want it to name the failing object", err)
	}
}

func TestProvisionRejectsInvalidSpec(t *testing.T) {
	cluster := newFakeCluster()
	if err := testProvisioner(cluster).Provision(t.Context(), Spec{CustomerID: "BAD", Profile: DevProfile, Platform: KindPlatform}); err == nil {
		t.Fatal("Provision() = nil, want a validation error")
	}
	if len(cluster.applied) != 0 {
		t.Errorf("applied %v for an invalid spec", cluster.applied)
	}
}

func TestWaitReadyReturnsOnceKafkaAndUsersAreReady(t *testing.T) {
	cluster := newFakeCluster()
	p := testProvisioner(cluster)
	if err := p.Provision(t.Context(), acme); err != nil {
		t.Fatal(err)
	}
	polls := 0
	cluster.onGet = func(f *fakeCluster) {
		polls++
		switch polls {
		case 2:
			f.setStatus(kafkaGVK, acme.Namespace(), acme.ClusterName(), map[string]any{"type": "NotReady", "status": "True", "reason": "Creating"})
		case 4:
			f.setStatus(kafkaGVK, acme.Namespace(), acme.ClusterName(), readyTrue)
		case 6:
			f.setUsersReady(acme)
		}
	}
	if err := p.WaitReady(t.Context(), acme); err != nil {
		t.Fatal(err)
	}
	// Kafka is ready on poll 4, the admin user from poll 6 and the agent on poll 7.
	if polls != 7 {
		t.Errorf("polled %d times, want 7", polls)
	}
}

func TestWaitReadyTimesOutWithLastStatus(t *testing.T) {
	cluster := newFakeCluster()
	p := testProvisioner(cluster)
	if err := p.Provision(t.Context(), acme); err != nil {
		t.Fatal(err)
	}
	cluster.setStatus(kafkaGVK, acme.Namespace(), acme.ClusterName(), map[string]any{"type": "NotReady", "status": "True", "reason": "Creating"})
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	err := p.WaitReady(ctx, acme)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "NotReady=True (Creating)") {
		t.Fatalf("WaitReady() error = %v, want a deadline error with the last status", err)
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

func TestDeprovisionDeletesNamespaceAndWaits(t *testing.T) {
	cluster := newFakeCluster()
	p := testProvisioner(cluster)
	if err := p.Provision(t.Context(), acme); err != nil {
		t.Fatal(err)
	}
	if err := p.Deprovision(t.Context(), acme); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(cluster.deleted, ","); got != "Namespace:/customer-acme" {
		t.Errorf("deleted %s, want only the namespace on kind", got)
	}
	if err := p.WaitDeleted(t.Context(), acme); err != nil {
		t.Fatal(err)
	}
}

func TestProvisionAssociatesTheBrokersOnEKS(t *testing.T) {
	spec := Spec{CustomerID: "acme", Profile: ProductionProfile, Platform: EKSProdPlatform}
	cluster := newFakeCluster()
	p := testProvisioner(cluster)
	if err := p.Provision(t.Context(), spec); err == nil || len(cluster.applied) != 0 {
		t.Fatalf("Provision() without AWS access = %v after applying %v; want an error before applying anything", err, cluster.applied)
	}
	identities := &fakePodIdentities{}
	p.PodIdentities = identities
	if err := p.Provision(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
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

func TestProvisionLeavesPodIdentityAloneOnKind(t *testing.T) {
	identities := &fakePodIdentities{}
	p := testProvisioner(newFakeCluster())
	p.PodIdentities = identities
	if err := p.Provision(t.Context(), acme); err != nil {
		t.Fatal(err)
	}
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
	if err := p.Provision(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
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
	polls := 0
	cluster.onGet = func(f *fakeCluster) {
		if polls++; polls == 6 {
			delete(f.objects, fakeKey(nodeClaimGVK, "", nodeClaim.GetName()))
		}
	}
	if err := p.WaitDeleted(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	if _, still := cluster.objects[fakeKey(nodeClaimGVK, "", nodeClaim.GetName())]; still {
		t.Error("WaitDeleted returned before the nodes were gone")
	}
}

func TestDeprovisionRejectsInvalidCustomerID(t *testing.T) {
	spec := Spec{CustomerID: "kube-system/../x", Profile: DevProfile, Platform: KindPlatform}
	if err := testProvisioner(newFakeCluster()).Deprovision(t.Context(), spec); err == nil {
		t.Fatal("Deprovision() = nil, want a validation error")
	}
}
