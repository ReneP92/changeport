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

// fakeCluster is an in-memory Cluster. onGet, when set, runs before every Get
// so tests can change objects while the provisioner polls.
type fakeCluster struct {
	mu       sync.Mutex
	objects  map[string]*unstructured.Unstructured
	applied  []string
	applyErr error
	onGet    func(f *fakeCluster)
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{objects: map[string]*unstructured.Unstructured{}}
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
	delete(f.objects, fakeKey(gvk, namespace, name))
	return nil
}

func testProvisioner(cluster Cluster) *Provisioner {
	p := New(cluster, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.PollInterval = time.Millisecond
	return p
}

var acme = Spec{CustomerID: "acme", Profile: DevProfile}

func TestProvisionAppliesTemplateInOrder(t *testing.T) {
	cluster := newFakeCluster()
	if err := testProvisioner(cluster).Provision(t.Context(), acme); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"Namespace:/customer-acme",
		"KafkaNodePool:customer-acme/dual-role",
		"Kafka:customer-acme/acme",
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
	if len(cluster.objects) != 3 {
		t.Errorf("cluster holds %d objects after provisioning twice, want 3", len(cluster.objects))
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
	if err := testProvisioner(cluster).Provision(t.Context(), Spec{CustomerID: "BAD", Profile: DevProfile}); err == nil {
		t.Fatal("Provision() = nil, want a validation error")
	}
	if len(cluster.applied) != 0 {
		t.Errorf("applied %v for an invalid spec", cluster.applied)
	}
}

func setKafkaStatus(f *fakeCluster, observedGeneration int64, conditions ...map[string]any) {
	kafka := f.objects[fakeKey(kafkaGVK, acme.Namespace(), acme.ClusterName())]
	condList := make([]any, len(conditions))
	for i, c := range conditions {
		condList[i] = c
	}
	kafka.Object["status"] = map[string]any{
		"observedGeneration": observedGeneration,
		"conditions":         condList,
	}
}

func TestWaitReadyReturnsOnceKafkaIsReady(t *testing.T) {
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
			setKafkaStatus(f, 1, map[string]any{"type": "NotReady", "status": "True", "reason": "Creating"})
		case 4:
			setKafkaStatus(f, 1, map[string]any{"type": "Ready", "status": "True"})
		}
	}
	if err := p.WaitReady(t.Context(), acme); err != nil {
		t.Fatal(err)
	}
	if polls != 4 {
		t.Errorf("polled %d times, want 4", polls)
	}
}

func TestWaitReadyTimesOutWithLastStatus(t *testing.T) {
	cluster := newFakeCluster()
	p := testProvisioner(cluster)
	if err := p.Provision(t.Context(), acme); err != nil {
		t.Fatal(err)
	}
	setKafkaStatus(cluster, 1, map[string]any{"type": "NotReady", "status": "True", "reason": "Creating"})
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	err := p.WaitReady(ctx, acme)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "NotReady=True (Creating)") {
		t.Fatalf("WaitReady() error = %v, want a deadline error with the last status", err)
	}
}

func TestKafkaReady(t *testing.T) {
	ready := map[string]any{"type": "Ready", "status": "True"}
	tests := []struct {
		name       string
		generation int64
		status     map[string]any
		want       bool
		message    string
	}{
		{"no status", 1, nil, false, "operator has not reconciled the latest spec"},
		{"stale generation", 2, map[string]any{"observedGeneration": int64(1), "conditions": []any{ready}}, false, "operator has not reconciled the latest spec"},
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
			got, message := kafkaReady(kafka)
			if got != tt.want || message != tt.message {
				t.Errorf("kafkaReady() = %v, %q; want %v, %q", got, message, tt.want, tt.message)
			}
		})
	}
}

func TestKafkaWarnings(t *testing.T) {
	kafka := &unstructured.Unstructured{Object: map[string]any{"status": map[string]any{"conditions": []any{
		map[string]any{"type": "Warning", "status": "True", "reason": "KafkaStorage", "message": "data loss on restart"},
		map[string]any{"type": "Warning", "status": "False", "reason": "Resolved", "message": "ignored"},
		map[string]any{"type": "Ready", "status": "True"},
	}}}}
	got := kafkaWarnings(kafka)
	if len(got) != 1 || got[0] != "KafkaStorage: data loss on restart" {
		t.Errorf("kafkaWarnings() = %q, want the one active warning", got)
	}
	if got := kafkaWarnings(&unstructured.Unstructured{Object: map[string]any{}}); len(got) != 0 {
		t.Errorf("kafkaWarnings() without status = %q, want none", got)
	}
}

func TestDeprovisionDeletesNamespaceAndWaits(t *testing.T) {
	cluster := newFakeCluster()
	p := testProvisioner(cluster)
	if err := p.Provision(t.Context(), acme); err != nil {
		t.Fatal(err)
	}
	if err := p.Deprovision(t.Context(), "acme"); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.Get(t.Context(), namespaceGVK, "", "customer-acme"); !apierrors.IsNotFound(err) {
		t.Fatalf("namespace still present after Deprovision: %v", err)
	}
	if err := p.WaitDeleted(t.Context(), "acme"); err != nil {
		t.Fatal(err)
	}
}

func TestDeprovisionRejectsInvalidCustomerID(t *testing.T) {
	if err := testProvisioner(newFakeCluster()).Deprovision(t.Context(), "kube-system/../x"); err == nil {
		t.Fatal("Deprovision() = nil, want a validation error")
	}
}
