package controller

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/provisioning"
)

// testEnv is a real API server with the CRDs of Changeport, Strimzi and
// cert-manager, and nothing else: no controllers, so the tests play
// cert-manager, Strimzi and the namespace controller themselves. It is nil
// when envtest's binaries are not installed (KUBEBUILDER_ASSETS).
var testEnv *rest.Config

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		return m.Run()
	}
	root := filepath.Join("..", "..")
	env := &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join(root, "deploy", "base", "provisioner"),
			filepath.Join(root, "deploy", "base", "strimzi", "upstream"),
			filepath.Join(root, "deploy", "base", "cert-manager", "upstream"),
		},
		ErrorIfCRDPathMissing: true,
	}
	config, err := env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start envtest:", err)
		return 1
	}
	testEnv = config
	// The controller's own logs only clutter test output.
	ctrl.SetLogger(logr.Discard())
	code := m.Run()
	if err := env.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "stop envtest:", err)
		return 1
	}
	return code
}

// requireEnv skips a test that needs the API server when envtest is not
// installed, except in CI, where that is a failure.
func requireEnv(t *testing.T) {
	t.Helper()
	if testEnv != nil {
		return
	}
	if os.Getenv("CI") != "" {
		t.Fatal("KUBEBUILDER_ASSETS is not set; run the tests with make test")
	}
	t.Skip("envtest is not installed; run the tests with make test")
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

// startController runs the Customer controller with the real provisioner
// against the API server until the test ends, and returns a client.
func startController(t *testing.T) client.Client {
	t.Helper()
	requireEnv(t)
	scheme := testScheme(t)
	mgr, err := ctrl.NewManager(testEnv, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		// Runs with -count above 1 start a controller of the same name again.
		Controller: config.Controller{SkipNameValidation: ptr.To(true)},
	})
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := provisioning.NewKubeCluster(testEnv)
	if err != nil {
		t.Fatal(err)
	}
	provisioner := provisioning.New(cluster, slog.New(slog.NewTextHandler(io.Discard, nil)))
	reconciler := &CustomerReconciler{
		Client:         mgr.GetClient(),
		Provisioner:    provisioner,
		Platform:       provisioning.KindPlatform,
		PollInterval:   100 * time.Millisecond,
		ResyncInterval: time.Minute,
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("controller manager: %v", err)
		}
	})
	c, err := client.New(testEnv, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// eventually polls condition until it holds, failing the test with the last
// state it describes after a timeout.
func eventually(t *testing.T, what string, condition func() (bool, string)) {
	t.Helper()
	last := ""
	err := wait.PollUntilContextTimeout(t.Context(), 50*time.Millisecond, 20*time.Second, true, func(context.Context) (bool, error) {
		done, state := condition()
		last = state
		return done, nil
	})
	if err != nil {
		t.Fatalf("%s: %v; last state: %s", what, err, last)
	}
}

// readyReason waits until the Customer's Ready condition has reason.
func readyReason(t *testing.T, c client.Client, name, reason string) *v1alpha1.Customer {
	t.Helper()
	var customer v1alpha1.Customer
	eventually(t, "Ready reason "+reason, func() (bool, string) {
		if err := c.Get(t.Context(), types.NamespacedName{Name: name}, &customer); err != nil {
			return false, err.Error()
		}
		ready := meta.FindStatusCondition(customer.Status.Conditions, v1alpha1.ConditionReady)
		if ready == nil {
			return false, "no Ready condition"
		}
		return ready.Reason == reason, fmt.Sprintf("%s %s: %s", ready.Status, ready.Reason, ready.Message)
	})
	return &customer
}

var (
	certificateGVK = schema.GroupVersionKind{Group: "cert-manager.io", Version: "v1", Kind: "Certificate"}
	kafkaGVK       = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1", Kind: "Kafka"}
	kafkaUserGVK   = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1", Kind: "KafkaUser"}
	deploymentGVK  = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
)

func getObject(t *testing.T, c client.Client, gvk schema.GroupVersionKind, namespace, name string) (*unstructured.Unstructured, error) {
	t.Helper()
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	err := c.Get(t.Context(), types.NamespacedName{Namespace: namespace, Name: name}, obj)
	return obj, err
}

// setStatus writes the status an operator would, for the object's current
// generation.
func setStatus(t *testing.T, c client.Client, gvk schema.GroupVersionKind, namespace, name string, status func(generation int64) map[string]any) {
	t.Helper()
	obj, err := getObject(t, c, gvk, namespace, name)
	if err != nil {
		t.Fatal(err)
	}
	obj.Object["status"] = status(obj.GetGeneration())
	if err := c.Status().Update(t.Context(), obj); err != nil {
		t.Fatalf("set the status of %s %s/%s: %v", gvk.Kind, namespace, name, err)
	}
}

func strimziReadyStatus(generation int64) map[string]any {
	return map[string]any{
		"observedGeneration": generation,
		"conditions":         []any{map[string]any{"type": "Ready", "status": "True", "lastTransitionTime": "2026-10-21T00:00:00Z"}},
	}
}

// lifecycleRuns numbers the runs of TestCustomerLifecycle, which share the
// API server with -count above 1: each runs its own customer, since envtest
// has no garbage collector to remove the last one's resources.
var lifecycleRuns atomic.Int64

func TestCustomerLifecycle(t *testing.T) {
	c := startController(t)
	name := fmt.Sprintf("lifecycle-%d", lifecycleRuns.Add(1))
	customer := &v1alpha1.Customer{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1alpha1.CustomerSpec{DisplayName: "Acme Corp", Profile: "dev", ClerkOrganizationID: "org_2acme"},
	}
	if err := c.Create(t.Context(), customer); err != nil {
		t.Fatal(err)
	}
	namespace := "customer-" + name

	// The controller applies the template up to the listener certificate and
	// waits for cert-manager.
	pending := readyReason(t, c, name, "CertificatePending")
	if got := pending.Labels[v1alpha1.ClerkOrganizationLabel]; got != "org_2acme" {
		t.Errorf("Clerk organization label = %q, want org_2acme", got)
	}
	if _, err := getObject(t, c, kafkaGVK, namespace, name); !apierrors.IsNotFound(err) {
		t.Fatalf("get the Kafka cluster before the certificate is issued: %v, want not found", err)
	}
	setStatus(t, c, certificateGVK, namespace, "external-listener", func(generation int64) map[string]any {
		return map[string]any{"conditions": []any{map[string]any{
			"type": "Ready", "status": "True", "observedGeneration": generation, "lastTransitionTime": "2026-10-21T00:00:00Z",
		}}}
	})

	// Then it creates the cluster and waits for Strimzi.
	readyReason(t, c, name, "KafkaPending")
	setStatus(t, c, kafkaGVK, namespace, name, strimziReadyStatus)
	readyReason(t, c, name, "UsersPending")
	for _, user := range []string{provisioning.AdminUser, provisioning.AgentUser} {
		setStatus(t, c, kafkaUserGVK, namespace, user, strimziReadyStatus)
	}
	readyReason(t, c, name, "AgentPending")
	setStatus(t, c, deploymentGVK, namespace, "changeport-agent", func(generation int64) map[string]any {
		return map[string]any{
			"observedGeneration": generation,
			"replicas":           int64(1),
			"updatedReplicas":    int64(1),
			"readyReplicas":      int64(1),
			"availableReplicas":  int64(1),
		}
	})
	ready := readyReason(t, c, name, "Done")
	if ready.Status.BootstrapAddress != "bootstrap."+name+".kafka.changeport.test:9094" {
		t.Errorf("bootstrap address = %q", ready.Status.BootstrapAddress)
	}
	for _, conditionType := range []string{v1alpha1.ConditionCertificateIssued, v1alpha1.ConditionKafkaReady, v1alpha1.ConditionUsersReady, v1alpha1.ConditionAgentAvailable, v1alpha1.ConditionReady} {
		if !meta.IsStatusConditionTrue(ready.Status.Conditions, conditionType) {
			t.Errorf("%s is not True: %+v", conditionType, ready.Status.Conditions)
		}
	}

	// Deleting the Customer deletes its namespace, and the Customer goes once
	// the namespace has.
	if err := c.Delete(t.Context(), ready); err != nil {
		t.Fatal(err)
	}
	readyReason(t, c, name, "Deprovisioning")
	finalizeNamespace(t, namespace)
	eventually(t, "the Customer is gone", func() (bool, string) {
		err := c.Get(t.Context(), types.NamespacedName{Name: name}, &v1alpha1.Customer{})
		if apierrors.IsNotFound(err) {
			return true, ""
		}
		return false, fmt.Sprintf("get: %v", err)
	})
}

// finalizeNamespace does what the namespace controller would once a
// namespace is empty: it removes the namespace's finalizer, so that the API
// server deletes it.
func finalizeNamespace(t *testing.T, name string) {
	t.Helper()
	clientset, err := kubernetes.NewForConfig(testEnv)
	if err != nil {
		t.Fatal(err)
	}
	namespace, err := clientset.CoreV1().Namespaces().Get(t.Context(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if namespace.DeletionTimestamp == nil {
		t.Fatalf("namespace %s is not being deleted", name)
	}
	namespace.Spec.Finalizers = []corev1.FinalizerName{}
	if _, err := clientset.CoreV1().Namespaces().Finalize(t.Context(), namespace, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestCustomerValidation(t *testing.T) {
	requireEnv(t)
	c, err := client.New(testEnv, client.Options{Scheme: testScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	valid := func(name string) *v1alpha1.Customer {
		return &v1alpha1.Customer{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       v1alpha1.CustomerSpec{DisplayName: "Valid", Profile: "dev"},
		}
	}
	tests := []struct {
		name     string
		customer func() *v1alpha1.Customer
		want     string
	}{
		{"a name with a dot", func() *v1alpha1.Customer { return valid("acme.corp") }, "the name is the customer ID"},
		{"a name starting with a digit", func() *v1alpha1.Customer { return valid("1acme") }, "the name is the customer ID"},
		{"a name over 30 characters", func() *v1alpha1.Customer { return valid(strings.Repeat("a", 31)) }, "the name is the customer ID"},
		{"an unknown profile", func() *v1alpha1.Customer {
			customer := valid("profile-test")
			customer.Spec.Profile = "huge"
			return customer
		}, `spec.profile: Unsupported value: "huge"`},
		{"no display name", func() *v1alpha1.Customer {
			customer := valid("name-test")
			customer.Spec.DisplayName = ""
			return customer
		}, "spec.displayName"},
		{"a Clerk organization ID that is not one", func() *v1alpha1.Customer {
			customer := valid("clerk-test")
			customer.Spec.ClerkOrganizationID = "user_2abc"
			return customer
		}, "spec.clerkOrganizationID"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := c.Create(t.Context(), test.customer())
			if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), test.want) {
				t.Errorf("Create() error = %v, want it invalid with %q", err, test.want)
			}
		})
	}

	t.Run("a changed profile", func(t *testing.T) {
		customer := valid("immutable-test")
		if err := c.Create(t.Context(), customer); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Delete(context.Background(), customer) })
		customer.Spec.Profile = "production"
		if err := c.Update(t.Context(), customer); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "the profile cannot change") {
			t.Errorf("Update() error = %v, want the profile refused", err)
		}
	})
}
