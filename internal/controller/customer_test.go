package controller

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/yaml"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/provisioning"
)

// fakeProvisioner reports the progress and deletion state a test sets, and
// records what it was asked to do.
type fakeProvisioner struct {
	progress     provisioning.Progress
	reconcileErr error
	deleted      bool
	left         string
	calls        []string
}

func (f *fakeProvisioner) Reconcile(_ context.Context, spec provisioning.Spec) (provisioning.Progress, error) {
	f.calls = append(f.calls, "reconcile "+spec.CustomerID+" "+spec.Profile.Name+" "+spec.Platform.Name)
	return f.progress, f.reconcileErr
}

func (f *fakeProvisioner) Deprovision(_ context.Context, spec provisioning.Spec) error {
	f.calls = append(f.calls, "deprovision "+spec.CustomerID)
	return nil
}

func (f *fakeProvisioner) Deleted(context.Context, provisioning.Spec) (bool, string, error) {
	return f.deleted, f.left, nil
}

var (
	issued      = provisioning.Readiness{Ready: true, Status: "Issued"}
	readyPart   = provisioning.Readiness{Ready: true, Status: "Ready"}
	creating    = provisioning.Readiness{Object: "Kafka customer-acme/acme", Status: "NotReady=True (Creating)"}
	waiting     = provisioning.Readiness{Status: "waiting for the listener certificate"}
	allReady    = provisioning.Progress{Certificate: issued, Kafka: readyPart, Users: readyPart, Agent: readyPart}
	kafkaCreate = provisioning.Progress{Certificate: issued, Kafka: creating, Users: waiting, Agent: waiting}
)

const (
	pollInterval   = 10 * time.Second
	resyncInterval = 10 * time.Minute
)

// newCustomer returns the Customer acme, which a test may change before
// creating it.
func newCustomer() *v1alpha1.Customer {
	return &v1alpha1.Customer{
		ObjectMeta: metav1.ObjectMeta{Name: "acme", Generation: 1},
		Spec:       v1alpha1.CustomerSpec{DisplayName: "Acme Corp", Profile: "dev", ClerkOrganizationID: "org_2abc"},
	}
}

func newReconciler(t *testing.T, provisioner Provisioner, objects ...client.Object) (*CustomerReconciler, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&v1alpha1.Customer{}).
		Build()
	return &CustomerReconciler{
		Client:         c,
		Provisioner:    provisioner,
		Platform:       provisioning.KindPlatform,
		PollInterval:   pollInterval,
		ResyncInterval: resyncInterval,
	}, c
}

func reconcile(t *testing.T, r *CustomerReconciler, name string) (ctrl.Result, error) {
	t.Helper()
	return r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: name}})
}

func get(t *testing.T, c client.Client, name string) *v1alpha1.Customer {
	t.Helper()
	var customer v1alpha1.Customer
	if err := c.Get(t.Context(), types.NamespacedName{Name: name}, &customer); err != nil {
		t.Fatal(err)
	}
	return &customer
}

func condition(t *testing.T, customer *v1alpha1.Customer, conditionType string) metav1.Condition {
	t.Helper()
	c := meta.FindStatusCondition(customer.Status.Conditions, conditionType)
	if c == nil {
		t.Fatalf("no %s condition in %+v", conditionType, customer.Status.Conditions)
	}
	return *c
}

func TestReconcileProvisionsANewCustomer(t *testing.T) {
	provisioner := &fakeProvisioner{progress: kafkaCreate}
	r, c := newReconciler(t, provisioner, newCustomer())
	result, err := reconcile(t, r, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != pollInterval {
		t.Errorf("requeue after %v, want %v while the cluster is not ready", result.RequeueAfter, pollInterval)
	}
	if len(provisioner.calls) != 1 || provisioner.calls[0] != "reconcile acme dev kind" {
		t.Errorf("provisioner calls = %v, want one reconcile of acme's dev cluster on kind", provisioner.calls)
	}

	customer := get(t, c, "acme")
	if len(customer.Finalizers) != 1 || customer.Finalizers[0] != v1alpha1.Finalizer {
		t.Errorf("finalizers = %v, want %s", customer.Finalizers, v1alpha1.Finalizer)
	}
	if got := customer.Labels[v1alpha1.ClerkOrganizationLabel]; got != "org_2abc" {
		t.Errorf("Clerk organization label = %q, want org_2abc", got)
	}
	if customer.Status.BootstrapAddress != "bootstrap.acme.kafka.changeport.test:9094" {
		t.Errorf("bootstrap address = %q", customer.Status.BootstrapAddress)
	}
	if customer.Status.ObservedGeneration != 1 {
		t.Errorf("observed generation = %d, want 1", customer.Status.ObservedGeneration)
	}
	want := map[string]struct {
		status  metav1.ConditionStatus
		reason  string
		message string
	}{
		v1alpha1.ConditionCertificateIssued: {metav1.ConditionTrue, "Done", "Issued"},
		v1alpha1.ConditionKafkaReady:        {metav1.ConditionFalse, "Pending", "Kafka customer-acme/acme: NotReady=True (Creating)"},
		v1alpha1.ConditionUsersReady:        {metav1.ConditionFalse, "Pending", "waiting for the listener certificate"},
		v1alpha1.ConditionAgentAvailable:    {metav1.ConditionFalse, "Pending", "waiting for the listener certificate"},
		v1alpha1.ConditionReady:             {metav1.ConditionFalse, "KafkaPending", "Kafka customer-acme/acme: NotReady=True (Creating)"},
	}
	for conditionType, w := range want {
		got := condition(t, customer, conditionType)
		if got.Status != w.status || got.Reason != w.reason || got.Message != w.message || got.ObservedGeneration != 1 {
			t.Errorf("%s = %s %s %q (generation %d), want %s %s %q (generation 1)",
				conditionType, got.Status, got.Reason, got.Message, got.ObservedGeneration, w.status, w.reason, w.message)
		}
	}
}

func TestReconcileReportsAReadyCluster(t *testing.T) {
	r, c := newReconciler(t, &fakeProvisioner{progress: allReady}, newCustomer())
	result, err := reconcile(t, r, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != resyncInterval {
		t.Errorf("requeue after %v, want the resync interval %v", result.RequeueAfter, resyncInterval)
	}
	ready := condition(t, get(t, c, "acme"), v1alpha1.ConditionReady)
	if ready.Status != metav1.ConditionTrue || ready.Message != "Clients can connect to bootstrap.acme.kafka.changeport.test:9094" {
		t.Errorf("Ready = %s %q", ready.Status, ready.Message)
	}
}

func TestReconcileWritesNothingWhenNothingChanged(t *testing.T) {
	r, c := newReconciler(t, &fakeProvisioner{progress: allReady}, newCustomer())
	if _, err := reconcile(t, r, "acme"); err != nil {
		t.Fatal(err)
	}
	before := get(t, c, "acme").ResourceVersion
	if _, err := reconcile(t, r, "acme"); err != nil {
		t.Fatal(err)
	}
	if after := get(t, c, "acme").ResourceVersion; after != before {
		t.Errorf("resource version went from %s to %s although nothing changed", before, after)
	}
}

func TestReconcileKeepsTransitionTimesOfUnchangedConditions(t *testing.T) {
	provisioner := &fakeProvisioner{progress: kafkaCreate}
	r, c := newReconciler(t, provisioner, newCustomer())
	if _, err := reconcile(t, r, "acme"); err != nil {
		t.Fatal(err)
	}
	// Condition times have a resolution of one second.
	issuedAt := condition(t, get(t, c, "acme"), v1alpha1.ConditionCertificateIssued).LastTransitionTime
	time.Sleep(1100 * time.Millisecond)
	provisioner.progress = allReady
	if _, err := reconcile(t, r, "acme"); err != nil {
		t.Fatal(err)
	}
	customer := get(t, c, "acme")
	if got := condition(t, customer, v1alpha1.ConditionCertificateIssued).LastTransitionTime; !got.Equal(&issuedAt) {
		t.Errorf("CertificateIssued moved from %v to %v without changing", issuedAt, got)
	}
	if got := condition(t, customer, v1alpha1.ConditionKafkaReady).LastTransitionTime; !got.After(issuedAt.Time) {
		t.Errorf("KafkaReady transitioned at %v, want after %v", got, issuedAt)
	}
}

func TestReconcileReportsProvisioningErrors(t *testing.T) {
	r, c := newReconciler(t, &fakeProvisioner{reconcileErr: errors.New("apply Kafka customer-acme/acme: forbidden")}, newCustomer())
	if _, err := reconcile(t, r, "acme"); err == nil {
		t.Fatal("Reconcile() = nil, want the error so that it retries with backoff")
	}
	ready := condition(t, get(t, c, "acme"), v1alpha1.ConditionReady)
	if ready.Status != metav1.ConditionFalse || ready.Reason != "ProvisioningFailed" || ready.Message != "apply Kafka customer-acme/acme: forbidden" {
		t.Errorf("Ready = %s %s %q", ready.Status, ready.Reason, ready.Message)
	}
}

func TestReconcileRefusesASpecThePlatformCannotRun(t *testing.T) {
	provisioner := &fakeProvisioner{progress: allReady}
	r, c := newReconciler(t, provisioner, newCustomer())
	r.Platform.Zones = 1
	customer := get(t, c, "acme")
	customer.Spec.Profile = "production"
	if err := c.Update(t.Context(), customer); err != nil {
		t.Fatal(err)
	}
	if _, err := reconcile(t, r, "acme"); err != nil {
		t.Fatalf("Reconcile() = %v, want no retry until the Customer changes", err)
	}
	if len(provisioner.calls) != 0 {
		t.Errorf("provisioner calls = %v for a spec the platform cannot run", provisioner.calls)
	}
	if ready := condition(t, get(t, c, "acme"), v1alpha1.ConditionReady); ready.Reason != "InvalidSpec" {
		t.Errorf("Ready reason = %s, want InvalidSpec", ready.Reason)
	}
}

func TestReconcileRemovesTheClerkLabelWithTheOrganization(t *testing.T) {
	customer := newCustomer()
	customer.Labels = map[string]string{v1alpha1.ClerkOrganizationLabel: "org_old", "team": "sales"}
	customer.Spec.ClerkOrganizationID = ""
	r, c := newReconciler(t, &fakeProvisioner{progress: allReady}, customer)
	if _, err := reconcile(t, r, "acme"); err != nil {
		t.Fatal(err)
	}
	labels := get(t, c, "acme").Labels
	if _, ok := labels[v1alpha1.ClerkOrganizationLabel]; ok || labels["team"] != "sales" {
		t.Errorf("labels = %v, want only the Clerk organization label gone", labels)
	}
}

// deleting deletes a Customer, which its finalizer keeps until the
// reconciler removes it.
func deleting(t *testing.T, c client.Client, name string) {
	t.Helper()
	if err := c.Delete(t.Context(), get(t, c, name)); err != nil {
		t.Fatal(err)
	}
}

func TestReconcileDeprovisionsADeletedCustomer(t *testing.T) {
	provisioner := &fakeProvisioner{progress: allReady, left: "Namespace customer-acme is being deleted"}
	r, c := newReconciler(t, provisioner, newCustomer())
	if _, err := reconcile(t, r, "acme"); err != nil {
		t.Fatal(err)
	}
	deleting(t, c, "acme")

	result, err := reconcile(t, r, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != pollInterval {
		t.Errorf("requeue after %v, want %v while the cluster is being deleted", result.RequeueAfter, pollInterval)
	}
	ready := condition(t, get(t, c, "acme"), v1alpha1.ConditionReady)
	if ready.Reason != "Deprovisioning" || ready.Message != "Namespace customer-acme is being deleted" {
		t.Errorf("Ready = %s %q", ready.Reason, ready.Message)
	}

	provisioner.deleted = true
	if _, err := reconcile(t, r, "acme"); err != nil {
		t.Fatal(err)
	}
	var gone v1alpha1.Customer
	if err := c.Get(t.Context(), types.NamespacedName{Name: "acme"}, &gone); err == nil {
		t.Errorf("the Customer is still there with finalizers %v", gone.Finalizers)
	}
	want := []string{"reconcile acme dev kind", "deprovision acme", "deprovision acme"}
	if !slices.Equal(provisioner.calls, want) {
		t.Errorf("provisioner calls = %v, want %v", provisioner.calls, want)
	}
}

func TestReconcileKeepsAProtectedClusterUntilProtectionIsOff(t *testing.T) {
	customer := newCustomer()
	customer.Spec.DeletionProtection = true
	provisioner := &fakeProvisioner{progress: allReady, deleted: true}
	r, c := newReconciler(t, provisioner, customer)
	if _, err := reconcile(t, r, "acme"); err != nil {
		t.Fatal(err)
	}
	deleting(t, c, "acme")
	if _, err := reconcile(t, r, "acme"); err != nil {
		t.Fatal(err)
	}
	protected := get(t, c, "acme")
	if ready := condition(t, protected, v1alpha1.ConditionReady); ready.Reason != "DeletionProtected" {
		t.Errorf("Ready reason = %s, want DeletionProtected", ready.Reason)
	}
	if got := provisioner.calls[len(provisioner.calls)-1]; got == "deprovision acme" {
		t.Fatal("a protected cluster was deprovisioned")
	}

	protected.Spec.DeletionProtection = false
	if err := c.Update(t.Context(), protected); err != nil {
		t.Fatal(err)
	}
	if _, err := reconcile(t, r, "acme"); err != nil {
		t.Fatal(err)
	}
	if got := provisioner.calls[len(provisioner.calls)-1]; got != "deprovision acme" {
		t.Errorf("last provisioner call = %s, want the deprovisioning once protection is off", got)
	}
}

func TestReconcileIgnoresAMissingCustomer(t *testing.T) {
	provisioner := &fakeProvisioner{}
	r, _ := newReconciler(t, provisioner)
	if result, err := reconcile(t, r, "gone"); err != nil || result != (ctrl.Result{}) || len(provisioner.calls) != 0 {
		t.Errorf("Reconcile() of a missing Customer = %+v, %v after calls %v", result, err, provisioner.calls)
	}
}

// The CRD offers exactly the provisioner's profiles.
func TestCRDProfilesAreTheProvisioners(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "deploy", "base", "provisioner", "changeport.io_customers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatal(err)
	}
	var offered []string
	for _, value := range crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].Properties["profile"].Enum {
		var profile string
		if err := json.Unmarshal(value.Raw, &profile); err != nil {
			t.Fatal(err)
		}
		offered = append(offered, profile)
	}
	known := slices.Sorted(maps.Keys(provisioning.Profiles))
	slices.Sort(offered)
	if !slices.Equal(offered, known) {
		t.Errorf("the CRD offers the profiles %v, the provisioner knows %v", offered, known)
	}
}

func TestReconcileLeavesConflictsToTheNextReconciliation(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	// Someone else changed the Customer between the read and the write.
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(newCustomer()).
		WithStatusSubresource(&v1alpha1.Customer{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
				return apierrors.NewConflict(schema.GroupResource{Group: "changeport.io", Resource: "customers"}, "acme", errors.New("the object has been modified"))
			},
		}).
		Build()
	r := &CustomerReconciler{Client: c, Provisioner: &fakeProvisioner{progress: allReady}, Platform: provisioning.KindPlatform}
	if result, err := reconcile(t, r, "acme"); err != nil || result != (ctrl.Result{}) {
		t.Errorf("Reconcile() after a conflict = %+v, %v; want no error, since the change triggers another reconciliation", result, err)
	}
}
