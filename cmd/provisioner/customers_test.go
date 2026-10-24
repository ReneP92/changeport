package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ReneP92/changeport/api/v1alpha1"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func init() { pollInterval = time.Millisecond }

func fakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithStatusSubresource(&v1alpha1.Customer{}).Build()
}

// getCustomer returns the Customer acme.
func getCustomer(t *testing.T, c client.Client) *v1alpha1.Customer {
	t.Helper()
	var customer v1alpha1.Customer
	if err := c.Get(t.Context(), types.NamespacedName{Name: "acme"}, &customer); err != nil {
		t.Fatal(err)
	}
	return &customer
}

func TestApplyCustomerCreatesAndUpdates(t *testing.T) {
	c := fakeClient(t)
	if err := applyCustomer(t.Context(), c, "acme", "acme", "dev", nil); err != nil {
		t.Fatal(err)
	}
	if err := applyCustomer(t.Context(), c, "acme", "Acme Corp", "dev", nil); err != nil {
		t.Fatal(err)
	}
	spec := getCustomer(t, c).Spec
	if spec.DisplayName != "Acme Corp" || spec.Profile != "dev" {
		t.Errorf("spec = %+v, want display name Acme Corp and profile dev", spec)
	}
}

func TestNewProductionCustomersAreProtected(t *testing.T) {
	off, on := false, true
	tests := []struct {
		name, profile string
		protection    *bool
		want          bool
	}{
		{"dev by default", "dev", nil, false},
		{"production by default", "production", nil, true},
		{"production unprotected", "production", &off, false},
		{"dev protected", "dev", &on, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fakeClient(t)
			if err := applyCustomer(t.Context(), c, "acme", "Acme Corp", tt.profile, tt.protection); err != nil {
				t.Fatal(err)
			}
			if got := getCustomer(t, c).Spec.DeletionProtection; got != tt.want {
				t.Errorf("deletion protection = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestReprovisioningKeepsProtectionAsItIs(t *testing.T) {
	c := fakeClient(t)
	if err := applyCustomer(t.Context(), c, "acme", "Acme Corp", "production", nil); err != nil {
		t.Fatal(err)
	}
	if err := setProtection(t.Context(), c, "acme", false); err != nil {
		t.Fatal(err)
	}
	// Applying again, as dev-up does, leaves protection off.
	if err := applyCustomer(t.Context(), c, "acme", "Acme Corp", "production", nil); err != nil {
		t.Fatal(err)
	}
	if getCustomer(t, c).Spec.DeletionProtection {
		t.Error("applying an unprotected production Customer again protected it")
	}
	if err := setProtection(t.Context(), c, "acme", true); err != nil {
		t.Fatal(err)
	}
	if !getCustomer(t, c).Spec.DeletionProtection {
		t.Error("protect left protection off")
	}
}

// setReady gives the Customer acme a Ready condition for generation.
func setReady(t *testing.T, c client.Client, status metav1.ConditionStatus, reason string, generation int64) {
	t.Helper()
	customer := getCustomer(t, c)
	meta.SetStatusCondition(&customer.Status.Conditions, metav1.Condition{
		Type: v1alpha1.ConditionReady, Status: status, Reason: reason, Message: "details", ObservedGeneration: generation,
	})
	customer.Status.BootstrapAddress = "bootstrap.acme.kafka.changeport.test:9094"
	if err := c.Status().Update(t.Context(), customer); err != nil {
		t.Fatal(err)
	}
}

func newCustomerObject(generation int64) *v1alpha1.Customer {
	return &v1alpha1.Customer{
		ObjectMeta: metav1.ObjectMeta{Name: "acme", Generation: generation},
		Spec:       v1alpha1.CustomerSpec{DisplayName: "acme", Profile: "dev"},
	}
}

func TestWaitReadyWaitsForTheLatestGeneration(t *testing.T) {
	c := fakeClient(t, newCustomerObject(2))
	// Ready for an older spec does not count.
	setReady(t, c, metav1.ConditionTrue, "Done", 1)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := waitReady(ctx, c, "acme", discard); !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "not reconciled yet") {
		t.Fatalf("waitReady() = %v, want a deadline error: the latest spec is not reconciled", err)
	}

	setReady(t, c, metav1.ConditionTrue, "Done", 2)
	if err := waitReady(t.Context(), c, "acme", discard); err != nil {
		t.Fatal(err)
	}
}

func TestWaitReadyTimesOutWithTheLastStatus(t *testing.T) {
	c := fakeClient(t, newCustomerObject(1))
	setReady(t, c, metav1.ConditionFalse, "KafkaPending", 1)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err := waitReady(ctx, c, "acme", discard)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "KafkaPending: details") {
		t.Fatalf("waitReady() = %v, want a deadline error with the last status", err)
	}
}

func TestDeleteCustomer(t *testing.T) {
	protected := newCustomerObject(1)
	protected.Spec.DeletionProtection = true
	c := fakeClient(t, protected)
	if err := deleteCustomer(t.Context(), c, "acme"); !errors.Is(err, errProtected) {
		t.Fatalf("deleteCustomer() of a protected Customer = %v, want errProtected", err)
	}
	getCustomer(t, c)

	protected = getCustomer(t, c)
	protected.Spec.DeletionProtection = false
	if err := c.Update(t.Context(), protected); err != nil {
		t.Fatal(err)
	}
	if err := deleteCustomer(t.Context(), c, "acme"); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), types.NamespacedName{Name: "acme"}, &v1alpha1.Customer{}); !apierrors.IsNotFound(err) {
		t.Errorf("get after deleting = %v, want not found", err)
	}
	if err := deleteCustomer(t.Context(), c, "acme"); err != nil {
		t.Errorf("deleteCustomer() of a missing Customer = %v, want nil", err)
	}
	if err := waitGone(t.Context(), c, "acme", discard); err != nil {
		t.Errorf("waitGone() of a missing Customer = %v", err)
	}
}

func TestWaitGoneTimesOutWithTheLastStatus(t *testing.T) {
	c := fakeClient(t, newCustomerObject(1))
	setReady(t, c, metav1.ConditionFalse, "Deprovisioning", 1)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err := waitGone(ctx, c, "acme", discard)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "Deprovisioning: details") {
		t.Fatalf("waitGone() = %v, want a deadline error with the last status", err)
	}
}

func TestWaitGoneIgnoresTheStatusBeforeTheDeletion(t *testing.T) {
	c := fakeClient(t, newCustomerObject(1))
	setReady(t, c, metav1.ConditionTrue, v1alpha1.ReasonDone, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err := waitGone(ctx, c, "acme", discard)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "waiting for the controller") {
		t.Fatalf("waitGone() = %v, want a deadline error waiting for the controller", err)
	}
}
