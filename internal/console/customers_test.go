package console

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ReneP92/changeport/api/v1alpha1"
)

func kubeCustomers(t *testing.T, customers ...client.Object) KubeCustomers {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return KubeCustomers{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(customers...).Build()}
}

func labelled(id, org string) *v1alpha1.Customer {
	return &v1alpha1.Customer{ObjectMeta: metav1.ObjectMeta{Name: id, Labels: map[string]string{v1alpha1.ClerkOrganizationLabel: org}}}
}

func TestKubeCustomersByOrganization(t *testing.T) {
	k := kubeCustomers(t, labelled("acme", "org_acme"), labelled("globex", "org_globex"))
	customer, err := k.ByOrganization(t.Context(), "org_acme")
	if err != nil || customer == nil || customer.Name != "acme" {
		t.Errorf("ByOrganization(org_acme) = %v, %v, want acme", customer, err)
	}
	for _, org := range []string{"org_unknown", "", "not a label value!"} {
		if customer, err := k.ByOrganization(t.Context(), org); err != nil || customer != nil {
			t.Errorf("ByOrganization(%q) = %v, %v, want none", org, customer, err)
		}
	}
}

func TestKubeCustomersRefuseSharedOrganizations(t *testing.T) {
	k := kubeCustomers(t, labelled("acme", "org_shared"), labelled("globex", "org_shared"))
	if customer, err := k.ByOrganization(t.Context(), "org_shared"); err == nil {
		t.Errorf("ByOrganization() of an organization with two customers = %v, want an error", customer)
	}
}

func TestKubeCustomersGet(t *testing.T) {
	k := kubeCustomers(t, labelled("acme", "org_acme"))
	if customer, err := k.Get(t.Context(), "acme"); err != nil || customer == nil {
		t.Errorf("Get(acme) = %v, %v", customer, err)
	}
	for _, id := range []string{"initech", "Not_An_ID", "../acme"} {
		if customer, err := k.Get(t.Context(), id); err != nil || customer != nil {
			t.Errorf("Get(%q) = %v, %v, want none", id, customer, err)
		}
	}
	list, err := k.List(t.Context())
	if err != nil || len(list) != 1 {
		t.Errorf("List() = %v, %v", list, err)
	}
}
