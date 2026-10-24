package console

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/provisioning"
)

// Customers reads, creates and deletes the Customer resources.
type Customers interface {
	// ByOrganization returns the customer of a Clerk organization, or nil.
	ByOrganization(ctx context.Context, organizationID string) (*v1alpha1.Customer, error)
	// List returns every customer.
	List(ctx context.Context) ([]v1alpha1.Customer, error)
	// Get returns a customer by ID, or nil.
	Get(ctx context.Context, id string) (*v1alpha1.Customer, error)
	// Create creates a customer, filling in what the API server sets, or
	// returns ErrCustomerExists.
	Create(ctx context.Context, customer *v1alpha1.Customer) error
	// Delete deletes a customer, whose cluster the provisioning controller
	// then deletes, or returns ErrNoSuchCustomer.
	Delete(ctx context.Context, id string) error
}

// ErrCustomerExists means a customer of that ID exists already.
var ErrCustomerExists = errors.New("the customer exists")

// ErrNoSuchCustomer means there is no customer of that ID.
var ErrNoSuchCustomer = errors.New("no such customer")

// KubeCustomers reads, creates and deletes Customers through the Kubernetes
// API.
type KubeCustomers struct {
	Client client.Client
}

var _ Customers = KubeCustomers{}

// ByOrganization implements Customers, through the organization label the
// provisioning controller keeps on every Customer.
func (k KubeCustomers) ByOrganization(ctx context.Context, organizationID string) (*v1alpha1.Customer, error) {
	if len(validation.IsValidLabelValue(organizationID)) > 0 {
		return nil, nil
	}
	var list v1alpha1.CustomerList
	if err := k.Client.List(ctx, &list, client.MatchingLabels{v1alpha1.ClerkOrganizationLabel: organizationID}); err != nil {
		return nil, fmt.Errorf("list the customers of organization %s: %w", organizationID, err)
	}
	switch len(list.Items) {
	case 0:
		return nil, nil
	case 1:
		return &list.Items[0], nil
	default:
		// One organization is one customer; anything else needs an operator.
		return nil, fmt.Errorf("organization %s belongs to %d customers", organizationID, len(list.Items))
	}
}

// List implements Customers.
func (k KubeCustomers) List(ctx context.Context) ([]v1alpha1.Customer, error) {
	var list v1alpha1.CustomerList
	if err := k.Client.List(ctx, &list); err != nil {
		return nil, fmt.Errorf("list customers: %w", err)
	}
	return list.Items, nil
}

// Get implements Customers. No customer has an invalid ID.
func (k KubeCustomers) Get(ctx context.Context, id string) (*v1alpha1.Customer, error) {
	if provisioning.ValidateCustomerID(id) != nil {
		return nil, nil
	}
	var customer v1alpha1.Customer
	err := k.Client.Get(ctx, types.NamespacedName{Name: id}, &customer)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get customer %s: %w", id, err)
	}
	return &customer, nil
}

// Create implements Customers.
func (k KubeCustomers) Create(ctx context.Context, customer *v1alpha1.Customer) error {
	err := k.Client.Create(ctx, customer)
	if apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("customer %s: %w", customer.Name, ErrCustomerExists)
	}
	if err != nil {
		return fmt.Errorf("create customer %s: %w", customer.Name, err)
	}
	return nil
}

// Delete implements Customers. It returns once the deletion has begun.
func (k KubeCustomers) Delete(ctx context.Context, id string) error {
	if provisioning.ValidateCustomerID(id) != nil {
		return fmt.Errorf("customer %s: %w", id, ErrNoSuchCustomer)
	}
	err := k.Client.Delete(ctx, &v1alpha1.Customer{ObjectMeta: metav1.ObjectMeta{Name: id}})
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("customer %s: %w", id, ErrNoSuchCustomer)
	}
	if err != nil {
		return fmt.Errorf("delete customer %s: %w", id, err)
	}
	return nil
}
