package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/provisioning"
)

// pollInterval is how often -wait looks at a Customer.
var pollInterval = 2 * time.Second

// newCustomerClient connects to the cluster of kubeContext to manage
// Customers.
func newCustomerClient(kubeContext string) (client.Client, error) {
	config, err := restConfig(kubeContext)
	if err != nil {
		return nil, err
	}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	c, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("connect to the cluster: %w", err)
	}
	return c, nil
}

// applyCustomer creates the Customer id, or updates its display name and
// profile; the controller provisions its cluster. A new Customer gets
// deletion protection when protection says so, or else when it is a
// production one; an existing one keeps its protection unless protection
// says otherwise.
func applyCustomer(ctx context.Context, c client.Client, id, displayName, profile string, protection *bool) error {
	customer := &v1alpha1.Customer{}
	customer.Name = id
	_, err := controllerutil.CreateOrPatch(ctx, c, customer, func() error {
		// Only a Customer not created yet has no resource version.
		if customer.ResourceVersion == "" {
			customer.Spec.DeletionProtection = profile == provisioning.ProductionProfile.Name
		}
		if protection != nil {
			customer.Spec.DeletionProtection = *protection
		}
		customer.Spec.DisplayName = displayName
		customer.Spec.Profile = profile
		return nil
	})
	if err != nil {
		return fmt.Errorf("apply Customer %s: %w", id, err)
	}
	return nil
}

// waitReady waits until the controller reports the Customer's cluster ready
// for its latest spec, logging each new status.
func waitReady(ctx context.Context, c client.Client, id string, logger *slog.Logger) error {
	last := "not reconciled yet"
	err := wait.PollUntilContextCancel(ctx, pollInterval, true, func(ctx context.Context) (bool, error) {
		var customer v1alpha1.Customer
		if err := c.Get(ctx, types.NamespacedName{Name: id}, &customer); err != nil {
			return false, fmt.Errorf("get Customer %s: %w", id, err)
		}
		status := "not reconciled yet"
		ready := meta.FindStatusCondition(customer.Status.Conditions, v1alpha1.ConditionReady)
		if ready != nil && ready.ObservedGeneration >= customer.Generation {
			if ready.Status == "True" {
				logger.Info("Kafka cluster ready", "tenant", id, "bootstrap", customer.Status.BootstrapAddress)
				return true, nil
			}
			status = ready.Reason + ": " + ready.Message
		}
		if status != last {
			logger.Info("waiting", "tenant", id, "status", status)
			last = status
		}
		return false, nil
	})
	if err != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) {
		return fmt.Errorf("customer %s not ready (%s): %w", id, last, err)
	}
	return err
}

// errProtected is returned for deleting a Customer with deletion protection.
var errProtected = errors.New("has deletion protection; turn it off first with provisioner unprotect")

// errNotConfirmed is returned for turning protection off without the
// customer ID typed again.
var errNotConfirmed = errors.New("turning deletion protection off takes the customer ID typed again with -confirm")

// setProtection turns the deletion protection of the Customer id on or off.
func setProtection(ctx context.Context, c client.Client, id string, on bool) error {
	var customer v1alpha1.Customer
	if err := c.Get(ctx, types.NamespacedName{Name: id}, &customer); err != nil {
		return fmt.Errorf("get Customer %s: %w", id, err)
	}
	patch := client.MergeFrom(customer.DeepCopy())
	customer.Spec.DeletionProtection = on
	if err := c.Patch(ctx, &customer, patch); err != nil {
		return fmt.Errorf("set the deletion protection of Customer %s: %w", id, err)
	}
	return nil
}

// deleteCustomer deletes the Customer id, which makes the controller delete
// its cluster. Deleting a missing Customer is not an error.
func deleteCustomer(ctx context.Context, c client.Client, id string) error {
	var customer v1alpha1.Customer
	err := c.Get(ctx, types.NamespacedName{Name: id}, &customer)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get Customer %s: %w", id, err)
	}
	if customer.Spec.DeletionProtection {
		return fmt.Errorf("customer %s %w", id, errProtected)
	}
	if err := client.IgnoreNotFound(c.Delete(ctx, &customer)); err != nil {
		return fmt.Errorf("delete Customer %s: %w", id, err)
	}
	return nil
}

// waitGone waits until the Customer id is gone, which the controller allows
// once its cluster is, logging each new status.
func waitGone(ctx context.Context, c client.Client, id string, logger *slog.Logger) error {
	last := ""
	err := wait.PollUntilContextCancel(ctx, pollInterval, true, func(ctx context.Context) (bool, error) {
		var customer v1alpha1.Customer
		err := c.Get(ctx, types.NamespacedName{Name: id}, &customer)
		if apierrors.IsNotFound(err) {
			logger.Info("deleted", "tenant", id)
			return true, nil
		}
		if err != nil {
			return false, fmt.Errorf("get Customer %s: %w", id, err)
		}
		// Until the controller reacts, Ready still describes the cluster
		// before the deletion.
		status := "waiting for the controller"
		ready := meta.FindStatusCondition(customer.Status.Conditions, v1alpha1.ConditionReady)
		if ready != nil && (ready.Reason == v1alpha1.ReasonDeprovisioning || ready.Reason == v1alpha1.ReasonDeletionProtected) {
			status = ready.Reason + ": " + ready.Message
		}
		if status != last {
			logger.Info("waiting", "tenant", id, "status", status)
			last = status
		}
		return false, nil
	})
	if err != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) {
		return fmt.Errorf("customer %s not deleted (%s): %w", id, last, err)
	}
	return err
}
