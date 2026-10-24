// Package controller runs Changeport's Kubernetes controllers: today the one
// that turns every Customer into its dedicated Kafka cluster.
package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/provisioning"
)

// Provisioner is the part of provisioning.Provisioner the controller uses.
type Provisioner interface {
	Reconcile(ctx context.Context, spec provisioning.Spec) (provisioning.Progress, error)
	Deprovision(ctx context.Context, spec provisioning.Spec) error
	Deleted(ctx context.Context, spec provisioning.Spec) (bool, string, error)
}

var _ Provisioner = (*provisioning.Provisioner)(nil)

// CustomerReconciler provisions every Customer's Kafka cluster from the one
// template, and deprovisions it before the Customer goes.
//
// It only ever requeues: it never blocks on Strimzi or cert-manager, so one
// slow cluster cannot hold up the others.
type CustomerReconciler struct {
	Client      client.Client
	Provisioner Provisioner
	// Platform is where this controller's clusters run, such as kind.
	Platform provisioning.Platform
	// PollInterval is how soon a cluster that is not ready yet, or not
	// deleted yet, is looked at again.
	PollInterval time.Duration
	// ResyncInterval is how often a ready cluster is converged to the
	// template again, which repairs changes made by hand.
	ResyncInterval time.Duration
}

// SetupWithManager registers the reconciler with mgr.
func (r *CustomerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Customer{}).
		Named("customer").
		WithOptions(controller.Options{MaxConcurrentReconciles: 4}).
		Complete(r)
}

// Reconcile implements reconcile.Reconciler.
func (r *CustomerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	result, err := r.reconcile(ctx, req)
	if apierrors.IsConflict(err) {
		// The Customer changed since it was read, and the change triggers
		// another reconciliation with the new version.
		return ctrl.Result{}, nil
	}
	return result, err
}

func (r *CustomerReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var customer v1alpha1.Customer
	if err := r.Client.Get(ctx, req.NamespacedName, &customer); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !customer.DeletionTimestamp.IsZero() {
		return r.deprovision(ctx, &customer)
	}
	spec, err := r.spec(&customer)
	if err != nil {
		// Only a change to the Customer can fix it, which triggers a new
		// reconciliation.
		return ctrl.Result{}, r.notReady(ctx, &customer, v1alpha1.ReasonInvalidSpec, err.Error())
	}
	if err := r.ensureMetadata(ctx, &customer); err != nil {
		return ctrl.Result{}, err
	}

	progress, err := r.Provisioner.Reconcile(ctx, spec)
	if err != nil {
		// Returning the error retries with backoff.
		if statusErr := r.notReady(ctx, &customer, v1alpha1.ReasonProvisioningFailed, err.Error()); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{}, err
	}
	status := customer.Status.DeepCopy()
	status.BootstrapAddress = spec.BootstrapAddress()
	parts := []struct {
		condition string
		readiness provisioning.Readiness
		// waiting is Ready's reason while the cluster waits for this part.
		waiting string
	}{
		{v1alpha1.ConditionCertificateIssued, progress.Certificate, v1alpha1.ReasonCertificatePending},
		{v1alpha1.ConditionKafkaReady, progress.Kafka, v1alpha1.ReasonKafkaPending},
		{v1alpha1.ConditionUsersReady, progress.Users, v1alpha1.ReasonUsersPending},
		{v1alpha1.ConditionAgentAvailable, progress.Agent, v1alpha1.ReasonAgentPending},
	}
	ready := metav1.Condition{Type: v1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonDone, Message: "Clients can connect to " + spec.BootstrapAddress()}
	for _, part := range parts {
		condition := metav1.Condition{Type: part.condition, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonDone, Message: part.readiness.String()}
		if !part.readiness.Ready {
			condition.Status, condition.Reason = metav1.ConditionFalse, v1alpha1.ReasonPending
			// Ready names the first part the cluster waits for.
			if ready.Status == metav1.ConditionTrue {
				ready.Status, ready.Reason = metav1.ConditionFalse, part.waiting
				ready.Message = part.readiness.String()
			}
		}
		setCondition(status, customer.Generation, condition)
	}
	setCondition(status, customer.Generation, ready)
	if err := r.updateStatus(ctx, &customer, status); err != nil {
		return ctrl.Result{}, err
	}
	if !progress.Ready() {
		return ctrl.Result{RequeueAfter: r.PollInterval}, nil
	}
	return ctrl.Result{RequeueAfter: r.ResyncInterval}, nil
}

// deprovision deletes a deleted Customer's cluster, unless it is protected,
// and lets the Customer go once everything is gone.
func (r *CustomerReconciler) deprovision(ctx context.Context, customer *v1alpha1.Customer) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(customer, v1alpha1.Finalizer) {
		return ctrl.Result{}, nil
	}
	if customer.Spec.DeletionProtection {
		// Turning protection off changes the Customer, which triggers a new
		// reconciliation.
		return ctrl.Result{}, r.notReady(ctx, customer, v1alpha1.ReasonDeletionProtected,
			"The Customer is deleted, but its Kafka cluster is kept until deletion protection is turned off")
	}
	spec, err := r.spec(customer)
	if err != nil {
		return ctrl.Result{}, r.notReady(ctx, customer, v1alpha1.ReasonInvalidSpec, err.Error())
	}
	if err := r.Provisioner.Deprovision(ctx, spec); err != nil {
		return ctrl.Result{}, err
	}
	deleted, left, err := r.Provisioner.Deleted(ctx, spec)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !deleted {
		if err := r.notReady(ctx, customer, v1alpha1.ReasonDeprovisioning, left); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: r.PollInterval}, nil
	}
	patch := client.MergeFromWithOptions(customer.DeepCopy(), client.MergeFromWithOptimisticLock{})
	controllerutil.RemoveFinalizer(customer, v1alpha1.Finalizer)
	if err := r.Client.Patch(ctx, customer, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove the finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

// spec is the provisioning spec of customer on the controller's platform.
func (r *CustomerReconciler) spec(customer *v1alpha1.Customer) (provisioning.Spec, error) {
	profile, err := provisioning.ProfileByName(customer.Spec.Profile)
	if err != nil {
		return provisioning.Spec{}, err
	}
	spec := provisioning.Spec{
		CustomerID: customer.Name, Profile: profile, Platform: r.Platform,
		DeletionProtection: customer.Spec.DeletionProtection,
	}
	return spec, spec.Validate()
}

// ensureMetadata adds the finalizer, before anything is created that it must
// delete, and keeps the Clerk organization label equal to the spec.
func (r *CustomerReconciler) ensureMetadata(ctx context.Context, customer *v1alpha1.Customer) error {
	original := customer.DeepCopy()
	controllerutil.AddFinalizer(customer, v1alpha1.Finalizer)
	labels := customer.GetLabels()
	if id := customer.Spec.ClerkOrganizationID; id != "" {
		if labels == nil {
			labels = map[string]string{}
		}
		labels[v1alpha1.ClerkOrganizationLabel] = id
	} else {
		delete(labels, v1alpha1.ClerkOrganizationLabel)
	}
	customer.SetLabels(labels)
	if equality.Semantic.DeepEqual(original.ObjectMeta, customer.ObjectMeta) {
		return nil
	}
	patch := client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{})
	if err := r.Client.Patch(ctx, customer, patch); err != nil {
		return fmt.Errorf("add the finalizer and labels: %w", err)
	}
	return nil
}

// notReady sets only the Ready condition, to False, for states in which the
// parts of the cluster are not looked at.
func (r *CustomerReconciler) notReady(ctx context.Context, customer *v1alpha1.Customer, reason, message string) error {
	updated := customer.Status.DeepCopy()
	setCondition(updated, customer.Generation, metav1.Condition{Type: v1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: message})
	return r.updateStatus(ctx, customer, updated)
}

// updateStatus writes status unless it is what the Customer already has, so
// that a cluster that does not change causes no writes.
func (r *CustomerReconciler) updateStatus(ctx context.Context, customer *v1alpha1.Customer, status *v1alpha1.CustomerStatus) error {
	status.ObservedGeneration = customer.Generation
	if equality.Semantic.DeepEqual(customer.Status, *status) {
		return nil
	}
	customer.Status = *status
	if err := r.Client.Status().Update(ctx, customer); err != nil {
		return fmt.Errorf("update the status: %w", err)
	}
	return nil
}

// setCondition sets a condition for generation; its transition time only
// changes with its status.
func setCondition(status *v1alpha1.CustomerStatus, generation int64, condition metav1.Condition) {
	condition.ObservedGeneration = generation
	meta.SetStatusCondition(&status.Conditions, condition)
}
