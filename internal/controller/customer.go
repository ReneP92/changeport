// Package controller runs Changeport's Kubernetes controllers: today the one
// that turns every Customer into its dedicated Kafka cluster.
package controller

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/cdc"
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

// SetupWithManager registers the reconciler with mgr. A change to a
// PostgresSource reconciles the Customer of its namespace, whose template
// renders what the sources run on.
func (r *CustomerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Customer{}).
		Watches(&v1alpha1.PostgresSource{}, handler.EnqueueRequestsFromMapFunc(customerOfSource), builder.WithPredicates(sourceChanged)).
		Named("customer").
		WithOptions(controller.Options{MaxConcurrentReconciles: 4}).
		Complete(r)
}

// sourceChanged lets through the changes to a source that change what the
// template renders for it: its spec, its deletion, and its credentials
// reaching the Connect worker. The source controller's other status updates,
// such as the slot's every minute, do not reconcile the Customer.
var sourceChanged = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		before, ok1 := e.ObjectOld.(*v1alpha1.PostgresSource)
		after, ok2 := e.ObjectNew.(*v1alpha1.PostgresSource)
		if !ok1 || !ok2 {
			return true
		}
		return before.Generation != after.Generation ||
			!before.DeletionTimestamp.Equal(after.DeletionTimestamp) ||
			before.Status.PasswordSecret != after.Status.PasswordSecret
	},
}

// customerOfSource is the Customer whose namespace a source is in.
func customerOfSource(_ context.Context, source client.Object) []ctrl.Request {
	id, ok := strings.CutPrefix(source.GetNamespace(), provisioning.NamespacePrefix)
	if !ok {
		return nil
	}
	return []ctrl.Request{{NamespacedName: client.ObjectKey{Name: id}}}
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
	if spec.Sources, err = r.sources(ctx, spec); err != nil {
		return ctrl.Result{}, err
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
	cdcReady := metav1.Condition{Type: v1alpha1.ConditionCDCReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonDone, Message: progress.CDC.String()}
	if !progress.CDC.Ready {
		cdcReady.Status, cdcReady.Reason = metav1.ConditionFalse, v1alpha1.ReasonPending
	}
	setCondition(status, customer.Generation, cdcReady)
	if err := r.updateStatus(ctx, &customer, status); err != nil {
		return ctrl.Result{}, err
	}
	if !progress.Ready() || !progress.CDC.Ready {
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

// sources lists the customer's Postgres CDC sources, by name, including those
// being deleted: they need their runtime until their slot is dropped.
func (r *CustomerReconciler) sources(ctx context.Context, spec provisioning.Spec) ([]provisioning.Source, error) {
	var list v1alpha1.PostgresSourceList
	if err := r.Client.List(ctx, &list, client.InNamespace(spec.Namespace())); err != nil {
		return nil, fmt.Errorf("list the Postgres sources of customer %s: %w", spec.CustomerID, err)
	}
	sources := make([]provisioning.Source, len(list.Items))
	for i, source := range list.Items {
		sources[i] = provisioning.Source{Name: source.Name, Connector: connector(&source)}
	}
	slices.SortFunc(sources, func(a, b provisioning.Source) int { return strings.Compare(a.Name, b.Name) })
	return sources, nil
}

// connector is a source's Debezium connector: none until the source
// controller has handed its password to the Connect worker, and none once
// the source is deleted, so that the connector lets go of the slot before
// the source controller drops it.
func connector(source *v1alpha1.PostgresSource) *provisioning.Connector {
	if !source.DeletionTimestamp.IsZero() || source.Status.PasswordSecret == "" {
		return nil
	}
	names, err := cdc.NamesFor(source.Name, source.Spec.ID)
	if err != nil {
		// The CRD's validation keeps such sources out.
		return nil
	}
	tables := make([]cdc.Table, len(source.Spec.Tables))
	for i, table := range source.Spec.Tables {
		tables[i] = cdc.Table{Schema: table.Schema, Name: table.Name}
	}
	connection := source.Spec.Connection
	return &provisioning.Connector{
		Name:               names.Connector,
		Host:               connection.Host,
		Port:               int(connection.Port),
		Database:           connection.Database,
		Role:               names.Role,
		Publication:        names.Publication,
		Slot:               names.Slot,
		TLSMode:            cmp.Or(connection.TLS.Mode, v1alpha1.TLSVerifyFull),
		CA:                 connection.TLS.CA != "",
		TableIncludeList:   cdc.TableIncludeList(tables),
		RetentionMs:        source.Spec.RetentionMs,
		Running:            source.Spec.State != v1alpha1.StateStopped,
		CredentialsVersion: CredentialsVersion(source),
	}
}

// CredentialsVersion names the version of a source's credentials in the
// Connect worker: its password Secret, and a digest of its CA.
func CredentialsVersion(source *v1alpha1.PostgresSource) string {
	digest := sha256.Sum256([]byte(source.Spec.Connection.TLS.CA))
	return source.Status.PasswordSecret + "/" + hex.EncodeToString(digest[:4])
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
