package console

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/cdc"
	"github.com/ReneP92/changeport/internal/provisioning"
)

// Sources reads and changes the customers' Postgres CDC sources.
type Sources interface {
	// List returns a customer's sources, by name.
	List(ctx context.Context, customerID string) ([]v1alpha1.PostgresSource, error)
	// Get returns a source, or nil.
	Get(ctx context.Context, customerID, name string) (*v1alpha1.PostgresSource, error)
	// Create creates a source and then its write-once password Secret, which
	// the source owns; or returns ErrSourceExists.
	Create(ctx context.Context, customerID string, source *v1alpha1.PostgresSource, password string) error
	// Update writes a source's spec, refusing a source changed meanwhile.
	Update(ctx context.Context, source *v1alpha1.PostgresSource) error
	// SetPassword writes a new password Secret and names it in the source.
	SetPassword(ctx context.Context, source *v1alpha1.PostgresSource, password string) error
	// Delete deletes a source; its finalizer drops its slot first.
	Delete(ctx context.Context, source *v1alpha1.PostgresSource) error
	// ConfirmSlotDropped records that the customer dropped a deleted
	// source's slot themselves.
	ConfirmSlotDropped(ctx context.Context, source *v1alpha1.PostgresSource) error
}

// ErrSourceExists means a source of that name exists already.
var ErrSourceExists = errors.New("the source exists")

// ErrSourceChanged means the source changed since it was read.
var ErrSourceChanged = errors.New("the source changed meanwhile")

// passwordSecretPrefix starts the names of the password Secrets the console
// writes; the admission policy in deploy/base/console allows it no others.
const passwordSecretPrefix = "postgres-password-"

// KubeSources keeps sources as PostgresSource resources, through the
// Kubernetes API as the console's service account.
type KubeSources struct {
	Client client.Client
}

var _ Sources = KubeSources{}

func namespaceOf(customerID string) string {
	return provisioning.Spec{CustomerID: customerID}.Namespace()
}

// List implements Sources.
func (k KubeSources) List(ctx context.Context, customerID string) ([]v1alpha1.PostgresSource, error) {
	var list v1alpha1.PostgresSourceList
	if err := k.Client.List(ctx, &list, client.InNamespace(namespaceOf(customerID))); err != nil {
		return nil, fmt.Errorf("list the sources of customer %s: %w", customerID, err)
	}
	slices.SortFunc(list.Items, func(a, b v1alpha1.PostgresSource) int { return strings.Compare(a.Name, b.Name) })
	return list.Items, nil
}

// Get implements Sources.
func (k KubeSources) Get(ctx context.Context, customerID, name string) (*v1alpha1.PostgresSource, error) {
	if cdc.ValidateName(name) != nil {
		return nil, nil
	}
	var source v1alpha1.PostgresSource
	err := k.Client.Get(ctx, client.ObjectKey{Namespace: namespaceOf(customerID), Name: name}, &source)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get source %s of customer %s: %w", name, customerID, err)
	}
	return &source, nil
}

// Create implements Sources. The source comes first, and waits for its
// password; the Secret then names it as its owner, so that Kubernetes
// deletes the Secret with the source, should anything fail in between.
func (k KubeSources) Create(ctx context.Context, customerID string, source *v1alpha1.PostgresSource, password string) error {
	source.Namespace = namespaceOf(customerID)
	source.Spec.Connection.PasswordSecret = newPasswordSecretName(source.Name)
	err := k.Client.Create(ctx, source)
	if apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("source %s: %w", source.Name, ErrSourceExists)
	}
	if err != nil {
		return fmt.Errorf("create source %s: %w", source.Name, err)
	}
	return k.writePassword(ctx, source, source.Spec.Connection.PasswordSecret, password)
}

// writePassword creates a write-once password Secret that the source owns.
func (k KubeSources) writePassword(ctx context.Context, source *v1alpha1.PostgresSource, name, password string) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: source.Namespace, Name: name,
			Labels: map[string]string{"changeport.io/source": source.Name},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1alpha1.GroupVersion.String(), Kind: "PostgresSource", Name: source.Name, UID: source.UID,
			}},
		},
		Type:      v1alpha1.PostgresPasswordSecretType,
		Immutable: ptr.To(true),
		Data:      map[string][]byte{v1alpha1.PostgresPasswordKey: []byte(password)},
	}
	if err := k.Client.Create(ctx, secret); err != nil {
		return fmt.Errorf("write the password of source %s: %w", source.Name, err)
	}
	return nil
}

// newPasswordSecretName names a new password Secret of a source, uniquely,
// as a Secret is written once.
func newPasswordSecretName(source string) string {
	suffix := make([]byte, 3)
	_, _ = rand.Read(suffix)
	return fmt.Sprintf("%s%s-%x", passwordSecretPrefix, source, suffix)
}

// Update implements Sources.
func (k KubeSources) Update(ctx context.Context, source *v1alpha1.PostgresSource) error {
	err := k.Client.Update(ctx, source)
	if apierrors.IsConflict(err) {
		return fmt.Errorf("source %s: %w", source.Name, ErrSourceChanged)
	}
	if err != nil {
		return fmt.Errorf("update source %s: %w", source.Name, err)
	}
	return nil
}

// SetPassword implements Sources.
func (k KubeSources) SetPassword(ctx context.Context, source *v1alpha1.PostgresSource, password string) error {
	name := newPasswordSecretName(source.Name)
	if err := k.writePassword(ctx, source, name, password); err != nil {
		return err
	}
	source.Spec.Connection.PasswordSecret = name
	return k.Update(ctx, source)
}

// Delete implements Sources.
func (k KubeSources) Delete(ctx context.Context, source *v1alpha1.PostgresSource) error {
	err := k.Client.Delete(ctx, source, client.Preconditions{UID: &source.UID})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete source %s: %w", source.Name, err)
	}
	return nil
}

// ConfirmSlotDropped implements Sources.
func (k KubeSources) ConfirmSlotDropped(ctx context.Context, source *v1alpha1.PostgresSource) error {
	patch := client.MergeFromWithOptions(source.DeepCopy(), client.MergeFromWithOptimisticLock{})
	if source.Annotations == nil {
		source.Annotations = map[string]string{}
	}
	source.Annotations[v1alpha1.SlotDroppedAnnotation] = "true"
	if err := k.Client.Patch(ctx, source, patch); err != nil {
		return fmt.Errorf("confirm the slot of source %s dropped: %w", source.Name, err)
	}
	return nil
}
