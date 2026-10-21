package provisioning

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
)

var (
	namespaceGVK   = schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}
	secretGVK      = schema.GroupVersionKind{Version: "v1", Kind: "Secret"}
	kafkaGVK       = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1", Kind: "Kafka"}
	kafkaUserGVK   = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1", Kind: "KafkaUser"}
	certificateGVK = schema.GroupVersionKind{Group: "cert-manager.io", Version: "v1", Kind: "Certificate"}
)

// Cluster is the part of the Kubernetes API the provisioner uses.
type Cluster interface {
	// Apply creates or updates obj with server-side apply.
	Apply(ctx context.Context, obj *unstructured.Unstructured) error
	// Get returns the object, or an error satisfying apierrors.IsNotFound.
	Get(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) (*unstructured.Unstructured, error)
	// Delete deletes the object; deleting a missing object is not an error.
	Delete(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) error
}

// Provisioner creates and removes customer Kafka clusters.
type Provisioner struct {
	Cluster      Cluster
	Logger       *slog.Logger
	PollInterval time.Duration
}

// New returns a Provisioner with default polling.
func New(cluster Cluster, logger *slog.Logger) *Provisioner {
	return &Provisioner{Cluster: cluster, Logger: logger, PollInterval: 5 * time.Second}
}

// Provision applies every resource of the customer's cluster in template
// order. After a Certificate it waits until cert-manager has issued it, so
// that the Kafka cluster never starts without its listener certificate. It is
// idempotent: running it again converges the resources to the template.
func (p *Provisioner) Provision(ctx context.Context, spec Spec) error {
	objects, err := Render(spec)
	if err != nil {
		return err
	}
	for _, obj := range objects {
		if err := p.Cluster.Apply(ctx, obj); err != nil {
			return fmt.Errorf("apply %s %s: %w", obj.GetKind(), objectKey(obj), err)
		}
		p.Logger.Info("applied", "kind", obj.GetKind(), "object", objectKey(obj), "tenant", spec.CustomerID)
		if obj.GroupVersionKind() == certificateGVK {
			if err := p.waitFor(ctx, obj, certificateReady, spec.CustomerID); err != nil {
				return err
			}
		}
	}
	return nil
}

// WaitReady blocks until Strimzi reports the customer's Kafka cluster and
// users ready for the latest spec, or ctx is done.
func (p *Provisioner) WaitReady(ctx context.Context, spec Spec) error {
	objects, err := Render(spec)
	if err != nil {
		return err
	}
	for _, kind := range []schema.GroupVersionKind{kafkaGVK, kafkaUserGVK} {
		for _, obj := range objects {
			if obj.GroupVersionKind() == kind {
				if err := p.waitFor(ctx, obj, strimziReady, spec.CustomerID); err != nil {
					return err
				}
			}
		}
	}
	p.Logger.Info("Kafka cluster ready", "tenant", spec.CustomerID, "bootstrap", spec.BootstrapAddress())
	return nil
}

// waitFor polls obj until ready reports it ready for its latest spec, logging
// each new status, or until ctx is done.
func (p *Provisioner) waitFor(ctx context.Context, obj *unstructured.Unstructured, ready func(*unstructured.Unstructured) (bool, string), tenant string) error {
	kind, key := obj.GetKind(), objectKey(obj)
	lastMessage := "not observed yet"
	err := wait.PollUntilContextCancel(ctx, p.PollInterval, true, func(ctx context.Context) (bool, error) {
		current, err := p.Cluster.Get(ctx, obj.GroupVersionKind(), obj.GetNamespace(), obj.GetName())
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		done, message := ready(current)
		if done {
			for _, warning := range strimziWarnings(current) {
				p.Logger.Warn("warning", "kind", kind, "object", key, "tenant", tenant, "warning", warning)
			}
			return true, nil
		}
		if message != lastMessage {
			lastMessage = message
			p.Logger.Info("waiting", "kind", kind, "object", key, "tenant", tenant, "status", message)
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("%s %s not ready (%s): %w", kind, key, lastMessage, err)
	}
	return nil
}

// Deprovision deletes the customer's namespace, which removes the Kafka
// cluster and everything else the template created.
func (p *Provisioner) Deprovision(ctx context.Context, customerID string) error {
	if err := ValidateCustomerID(customerID); err != nil {
		return err
	}
	namespace := Spec{CustomerID: customerID}.Namespace()
	if err := p.Cluster.Delete(ctx, namespaceGVK, "", namespace); err != nil {
		return fmt.Errorf("delete namespace %s: %w", namespace, err)
	}
	p.Logger.Info("deleting namespace", "namespace", namespace, "tenant", customerID)
	return nil
}

// WaitDeleted blocks until the customer's namespace is gone, or ctx is done.
func (p *Provisioner) WaitDeleted(ctx context.Context, customerID string) error {
	namespace := Spec{CustomerID: customerID}.Namespace()
	err := wait.PollUntilContextCancel(ctx, p.PollInterval, true, func(ctx context.Context) (bool, error) {
		_, err := p.Cluster.Get(ctx, namespaceGVK, "", namespace)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
	if err != nil {
		return fmt.Errorf("namespace %s not deleted: %w", namespace, err)
	}
	p.Logger.Info("namespace deleted", "namespace", namespace, "tenant", customerID)
	return nil
}

// strimziReady reports whether Strimzi has reconciled the current generation
// of one of its resources, such as a Kafka or KafkaUser, and marked it Ready,
// plus a short status for logging.
// Strimzi only updates observedGeneration once a reconciliation finishes, so
// while it works on the latest spec the status names its current conditions,
// such as NotReady=True (Creating).
func strimziReady(obj *unstructured.Unstructured) (bool, string) {
	conditions, _, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil {
		return false, "unreadable status conditions"
	}
	ready, message := readyCondition(conditions)
	observed, found, err := unstructured.NestedInt64(obj.Object, "status", "observedGeneration")
	if err != nil || !found || observed < obj.GetGeneration() {
		if len(conditions) == 0 {
			return false, "operator has not reconciled the latest spec"
		}
		return false, "operator has not reconciled the latest spec; last status " + message
	}
	return ready, message
}

// readyCondition reports whether conditions hold Ready=True, and otherwise
// summarizes the last condition for logging.
func readyCondition(conditions []any) (bool, string) {
	message := "no Ready condition"
	for _, c := range conditions {
		condition, ok := c.(map[string]any)
		if !ok {
			continue
		}
		conditionType, _ := condition["type"].(string)
		status, _ := condition["status"].(string)
		if conditionType == "Ready" && status == "True" {
			return true, "Ready"
		}
		if reason, _ := condition["reason"].(string); reason != "" {
			message = fmt.Sprintf("%s=%s (%s)", conditionType, status, reason)
		} else {
			message = fmt.Sprintf("%s=%s", conditionType, status)
		}
	}
	return false, message
}

// certificateReady reports whether cert-manager has issued a Certificate for
// its current generation, plus a short status for logging. cert-manager keeps
// the observed generation on the Ready condition itself.
func certificateReady(certificate *unstructured.Unstructured) (bool, string) {
	conditions, _, err := unstructured.NestedSlice(certificate.Object, "status", "conditions")
	if err != nil {
		return false, "unreadable status conditions"
	}
	for _, c := range conditions {
		condition, ok := c.(map[string]any)
		if !ok || condition["type"] != "Ready" {
			continue
		}
		observed, _ := condition["observedGeneration"].(int64)
		if condition["status"] == "True" && observed >= certificate.GetGeneration() {
			return true, "Ready"
		}
		reason, _ := condition["reason"].(string)
		return false, fmt.Sprintf("Ready=%v (%s)", condition["status"], reason)
	}
	return false, "no Ready condition"
}

// strimziWarnings returns the messages of Strimzi's Warning conditions, such
// as deprecated configuration or unsafe storage.
func strimziWarnings(obj *unstructured.Unstructured) []string {
	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	var warnings []string
	for _, c := range conditions {
		condition, ok := c.(map[string]any)
		if !ok || condition["type"] != "Warning" || condition["status"] != "True" {
			continue
		}
		message, _ := condition["message"].(string)
		reason, _ := condition["reason"].(string)
		warnings = append(warnings, fmt.Sprintf("%s: %s", reason, message))
	}
	return warnings
}

func objectKey(obj *unstructured.Unstructured) string {
	if obj.GetNamespace() == "" {
		return obj.GetName()
	}
	return obj.GetNamespace() + "/" + obj.GetName()
}
