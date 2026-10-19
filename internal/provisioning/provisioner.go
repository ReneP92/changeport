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
	namespaceGVK = schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}
	kafkaGVK     = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1", Kind: "Kafka"}
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

// Provision applies every resource of the customer's cluster. It is
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
	}
	return nil
}

// WaitReady blocks until Strimzi reports the customer's Kafka cluster ready
// for the latest spec, or ctx is done.
func (p *Provisioner) WaitReady(ctx context.Context, spec Spec) error {
	lastMessage := "Kafka resource not observed yet"
	err := wait.PollUntilContextCancel(ctx, p.PollInterval, true, func(ctx context.Context) (bool, error) {
		kafka, err := p.Cluster.Get(ctx, kafkaGVK, spec.Namespace(), spec.ClusterName())
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		ready, message := kafkaReady(kafka)
		if ready {
			for _, warning := range kafkaWarnings(kafka) {
				p.Logger.Warn("Kafka cluster warning", "tenant", spec.CustomerID, "warning", warning)
			}
			return true, nil
		}
		if message != lastMessage {
			lastMessage = message
			p.Logger.Info("waiting for Kafka cluster", "tenant", spec.CustomerID, "status", message)
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("kafka cluster %s/%s not ready (%s): %w", spec.Namespace(), spec.ClusterName(), lastMessage, err)
	}
	p.Logger.Info("Kafka cluster ready", "tenant", spec.CustomerID, "bootstrap", spec.BootstrapAddress())
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

// kafkaReady reports whether Strimzi has reconciled the current generation of
// a Kafka resource and marked it Ready, plus a short status for logging.
func kafkaReady(kafka *unstructured.Unstructured) (bool, string) {
	observed, found, err := unstructured.NestedInt64(kafka.Object, "status", "observedGeneration")
	if err != nil || !found || observed < kafka.GetGeneration() {
		return false, "operator has not reconciled the latest spec"
	}
	conditions, _, err := unstructured.NestedSlice(kafka.Object, "status", "conditions")
	if err != nil {
		return false, "unreadable status conditions"
	}
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

// kafkaWarnings returns the messages of Strimzi's Warning conditions, such as
// deprecated configuration or unsafe storage.
func kafkaWarnings(kafka *unstructured.Unstructured) []string {
	conditions, _, _ := unstructured.NestedSlice(kafka.Object, "status", "conditions")
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
