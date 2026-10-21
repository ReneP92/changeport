package provisioning

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
)

var (
	secretGVK      = schema.GroupVersionKind{Version: "v1", Kind: "Secret"}
	kafkaGVK       = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1", Kind: "Kafka"}
	kafkaUserGVK   = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1", Kind: "KafkaUser"}
	certificateGVK = schema.GroupVersionKind{Group: "cert-manager.io", Version: "v1", Kind: "Certificate"}
	nodeClaimGVK   = schema.GroupVersionKind{Group: "karpenter.sh", Version: "v1", Kind: "NodeClaim"}
)

// Cluster is the part of the Kubernetes API the provisioner uses.
type Cluster interface {
	// Apply creates or updates obj with server-side apply.
	Apply(ctx context.Context, obj *unstructured.Unstructured) error
	// Get returns the object, or an error satisfying apierrors.IsNotFound.
	Get(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) (*unstructured.Unstructured, error)
	// Delete deletes the object; deleting a missing object is not an error.
	Delete(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) error
	// List returns the objects of a kind in namespace ("" for cluster-scoped
	// kinds) that match labelSelector, such as "a=b".
	List(ctx context.Context, gvk schema.GroupVersionKind, namespace, labelSelector string) ([]unstructured.Unstructured, error)
}

// Provisioner creates and removes customer Kafka clusters.
type Provisioner struct {
	Cluster Cluster
	// PodIdentities is needed on platforms whose brokers get their tiered
	// storage credentials from EKS Pod Identity.
	PodIdentities PodIdentities
	Logger        *slog.Logger
	PollInterval  time.Duration
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
	// Pod Identity gives pods credentials when they start, so the association
	// comes before the brokers.
	if identity := spec.Platform.TieredStorage.PodIdentity; identity != nil {
		if p.PodIdentities == nil {
			return fmt.Errorf("platform %q needs EKS Pod Identity, but the provisioner has no AWS access", spec.Platform.Name)
		}
		if err := p.PodIdentities.Associate(ctx, identity.ClusterName, spec.Namespace(), spec.BrokerServiceAccount(), identity.RoleName); err != nil {
			return err
		}
		p.Logger.Info("associated", "serviceaccount", spec.Namespace()+"/"+spec.BrokerServiceAccount(), "role", identity.RoleName, "tenant", spec.CustomerID)
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

// Deprovision deletes the cluster-scoped resources the template created for
// the customer: the namespace, which removes the Kafka cluster and everything
// else in it, and on platforms with dedicated nodes the NodePool and
// NodeClass, which removes the nodes.
func (p *Provisioner) Deprovision(ctx context.Context, spec Spec) error {
	objects, err := clusterScoped(spec)
	if err != nil {
		return err
	}
	for _, obj := range objects {
		if err := p.Cluster.Delete(ctx, obj.GroupVersionKind(), "", obj.GetName()); err != nil {
			return fmt.Errorf("delete %s %s: %w", obj.GetKind(), obj.GetName(), err)
		}
		p.Logger.Info("deleting", "kind", obj.GetKind(), "object", obj.GetName(), "tenant", spec.CustomerID)
	}
	if identity := spec.Platform.TieredStorage.PodIdentity; identity != nil {
		if p.PodIdentities == nil {
			return fmt.Errorf("platform %q needs EKS Pod Identity, but the provisioner has no AWS access", spec.Platform.Name)
		}
		if err := p.PodIdentities.Disassociate(ctx, identity.ClusterName, spec.Namespace(), spec.BrokerServiceAccount()); err != nil {
			return err
		}
		p.Logger.Info("disassociated", "serviceaccount", spec.Namespace()+"/"+spec.BrokerServiceAccount(), "tenant", spec.CustomerID)
	}
	return nil
}

// WaitDeleted blocks until the resources Deprovision deletes are gone, and on
// platforms with dedicated nodes until the nodes are, or ctx is done.
func (p *Provisioner) WaitDeleted(ctx context.Context, spec Spec) error {
	objects, err := clusterScoped(spec)
	if err != nil {
		return err
	}
	for _, obj := range objects {
		err := wait.PollUntilContextCancel(ctx, p.PollInterval, true, func(ctx context.Context) (bool, error) {
			_, err := p.Cluster.Get(ctx, obj.GroupVersionKind(), "", obj.GetName())
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		})
		if err != nil {
			return fmt.Errorf("%s %s not deleted: %w", obj.GetKind(), obj.GetName(), err)
		}
		p.Logger.Info("deleted", "kind", obj.GetKind(), "object", obj.GetName(), "tenant", spec.CustomerID)
	}
	if spec.Platform.DedicatedNodes == nil {
		return nil
	}
	// Deleting a NodePool deletes its NodeClaims in the background.
	selector := "karpenter.sh/nodepool=" + spec.DedicatedNodePool()
	err = wait.PollUntilContextCancel(ctx, p.PollInterval, true, func(ctx context.Context) (bool, error) {
		nodeClaims, err := p.Cluster.List(ctx, nodeClaimGVK, "", selector)
		return len(nodeClaims) == 0, err
	})
	if err != nil {
		return fmt.Errorf("nodes of NodePool %s not deleted: %w", spec.DedicatedNodePool(), err)
	}
	p.Logger.Info("nodes deleted", "nodepool", spec.DedicatedNodePool(), "tenant", spec.CustomerID)
	return nil
}

// clusterScoped returns the template's cluster-scoped objects for spec, in
// the order to delete them: the namespace first, so that its pods leave the
// dedicated nodes, then the NodePool before its NodeClass.
func clusterScoped(spec Spec) ([]*unstructured.Unstructured, error) {
	objects, err := Render(spec)
	if err != nil {
		return nil, err
	}
	var scoped []*unstructured.Unstructured
	for _, obj := range objects {
		if obj.GetNamespace() == "" {
			if _, ok := deleteOrder[obj.GetKind()]; !ok {
				return nil, fmt.Errorf("the template has a cluster-scoped %s with no delete order", obj.GetKind())
			}
			scoped = append(scoped, obj)
		}
	}
	slices.SortStableFunc(scoped, func(a, b *unstructured.Unstructured) int {
		return deleteOrder[a.GetKind()] - deleteOrder[b.GetKind()]
	})
	return scoped, nil
}

// deleteOrder ranks the template's cluster-scoped kinds for deletion.
var deleteOrder = map[string]int{"Namespace": 0, "NodePool": 1, "NodeClass": 2}

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
