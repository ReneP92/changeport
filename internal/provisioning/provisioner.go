package provisioning

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	secretGVK      = schema.GroupVersionKind{Version: "v1", Kind: "Secret"}
	kafkaGVK       = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1", Kind: "Kafka"}
	kafkaUserGVK   = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1", Kind: "KafkaUser"}
	certificateGVK = schema.GroupVersionKind{Group: "cert-manager.io", Version: "v1", Kind: "Certificate"}
	nodeClaimGVK   = schema.GroupVersionKind{Group: "karpenter.sh", Version: "v1", Kind: "NodeClaim"}
	deploymentGVK  = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	pvcGVK         = schema.GroupVersionKind{Version: "v1", Kind: "PersistentVolumeClaim"}
	pvGVK          = schema.GroupVersionKind{Version: "v1", Kind: "PersistentVolume"}
)

// The marks Deprovision leaves on a customer's volumes. Where the storage
// class retains volumes, as in production, they outlive the namespace, and
// the purge in docs/runbook.md finds them by these after 30 days.
const (
	// DeprovisionedAtAnnotation holds when the customer was deprovisioned,
	// in RFC 3339.
	DeprovisionedAtAnnotation = "changeport.io/deprovisioned-at"
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

// Provisioner creates and removes customer Kafka clusters. It never waits:
// the Customer controller (internal/controller) calls it again until a
// cluster is ready or gone.
type Provisioner struct {
	Cluster Cluster
	// PodIdentities is needed on platforms whose brokers get their tiered
	// storage credentials from EKS Pod Identity.
	PodIdentities PodIdentities
	Logger        *slog.Logger
	// Now is the time Deprovision marks volumes with.
	Now func() time.Time
}

// New returns a Provisioner.
func New(cluster Cluster, logger *slog.Logger) *Provisioner {
	return &Provisioner{Cluster: cluster, Logger: logger, Now: time.Now}
}

// Readiness is how far one part of a customer's cluster is.
type Readiness struct {
	Ready bool
	// Object names the first object of the part that is not ready yet, such
	// as "Kafka customer-acme/acme"; empty when Ready.
	Object string
	// Status describes the object's state for people, such as
	// "NotReady=True (Creating)".
	Status string
}

func (r Readiness) String() string {
	if r.Object == "" {
		return r.Status
	}
	return r.Object + ": " + r.Status
}

// Progress is how far a customer's cluster is, part by part, in the order
// the parts get ready.
type Progress struct {
	// Certificate is the public listener's certificate. Nothing after it in
	// the template is applied until cert-manager has issued it.
	Certificate Readiness
	Kafka       Readiness
	// Users are the Kafka users and their credentials.
	Users Readiness
	// Agent is the Kafka agent next to the cluster, ready when the platform
	// runs none.
	Agent Readiness
}

// Ready reports whether every part of the cluster is ready.
func (p Progress) Ready() bool {
	return p.Certificate.Ready && p.Kafka.Ready && p.Users.Ready && p.Agent.Ready
}

// Reconcile applies the customer's resources in template order and reports
// how far the cluster is, without waiting for anything: after a Certificate
// that cert-manager has not issued yet it stops, so that the Kafka cluster
// never starts without its listener certificate. Calling it again carries on
// from there. It is idempotent: it converges the resources to the template.
func (p *Provisioner) Reconcile(ctx context.Context, spec Spec) (Progress, error) {
	objects, err := Render(spec)
	if err != nil {
		return Progress{}, err
	}
	// Pod Identity gives pods credentials when they start, so the association
	// comes before the brokers.
	if identity := spec.Platform.TieredStorage.PodIdentity; identity != nil {
		if p.PodIdentities == nil {
			return Progress{}, fmt.Errorf("platform %q needs EKS Pod Identity, but the provisioner has no AWS access", spec.Platform.Name)
		}
		if err := p.PodIdentities.Associate(ctx, identity.ClusterName, spec.Namespace(), spec.BrokerServiceAccount(), identity.RoleName); err != nil {
			return Progress{}, err
		}
	}
	for _, obj := range objects {
		if err := p.Cluster.Apply(ctx, obj); err != nil {
			return Progress{}, fmt.Errorf("apply %s %s: %w", obj.GetKind(), objectKey(obj), err)
		}
		p.Logger.Debug("applied", "kind", obj.GetKind(), "object", objectKey(obj), "tenant", spec.CustomerID)
		if obj.GroupVersionKind() != certificateGVK {
			continue
		}
		certificate, err := p.observe(ctx, []*unstructured.Unstructured{obj}, certificateGVK, certificateReady)
		if err != nil {
			return Progress{}, err
		}
		if !certificate.Ready {
			waiting := Readiness{Status: "waiting for the listener certificate"}
			return Progress{Certificate: certificate, Kafka: waiting, Users: waiting, Agent: waiting}, nil
		}
	}
	progress := Progress{Certificate: Readiness{Ready: true, Status: "Issued"}}
	parts := []struct {
		readiness *Readiness
		kind      schema.GroupVersionKind
		ready     func(*unstructured.Unstructured) (bool, string)
	}{
		{&progress.Kafka, kafkaGVK, strimziReady},
		{&progress.Users, kafkaUserGVK, strimziReady},
		{&progress.Agent, deploymentGVK, deploymentReady},
	}
	for _, part := range parts {
		if *part.readiness, err = p.observe(ctx, objects, part.kind, part.ready); err != nil {
			return Progress{}, err
		}
	}
	return progress, nil
}

// observe reports the readiness of the objects of one kind: that of the
// first one not ready yet, or Ready when all are, or when there are none.
// Ready objects add the warnings Strimzi reports about them, such as
// deprecated configuration.
func (p *Provisioner) observe(ctx context.Context, objects []*unstructured.Unstructured, kind schema.GroupVersionKind, ready func(*unstructured.Unstructured) (bool, string)) (Readiness, error) {
	found := false
	var warnings []string
	for _, obj := range objects {
		if obj.GroupVersionKind() != kind {
			continue
		}
		found = true
		name := obj.GetKind() + " " + objectKey(obj)
		current, err := p.Cluster.Get(ctx, kind, obj.GetNamespace(), obj.GetName())
		if apierrors.IsNotFound(err) {
			return Readiness{Object: name, Status: "not observed yet"}, nil
		}
		if err != nil {
			return Readiness{}, fmt.Errorf("get %s: %w", name, err)
		}
		if done, status := ready(current); !done {
			return Readiness{Object: name, Status: status}, nil
		}
		warnings = append(warnings, strimziWarnings(current)...)
	}
	if !found {
		return Readiness{Ready: true, Status: "None on this platform"}, nil
	}
	if len(warnings) > 0 {
		return Readiness{Ready: true, Status: "Ready, with warnings: " + strings.Join(warnings, "; ")}, nil
	}
	return Readiness{Ready: true, Status: "Ready"}, nil
}

// Deprovision deletes the cluster-scoped resources the template created for
// the customer: the namespace, which removes the Kafka cluster and everything
// else in it, and on platforms with dedicated nodes the NodePool and
// NodeClass, which removes the nodes. It is idempotent, and does not wait;
// Deleted reports when everything is gone. It refuses a spec with deletion
// protection, and before deleting the namespace it takes the namespace's
// protection label off and marks the customer's volumes with when it was
// deprovisioned.
func (p *Provisioner) Deprovision(ctx context.Context, spec Spec) error {
	if spec.DeletionProtection {
		return fmt.Errorf("customer %s has deletion protection", spec.CustomerID)
	}
	objects, err := clusterScoped(spec)
	if err != nil {
		return err
	}
	for _, obj := range objects {
		current, err := p.Cluster.Get(ctx, obj.GroupVersionKind(), "", obj.GetName())
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("get %s %s: %w", obj.GetKind(), obj.GetName(), err)
		}
		// Calling Deprovision again while the deletion runs changes nothing.
		if current.GetDeletionTimestamp() != nil {
			continue
		}
		if obj.GetKind() == "Namespace" {
			if err := p.releaseNamespace(ctx, spec, obj, current); err != nil {
				return err
			}
		}
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

// releaseNamespace readies the customer's namespace for deletion: it takes
// the protection label off, which the admission policy would refuse the
// deletion for, by applying the namespace as rendered for the unprotected
// spec, and it marks the volumes of the namespace's claims.
func (p *Provisioner) releaseNamespace(ctx context.Context, spec Spec, rendered, current *unstructured.Unstructured) error {
	if _, ok := current.GetLabels()[DeletionProtectionLabel]; ok {
		if err := p.Cluster.Apply(ctx, rendered); err != nil {
			return fmt.Errorf("take the deletion protection label off namespace %s: %w", spec.Namespace(), err)
		}
		p.Logger.Info("took off the deletion protection label", "namespace", spec.Namespace(), "tenant", spec.CustomerID)
	}
	claims, err := p.Cluster.List(ctx, pvcGVK, spec.Namespace(), "")
	if err != nil {
		return fmt.Errorf("list the volume claims of namespace %s: %w", spec.Namespace(), err)
	}
	now := p.Now().UTC().Format(time.RFC3339)
	for _, claim := range claims {
		volume, _, _ := unstructured.NestedString(claim.Object, "spec", "volumeName")
		if volume == "" {
			continue
		}
		mark := &unstructured.Unstructured{}
		mark.SetGroupVersionKind(pvGVK)
		mark.SetName(volume)
		mark.SetLabels(map[string]string{TenantLabel: spec.CustomerID})
		mark.SetAnnotations(map[string]string{DeprovisionedAtAnnotation: now})
		if err := p.Cluster.Apply(ctx, mark); err != nil {
			return fmt.Errorf("mark volume %s of customer %s: %w", volume, spec.CustomerID, err)
		}
	}
	return nil
}

// Deleted reports whether the resources Deprovision deletes are gone, and on
// platforms with dedicated nodes whether the nodes are, and otherwise what
// is left, without waiting.
func (p *Provisioner) Deleted(ctx context.Context, spec Spec) (bool, string, error) {
	objects, err := clusterScoped(spec)
	if err != nil {
		return false, "", err
	}
	for _, obj := range objects {
		_, err := p.Cluster.Get(ctx, obj.GroupVersionKind(), "", obj.GetName())
		if err == nil {
			return false, fmt.Sprintf("%s %s is being deleted", obj.GetKind(), obj.GetName()), nil
		}
		if !apierrors.IsNotFound(err) {
			return false, "", fmt.Errorf("get %s %s: %w", obj.GetKind(), obj.GetName(), err)
		}
	}
	if spec.Platform.DedicatedNodes == nil {
		return true, "", nil
	}
	// Deleting a NodePool deletes its NodeClaims in the background.
	nodeClaims, err := p.Cluster.List(ctx, nodeClaimGVK, "", "karpenter.sh/nodepool="+spec.DedicatedNodePool())
	if err != nil {
		return false, "", fmt.Errorf("list the nodes of NodePool %s: %w", spec.DedicatedNodePool(), err)
	}
	if len(nodeClaims) > 0 {
		return false, fmt.Sprintf("%d nodes of NodePool %s are being deleted", len(nodeClaims), spec.DedicatedNodePool()), nil
	}
	return true, "", nil
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

// deploymentReady reports whether a Deployment has rolled out its latest spec
// and all its replicas are available, plus a short status for logging.
func deploymentReady(deployment *unstructured.Unstructured) (bool, string) {
	observed, _, _ := unstructured.NestedInt64(deployment.Object, "status", "observedGeneration")
	if observed < deployment.GetGeneration() {
		return false, "rollout not observed yet"
	}
	replicas, found, _ := unstructured.NestedInt64(deployment.Object, "spec", "replicas")
	if !found {
		replicas = 1
	}
	updated, _, _ := unstructured.NestedInt64(deployment.Object, "status", "updatedReplicas")
	available, _, _ := unstructured.NestedInt64(deployment.Object, "status", "availableReplicas")
	if updated < replicas || available < replicas {
		return false, fmt.Sprintf("%d of %d replicas updated, %d available", updated, replicas, available)
	}
	return true, "Available"
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
