package console

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ReneP92/changeport/internal/clusters"
	"github.com/ReneP92/changeport/internal/metrics"
	"github.com/ReneP92/changeport/internal/provisioning"
)

// Clusters reads what the console shows of a customer's Kafka cluster.
type Clusters interface {
	// Brokers returns the cluster's broker pods.
	Brokers(ctx context.Context, customerID string) ([]Broker, error)
	// Topics returns the customer's topics and their settings, by name, or
	// an error satisfying errors.Is(err, clusters.ErrNotReady).
	Topics(ctx context.Context, customerID string) ([]TopicSettings, error)
	// AdminCredentials returns the admin user's sasl.jaas.config, or an
	// error satisfying errors.Is(err, clusters.ErrNotReady).
	AdminCredentials(ctx context.Context, customerID string) (string, error)
	// ListenerCA returns the PEM certificate of the CA of the public
	// listener's certificate, or "" when clients trust it by default or it
	// is not issued yet.
	ListenerCA(ctx context.Context, customerID string) (string, error)
}

// Metrics reads the console's named queries.
type Metrics interface {
	Instant(ctx context.Context, tenant string, q metrics.Query, filter metrics.Filter, at time.Time) ([]metrics.Series, error)
	Range(ctx context.Context, tenant string, q metrics.Query, filter metrics.Filter, start, end time.Time, step time.Duration) ([]metrics.Series, error)
}

// Broker is one broker pod.
type Broker struct {
	Name     string
	Ready    bool
	Zone     string
	Restarts int
}

// TopicSettings is a topic as Kafka describes it.
type TopicSettings struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	// Tiered is whether remote.storage.enable is on.
	Tiered        bool
	CleanupPolicy string
	// RetentionMs is retention.ms, or nil for no limit.
	RetentionMs *int64
}

// KubeClusters implements Clusters with the Kubernetes API, as the console's
// service account, and the customers' Kafka clusters, as their
// changeport-console Kafka users.
type KubeClusters struct {
	Client client.Reader
	Kafka  *clusters.Pool
}

var _ Clusters = KubeClusters{}

// Brokers implements Clusters.
func (k KubeClusters) Brokers(ctx context.Context, customerID string) ([]Broker, error) {
	spec := provisioning.Spec{CustomerID: customerID}
	var pods corev1.PodList
	err := k.Client.List(ctx, &pods, client.InNamespace(spec.Namespace()),
		client.MatchingLabels{"strimzi.io/cluster": spec.ClusterName(), "strimzi.io/broker-role": "true"})
	if err != nil {
		return nil, fmt.Errorf("list the brokers of customer %s: %w", customerID, err)
	}
	brokers := make([]Broker, 0, len(pods.Items))
	for _, pod := range pods.Items {
		broker := Broker{Name: pod.Name, Zone: pod.Labels["topology.kubernetes.io/zone"]}
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady {
				broker.Ready = condition.Status == corev1.ConditionTrue
			}
		}
		for _, container := range pod.Status.ContainerStatuses {
			broker.Restarts += int(container.RestartCount)
		}
		brokers = append(brokers, broker)
	}
	slices.SortFunc(brokers, func(a, b Broker) int { return strings.Compare(a.Name, b.Name) })
	return brokers, nil
}

// Topics implements Clusters. Internal topics, whose names start with two
// underscores, are Kafka's own.
func (k KubeClusters) Topics(ctx context.Context, customerID string) ([]TopicSettings, error) {
	admin, err := k.Kafka.Admin(ctx, customerID)
	if err != nil {
		return nil, err
	}
	listed, err := admin.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("list the topics of customer %s: %w", customerID, err)
	}
	var topics []TopicSettings
	for _, detail := range listed.Sorted() {
		if detail.Err != nil || strings.HasPrefix(detail.Topic, "__") {
			continue
		}
		topics = append(topics, TopicSettings{
			Name:              detail.Topic,
			Partitions:        len(detail.Partitions),
			ReplicationFactor: detail.Partitions.NumReplicas(),
		})
	}
	if len(topics) == 0 {
		return topics, nil
	}
	names := make([]string, len(topics))
	for i, topic := range topics {
		names[i] = topic.Name
	}
	configs, err := admin.DescribeTopicConfigs(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("describe the topics of customer %s: %w", customerID, err)
	}
	for i := range topics {
		described, err := configs.On(topics[i].Name, nil)
		if err != nil {
			continue
		}
		for _, config := range described.Configs {
			value := ""
			if config.Value != nil {
				value = *config.Value
			}
			switch config.Key {
			case "remote.storage.enable":
				topics[i].Tiered = value == "true"
			case "cleanup.policy":
				topics[i].CleanupPolicy = value
			case "retention.ms":
				if ms, err := strconv.ParseInt(value, 10, 64); err == nil && ms >= 0 {
					topics[i].RetentionMs = &ms
				}
			}
		}
	}
	return topics, nil
}

// AdminCredentials implements Clusters.
func (k KubeClusters) AdminCredentials(ctx context.Context, customerID string) (string, error) {
	jaas, err := k.secretValue(ctx, customerID, provisioning.AdminUser, "sasl.jaas.config")
	if err != nil {
		return "", err
	}
	if jaas == "" {
		return "", fmt.Errorf("customer %s: the admin user has no credentials yet: %w", customerID, clusters.ErrNotReady)
	}
	return jaas, nil
}

// ListenerCA implements Clusters.
func (k KubeClusters) ListenerCA(ctx context.Context, customerID string) (string, error) {
	ca, err := k.secretValue(ctx, customerID, provisioning.ExternalListenerSecret, "ca.crt")
	if errors.Is(err, clusters.ErrNotReady) {
		return "", nil
	}
	return strings.TrimSpace(ca), err
}

// secretValue returns one key of a Secret in the customer's namespace.
func (k KubeClusters) secretValue(ctx context.Context, customerID, name, key string) (string, error) {
	data, _, err := KubeSecrets{Client: k.Client}.Get(ctx, provisioning.Spec{CustomerID: customerID}.Namespace(), name)
	if errors.Is(err, clusters.ErrNotFound) {
		return "", fmt.Errorf("customer %s: secret %s is missing: %w", customerID, name, clusters.ErrNotReady)
	}
	if err != nil {
		return "", err
	}
	return string(data[key]), nil
}

// KubeSecrets implements clusters.Secrets with the Kubernetes API.
type KubeSecrets struct {
	Client client.Reader
}

var _ clusters.Secrets = KubeSecrets{}

// Get implements clusters.Secrets.
func (k KubeSecrets) Get(ctx context.Context, namespace, name string) (map[string][]byte, string, error) {
	var secret corev1.Secret
	err := k.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &secret)
	if apierrors.IsNotFound(err) {
		return nil, "", clusters.ErrNotFound
	}
	if err != nil {
		return nil, "", fmt.Errorf("get secret %s/%s: %w", namespace, name, err)
	}
	return secret.Data, secret.ResourceVersion, nil
}
