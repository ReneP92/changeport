package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Client telemetry (KIP-714): Kafka clients push their own metrics to a
// broker, whose metrics reporter exposes them as clients_* series next to
// the broker's own (docs/metrics-labels.md). Two settings make that happen,
// and Strimzi manages neither:
//
//   - Clients push only what a client metrics subscription asks for, and a
//     cluster has none until the agent creates the platform's.
//   - The reporter labels each client's series with what the broker knows
//     about the client, as its client.telemetry.labels config says. Strimzi
//     forbids prometheus.metrics.reporter.* options in the Kafka resource, so
//     the agent sets it as a cluster-wide dynamic broker config instead,
//     which the reporter applies without a restart. Strimzi leaves dynamic
//     configs it does not know alone. Kafka reports options it does not know
//     as sensitive, without their value, so the agent sets this one when it
//     starts and whenever it is missing, rather than comparing it. Only the
//     platform may alter cluster configs.

// SubscriptionName is the client metrics subscription the agent owns. It
// leaves any other subscription on the cluster alone.
const SubscriptionName = "changeport"

// SubscribedMetrics are the client metrics the platform collects, by full
// name: a subscription matches metric names by prefix, so a full name
// matches only itself and the set of series stays fixed. They are the
// metrics KIP-714 asks every client to report, which non-Java clients such as
// librdkafka do too, and the Java clients' counts of records, errors and
// bytes. Per-broker metrics (node.*) are left out: there are as many of them
// as brokers, and their node_id attribute clashes with the broker's own.
var SubscribedMetrics = []string{
	"org.apache.kafka.producer.record.send.total",
	"org.apache.kafka.producer.record.error.total",
	"org.apache.kafka.producer.record.retry.total",
	"org.apache.kafka.producer.outgoing.byte.total",
	"org.apache.kafka.producer.topic.record.send.total",
	"org.apache.kafka.producer.topic.record.error.total",
	"org.apache.kafka.producer.topic.byte.total",
	"org.apache.kafka.producer.request.latency.avg",
	"org.apache.kafka.producer.request.latency.max",
	"org.apache.kafka.producer.record.queue.time.avg",
	"org.apache.kafka.producer.record.queue.time.max",
	"org.apache.kafka.producer.produce.throttle.time.avg",
	"org.apache.kafka.producer.produce.throttle.time.max",
	"org.apache.kafka.producer.buffer.available.bytes",
	"org.apache.kafka.producer.connection.count",
	"org.apache.kafka.producer.connection.creation.total",
	"org.apache.kafka.consumer.fetch.manager.records.consumed.total",
	"org.apache.kafka.consumer.fetch.manager.bytes.consumed.total",
	"org.apache.kafka.consumer.fetch.manager.fetch.latency.avg",
	"org.apache.kafka.consumer.fetch.manager.fetch.latency.max",
	"org.apache.kafka.consumer.fetch.manager.fetch.throttle.time.avg",
	"org.apache.kafka.consumer.fetch.manager.fetch.throttle.time.max",
	"org.apache.kafka.consumer.coordinator.assigned.partitions",
	"org.apache.kafka.consumer.coordinator.commit.latency.avg",
	"org.apache.kafka.consumer.coordinator.commit.latency.max",
	"org.apache.kafka.consumer.coordinator.rebalance.total",
	"org.apache.kafka.consumer.coordinator.failed.rebalance.total",
	"org.apache.kafka.consumer.poll.idle.ratio.avg",
	"org.apache.kafka.consumer.time.between.poll.max",
	"org.apache.kafka.consumer.connection.count",
	"org.apache.kafka.consumer.connection.creation.total",
}

// TelemetryLabels are the labels the brokers' metrics reporter adds to each
// client's series, besides the client instance ID it always adds: the client
// ID the application sets and the Kafka user it connects as. Both take
// bounded values, unlike client_address.
const TelemetryLabels = "client_id,principal"

// The configs the agent keeps in place.
const (
	telemetryLabelsConfig = "prometheus.metrics.reporter.client.telemetry.labels"
	subscriptionMetrics   = "metrics"
	subscriptionInterval  = "interval.ms"
	// subscriptionMatch selects the clients a subscription applies to; the
	// platform's applies to all of them, so it has none.
	subscriptionMatch = "match"
)

// resourceConfigs are configs the agent keeps on one config resource.
type resourceConfigs struct {
	// description names the resource in logs and errors.
	description  string
	resourceType kmsg.ConfigResourceType
	// name is the resource's name; the empty name of a broker resource is
	// the cluster-wide default of every broker.
	name string
	// set are the configs with the value they must have.
	set map[string]string
	// unset are the configs that must not be set.
	unset []string
}

// changes returns the operations that turn the current configs into the
// wanted ones, none when they are in place. A nil value in current is one
// Kafka does not reveal; it counts as in place if trustHidden is set.
func (r resourceConfigs) changes(current map[string]*string, trustHidden bool) []kmsg.IncrementalAlterConfigsRequestResourceConfig {
	var changes []kmsg.IncrementalAlterConfigsRequestResourceConfig
	for _, name := range slices.Sorted(maps.Keys(r.set)) {
		value, ok := current[name]
		if ok && (value == nil && trustHidden || value != nil && *value == r.set[name]) {
			continue
		}
		change := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		change.Name = name
		change.Op = kmsg.IncrementalAlterConfigOpSet
		change.Value = kmsg.StringPtr(r.set[name])
		changes = append(changes, change)
	}
	for _, name := range r.unset {
		if value, ok := current[name]; !ok || value != nil && *value == "" {
			continue
		}
		change := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		change.Name = name
		change.Op = kmsg.IncrementalAlterConfigOpDelete
		changes = append(changes, change)
	}
	return changes
}

// telemetryConfigs returns the configs the agent keeps in place for client
// telemetry: the reporter's labels on every broker, and the subscription to
// SubscribedMetrics, pushed every interval by all clients.
func telemetryConfigs(interval time.Duration) []resourceConfigs {
	return []resourceConfigs{
		{
			description:  "the brokers' default configs",
			resourceType: kmsg.ConfigResourceTypeBroker,
			set:          map[string]string{telemetryLabelsConfig: TelemetryLabels},
		},
		{
			description:  "the client metrics subscription",
			resourceType: kmsg.ConfigResourceTypeClientMetrics,
			name:         SubscriptionName,
			set: map[string]string{
				subscriptionMetrics:  strings.Join(SubscribedMetrics, ","),
				subscriptionInterval: strconv.FormatInt(interval.Milliseconds(), 10),
			},
			unset: []string{subscriptionMatch},
		},
	}
}

// Telemetry keeps the settings of client telemetry in place.
type Telemetry struct {
	// Client sends the Kafka requests; *kgo.Client implements it. kadm has
	// no helpers for client metrics or default broker configs.
	Client kmsg.Requestor
	// Interval is how often clients push their metrics.
	Interval time.Duration
	Logger   *slog.Logger
	results  *prometheus.CounterVec
	// inPlace holds the resources this Telemetry has put in place, by
	// description, whose hidden values it trusts from then on.
	inPlace map[string]bool
}

// NewTelemetry returns a Telemetry whose metrics are registered with
// registry.
func NewTelemetry(client kmsg.Requestor, interval time.Duration, logger *slog.Logger, registry prometheus.Registerer) *Telemetry {
	t := &Telemetry{
		Client:   client,
		Interval: interval,
		Logger:   logger,
		inPlace:  map[string]bool{},
		results: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "changeport_agent_telemetry_reconciles_total",
			Help: "Reconciliations of the client telemetry settings, by result (ok, updated or error).",
		}, []string{"result"}),
	}
	registry.MustRegister(t.results)
	for _, result := range []string{"ok", "updated", "error"} {
		t.results.WithLabelValues(result)
	}
	return t
}

// Reconcile creates or corrects the settings. It returns the first error
// after trying each of them.
func (t *Telemetry) Reconcile(ctx context.Context) error {
	var errs []error
	updated := false
	for _, configs := range telemetryConfigs(t.Interval) {
		changed, err := t.reconcile(ctx, configs)
		if err != nil {
			errs = append(errs, err)
		}
		updated = updated || changed
	}
	switch {
	case len(errs) > 0:
		t.results.WithLabelValues("error").Inc()
	case updated:
		t.results.WithLabelValues("updated").Inc()
	default:
		t.results.WithLabelValues("ok").Inc()
	}
	return errors.Join(errs...)
}

func (t *Telemetry) reconcile(ctx context.Context, configs resourceConfigs) (bool, error) {
	current, err := t.describe(ctx, configs)
	if err != nil {
		return false, fmt.Errorf("describe %s: %w", configs.description, err)
	}
	changes := configs.changes(current, t.inPlace[configs.description])
	if len(changes) == 0 {
		t.inPlace[configs.description] = true
		return false, nil
	}
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = configs.resourceType
	resource.ResourceName = configs.name
	resource.Configs = changes
	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	req.Resources = append(req.Resources, resource)
	resp, err := req.RequestWith(ctx, t.Client)
	if err != nil {
		return false, fmt.Errorf("alter %s: %w", configs.description, err)
	}
	for _, r := range resp.Resources {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			return false, fmt.Errorf("alter %s: %w (%s)", configs.description, err, derefString(r.ErrorMessage))
		}
	}
	names := make([]string, 0, len(changes))
	for _, change := range changes {
		names = append(names, change.Name)
	}
	t.Logger.Info("updated "+configs.description, "configs", names)
	t.inPlace[configs.description] = true
	return true, nil
}

// describe returns the resource's configs, with a nil value for those Kafka
// does not reveal; a client metrics subscription that does not exist has
// none.
func (t *Telemetry) describe(ctx context.Context, configs resourceConfigs) (map[string]*string, error) {
	resource := kmsg.NewDescribeConfigsRequestResource()
	resource.ResourceType = configs.resourceType
	resource.ResourceName = configs.name
	req := kmsg.NewPtrDescribeConfigsRequest()
	req.Resources = append(req.Resources, resource)
	resp, err := req.RequestWith(ctx, t.Client)
	if err != nil {
		return nil, err
	}
	current := map[string]*string{}
	for _, r := range resp.Resources {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			if errors.Is(err, kerr.ResourceNotFound) {
				continue
			}
			return nil, fmt.Errorf("%w (%s)", err, derefString(r.ErrorMessage))
		}
		for _, c := range r.Configs {
			current[c.Name] = c.Value
		}
	}
	return current, nil
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
