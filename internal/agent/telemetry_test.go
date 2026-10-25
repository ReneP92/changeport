package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type resource struct {
	resourceType kmsg.ConfigResourceType
	name         string
}

var (
	brokerDefaults = resource{kmsg.ConfigResourceTypeBroker, ""}
	subscription   = resource{kmsg.ConfigResourceTypeClientMetrics, SubscriptionName}
)

// fakeConfigs is a broker that stores dynamic configs by resource and
// answers the two requests Telemetry sends.
type fakeConfigs struct {
	configs     map[resource]map[string]string
	alters      int
	describeErr map[resource]*kerr.Error
	alterErr    map[resource]*kerr.Error
}

func errorCode(errs map[resource]*kerr.Error, r resource) int16 {
	if err := errs[r]; err != nil {
		return err.Code
	}
	return 0
}

func (f *fakeConfigs) Request(_ context.Context, req kmsg.Request) (kmsg.Response, error) {
	switch req := req.(type) {
	case *kmsg.DescribeConfigsRequest:
		resp := req.ResponseKind().(*kmsg.DescribeConfigsResponse)
		for _, rr := range req.Resources {
			key := resource{rr.ResourceType, rr.ResourceName}
			r := kmsg.NewDescribeConfigsResponseResource()
			r.ResourceType, r.ResourceName = rr.ResourceType, rr.ResourceName
			r.ErrorCode = errorCode(f.describeErr, key)
			configs := map[string]string{}
			// Like Kafka, a subscription that does not exist has its defaults.
			if key.resourceType == kmsg.ConfigResourceTypeClientMetrics {
				configs = map[string]string{subscriptionMetrics: "", subscriptionInterval: "300000", subscriptionMatch: ""}
			}
			for name, value := range f.configs[key] {
				configs[name] = value
			}
			for name, value := range configs {
				c := kmsg.NewDescribeConfigsResponseResourceConfig()
				c.Name, c.Value = name, kmsg.StringPtr(value)
				// Like Kafka, options it does not know are sensitive.
				if name == telemetryLabelsConfig {
					c.Value, c.IsSensitive = nil, true
				}
				r.Configs = append(r.Configs, c)
			}
			resp.Resources = append(resp.Resources, r)
		}
		return resp, nil
	case *kmsg.IncrementalAlterConfigsRequest:
		f.alters++
		resp := req.ResponseKind().(*kmsg.IncrementalAlterConfigsResponse)
		for _, rr := range req.Resources {
			key := resource{rr.ResourceType, rr.ResourceName}
			r := kmsg.NewIncrementalAlterConfigsResponseResource()
			r.ResourceType, r.ResourceName = rr.ResourceType, rr.ResourceName
			r.ErrorCode = errorCode(f.alterErr, key)
			resp.Resources = append(resp.Resources, r)
			if r.ErrorCode != 0 {
				continue
			}
			if f.configs == nil {
				f.configs = map[resource]map[string]string{}
			}
			if f.configs[key] == nil {
				f.configs[key] = map[string]string{}
			}
			for _, c := range rr.Configs {
				switch c.Op {
				case kmsg.IncrementalAlterConfigOpSet:
					f.configs[key][c.Name] = *c.Value
				case kmsg.IncrementalAlterConfigOpDelete:
					delete(f.configs[key], c.Name)
				default:
					return nil, errors.New("unexpected operation")
				}
			}
		}
		return resp, nil
	default:
		return nil, errors.New("unexpected request")
	}
}

func newTestTelemetry(f *fakeConfigs) *Telemetry {
	return NewTelemetry(f, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)), prometheus.NewRegistry())
}

func TestTelemetrySetsEverythingUp(t *testing.T) {
	f := &fakeConfigs{}
	tel := newTestTelemetry(f)
	if err := tel.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := f.configs[brokerDefaults][telemetryLabelsConfig]; got != "client_id,principal" {
		t.Errorf("the brokers' telemetry labels = %q, want client_id,principal", got)
	}
	got := f.configs[subscription]
	if got[subscriptionMetrics] != strings.Join(SubscribedMetrics, ",") {
		t.Errorf("metrics = %q, want the subscribed metrics", got[subscriptionMetrics])
	}
	if got[subscriptionInterval] != "60000" {
		t.Errorf("interval.ms = %q, want 60000", got[subscriptionInterval])
	}
	if f.alters != 2 {
		t.Errorf("%d alter requests, want 2", f.alters)
	}
	if got := testutil.ToFloat64(tel.results.WithLabelValues("updated")); got != 1 {
		t.Errorf("updated reconciles = %v, want 1", got)
	}
}

func TestTelemetryLeavesCorrectSettingsAlone(t *testing.T) {
	f := &fakeConfigs{}
	tel := newTestTelemetry(f)
	for range 2 {
		if err := tel.Reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if f.alters != 2 {
		t.Errorf("%d alter requests, want 2", f.alters)
	}
	if got := testutil.ToFloat64(tel.results.WithLabelValues("ok")); got != 1 {
		t.Errorf("ok reconciles = %v, want 1", got)
	}
}

func TestTelemetryCorrectsChangedSettings(t *testing.T) {
	customers := resource{kmsg.ConfigResourceTypeClientMetrics, "customers-own"}
	f := &fakeConfigs{configs: map[resource]map[string]string{
		brokerDefaults: {telemetryLabelsConfig: "client_id,client_address", "log.retention.ms": "1000"},
		subscription:   {subscriptionMetrics: "*", subscriptionInterval: "1000", subscriptionMatch: "client_id=app-.*"},
		customers:      {subscriptionMetrics: "*"},
	}}
	tel := newTestTelemetry(f)
	if err := tel.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := f.configs[brokerDefaults]; got[telemetryLabelsConfig] != TelemetryLabels || got["log.retention.ms"] != "1000" {
		t.Errorf("the brokers' default configs = %v, want the telemetry labels corrected and nothing else changed", got)
	}
	got := f.configs[subscription]
	want := map[string]string{subscriptionMetrics: strings.Join(SubscribedMetrics, ","), subscriptionInterval: "60000"}
	if len(got) != len(want) || got[subscriptionMetrics] != want[subscriptionMetrics] || got[subscriptionInterval] != want[subscriptionInterval] {
		t.Errorf("subscription = %v, want %v", got, want)
	}
	if f.configs[customers][subscriptionMetrics] != "*" {
		t.Error("another subscription was changed")
	}
}

// The brokers' telemetry labels cannot be read back, so they are set when
// the agent starts and whenever they are missing.
func TestTelemetrySetsHiddenLabelsOncePerStart(t *testing.T) {
	f := &fakeConfigs{configs: map[resource]map[string]string{
		brokerDefaults: {telemetryLabelsConfig: "client_address"},
	}}
	tel := newTestTelemetry(f)
	for range 3 {
		if err := tel.Reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if got := f.configs[brokerDefaults][telemetryLabelsConfig]; got != TelemetryLabels {
		t.Errorf("the brokers' telemetry labels = %q, want %q", got, TelemetryLabels)
	}
	// The subscription and the labels once each.
	if f.alters != 2 {
		t.Errorf("%d alter requests, want 2", f.alters)
	}

	delete(f.configs[brokerDefaults], telemetryLabelsConfig)
	if err := tel.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := f.configs[brokerDefaults][telemetryLabelsConfig]; got != TelemetryLabels {
		t.Errorf("missing telemetry labels were not set again, got %q", got)
	}

	f.configs[brokerDefaults][telemetryLabelsConfig] = "client_address"
	restarted := newTestTelemetry(f)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := f.configs[brokerDefaults][telemetryLabelsConfig]; got != TelemetryLabels {
		t.Errorf("a restarted agent did not set the telemetry labels, got %q", got)
	}
}

func TestTelemetryErrors(t *testing.T) {
	tests := []struct {
		name string
		f    *fakeConfigs
		// other is the resource whose settings are still put in place.
		other resource
		key   string
	}{
		{"describe denied", &fakeConfigs{describeErr: map[resource]*kerr.Error{subscription: kerr.ClusterAuthorizationFailed}}, brokerDefaults, telemetryLabelsConfig},
		{"alter denied", &fakeConfigs{alterErr: map[resource]*kerr.Error{brokerDefaults: kerr.ClusterAuthorizationFailed}}, subscription, subscriptionMetrics},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tel := newTestTelemetry(tt.f)
			err := tel.Reconcile(t.Context())
			if !errors.Is(err, kerr.ClusterAuthorizationFailed) {
				t.Errorf("Reconcile() = %v, want %v", err, kerr.ClusterAuthorizationFailed)
			}
			if tt.f.configs[tt.other][tt.key] == "" {
				t.Errorf("%s was not set despite the other error", tt.key)
			}
			if got := testutil.ToFloat64(tel.results.WithLabelValues("error")); got != 1 {
				t.Errorf("error reconciles = %v, want 1", got)
			}
		})
	}
}

func TestTelemetryTreatsAMissingSubscriptionAsEmpty(t *testing.T) {
	f := &fakeConfigs{describeErr: map[resource]*kerr.Error{subscription: kerr.ResourceNotFound}}
	tel := newTestTelemetry(f)
	if err := tel.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if f.configs[subscription][subscriptionMetrics] == "" {
		t.Error("the subscription was not created")
	}
}

func TestSubscribedMetrics(t *testing.T) {
	seen := map[string]bool{}
	for _, name := range SubscribedMetrics {
		if seen[name] {
			t.Errorf("%s is subscribed twice", name)
		}
		seen[name] = true
		if !strings.HasPrefix(name, "org.apache.kafka.producer.") && !strings.HasPrefix(name, "org.apache.kafka.consumer.") {
			t.Errorf("%s is not a producer or consumer metric", name)
		}
		// Per-broker metrics carry a node_id that clashes with the broker's.
		if strings.Contains(name, ".node.") {
			t.Errorf("%s is a per-broker metric", name)
		}
	}
}
