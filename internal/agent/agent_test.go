package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func cfg(key, value string, source kmsg.ConfigSource) kadm.Config {
	return kadm.Config{Key: key, Value: kadm.StringPtr(value), Source: source}
}

var (
	remoteDefault = cfg(remoteStorageEnable, "false", kmsg.ConfigSourceDefaultConfig)
	deletePolicy  = cfg(cleanupPolicy, "delete", kmsg.ConfigSourceDefaultConfig)
)

func TestDecide(t *testing.T) {
	tests := []struct {
		name    string
		configs []kadm.Config
		want    State
	}{
		{"defaults", []kadm.Config{remoteDefault, deletePolicy}, Enable},
		{"no configs returned", nil, Enable},
		{"already tiered", []kadm.Config{cfg(remoteStorageEnable, "true", kmsg.ConfigSourceDynamicTopicConfig), deletePolicy}, Tiered},
		{"opted out", []kadm.Config{cfg(remoteStorageEnable, "false", kmsg.ConfigSourceDynamicTopicConfig), deletePolicy}, OptedOut},
		{"compacted", []kadm.Config{remoteDefault, cfg(cleanupPolicy, "compact", kmsg.ConfigSourceDynamicTopicConfig)}, Compacted},
		{"compacted and deleted", []kadm.Config{remoteDefault, cfg(cleanupPolicy, "compact,delete", kmsg.ConfigSourceDynamicTopicConfig)}, Compacted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Decide(tt.configs); got != tt.want {
				t.Errorf("Decide() = %s, want %s", got, tt.want)
			}
		})
	}
}

// fakeAdmin holds topic configs and applies incremental SET operations.
type fakeAdmin struct {
	topics   map[string][]kadm.Config
	internal map[string]bool
	altered  [][]string
	alterErr map[string]error
}

func (f *fakeAdmin) ListTopics(context.Context, ...string) (kadm.TopicDetails, error) {
	details := kadm.TopicDetails{}
	for name := range f.topics {
		details[name] = kadm.TopicDetail{Topic: name, IsInternal: f.internal[name]}
	}
	return details, nil
}

func (f *fakeAdmin) DescribeTopicConfigs(_ context.Context, topics ...string) (kadm.ResourceConfigs, error) {
	var out kadm.ResourceConfigs
	for _, name := range topics {
		configs, ok := f.topics[name]
		if !ok {
			out = append(out, kadm.ResourceConfig{Name: name, Err: kerr.UnknownTopicOrPartition})
			continue
		}
		out = append(out, kadm.ResourceConfig{Name: name, Configs: slices.Clone(configs)})
	}
	return out, nil
}

func (f *fakeAdmin) AlterTopicConfigs(_ context.Context, configs []kadm.AlterConfig, topics ...string) (kadm.AlterConfigsResponses, error) {
	f.altered = append(f.altered, slices.Clone(topics))
	var out kadm.AlterConfigsResponses
	for _, name := range topics {
		if err := f.alterErr[name]; err != nil {
			out = append(out, kadm.AlterConfigsResponse{Name: name, Err: err, ErrMessage: "refused"})
			continue
		}
		for _, c := range configs {
			f.set(name, c.Name, *c.Value)
		}
		out = append(out, kadm.AlterConfigsResponse{Name: name})
	}
	return out, nil
}

func (f *fakeAdmin) set(topic, key, value string) {
	configs := f.topics[topic]
	for i := range configs {
		if configs[i].Key == key {
			configs[i] = cfg(key, value, kmsg.ConfigSourceDynamicTopicConfig)
			return
		}
	}
	f.topics[topic] = append(configs, cfg(key, value, kmsg.ConfigSourceDynamicTopicConfig))
}

func newFakeAdmin() *fakeAdmin {
	return &fakeAdmin{
		topics: map[string][]kadm.Config{
			"orders":                {remoteDefault, deletePolicy},
			"payments":              {remoteDefault, deletePolicy},
			"customers":             {remoteDefault, cfg(cleanupPolicy, "compact", kmsg.ConfigSourceDynamicTopicConfig)},
			"audit":                 {cfg(remoteStorageEnable, "false", kmsg.ConfigSourceDynamicTopicConfig), deletePolicy},
			"clicks":                {cfg(remoteStorageEnable, "true", kmsg.ConfigSourceDynamicTopicConfig), deletePolicy},
			"__consumer_offsets":    {remoteDefault, cfg(cleanupPolicy, "compact", kmsg.ConfigSourceDefaultConfig)},
			"__remote_log_metadata": {remoteDefault, deletePolicy},
		},
		internal: map[string]bool{"__consumer_offsets": true},
	}
}

func newReconciler(admin Admin) *Reconciler {
	return &Reconciler{Admin: admin, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: NewMetrics(prometheus.NewRegistry())}
}

func TestReconcileTiersTopicsOnce(t *testing.T) {
	admin := newFakeAdmin()
	r := newReconciler(admin)
	for range 2 {
		if err := r.Reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if len(admin.altered) != 1 {
		t.Fatalf("altered configs %d times (%v), want once: the second pass has nothing to do", len(admin.altered), admin.altered)
	}
	slices.Sort(admin.altered[0])
	if got := strings.Join(admin.altered[0], ","); got != "orders,payments" {
		t.Errorf("turned on tiered storage for %s, want orders,payments", got)
	}
	if got := testutil.ToFloat64(r.Metrics.enabled); got != 2 {
		t.Errorf("changeport_agent_tiering_enabled_total = %v, want 2", got)
	}
	for state, want := range map[State]float64{Tiered: 3, Compacted: 1, OptedOut: 1} {
		if got := testutil.ToFloat64(r.Metrics.topics.WithLabelValues(string(state))); got != want {
			t.Errorf("changeport_agent_topics{state=%q} = %v, want %v", state, got, want)
		}
	}
	if got := testutil.ToFloat64(r.Metrics.reconciles.WithLabelValues("ok")); got != 2 {
		t.Errorf("successful reconciliations = %v, want 2", got)
	}
}

func TestReconcileReportsFailedTopicsAndCarriesOn(t *testing.T) {
	admin := newFakeAdmin()
	admin.alterErr = map[string]error{"orders": kerr.PolicyViolation, "payments": kerr.UnknownTopicOrPartition}
	r := newReconciler(admin)
	err := r.Reconcile(t.Context())
	if !errors.Is(err, kerr.PolicyViolation) || !strings.Contains(err.Error(), "orders") {
		t.Fatalf("Reconcile() error = %v, want the policy violation for orders", err)
	}
	if strings.Contains(err.Error(), "payments") {
		t.Errorf("Reconcile() reports payments, deleted meanwhile: %v", err)
	}
	if got := testutil.ToFloat64(r.Metrics.reconciles.WithLabelValues("error")); got != 1 {
		t.Errorf("failed reconciliations = %v, want 1", got)
	}
}

func TestReconcileWithoutTopics(t *testing.T) {
	r := newReconciler(&fakeAdmin{topics: map[string][]kadm.Config{}})
	if err := r.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
}
