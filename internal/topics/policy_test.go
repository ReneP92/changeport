package topics

import (
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ReneP92/changeport/internal/provisioning"
)

func devPolicy(t *testing.T) Policy {
	t.Helper()
	p, err := ForProfile(provisioning.DevProfile)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

var quiet = Cluster{Topics: []string{"orders", "payments.eu"}, Replicas: 100, Brokers: 1}

func valid() Request {
	return Request{Name: "invoices", Partitions: 3, Retention: 7 * 24 * time.Hour, CleanupPolicy: Delete}
}

func TestCheckTieredTopic(t *testing.T) {
	topic, err := devPolicy(t).Check(valid(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"retention.ms": "604800000", "cleanup.policy": "delete", "remote.storage.enable": "true"}
	if topic.Name != "invoices" || topic.Partitions != 3 || topic.ReplicationFactor != 1 || !maps.Equal(topic.Configs, want) {
		t.Errorf("Check() = %+v, want 3 partitions, replication factor 1 and %v", topic, want)
	}
}

func TestCheckShortRetentionKeepsTieringValid(t *testing.T) {
	request := valid()
	request.Retention = time.Hour
	topic, err := devPolicy(t).Check(request, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if topic.Configs["local.retention.ms"] != "3600000" || topic.Configs["remote.storage.enable"] != "true" {
		t.Errorf("configs = %v, want a tiered topic keeping an hour locally", topic.Configs)
	}
	request.Retention = 24 * time.Hour
	topic, _ = devPolicy(t).Check(request, quiet)
	if _, ok := topic.Configs["local.retention.ms"]; ok {
		t.Errorf("a day's retention sets local.retention.ms: %v", topic.Configs)
	}
}

func TestCheckCompactedTopicIsNotTiered(t *testing.T) {
	request := valid()
	request.CleanupPolicy = Compact
	topic, err := devPolicy(t).Check(request, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if _, tiered := topic.Configs["remote.storage.enable"]; tiered || topic.Configs["cleanup.policy"] != "compact" {
		t.Errorf("configs = %v, want a compacted topic without tiering", topic.Configs)
	}
}

func TestCheckRefuses(t *testing.T) {
	tests := []struct {
		name    string
		change  func(*Request)
		cluster Cluster
		field   string
		kind    error
	}{
		{"no name", func(r *Request) { r.Name = "" }, quiet, "name", ErrInvalid},
		{"a long name", func(r *Request) { r.Name = strings.Repeat("a", 201) }, quiet, "name", ErrInvalid},
		{"a dot", func(r *Request) { r.Name = "." }, quiet, "name", ErrInvalid},
		{"spaces", func(r *Request) { r.Name = "my topic" }, quiet, "name", ErrInvalid},
		{"an internal name", func(r *Request) { r.Name = "__consumer_offsets" }, quiet, "name", ErrInvalid},
		{"the platform's prefix", func(r *Request) { r.Name = "Changeport-audit" }, quiet, "name", ErrInvalid},
		{"Strimzi's prefix", func(r *Request) { r.Name = "strimzi-store" }, quiet, "name", ErrInvalid},
		{"Kafka Connect's prefix", func(r *Request) { r.Name = "connect-offsets" }, quiet, "name", ErrInvalid},
		{"a taken name", func(r *Request) { r.Name = "orders" }, quiet, "name", ErrConflict},
		{"a dot for an underscore", func(r *Request) { r.Name = "payments_eu" }, quiet, "name", ErrConflict},
		{"no partitions", func(r *Request) { r.Partitions = 0 }, quiet, "partitions", ErrInvalid},
		{"more than the profile allows", func(r *Request) { r.Partitions = 13 }, quiet, "partitions", ErrInvalid},
		{"more than the brokers hold", func(r *Request) { r.Partitions = 12 }, Cluster{Replicas: 995, Brokers: 1}, "partitions", ErrInvalid},
		{"a retention that is no preset", func(r *Request) { r.Retention = 90 * time.Minute }, quiet, "retention", ErrInvalid},
		{"retention forever", func(r *Request) { r.Retention = -1 }, quiet, "retention", ErrInvalid},
		{"another cleanup policy", func(r *Request) { r.CleanupPolicy = "compact,delete" }, quiet, "cleanupPolicy", ErrInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := valid()
			tt.change(&request)
			_, err := devPolicy(t).Check(request, tt.cluster)
			var fieldErr *FieldError
			if !errors.As(err, &fieldErr) || fieldErr.Field != tt.field || !errors.Is(err, tt.kind) {
				t.Errorf("Check() = %v, want a %v on %s", err, tt.kind, tt.field)
			}
		})
	}
}

func TestPolicyPerProfile(t *testing.T) {
	for name, want := range map[string]Policy{
		"dev":        {MaxPartitions: 12, ReplicationFactor: 1},
		"dev-ha":     {MaxPartitions: 24, ReplicationFactor: 3},
		"production": {MaxPartitions: 48, ReplicationFactor: 3},
	} {
		profile, err := provisioning.ProfileByName(name)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := ForProfile(profile); err != nil || got != want {
			t.Errorf("ForProfile(%s) = %+v, %v, want %+v", name, got, err, want)
		}
	}
	// Every profile has a policy.
	for _, name := range slices.Sorted(maps.Keys(provisioning.Profiles)) {
		if _, err := ForProfile(provisioning.Profiles[name]); err != nil {
			t.Errorf("profile %s: %v", name, err)
		}
	}
}

func TestReplicaBudgetCountsReplication(t *testing.T) {
	p, err := ForProfile(provisioning.ProductionProfile)
	if err != nil {
		t.Fatal(err)
	}
	request := valid()
	request.Partitions = 10
	// 3 brokers hold 3,000 replicas; 10 partitions of 3 replicas need 30.
	if _, err := p.Check(request, Cluster{Replicas: 2970, Brokers: 3}); err != nil {
		t.Errorf("Check() at the budget = %v", err)
	}
	if _, err := p.Check(request, Cluster{Replicas: 2971, Brokers: 3}); err == nil {
		t.Error("Check() past the budget succeeded")
	}
}
