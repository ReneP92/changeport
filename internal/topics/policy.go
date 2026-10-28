// Package topics holds the guardrails on the topics customers create in the
// console: what they may choose, what the platform fixes, and the topic
// configuration that follows.
package topics

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ReneP92/changeport/internal/provisioning"
)

// Limits that hold for every profile.
const (
	// MaxNameLength keeps room for a tenant prefix of about 49 characters
	// when shared clusters arrive in Phase 6 (Kafka allows 249).
	MaxNameLength = 200
	// DefaultPartitions is what the console proposes.
	DefaultPartitions = 3
	// ReplicaBudgetPerBroker caps the partition replicas a broker holds,
	// beyond which a small broker slows down.
	ReplicaBudgetPerBroker = 1000
	// localRetention is the brokers' log.local.retention.ms: how long tiered
	// topics keep data on the brokers' disks.
	localRetention = 24 * time.Hour
)

// ReservedPrefixes are the starts of names that belong to Kafka, the
// platform, Strimzi or Kafka Connect.
var ReservedPrefixes = []string{"_", "changeport", "strimzi", "connect-"}

// RetentionPreset is a retention customers may choose.
type RetentionPreset struct {
	Label    string
	Duration time.Duration
}

// RetentionPresets run from an hour to 30 days.
var RetentionPresets = []RetentionPreset{
	{"1 hour", time.Hour},
	{"6 hours", 6 * time.Hour},
	{"12 hours", 12 * time.Hour},
	{"1 day", 24 * time.Hour},
	{"3 days", 3 * 24 * time.Hour},
	{"7 days", 7 * 24 * time.Hour},
	{"14 days", 14 * 24 * time.Hour},
	{"30 days", 30 * 24 * time.Hour},
}

// DefaultRetention is what the console proposes.
const DefaultRetention = 7 * 24 * time.Hour

// The cleanup policies customers may choose.
const (
	// Delete removes messages older than the retention. Such topics are
	// tiered from the start.
	Delete = "delete"
	// Compact keeps the latest message per key. Tiered storage does not
	// support compacted topics, so they stay on the brokers.
	Compact = "compact"
)

// maxPartitionsByProfile caps the partitions of one topic per profile.
var maxPartitionsByProfile = map[string]int{
	provisioning.DevProfile.Name:        12,
	provisioning.DevHAProfile.Name:      24,
	provisioning.ProductionProfile.Name: 48,
}

// Policy is the guardrails for one cluster.
type Policy struct {
	// MaxPartitions caps the partitions of one topic.
	MaxPartitions int
	// ReplicationFactor is fixed by the profile.
	ReplicationFactor int
}

// ForProfile returns the policy of clusters of a profile.
func ForProfile(profile provisioning.Profile) (Policy, error) {
	maxPartitions, ok := maxPartitionsByProfile[profile.Name]
	if !ok {
		return Policy{}, fmt.Errorf("no topic policy for profile %q", profile.Name)
	}
	return Policy{MaxPartitions: maxPartitions, ReplicationFactor: profile.ReplicationFactor}, nil
}

// Request is what a customer asks for.
type Request struct {
	Name          string
	Partitions    int
	Retention     time.Duration
	CleanupPolicy string
}

// Cluster is what the policy needs to know of the cluster: the names of its
// topics, the partition replicas its brokers hold, and how many brokers.
type Cluster struct {
	Topics   []string
	Replicas int
	Brokers  int
}

// Topic is a topic to create: its name, partitions, the profile's
// replication factor, and its configuration.
type Topic struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]string
}

// ErrInvalid means the request breaks a guardrail; the error names the field
// and says why.
var ErrInvalid = errors.New("invalid topic")

// ErrConflict means the name is taken, or collides with a taken one.
var ErrConflict = errors.New("topic name conflict")

// FieldError is a broken guardrail on one field of a Request.
type FieldError struct {
	Field   string
	Message string
	kind    error
}

func (e *FieldError) Error() string { return e.Field + ": " + e.Message }

// Unwrap returns ErrInvalid or ErrConflict.
func (e *FieldError) Unwrap() error { return e.kind }

func invalid(field, format string, args ...any) *FieldError {
	return &FieldError{Field: field, Message: fmt.Sprintf(format, args...), kind: ErrInvalid}
}

// legalName is what Kafka allows in topic names.
var legalName = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

// Check applies the guardrails to request on cluster and returns the topic
// to create, or a *FieldError.
func (p Policy) Check(request Request, cluster Cluster) (Topic, error) {
	if err := checkName(request.Name, cluster.Topics); err != nil {
		return Topic{}, err
	}
	if request.Partitions < 1 || request.Partitions > p.MaxPartitions {
		return Topic{}, invalid("partitions", "choose 1 to %d partitions", p.MaxPartitions)
	}
	if budget := cluster.Brokers * ReplicaBudgetPerBroker; cluster.Replicas+request.Partitions*p.ReplicationFactor > budget {
		return Topic{}, invalid("partitions", "the cluster holds %d of its %d partition replicas; %d partitions would exceed them",
			cluster.Replicas, budget, request.Partitions)
	}
	if !slices.ContainsFunc(RetentionPresets, func(preset RetentionPreset) bool { return preset.Duration == request.Retention }) {
		return Topic{}, invalid("retention", "choose one of the retention presets, from 1 hour to 30 days")
	}
	configs := map[string]string{
		"retention.ms":   strconv.FormatInt(request.Retention.Milliseconds(), 10),
		"cleanup.policy": request.CleanupPolicy,
	}
	switch request.CleanupPolicy {
	case Delete:
		configs["remote.storage.enable"] = "true"
		// The brokers keep a day of tiered topics locally, which may not
		// exceed the topic's retention.
		if request.Retention < localRetention {
			configs["local.retention.ms"] = configs["retention.ms"]
		}
	case Compact:
	default:
		return Topic{}, invalid("cleanupPolicy", "choose %s or %s", Delete, Compact)
	}
	return Topic{
		Name:              request.Name,
		Partitions:        int32(request.Partitions),  //nolint:gosec // G115: at most MaxPartitions, checked above
		ReplicationFactor: int16(p.ReplicationFactor), //nolint:gosec // G115: a profile's, at most a few
		Configs:           configs,
	}, nil
}

// Reserved reports whether a topic name belongs to Kafka, the platform,
// Strimzi or Kafka Connect, so that customers may neither create nor delete
// it.
func Reserved(name string) bool {
	lower := strings.ToLower(name)
	return slices.ContainsFunc(ReservedPrefixes, func(prefix string) bool { return strings.HasPrefix(lower, prefix) })
}

// Collide reports whether two different topic names collide: Kafka's metrics
// replace dots with underscores, so names that differ only there clash.
func Collide(a, b string) bool {
	return a != b && strings.ReplaceAll(a, ".", "_") == strings.ReplaceAll(b, ".", "_")
}

// checkName applies the naming guardrails against the existing topics.
func checkName(name string, existing []string) error {
	switch {
	case name == "":
		return invalid("name", "name the topic")
	case len(name) > MaxNameLength:
		return invalid("name", "use at most %d characters", MaxNameLength)
	case name == "." || name == "..":
		return invalid("name", "%q is not a topic name", name)
	case !legalName.MatchString(name):
		return invalid("name", "use only letters, digits, dots, underscores and dashes")
	}
	lower := strings.ToLower(name)
	for _, prefix := range ReservedPrefixes {
		if strings.HasPrefix(lower, prefix) {
			return invalid("name", "names starting with %q are reserved for the platform", prefix)
		}
	}
	for _, other := range existing {
		if other == name {
			return &FieldError{Field: "name", Message: fmt.Sprintf("topic %s exists", name), kind: ErrConflict}
		}
		if Collide(other, name) {
			return &FieldError{Field: "name", Message: fmt.Sprintf("it collides with topic %s, as dots and underscores count as equal", other), kind: ErrConflict}
		}
	}
	return nil
}
