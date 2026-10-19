// Package provisioning creates and removes dedicated customer Kafka clusters.
//
// Every customer's Kubernetes resources are generated from one template
// (templates/customer-cluster.yaml.tmpl), so that adding shared clusters in
// Phase 6 means adding tenants rather than rewriting provisioning.
package provisioning

import (
	"errors"
	"fmt"
	"regexp"
)

const (
	// KafkaVersion is the newest Kafka 4.x supported by the pinned Strimzi release.
	KafkaVersion = "4.3.1"
	// MetadataVersion is the KRaft metadata version matching KafkaVersion.
	MetadataVersion = "4.3-IV0"

	// TenantLabel identifies the customer on every resource the provisioner creates.
	TenantLabel = "changeport.io/tenant"
	// ManagedBy is the value of app.kubernetes.io/managed-by and the server-side apply field manager.
	ManagedBy = "changeport-provisioner"

	namespacePrefix = "customer-"
	nodePoolName    = "dual-role"
	// maxCustomerIDLength keeps derived Strimzi names such as
	// "<id>-entity-operator" and "<id>-dual-role-<n>" well inside the
	// 63-character Kubernetes name limit.
	maxCustomerIDLength = 30
)

// customerIDPattern is an RFC 1035 label: Strimzi turns the cluster name into
// Service names, which must start with a letter.
var customerIDPattern = regexp.MustCompile(`^[a-z]([-a-z0-9]*[a-z0-9])?$`)

// Spec describes one customer's dedicated Kafka cluster.
type Spec struct {
	// CustomerID is a stable, DNS-safe identifier such as "acme".
	CustomerID string
	// Profile sizes the cluster.
	Profile Profile
}

// Namespace is the customer's own namespace.
func (s Spec) Namespace() string { return namespacePrefix + s.CustomerID }

// ClusterName is the name of the Strimzi Kafka resource.
func (s Spec) ClusterName() string { return s.CustomerID }

// BootstrapAddress is the in-cluster plaintext bootstrap address.
func (s Spec) BootstrapAddress() string {
	return fmt.Sprintf("%s-kafka-bootstrap.%s.svc:9092", s.ClusterName(), s.Namespace())
}

// Validate reports whether the spec can be rendered safely.
func (s Spec) Validate() error {
	if err := ValidateCustomerID(s.CustomerID); err != nil {
		return err
	}
	return s.Profile.validate()
}

// ValidateCustomerID reports whether id can be used to name a customer's resources.
func ValidateCustomerID(id string) error {
	if len(id) == 0 || len(id) > maxCustomerIDLength {
		return fmt.Errorf("customer ID %q must be 1 to %d characters long", id, maxCustomerIDLength)
	}
	if !customerIDPattern.MatchString(id) {
		return fmt.Errorf("customer ID %q must contain only lowercase letters, digits and dashes, start with a letter and end with a letter or digit", id)
	}
	return nil
}

// Storage is the persistent volume claim of each node.
type Storage struct {
	Size  string
	Class string
}

// Profile sizes a customer cluster. Profiles differ only in values; they all
// render through the same template.
type Profile struct {
	Name              string
	Replicas          int
	ReplicationFactor int
	MinInsyncReplicas int
	CPURequest        string
	Memory            string
	Storage           Storage
	// RackTopologyKey enables rack-aware replica placement across zones when set.
	RackTopologyKey string
}

func (p Profile) validate() error {
	switch {
	case p.Name == "":
		return errors.New("profile has no name")
	case p.Replicas < 1:
		return fmt.Errorf("profile %q: replicas must be at least 1", p.Name)
	case p.ReplicationFactor < 1 || p.ReplicationFactor > p.Replicas:
		return fmt.Errorf("profile %q: replication factor must be between 1 and the replica count %d", p.Name, p.Replicas)
	case p.MinInsyncReplicas < 1 || p.MinInsyncReplicas > p.ReplicationFactor:
		return fmt.Errorf("profile %q: min.insync.replicas must be between 1 and the replication factor %d", p.Name, p.ReplicationFactor)
	case p.CPURequest == "" || p.Memory == "":
		return fmt.Errorf("profile %q: CPU request and memory are required", p.Name)
	case p.Storage.Size == "" || p.Storage.Class == "":
		return fmt.Errorf("profile %q: storage needs a size and a storage class", p.Name)
	}
	return nil
}

// DevProfile is a single-node cluster small enough to run several side by
// side on kind, using kind's default "standard" storage class.
var DevProfile = Profile{
	Name:              "dev",
	Replicas:          1,
	ReplicationFactor: 1,
	MinInsyncReplicas: 1,
	CPURequest:        "250m",
	Memory:            "1Gi",
	Storage:           Storage{Size: "5Gi", Class: "standard"},
}

// ProductionProfile follows the durability decisions in docs/PROJECT_PLAN.md:
// three zones, replication factor 3, min.insync.replicas 2, gp3 volumes.
var ProductionProfile = Profile{
	Name:              "production",
	Replicas:          3,
	ReplicationFactor: 3,
	MinInsyncReplicas: 2,
	CPURequest:        "1",
	Memory:            "4Gi",
	Storage:           Storage{Size: "100Gi", Class: "gp3"},
	RackTopologyKey:   "topology.kubernetes.io/zone",
}

// Profiles lists the known profiles by name.
var Profiles = map[string]Profile{
	DevProfile.Name:        DevProfile,
	ProductionProfile.Name: ProductionProfile,
}

// ErrUnknownProfile is returned by ProfileByName for an unknown profile name.
var ErrUnknownProfile = errors.New("unknown profile")

// ProfileByName looks up one of Profiles.
func ProfileByName(name string) (Profile, error) {
	profile, ok := Profiles[name]
	if !ok {
		return Profile{}, fmt.Errorf("%w %q", ErrUnknownProfile, name)
	}
	return profile, nil
}
