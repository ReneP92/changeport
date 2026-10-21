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
	// zoneTopologyKey is the well-known node label holding the availability zone.
	zoneTopologyKey = "topology.kubernetes.io/zone"
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
	// Platform is where the cluster runs.
	Platform Platform
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
	if err := s.Profile.validate(); err != nil {
		return err
	}
	if err := s.Platform.validate(); err != nil {
		return err
	}
	if s.Profile.Zones > s.Platform.Zones {
		return fmt.Errorf("profile %q spreads brokers across %d zones, but platform %q has %d", s.Profile.Name, s.Profile.Zones, s.Platform.Name, s.Platform.Zones)
	}
	return nil
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

// Profile sizes a customer cluster. Profiles differ only in values; they all
// render through the same template, on any Platform with enough zones.
type Profile struct {
	Name              string
	Replicas          int
	ReplicationFactor int
	MinInsyncReplicas int
	CPURequest        string
	Memory            string
	// StorageSize is the size of each node's persistent volume.
	StorageSize string
	// Zones is the number of availability zones the brokers spread across.
	// Above 1, the template enables rack-aware replica placement.
	Zones int
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
	case p.StorageSize == "":
		return fmt.Errorf("profile %q: storage size is required", p.Name)
	case p.Zones < 1 || p.Zones > p.Replicas:
		return fmt.Errorf("profile %q: zones must be between 1 and the replica count %d", p.Name, p.Replicas)
	}
	return nil
}

// DevProfile is a single-node cluster small enough to run several side by
// side on kind.
var DevProfile = Profile{
	Name:              "dev",
	Replicas:          1,
	ReplicationFactor: 1,
	MinInsyncReplicas: 1,
	CPURequest:        "250m",
	Memory:            "1Gi",
	StorageSize:       "5Gi",
	Zones:             1,
}

// ProductionProfile follows the durability decisions in docs/PROJECT_PLAN.md:
// three zones, replication factor 3, min.insync.replicas 2.
var ProductionProfile = Profile{
	Name:              "production",
	Replicas:          3,
	ReplicationFactor: 3,
	MinInsyncReplicas: 2,
	CPURequest:        "1",
	Memory:            "4Gi",
	StorageSize:       "100Gi",
	Zones:             3,
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

// Platform describes where customer clusters run: kind on a laptop, or one of
// the EKS environments. Platforms differ only in values; they all render
// through the same template.
type Platform struct {
	Name string
	// StorageClass provisions each node's persistent volume.
	StorageClass string
	// Zones is the number of availability zones the platform's nodes span.
	Zones int
	// KafkaImage is the broker image: the Changeport Kafka image from
	// images/kafka, which adds the tiered storage plugin to Strimzi's image.
	// Empty means Strimzi's own image for KafkaVersion.
	KafkaImage string
}

func (p Platform) validate() error {
	switch {
	case p.Name == "":
		return errors.New("platform has no name")
	case p.StorageClass == "":
		return fmt.Errorf("platform %q: storage class is required", p.Name)
	case p.Zones < 1:
		return fmt.Errorf("platform %q: zones must be at least 1", p.Name)
	}
	return nil
}

// KindPlatform is the local kind cluster from deploy/kind: three workers in
// three zones, with kind's default "standard" storage class. make dev-up
// builds the Kafka image and loads it into the kind nodes.
var KindPlatform = Platform{
	Name:         "kind",
	StorageClass: "standard",
	Zones:        3,
	KafkaImage:   "changeport/kafka:local",
}

// EKSDevPlatform is the EKS cluster of the dev AWS account, with gp3 volumes
// and nodes across three availability zones.
var EKSDevPlatform = Platform{
	Name:         "eks-dev",
	StorageClass: "gp3",
	Zones:        3,
}

// EKSProdPlatform is the EKS cluster of the prod AWS account, where customers
// run.
var EKSProdPlatform = Platform{
	Name:         "eks-prod",
	StorageClass: "gp3",
	Zones:        3,
}

// Platforms lists the known platforms by name.
var Platforms = map[string]Platform{
	KindPlatform.Name:    KindPlatform,
	EKSDevPlatform.Name:  EKSDevPlatform,
	EKSProdPlatform.Name: EKSProdPlatform,
}

// ErrUnknownPlatform is returned by PlatformByName for an unknown platform name.
var ErrUnknownPlatform = errors.New("unknown platform")

// PlatformByName looks up one of Platforms.
func PlatformByName(name string) (Platform, error) {
	platform, ok := Platforms[name]
	if !ok {
		return Platform{}, fmt.Errorf("%w %q", ErrUnknownPlatform, name)
	}
	return platform, nil
}
