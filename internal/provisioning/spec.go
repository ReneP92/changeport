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
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
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

	// ExternalPort is the port of the public listener customers connect to.
	ExternalPort = 9094
	// AdminUser is the KafkaUser customers connect as; its Secret holds the
	// SCRAM password.
	AdminUser = "admin"
	// AgentUser is the KafkaUser of the platform's Kafka agent on the internal
	// mTLS listener.
	AgentUser = "changeport-agent"
	// ExternalListenerSecret holds the public listener's certificate, issued by
	// cert-manager for the customer's names.
	ExternalListenerSecret = "external-listener-tls"

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

// BrokerServiceAccount is the service account Strimzi runs the brokers under.
func (s Spec) BrokerServiceAccount() string { return s.ClusterName() + "-kafka" }

// TieredStoragePrefix is the customer's key prefix in the tiered storage
// bucket. It is the namespace, which EKS Pod Identity passes to IAM as the
// kubernetes-namespace session tag, so one IAM policy can limit every
// customer's brokers to their own prefix.
func (s Spec) TieredStoragePrefix() string { return s.Namespace() + "/" }

// DedicatedNodePool names the customer's dedicated EKS Auto Mode NodePool and
// NodeClass, both cluster-scoped.
func (s Spec) DedicatedNodePool() string { return namespacePrefix + s.CustomerID }

// Domain is the DNS zone of the customer's public names, under the platform's
// Kafka domain, such as acme.kafka.changeport.test.
func (s Spec) Domain() string { return s.CustomerID + "." + s.Platform.KafkaDomain }

// BootstrapHost is the public name clients bootstrap from.
func (s Spec) BootstrapHost() string { return "bootstrap." + s.Domain() }

// BrokerHostTemplate is the public name of each broker, with Strimzi's
// {nodeId} placeholder.
func (s Spec) BrokerHostTemplate() string { return "b{nodeId}." + s.Domain() }

// BootstrapAddress is the public bootstrap address customers connect to.
func (s Spec) BootstrapAddress() string {
	return fmt.Sprintf("%s:%d", s.BootstrapHost(), ExternalPort)
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
	// NodeCPUs is the vCPU count of each dedicated node, on platforms with
	// dedicated nodes: one broker per node.
	NodeCPUs int
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
	case p.NodeCPUs < 2:
		// EKS Auto Mode only runs instances with more than one vCPU.
		return fmt.Errorf("profile %q: dedicated nodes need at least 2 vCPUs", p.Name)
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
	NodeCPUs:          2,
}

// DevHAProfile is a three-broker cluster across three zones, as durable as
// production but small enough for kind, to test failover locally.
var DevHAProfile = Profile{
	Name:              "dev-ha",
	Replicas:          3,
	ReplicationFactor: 3,
	MinInsyncReplicas: 2,
	CPURequest:        "250m",
	Memory:            "1Gi",
	StorageSize:       "5Gi",
	Zones:             3,
	NodeCPUs:          2,
}

// ProductionProfile follows the durability decisions in docs/PROJECT_PLAN.md:
// three zones, replication factor 3, min.insync.replicas 2. It is sized for
// the design partner's volume (peak under 1 MB/s, under 50 GB a day) on
// dedicated 2-vCPU, 8 GiB nodes (m7g.large), leaving room on each node for
// the entity operator and the Kafka agent next to the broker.
var ProductionProfile = Profile{
	Name:              "production",
	Replicas:          3,
	ReplicationFactor: 3,
	MinInsyncReplicas: 2,
	CPURequest:        "1500m",
	Memory:            "5Gi",
	StorageSize:       "100Gi",
	Zones:             3,
	NodeCPUs:          2,
}

// Profiles lists the known profiles by name.
var Profiles = map[string]Profile{
	DevProfile.Name:        DevProfile,
	DevHAProfile.Name:      DevHAProfile,
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
	// KafkaDomain is the DNS zone the customers' public names live under, as
	// <customer>.<KafkaDomain>.
	KafkaDomain string
	// CertificateIssuer is the cert-manager ClusterIssuer of the public
	// listener certificates, from the platform's overlay in deploy/.
	CertificateIssuer string
	// LoadBalancer configures the Services of the public listener.
	LoadBalancer LoadBalancer
	// TieredStorage is where brokers offload topic history.
	TieredStorage TieredStorage
	// DedicatedNodes, when set, gives every customer an EKS Auto Mode
	// NodePool of their own: nodes tainted and labelled with the tenant, which
	// only that customer's pods tolerate and select.
	DedicatedNodes *DedicatedNodes
}

// TieredStorage configures Kafka tiered storage on S3: one bucket per
// environment, with a key prefix per customer.
type TieredStorage struct {
	Bucket string
	Region string
	// Endpoint, when set, is an S3-compatible store to use instead of AWS
	// S3, addressed with path-style URLs.
	Endpoint string
	// StaticCredentials are the keys of a local store; brokers on EKS get
	// short-lived credentials from EKS Pod Identity instead.
	StaticCredentials *StaticCredentials
	// PodIdentity, when set, makes the provisioner associate each customer's
	// broker service account with the broker IAM role.
	PodIdentity *PodIdentity
}

// StaticCredentials are an access key pair.
type StaticCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
}

// PodIdentity names the EKS cluster and the IAM role that broker pods assume
// through EKS Pod Identity. Terraform creates both (terraform/modules/eks,
// terraform/modules/iam).
type PodIdentity struct {
	ClusterName string
	RoleName    string
}

func (t TieredStorage) validate() error {
	switch {
	case t.Bucket == "" || t.Region == "":
		return errors.New("tiered storage needs a bucket and a region")
	case (t.StaticCredentials == nil) == (t.PodIdentity == nil):
		return errors.New("tiered storage needs either static credentials or EKS Pod Identity")
	case t.StaticCredentials != nil && (t.StaticCredentials.AccessKeyID == "" || t.StaticCredentials.SecretAccessKey == ""):
		return errors.New("tiered storage static credentials need a key ID and a secret")
	case t.PodIdentity != nil && (t.PodIdentity.ClusterName == "" || t.PodIdentity.RoleName == ""):
		return errors.New("tiered storage Pod Identity needs a cluster name and a role name")
	}
	return nil
}

// DedicatedNodes configures the per-customer EKS Auto Mode NodeClass and
// NodePool. The node role and the tags must match what Terraform creates in
// the environment (terraform/modules/eks, terraform/modules/vpc).
type DedicatedNodes struct {
	// NodeRole is the IAM role of the nodes, with an EC2 access entry.
	NodeRole string
	// SubnetTags select the private subnets nodes run in, one per zone.
	SubnetTags map[string]string
	// SecurityGroupTags select the nodes' security groups.
	SecurityGroupTags map[string]string
	// InstanceFamilies are the EC2 instance families nodes may use.
	InstanceFamilies []string
}

func (d *DedicatedNodes) validate() error {
	switch {
	case d == nil:
		return nil
	case d.NodeRole == "":
		return errors.New("dedicated nodes need a node role")
	case len(d.SubnetTags) == 0:
		return errors.New("dedicated nodes need subnet tags")
	case len(d.SecurityGroupTags) == 0:
		return errors.New("dedicated nodes need security group tags")
	case len(d.InstanceFamilies) == 0:
		return errors.New("dedicated nodes need instance families")
	}
	return nil
}

// LoadBalancer configures the LoadBalancer Services of a platform's public
// listener: one for bootstrap and one per broker.
type LoadBalancer struct {
	// Class is the Services' loadBalancerClass; empty means the cluster default.
	Class string
	// Annotations configure the load balancers, such as their scheme.
	Annotations map[string]string
	// ExternalTrafficPolicy, when set, is the Services' externalTrafficPolicy.
	ExternalTrafficPolicy string
}

func (p Platform) validate() error {
	switch {
	case p.Name == "":
		return errors.New("platform has no name")
	case p.StorageClass == "":
		return fmt.Errorf("platform %q: storage class is required", p.Name)
	case p.Zones < 1:
		return fmt.Errorf("platform %q: zones must be at least 1", p.Name)
	case p.CertificateIssuer == "":
		return fmt.Errorf("platform %q: certificate issuer is required", p.Name)
	}
	if errs := validation.IsDNS1123Subdomain(p.KafkaDomain); len(errs) > 0 {
		return fmt.Errorf("platform %q: Kafka domain %q: %s", p.Name, p.KafkaDomain, strings.Join(errs, "; "))
	}
	if err := p.DedicatedNodes.validate(); err != nil {
		return fmt.Errorf("platform %q: %w", p.Name, err)
	}
	if err := p.TieredStorage.validate(); err != nil {
		return fmt.Errorf("platform %q: %w", p.Name, err)
	}
	return nil
}

// KindPlatform is the local kind cluster from deploy/kind: three workers in
// three zones, with kind's default "standard" storage class. make dev-up
// builds the Kafka image and loads it into the kind nodes, and runs
// cloud-provider-kind for the load balancers. Public names use the reserved
// .test domain and a local CA; the dev scripts resolve them for clients.
var KindPlatform = Platform{
	Name:              "kind",
	StorageClass:      "standard",
	Zones:             3,
	KafkaImage:        "changeport/kafka:local",
	KafkaDomain:       "kafka.changeport.test",
	CertificateIssuer: "local-ca",
	// Like EKS load balancers with IP targets, send each broker's traffic only
	// to the node it runs on, rather than through any node, so that a frozen
	// node only affects its own broker.
	LoadBalancer: LoadBalancer{ExternalTrafficPolicy: "Local"},
	// SeaweedFS from deploy/base/object-store stands in for S3. Its keys only
	// guard a throwaway store inside the local cluster.
	TieredStorage: TieredStorage{
		Bucket:   "changeport-tiered-storage",
		Region:   "us-east-1",
		Endpoint: "http://seaweedfs.object-store.svc:8333",
		StaticCredentials: &StaticCredentials{
			AccessKeyID:     "changeport",
			SecretAccessKey: "changeport-local-only",
		},
	},
}

// eksLoadBalancer is an internet-facing EKS Auto Mode Network Load Balancer
// that sends traffic straight to the broker pods. Its idle timeout outlasts
// the brokers' connections.max.idle.ms (10 minutes), so brokers rather than
// the load balancer close idle connections.
var eksLoadBalancer = LoadBalancer{
	Class: "eks.amazonaws.com/nlb",
	Annotations: map[string]string{
		"service.beta.kubernetes.io/aws-load-balancer-scheme":                                                "internet-facing",
		"service.beta.kubernetes.io/aws-load-balancer-nlb-target-type":                                       "ip",
		fmt.Sprintf("service.beta.kubernetes.io/aws-load-balancer-listener-attributes.TCP-%d", ExternalPort): "tcp.idle_timeout.seconds=900",
	},
}

// eksDedicatedNodes runs customers on on-demand Graviton nodes in the
// environment's private subnets. Terraform tags those subnets and the
// cluster security group with changeport.io/customer-nodes=true.
func eksDedicatedNodes(environment string) *DedicatedNodes {
	return &DedicatedNodes{
		NodeRole:          "changeport-" + environment + "-customer-node",
		SubnetTags:        map[string]string{"changeport.io/customer-nodes": "true"},
		SecurityGroupTags: map[string]string{"changeport.io/customer-nodes": "true"},
		InstanceFamilies:  []string{"m7g", "m8g"},
	}
}

// eksTieredStorage offloads to the environment's bucket in us-east-1, with
// credentials from EKS Pod Identity. Terraform creates the bucket and the
// broker role (terraform/modules/s3, terraform/modules/iam).
func eksTieredStorage(environment string) TieredStorage {
	return TieredStorage{
		Bucket: "changeport-" + environment + "-tiered-storage",
		Region: "us-east-1",
		PodIdentity: &PodIdentity{
			ClusterName: "changeport-" + environment,
			RoleName:    "changeport-" + environment + "-kafka-broker",
		},
	}
}

// EKSDevPlatform is the EKS cluster of the dev AWS account, with gp3 volumes
// and nodes across three availability zones. Its Kafka domain is a reserved
// .invalid placeholder until the domain is registered (#8).
var EKSDevPlatform = Platform{
	Name:              "eks-dev",
	StorageClass:      "gp3",
	Zones:             3,
	KafkaDomain:       "kafka.dev.changeport.invalid",
	CertificateIssuer: "letsencrypt-staging",
	LoadBalancer:      eksLoadBalancer,
	TieredStorage:     eksTieredStorage("dev"),
	DedicatedNodes:    eksDedicatedNodes("dev"),
}

// EKSProdPlatform is the EKS cluster of the prod AWS account, where customers
// run. Its Kafka domain is a reserved .invalid placeholder until the domain is
// registered (#8).
var EKSProdPlatform = Platform{
	Name:              "eks-prod",
	StorageClass:      "gp3",
	Zones:             3,
	KafkaDomain:       "kafka.changeport.invalid",
	CertificateIssuer: "letsencrypt",
	LoadBalancer:      eksLoadBalancer,
	TieredStorage:     eksTieredStorage("prod"),
	DedicatedNodes:    eksDedicatedNodes("prod"),
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
