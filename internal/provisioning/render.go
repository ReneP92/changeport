package provisioning

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/netip"
	"strconv"
	"text/template"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

//go:embed templates/customer-cluster.yaml.tmpl
var templates embed.FS

// clusterTemplate quotes values that stand alone as YAML scalars with
// "quote": a customer ID such as "true", "no" or "null" would otherwise be
// read as a boolean or null.
var clusterTemplate = template.Must(
	template.New("customer-cluster.yaml.tmpl").
		Option("missingkey=error").
		Funcs(template.FuncMap{"quote": strconv.Quote}).
		ParseFS(templates, "templates/customer-cluster.yaml.tmpl"),
)

type templateValues struct {
	Tenant          string
	Namespace       string
	ClusterName     string
	NodePoolName    string
	ManagedBy       string
	KafkaVersion    string
	MetadataVersion string
	ZoneTopologyKey string
	Profile         Profile
	Platform        Platform

	Domain             string
	BrokerHostTemplate string
	// BootstrapAnnotations and BrokerAnnotations are the load balancer
	// Services' annotations: the platform's, plus the public name that
	// external-dns publishes.
	BootstrapAnnotations   map[string]string
	BrokerAnnotations      map[string]string
	ExternalPort           int
	ExternalListenerSecret string
	AdminUser              string
	AgentUser              string
	ConsoleUser            string
	InternalPort           int
	// ConsoleNamespace, ConsoleServiceAccount and ConsoleSecrets let the
	// console read the Secrets it needs, by name.
	ConsoleNamespace      string
	ConsoleServiceAccount string
	ConsoleSecrets        []string
	ClusterCASecret       string

	// DedicatedNodePool names the customer's NodePool and NodeClass;
	// DedicatedNodeCPULimit caps the NodePool at one node per broker, one for
	// the CDC runtime when there is one, plus one for replacing a node before
	// the old one goes.
	DedicatedNodePool     string
	DedicatedNodeCPULimit int

	TieredStoragePrefix string

	// DeletionProtectionLabel labels the namespace when DeletionProtection
	// is set.
	DeletionProtection      bool
	DeletionProtectionLabel string

	// CDC is the runtime of the customer's Postgres CDC sources; nil when
	// the template renders none.
	CDC *cdcValues
}

// cdcValues are the template's values for the runtime of a customer's
// Postgres CDC sources (docs/cdc.md).
type cdcValues struct {
	Sources              []Source
	PartLabel            string
	Part                 string
	ConnectCluster       string
	ConnectUser          string
	ConnectSourcesSecret string
	Registry             string
	RegistryUser         string
	RegistryPort         int
	RegistryImage        string
	RegistryURL          string
	// PrivateRanges are the address ranges the Connect worker may not reach
	// outside its namespace: everything but the internet.
	PrivateRanges []string
	// KubernetesAPI are the API server's addresses as CIDRs of one address,
	// with their ports, which the Connect worker may reach.
	KubernetesAPI []apiEndpoint
	// RegistryManagementPort serves the registry's health checks.
	RegistryManagementPort int
}

// apiEndpoint is an address of the Kubernetes API server.
type apiEndpoint struct {
	CIDR string
	Port uint16
}

// registryManagementPort is where Apicurio Registry, a Quarkus application,
// serves its health checks.
const registryManagementPort = 9000

// privateRanges are the IPv4 ranges that are not on the internet, which the
// Connect worker's NetworkPolicy keeps it from: the cluster's own networks,
// the cloud's metadata service, and the ranges cdc.Public refuses.
var privateRanges = []string{
	"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12",
	"192.0.0.0/24", "192.0.2.0/24", "192.168.0.0/16", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/3",
}

// RenderYAML renders the customer cluster template for spec.
func RenderYAML(spec Spec) ([]byte, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	var cdc *cdcValues
	nodes := spec.Profile.Replicas + 1
	if spec.CDC() {
		cdc = &cdcValues{
			Sources:                spec.Sources,
			PartLabel:              PartLabel,
			Part:                   PartCDC,
			ConnectCluster:         ConnectCluster,
			ConnectUser:            ConnectUser,
			ConnectSourcesSecret:   ConnectSourcesSecret,
			Registry:               Registry,
			RegistryUser:           RegistryUser,
			RegistryPort:           RegistryPort,
			RegistryImage:          spec.Platform.RegistryImage,
			RegistryURL:            spec.RegistryURL(),
			PrivateRanges:          privateRanges,
			RegistryManagementPort: registryManagementPort,
		}
		for _, endpoint := range spec.KubernetesAPI {
			prefix := netip.PrefixFrom(endpoint.Addr(), endpoint.Addr().BitLen())
			cdc.KubernetesAPI = append(cdc.KubernetesAPI, apiEndpoint{CIDR: prefix.String(), Port: endpoint.Port()})
		}
		// The Connect worker and the registry share a node of their own.
		nodes++
	}
	var buf bytes.Buffer
	err := clusterTemplate.Execute(&buf, templateValues{
		Tenant:          spec.CustomerID,
		Namespace:       spec.Namespace(),
		ClusterName:     spec.ClusterName(),
		NodePoolName:    nodePoolName,
		ManagedBy:       ManagedBy,
		KafkaVersion:    KafkaVersion,
		MetadataVersion: MetadataVersion,
		ZoneTopologyKey: zoneTopologyKey,
		Profile:         spec.Profile,
		Platform:        spec.Platform,

		Domain:                 spec.Domain(),
		BrokerHostTemplate:     spec.BrokerHostTemplate(),
		BootstrapAnnotations:   loadBalancerAnnotations(spec.Platform, spec.BootstrapHost()),
		BrokerAnnotations:      loadBalancerAnnotations(spec.Platform, spec.BrokerHostTemplate()),
		ExternalPort:           ExternalPort,
		ExternalListenerSecret: ExternalListenerSecret,
		AdminUser:              AdminUser,
		AgentUser:              AgentUser,
		ConsoleUser:            ConsoleUser,
		InternalPort:           InternalPort,

		ConsoleNamespace:      ConsoleNamespace,
		ConsoleServiceAccount: ConsoleServiceAccount,
		ConsoleSecrets:        spec.ConsoleSecrets(),
		ClusterCASecret:       spec.ClusterCASecret(),

		DedicatedNodePool:     spec.DedicatedNodePool(),
		DedicatedNodeCPULimit: nodes * spec.Profile.NodeCPUs,

		TieredStoragePrefix: spec.TieredStoragePrefix(),

		DeletionProtection:      spec.DeletionProtection,
		DeletionProtectionLabel: DeletionProtectionLabel,

		CDC: cdc,
	})
	if err != nil {
		return nil, fmt.Errorf("render customer cluster template: %w", err)
	}
	return buf.Bytes(), nil
}

// externalDNSHostname is the annotation external-dns publishes a Service's
// address under.
const externalDNSHostname = "external-dns.alpha.kubernetes.io/hostname"

func loadBalancerAnnotations(platform Platform, hostname string) map[string]string {
	annotations := map[string]string{externalDNSHostname: hostname}
	maps.Copy(annotations, platform.LoadBalancer.Annotations)
	return annotations
}

// Render renders the customer cluster template for spec into Kubernetes
// objects, in the order they must be applied.
func Render(spec Spec) ([]*unstructured.Unstructured, error) {
	rendered, err := RenderYAML(spec)
	if err != nil {
		return nil, err
	}
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(rendered), 4096)
	var objects []*unstructured.Unstructured
	for {
		obj := &unstructured.Unstructured{}
		if err := decoder.Decode(&obj.Object); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode rendered customer cluster template: %w", err)
		}
		if len(obj.Object) == 0 {
			continue
		}
		objects = append(objects, obj)
	}
	return objects, nil
}
