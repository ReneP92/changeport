package provisioning

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"io"
	"maps"
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
	// DedicatedNodeCPULimit caps the NodePool at one node per broker plus one
	// for replacing a node before the old one goes.
	DedicatedNodePool     string
	DedicatedNodeCPULimit int

	TieredStoragePrefix string
}

// RenderYAML renders the customer cluster template for spec.
func RenderYAML(spec Spec) ([]byte, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
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
		DedicatedNodeCPULimit: (spec.Profile.Replicas + 1) * spec.Profile.NodeCPUs,

		TieredStoragePrefix: spec.TieredStoragePrefix(),
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
