package provisioning

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"io"
	"text/template"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

//go:embed templates/customer-cluster.yaml.tmpl
var templates embed.FS

var clusterTemplate = template.Must(
	template.New("customer-cluster.yaml.tmpl").
		Option("missingkey=error").
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
	})
	if err != nil {
		return nil, fmt.Errorf("render customer cluster template: %w", err)
	}
	return buf.Bytes(), nil
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
