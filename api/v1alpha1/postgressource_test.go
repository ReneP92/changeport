package v1alpha1_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/cdc"
	"github.com/ReneP92/changeport/internal/topics"
)

// specSchema is the generated schema of a PostgresSource's spec.
func specSchema(t *testing.T) apiextensionsv1.JSONSchemaProps {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "deploy", "base", "provisioner", "changeport.io_postgressources.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatal(err)
	}
	return crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"]
}

func enum(t *testing.T, schema apiextensionsv1.JSONSchemaProps) []string {
	t.Helper()
	values := make([]string, len(schema.Enum))
	for i, value := range schema.Enum {
		var v any
		if err := json.Unmarshal(value.Raw, &v); err != nil {
			t.Fatal(err)
		}
		switch v := v.(type) {
		case string:
			values[i] = v
		case float64:
			values[i] = strconv.FormatInt(int64(v), 10)
		}
	}
	return values
}

// The CRD's limits are internal/cdc's and the topic policy's, which the
// console applies before it creates a source.
func TestPostgresSourceSchemaMatchesTheGuardrails(t *testing.T) {
	spec := specSchema(t)

	var providers []string
	for _, p := range cdc.Providers {
		providers = append(providers, string(p))
	}
	if got := enum(t, spec.Properties["provider"]); !slices.Equal(got, providers) {
		t.Errorf("provider enum = %v, want cdc.Providers %v", got, providers)
	}
	for _, provider := range []string{v1alpha1.ProviderGeneric, v1alpha1.ProviderRDS, v1alpha1.ProviderCloudSQL, v1alpha1.ProviderAzure, v1alpha1.ProviderSupabase, v1alpha1.ProviderNeon} {
		if !cdc.Provider(provider).Valid() {
			t.Errorf("v1alpha1 provider %q is not one of internal/cdc's", provider)
		}
	}

	var presets []string
	for _, preset := range topics.RetentionPresets {
		presets = append(presets, strconv.FormatInt(preset.Duration.Milliseconds(), 10))
	}
	if got := enum(t, spec.Properties["retentionMs"]); !slices.Equal(got, presets) {
		t.Errorf("retentionMs enum = %v, want the topic presets %v", got, presets)
	}
	if got := string(spec.Properties["retentionMs"].Default.Raw); got != strconv.FormatInt(topics.DefaultRetention.Milliseconds(), 10) {
		t.Errorf("retentionMs default = %s, want the topics' default", got)
	}

	if got := *spec.Properties["tables"].MaxItems; got != cdc.MaxTables {
		t.Errorf("tables maxItems = %d, want cdc.MaxTables %d", got, cdc.MaxTables)
	}
	if got := enum(t, spec.Properties["connection"].Properties["tls"].Properties["mode"]); !slices.Equal(got, []string{string(cdc.VerifyFull), string(cdc.VerifyCA)}) {
		t.Errorf("TLS mode enum = %v, want internal/cdc's", got)
	}
	if v1alpha1.TLSVerifyFull != string(cdc.VerifyFull) || v1alpha1.TLSVerifyCA != string(cdc.VerifyCA) {
		t.Error("v1alpha1's TLS modes differ from internal/cdc's")
	}
}
