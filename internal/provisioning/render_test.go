package provisioning

import (
	"bytes"
	"flag"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var update = flag.Bool("update", false, "rewrite the golden files in testdata")

// TestRenderYAMLMatchesGoldenFiles renders every platform and profile pair.
// Pairs the platform cannot run, such as a three-zone profile on kind, must
// be rejected; the others must match testdata/<platform>-<profile>.golden.yaml.
func TestRenderYAMLMatchesGoldenFiles(t *testing.T) {
	for _, platform := range sortedValues(Platforms) {
		for _, profile := range sortedValues(Profiles) {
			t.Run(platform.Name+"/"+profile.Name, func(t *testing.T) {
				testGoldenFile(t, Spec{CustomerID: "acme", Profile: profile, Platform: platform})
			})
		}
	}
}

func testGoldenFile(t *testing.T, spec Spec) {
	t.Helper()
	golden := filepath.Join("testdata", spec.Platform.Name+"-"+spec.Profile.Name+".golden.yaml")
	got, err := RenderYAML(spec)
	if spec.Profile.Zones > spec.Platform.Zones {
		if err == nil {
			t.Fatalf("RenderYAML() = nil error for %d zones on a %d-zone platform, want an error", spec.Profile.Zones, spec.Platform.Zones)
		}
		if _, statErr := os.Stat(golden); statErr == nil {
			t.Errorf("%s exists for a pair that cannot render; delete it", golden)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if *update {
		if err := os.WriteFile(golden, got, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("%v (run make golden to create it)", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("rendered YAML differs from %s; if the change is intended, run make golden\n--- got ---\n%s", golden, got)
	}
}

// TestNoStaleGoldenFiles fails for golden files of platforms or profiles that
// no longer exist.
func TestNoStaleGoldenFiles(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "*.golden.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		known := false
		for platform := range Platforms {
			for profile := range Profiles {
				known = known || filepath.Base(file) == platform+"-"+profile+".golden.yaml"
			}
		}
		if !known {
			t.Errorf("%s belongs to no platform and profile pair; delete it", file)
		}
	}
}

func sortedValues[T any](m map[string]T) []T {
	var values []T
	for _, key := range slices.Sorted(maps.Keys(m)) {
		values = append(values, m[key])
	}
	return values
}

func TestRenderProducesLabeledObjectsInApplyOrder(t *testing.T) {
	spec := Spec{CustomerID: "acme", Profile: DevProfile, Platform: KindPlatform}
	objects, err := Render(spec)
	if err != nil {
		t.Fatal(err)
	}
	// The certificate comes before the Kafka cluster that uses it.
	wantKinds := []string{"Namespace", "Certificate", "KafkaNodePool", "Kafka", "KafkaUser", "KafkaUser"}
	if len(objects) != len(wantKinds) {
		t.Fatalf("got %d objects, want %d", len(objects), len(wantKinds))
	}
	for i, obj := range objects {
		if obj.GetKind() != wantKinds[i] {
			t.Errorf("object %d is a %s, want %s", i, obj.GetKind(), wantKinds[i])
		}
		labels := obj.GetLabels()
		if labels[TenantLabel] != "acme" {
			t.Errorf("%s has tenant label %q, want acme", obj.GetKind(), labels[TenantLabel])
		}
		if labels["app.kubernetes.io/managed-by"] != ManagedBy {
			t.Errorf("%s has managed-by label %q, want %s", obj.GetKind(), labels["app.kubernetes.io/managed-by"], ManagedBy)
		}
		if obj.GetKind() != "Namespace" && obj.GetNamespace() != spec.Namespace() {
			t.Errorf("%s is in namespace %q, want %q", obj.GetKind(), obj.GetNamespace(), spec.Namespace())
		}
	}
	if got := objects[0].GetName(); got != spec.Namespace() {
		t.Errorf("namespace is named %q, want %q", got, spec.Namespace())
	}
	for _, i := range []int{2, 4, 5} {
		if got := objects[i].GetLabels()["strimzi.io/cluster"]; got != objects[3].GetName() {
			t.Errorf("%s belongs to cluster %q, want %q", objects[i].GetKind(), got, objects[3].GetName())
		}
	}
}

// TestRenderPublicListener checks the names, certificate and load balancer
// settings that customers depend on, beyond the golden files.
func TestRenderPublicListener(t *testing.T) {
	spec := Spec{CustomerID: "acme", Profile: ProductionProfile, Platform: EKSProdPlatform}
	objects, err := Render(spec)
	if err != nil {
		t.Fatal(err)
	}
	certificate, kafka := objects[1], objects[3]
	dnsNames, _, _ := unstructured.NestedStringSlice(certificate.Object, "spec", "dnsNames")
	if len(dnsNames) != 1 || dnsNames[0] != "*.acme.kafka.changeport.invalid" {
		t.Errorf("certificate covers %q, want the customer's wildcard name", dnsNames)
	}
	if encoding, _, _ := unstructured.NestedString(certificate.Object, "spec", "privateKey", "encoding"); encoding != "PKCS8" {
		t.Errorf("certificate key encoding is %q; Kafka only loads PKCS8 keys", encoding)
	}
	listeners, _, _ := unstructured.NestedSlice(kafka.Object, "spec", "kafka", "listeners")
	var external map[string]any
	for _, l := range listeners {
		listener := l.(map[string]any)
		if listener["name"] == "external" {
			external = listener
		}
		if listener["tls"] != true {
			t.Errorf("listener %v does not use TLS", listener["name"])
		}
	}
	if external == nil {
		t.Fatal("no external listener")
	}
	for _, field := range []struct {
		path []string
		want string
	}{
		{[]string{"authentication", "type"}, "scram-sha-512"},
		{[]string{"configuration", "class"}, "eks.amazonaws.com/nlb"},
		{[]string{"configuration", "brokerCertChainAndKey", "secretName"}, ExternalListenerSecret},
		{[]string{"configuration", "advertisedHostTemplate"}, "b{nodeId}.acme.kafka.changeport.invalid"},
		{[]string{"configuration", "bootstrap", "annotations", "external-dns.alpha.kubernetes.io/hostname"}, "bootstrap.acme.kafka.changeport.invalid"},
		{[]string{"configuration", "perBrokerAnnotationsTemplate", "external-dns.alpha.kubernetes.io/hostname"}, "b{nodeId}.acme.kafka.changeport.invalid"},
		{[]string{"configuration", "perBrokerAnnotationsTemplate", "service.beta.kubernetes.io/aws-load-balancer-scheme"}, "internet-facing"},
	} {
		if got, _, _ := unstructured.NestedString(external, field.path...); got != field.want {
			t.Errorf("external listener %v = %q, want %q", field.path, got, field.want)
		}
	}
}

func TestRenderRejectsInvalidSpecs(t *testing.T) {
	for _, spec := range []Spec{
		{CustomerID: "Bad_ID", Profile: DevProfile, Platform: KindPlatform},
		{CustomerID: "acme", Profile: Profile{}, Platform: KindPlatform},
		{CustomerID: "acme", Profile: DevProfile, Platform: Platform{}},
		{CustomerID: "acme", Profile: ProductionProfile, Platform: oneZonePlatform},
	} {
		if _, err := Render(spec); err == nil {
			t.Errorf("Render(%+v) = nil error, want an error", spec)
		}
	}
}
