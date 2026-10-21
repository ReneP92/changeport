package provisioning

import (
	"bytes"
	"flag"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
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
	wantKinds := []string{"Namespace", "KafkaNodePool", "Kafka"}
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
	if got := objects[1].GetLabels()["strimzi.io/cluster"]; got != objects[2].GetName() {
		t.Errorf("node pool belongs to cluster %q, want %q", got, objects[2].GetName())
	}
}

func TestRenderRejectsInvalidSpecs(t *testing.T) {
	for _, spec := range []Spec{
		{CustomerID: "Bad_ID", Profile: DevProfile, Platform: KindPlatform},
		{CustomerID: "acme", Profile: Profile{}, Platform: KindPlatform},
		{CustomerID: "acme", Profile: DevProfile, Platform: Platform{}},
		{CustomerID: "acme", Profile: ProductionProfile, Platform: Platform{Name: "one-zone", StorageClass: "standard", Zones: 1}},
	} {
		if _, err := Render(spec); err == nil {
			t.Errorf("Render(%+v) = nil error, want an error", spec)
		}
	}
}
