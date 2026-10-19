package provisioning

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"testing"
)

var update = flag.Bool("update", false, "rewrite the golden files in testdata")

func TestRenderYAMLMatchesGoldenFiles(t *testing.T) {
	for _, profile := range []Profile{DevProfile, ProductionProfile} {
		t.Run(profile.Name, func(t *testing.T) {
			got, err := RenderYAML(Spec{CustomerID: "acme", Profile: profile})
			if err != nil {
				t.Fatal(err)
			}
			golden := filepath.Join("testdata", profile.Name+".golden.yaml")
			if *update {
				if err := os.WriteFile(golden, got, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(golden)
			if err != nil {
				t.Fatalf("%v (run go test ./internal/provisioning -update to create it)", err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("rendered YAML differs from %s; if the change is intended, run go test ./internal/provisioning -update\n--- got ---\n%s", golden, got)
			}
		})
	}
}

func TestRenderProducesLabeledObjectsInApplyOrder(t *testing.T) {
	spec := Spec{CustomerID: "acme", Profile: DevProfile}
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
		{CustomerID: "Bad_ID", Profile: DevProfile},
		{CustomerID: "acme", Profile: Profile{}},
	} {
		if _, err := Render(spec); err == nil {
			t.Errorf("Render(%+v) = nil error, want an error", spec)
		}
	}
}
