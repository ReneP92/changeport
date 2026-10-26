package provisioning

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	testRegistry = "123456789012.dkr.ecr.us-east-1.amazonaws.com"
	testDigest   = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

func TestParseEKSImages(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		wantErr string
	}{
		{"none published", `{"kafka": "", "agent": "", "provisioner": "", "console": ""}`, ""},
		{"by digest", `{"kafka": "` + testRegistry + `/changeport/kafka@` + testDigest + `"}`, ""},
		{"by tag", `{"agent": "` + testRegistry + `/changeport/agent:abc123"}`, "not changeport/agent in ECR by digest"},
		{"another repository", `{"kafka": "` + testRegistry + `/changeport/agent@` + testDigest + `"}`, "not changeport/kafka"},
		{"another registry", `{"console": "quay.io/changeport/console@` + testDigest + `"}`, "not changeport/console"},
		{"not JSON", `kafka: x`, "parse EKS images"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseEKSImages([]byte(tt.json))
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("ParseEKSImages() = %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("ParseEKSImages() = %v, want an error containing %q", err, tt.wantErr)
			}
		})
	}
}

// The overlays' component must say what eks-images.json says; cmd/eksimages
// writes both.
func TestEKSImagesComponentIsCurrent(t *testing.T) {
	got, err := os.ReadFile(filepath.Join("..", "..", "deploy", "components", "eks-images", "kustomization.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if want := EKSImagesComponent(eksImages); string(got) != string(want) {
		t.Errorf("deploy/components/eks-images is not what eks-images.json says; run go run ./cmd/eksimages\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestEKSImagesComponent(t *testing.T) {
	got := string(EKSImagesComponent(EKSImages{Provisioner: testRegistry + "/changeport/provisioner@" + testDigest}))
	want := "images:\n  - name: changeport/provisioner\n    newName: " + testRegistry + "/changeport/provisioner\n    digest: " + testDigest + "\n"
	if !strings.HasSuffix(got, want) {
		t.Errorf("EKSImagesComponent() =\n%s\nwant it to end with\n%s", got, want)
	}
}

func TestEKSPlatformsUseThePublishedImages(t *testing.T) {
	for _, platform := range []Platform{EKSDevPlatform, EKSProdPlatform} {
		if platform.KafkaImage != eksImages.Kafka || platform.AgentImage != eksImages.Agent {
			t.Errorf("%s does not run the images of eks-images.json", platform.Name)
		}
	}
}
