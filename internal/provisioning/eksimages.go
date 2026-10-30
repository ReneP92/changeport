package provisioning

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// eksImagesJSON lists the platform's own images in ECR by digest, as
// .github/workflows/publish-images.yml last published them from main. The
// workflow proposes every change as a pull request, through cmd/eksimages,
// which also writes deploy/components/eks-images. An image that has not been
// published yet is empty.
//
//go:embed eks-images.json
var eksImagesJSON []byte

// EKSImages are the platform's own images the EKS platforms run.
type EKSImages struct {
	// Kafka is the broker image from images/kafka; empty means Strimzi's own.
	Kafka string `json:"kafka"`
	// Connect is the Kafka Connect image from images/connect, which runs the
	// Postgres CDC source (docs/cdc.md).
	Connect string `json:"connect"`
	// Agent is the Kafka agent's image; empty means no agent.
	Agent string `json:"agent"`
	// RegistryGateway is the registry gateway's image, through which
	// customers reach their schema registry.
	RegistryGateway string `json:"registry-gateway"`
	// Provisioner and Console are the platform components' images, which the
	// EKS overlays set (deploy/components/eks-images).
	Provisioner string `json:"provisioner"`
	Console     string `json:"console"`
}

// ecrImage is an image in the platform's ECR registry, by digest.
var ecrImage = regexp.MustCompile(`^[0-9]{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com/changeport/(kafka|connect|agent|registry-gateway|provisioner|console)@sha256:[0-9a-f]{64}$`)

// ParseEKSImages parses and checks a list of images like eks-images.json:
// every image is empty or its own repository's image by digest.
func ParseEKSImages(data []byte) (EKSImages, error) {
	var images EKSImages
	if err := json.Unmarshal(data, &images); err != nil {
		return EKSImages{}, fmt.Errorf("parse EKS images: %w", err)
	}
	for name, image := range map[string]string{
		"kafka":            images.Kafka,
		"connect":          images.Connect,
		"agent":            images.Agent,
		"registry-gateway": images.RegistryGateway,
		"provisioner":      images.Provisioner,
		"console":          images.Console,
	} {
		if image == "" {
			continue
		}
		match := ecrImage.FindStringSubmatch(image)
		if match == nil || match[1] != name {
			return EKSImages{}, fmt.Errorf("EKS image %s is %q, not changeport/%s in ECR by digest", name, image, name)
		}
	}
	return images, nil
}

// eksImages are the images of eks-images.json, checked by the tests.
var eksImages = func() EKSImages {
	images, err := ParseEKSImages(eksImagesJSON)
	if err != nil {
		panic(err)
	}
	return images
}()

// EKSImagesComponent renders the kustomize Component that sets the platform
// components' images in the EKS overlays (deploy/components/eks-images), with
// the images that have been published.
func EKSImagesComponent(images EKSImages) []byte {
	var b strings.Builder
	b.WriteString(`# Written by go run ./cmd/eksimages from internal/provisioning/eks-images.json:
# the platform components' images in ECR, by digest, as
# .github/workflows/publish-images.yml last published them from main. Both EKS
# overlays include it. Change the images with the command rather than here.
apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
`)
	var entries []string
	for _, c := range []struct{ name, image string }{
		{"changeport/provisioner", images.Provisioner},
		{"changeport/console", images.Console},
	} {
		if c.image == "" {
			continue
		}
		name, digest, _ := strings.Cut(c.image, "@")
		entries = append(entries, fmt.Sprintf("  - name: %s\n    newName: %s\n    digest: %s\n", c.name, name, digest))
	}
	if len(entries) == 0 {
		b.WriteString("images: []\n")
		return []byte(b.String())
	}
	b.WriteString("images:\n")
	for _, entry := range entries {
		b.WriteString(entry)
	}
	return []byte(b.String())
}
