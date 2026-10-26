// Command eksimages changes the platform's own images the EKS platforms run:
// it writes internal/provisioning/eks-images.json, which the EKS platforms of
// internal/provisioning embed, and the kustomize Component that sets the
// platform components' images in the EKS overlays. The image workflow
// (.github/workflows/publish-images.yml) runs it after publishing to ECR.
//
// Usage: go run ./cmd/eksimages <image>=<registry>/changeport/<image>@sha256:<digest> ...
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/ReneP92/changeport/internal/provisioning"
)

const (
	imagesFile    = "internal/provisioning/eks-images.json"
	componentFile = "deploy/components/eks-images/kustomization.yaml"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("name at least one image as <image>=<reference>")
	}
	data, err := os.ReadFile(imagesFile)
	if err != nil {
		return err
	}
	images, err := provisioning.ParseEKSImages(data)
	if err != nil {
		return err
	}
	for _, arg := range args {
		name, reference, ok := strings.Cut(arg, "=")
		if !ok {
			return fmt.Errorf("%q is not <image>=<reference>", arg)
		}
		switch name {
		case "kafka":
			images.Kafka = reference
		case "agent":
			images.Agent = reference
		case "provisioner":
			images.Provisioner = reference
		case "console":
			images.Console = reference
		default:
			return fmt.Errorf("unknown image %q: kafka, agent, provisioner or console", name)
		}
	}
	data, err = json.MarshalIndent(images, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	// Checks the result the way the platforms will read it.
	if _, err := provisioning.ParseEKSImages(data); err != nil {
		return err
	}
	if err := os.WriteFile(imagesFile, data, 0o644); err != nil { //nolint:gosec // G306: a source file
		return err
	}
	return os.WriteFile(componentFile, provisioning.EKSImagesComponent(images), 0o644) //nolint:gosec // G306: a source file
}
