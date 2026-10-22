package main

import (
	"bytes"
	"errors"
	"flag"
	"strings"
	"testing"
)

func TestRenderCommandPrintsCustomerResources(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{"render", "-customer", "acme", "-profile", "production", "-platform", "eks-prod"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run() = %v, stderr: %s", err, stderr.String())
	}
	for _, want := range []string{"name: customer-acme", "kind: Kafka\n", "class: \"gp3\"", "replicas: 3"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("output does not contain %q:\n%s", want, stdout.String())
		}
	}
}

func TestRenderUsesImageOverrides(t *testing.T) {
	var stdout, stderr bytes.Buffer
	args := []string{"render", "-customer", "acme", "-kafka-image", "changeport/kafka:0123456789ab", "-agent-image", "changeport/agent:ba9876543210"}
	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run() = %v, stderr: %s", err, stderr.String())
	}
	for _, want := range []string{"image: changeport/kafka:0123456789ab", "image: changeport/agent:ba9876543210"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("output does not contain %q", want)
		}
	}
}

func TestCommandErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"unknown command", []string{"explode"}, `unknown command "explode"`},
		{"invalid customer", []string{"render", "-customer", "Acme"}, `customer ID "Acme"`},
		{"missing customer", []string{"provision"}, `customer ID ""`},
		{"unknown profile", []string{"provision", "-customer", "acme", "-profile", "huge"}, `unknown profile "huge"`},
		{"unknown platform", []string{"render", "-customer", "acme", "-platform", "gke"}, `unknown platform "gke"`},
		{"deprovision invalid customer", []string{"deprovision", "-customer", "a_b"}, `customer ID "a_b"`},
		{"credentials invalid customer", []string{"credentials", "-customer", "A"}, `customer ID "A"`},
		{"credentials unknown platform", []string{"credentials", "-customer", "acme", "-platform", "gke"}, `unknown platform "gke"`},
		{"controller unknown platform", []string{"controller", "-platform", "gke"}, `unknown platform "gke"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := run(t.Context(), tt.args, &stdout, &stderr)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("run(%v) = %v, want an error containing %q", tt.args, err, tt.want)
			}
		})
	}
}

func TestNoArgumentsPrintsUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), nil, &stdout, &stderr)
	if !errors.Is(err, flag.ErrHelp) || !strings.Contains(stderr.String(), "Usage: provisioner") {
		t.Errorf("run() = %v, stderr %q; want flag.ErrHelp and usage", err, stderr.String())
	}
}
