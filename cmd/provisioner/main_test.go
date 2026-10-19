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
	err := run(t.Context(), []string{"render", "-customer", "acme", "-profile", "production"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run() = %v, stderr: %s", err, stderr.String())
	}
	for _, want := range []string{"name: customer-acme", "kind: Kafka\n", "class: \"gp3\""} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("output does not contain %q:\n%s", want, stdout.String())
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
		{"deprovision invalid customer", []string{"deprovision", "-customer", "a_b"}, `customer ID "a_b"`},
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
