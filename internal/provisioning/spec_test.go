package provisioning

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateCustomerID(t *testing.T) {
	tests := []struct {
		id    string
		valid bool
	}{
		{"acme", true},
		{"a", true},
		{"acme-2", true},
		{"a1b2", true},
		{strings.Repeat("a", maxCustomerIDLength), true},
		{"", false},
		{strings.Repeat("a", maxCustomerIDLength+1), false},
		{"Acme", false},
		{"1acme", false},
		{"acme-", false},
		{"-acme", false},
		{"ac_me", false},
		{"ac.me", false},
		{"acme\nkind: Secret", false},
	}
	for _, tt := range tests {
		err := ValidateCustomerID(tt.id)
		if tt.valid && err != nil {
			t.Errorf("ValidateCustomerID(%q) = %v, want nil", tt.id, err)
		}
		if !tt.valid && err == nil {
			t.Errorf("ValidateCustomerID(%q) = nil, want an error", tt.id)
		}
	}
}

func TestBuiltInProfilesAreValid(t *testing.T) {
	for name, profile := range Profiles {
		if profile.Name != name {
			t.Errorf("profile registered as %q is named %q", name, profile.Name)
		}
		if err := profile.validate(); err != nil {
			t.Errorf("profile %q: %v", name, err)
		}
	}
}

func TestProductionProfileFollowsDurabilityDecisions(t *testing.T) {
	p := ProductionProfile
	if p.ReplicationFactor != 3 || p.MinInsyncReplicas != 2 {
		t.Errorf("replication factor %d, min.insync.replicas %d; the plan requires 3 and 2", p.ReplicationFactor, p.MinInsyncReplicas)
	}
	if p.RackTopologyKey == "" {
		t.Error("production clusters must spread replicas across zones")
	}
}

func TestProfileValidate(t *testing.T) {
	valid := DevProfile
	tests := []struct {
		name   string
		mutate func(*Profile)
	}{
		{"no name", func(p *Profile) { p.Name = "" }},
		{"no replicas", func(p *Profile) { p.Replicas = 0 }},
		{"replication factor above replicas", func(p *Profile) { p.ReplicationFactor = 2 }},
		{"zero replication factor", func(p *Profile) { p.ReplicationFactor = 0 }},
		{"min ISR above replication factor", func(p *Profile) { p.MinInsyncReplicas = 2 }},
		{"zero min ISR", func(p *Profile) { p.MinInsyncReplicas = 0 }},
		{"no memory", func(p *Profile) { p.Memory = "" }},
		{"no CPU request", func(p *Profile) { p.CPURequest = "" }},
		{"storage without size", func(p *Profile) { p.Storage = Storage{Class: "gp3"} }},
		{"storage without class", func(p *Profile) { p.Storage = Storage{Size: "10Gi"} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := valid
			tt.mutate(&p)
			if err := p.validate(); err == nil {
				t.Error("validate() = nil, want an error")
			}
		})
	}
}

func TestProfileByName(t *testing.T) {
	p, err := ProfileByName("production")
	if err != nil || p.Name != "production" {
		t.Fatalf("ProfileByName(production) = %v, %v", p.Name, err)
	}
	if _, err := ProfileByName("huge"); !errors.Is(err, ErrUnknownProfile) {
		t.Fatalf("ProfileByName(huge) error = %v, want ErrUnknownProfile", err)
	}
}

func TestSpecNames(t *testing.T) {
	spec := Spec{CustomerID: "acme", Profile: DevProfile}
	if got, want := spec.Namespace(), "customer-acme"; got != want {
		t.Errorf("Namespace() = %q, want %q", got, want)
	}
	if got, want := spec.ClusterName(), "acme"; got != want {
		t.Errorf("ClusterName() = %q, want %q", got, want)
	}
	if got, want := spec.BootstrapAddress(), "acme-kafka-bootstrap.customer-acme.svc:9092"; got != want {
		t.Errorf("BootstrapAddress() = %q, want %q", got, want)
	}
}
