package provisioning

import (
	"errors"
	"os"
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
	if p.Zones != 3 {
		t.Errorf("production clusters spread across %d zones; the plan requires 3", p.Zones)
	}
	for name, platform := range Platforms {
		if platform.Zones < p.Zones {
			t.Errorf("platform %q has %d zones and cannot run the production profile", name, platform.Zones)
		}
	}
}

func TestBuiltInPlatformsAreValid(t *testing.T) {
	for name, platform := range Platforms {
		if platform.Name != name {
			t.Errorf("platform registered as %q is named %q", name, platform.Name)
		}
		if err := platform.validate(); err != nil {
			t.Errorf("platform %q: %v", name, err)
		}
	}
}

// TestKindKafkaImageMatchesDevScripts keeps the kind platform's image in step
// with the image the dev scripts build and load into kind.
func TestKindKafkaImageMatchesDevScripts(t *testing.T) {
	lib, err := os.ReadFile("../../hack/lib.sh")
	if err != nil {
		t.Fatal(err)
	}
	want := "\nKAFKA_IMAGE=" + KindPlatform.KafkaImage + "\n"
	if !strings.Contains(string(lib), want) {
		t.Errorf("hack/lib.sh does not set KAFKA_IMAGE=%s, the kind platform's image", KindPlatform.KafkaImage)
	}
}

func TestPlatformValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Platform)
	}{
		{"no name", func(p *Platform) { p.Name = "" }},
		{"no storage class", func(p *Platform) { p.StorageClass = "" }},
		{"no zones", func(p *Platform) { p.Zones = 0 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := KindPlatform
			tt.mutate(&p)
			if err := p.validate(); err == nil {
				t.Error("validate() = nil, want an error")
			}
		})
	}
}

func TestSpecValidateRejectsProfileWithMoreZonesThanPlatform(t *testing.T) {
	spec := Spec{CustomerID: "acme", Profile: ProductionProfile, Platform: Platform{Name: "one-zone", StorageClass: "standard", Zones: 1}}
	err := spec.Validate()
	if err == nil || !strings.Contains(err.Error(), `platform "one-zone" has 1`) {
		t.Errorf("Validate() = %v, want a zone mismatch error", err)
	}
	for _, platform := range Platforms {
		spec.Platform = platform
		if err := spec.Validate(); err != nil {
			t.Errorf("Validate() on %s = %v, want nil", platform.Name, err)
		}
	}
}

func TestPlatformByName(t *testing.T) {
	p, err := PlatformByName("eks-dev")
	if err != nil || p.Name != "eks-dev" {
		t.Fatalf("PlatformByName(eks-dev) = %v, %v", p.Name, err)
	}
	if _, err := PlatformByName("gke"); !errors.Is(err, ErrUnknownPlatform) {
		t.Fatalf("PlatformByName(gke) error = %v, want ErrUnknownPlatform", err)
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
		{"no storage size", func(p *Profile) { p.StorageSize = "" }},
		{"no zones", func(p *Profile) { p.Zones = 0 }},
		{"zones above replicas", func(p *Profile) { p.Zones = 2 }},
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
	spec := Spec{CustomerID: "acme", Profile: DevProfile, Platform: KindPlatform}
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
