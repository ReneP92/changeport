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

// TestKindImagesMatchDevScripts keeps the kind platform's images in step
// with the images the dev scripts build and load into kind.
func TestKindImagesMatchDevScripts(t *testing.T) {
	lib, err := os.ReadFile("../../hack/lib.sh")
	if err != nil {
		t.Fatal(err)
	}
	for variable, image := range map[string]string{"KAFKA_IMAGE": KindPlatform.KafkaImage, "AGENT_IMAGE": KindPlatform.AgentImage} {
		if !strings.Contains(string(lib), "\n"+variable+"="+image+"\n") {
			t.Errorf("hack/lib.sh does not set %s=%s, the kind platform's image", variable, image)
		}
	}
}

// TestKindTieredStorageMatchesTheObjectStore keeps the kind platform's bucket
// and keys in step with the local object store in deploy/base/object-store.
func TestKindTieredStorageMatchesTheObjectStore(t *testing.T) {
	manifest, err := os.ReadFile("../../deploy/base/object-store/seaweedfs.yaml")
	if err != nil {
		t.Fatal(err)
	}
	storage := KindPlatform.TieredStorage
	for _, want := range []string{
		"AWS_ACCESS_KEY_ID: " + storage.StaticCredentials.AccessKeyID + "\n",
		"AWS_SECRET_ACCESS_KEY: " + storage.StaticCredentials.SecretAccessKey + "\n",
		"value: " + storage.Bucket + "\n",
	} {
		if !strings.Contains(string(manifest), want) {
			t.Errorf("deploy/base/object-store/seaweedfs.yaml lacks %q from the kind platform", strings.TrimSpace(want))
		}
	}
	if !strings.HasPrefix(storage.Endpoint, "http://seaweedfs.object-store.svc:8333") {
		t.Errorf("kind tiered storage endpoint %q is not the object store's Service", storage.Endpoint)
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
		{"no rack label", func(p *Platform) { p.RackLabel = "" }},
		{"no certificate issuer", func(p *Platform) { p.CertificateIssuer = "" }},
		{"no Kafka domain", func(p *Platform) { p.KafkaDomain = "" }},
		{"invalid Kafka domain", func(p *Platform) { p.KafkaDomain = "Kafka_Example.com" }},
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

// oneZonePlatform is kind with a single zone.
var oneZonePlatform = func() Platform {
	p := KindPlatform
	p.Name, p.Zones = "one-zone", 1
	return p
}()

func TestSpecValidateRejectsProfileWithMoreZonesThanPlatform(t *testing.T) {
	spec := Spec{CustomerID: "acme", Profile: ProductionProfile, Platform: oneZonePlatform}
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

func TestDedicatedNodesValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*DedicatedNodes)
	}{
		{"no node role", func(d *DedicatedNodes) { d.NodeRole = "" }},
		{"no subnet tags", func(d *DedicatedNodes) { d.SubnetTags = nil }},
		{"no security group tags", func(d *DedicatedNodes) { d.SecurityGroupTags = nil }},
		{"no instance families", func(d *DedicatedNodes) { d.InstanceFamilies = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := EKSDevPlatform
			nodes := *p.DedicatedNodes
			tt.mutate(&nodes)
			p.DedicatedNodes = &nodes
			if err := p.validate(); err == nil {
				t.Error("validate() = nil, want an error")
			}
		})
	}
}

func TestTieredStorageValidate(t *testing.T) {
	tests := []struct {
		name     string
		platform Platform
		mutate   func(*TieredStorage)
	}{
		{"no bucket", KindPlatform, func(s *TieredStorage) { s.Bucket = "" }},
		{"no region", KindPlatform, func(s *TieredStorage) { s.Region = "" }},
		{"no credentials", KindPlatform, func(s *TieredStorage) { s.StaticCredentials = nil }},
		{"empty static secret", KindPlatform, func(s *TieredStorage) { s.StaticCredentials = &StaticCredentials{AccessKeyID: "a"} }},
		{"two kinds of credentials", EKSDevPlatform, func(s *TieredStorage) {
			s.StaticCredentials = &StaticCredentials{AccessKeyID: "a", SecretAccessKey: "b"}
		}},
		{"Pod Identity without role", EKSDevPlatform, func(s *TieredStorage) { s.PodIdentity = &PodIdentity{ClusterName: "c"} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.platform
			tt.mutate(&p.TieredStorage)
			if err := p.validate(); err == nil {
				t.Error("validate() = nil, want an error")
			}
		})
	}
}

func TestDevHAProfileIsAsDurableAsProduction(t *testing.T) {
	ha, prod := DevHAProfile, ProductionProfile
	if ha.Replicas != prod.Replicas || ha.ReplicationFactor != prod.ReplicationFactor ||
		ha.MinInsyncReplicas != prod.MinInsyncReplicas || ha.Zones != prod.Zones {
		t.Errorf("dev-ha %+v does not match production's durability %+v", ha, prod)
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
		{"one-vCPU nodes", func(p *Profile) { p.NodeCPUs = 1 }},
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
	if got, want := spec.BootstrapAddress(), "bootstrap.acme.kafka.changeport.test:9094"; got != want {
		t.Errorf("BootstrapAddress() = %q, want %q", got, want)
	}
	if got, want := spec.BrokerHostTemplate(), "b{nodeId}.acme.kafka.changeport.test"; got != want {
		t.Errorf("BrokerHostTemplate() = %q, want %q", got, want)
	}
}

// Customers set client.rack from their own account's view of the zones, so
// on EKS a broker's rack must be the zone ID, which is the same in every
// account, and not the zone name, which accounts map differently.
func TestEKSRacksAreZoneIDs(t *testing.T) {
	for _, platform := range []Platform{EKSDevPlatform, EKSProdPlatform} {
		if platform.RackLabel != "topology.k8s.aws/zone-id" {
			t.Errorf("%s racks brokers by %s, want topology.k8s.aws/zone-id", platform.Name, platform.RackLabel)
		}
	}
}
