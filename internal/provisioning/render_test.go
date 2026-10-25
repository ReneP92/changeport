package provisioning

import (
	"bytes"
	"flag"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var update = flag.Bool("update", false, "rewrite the golden files in testdata")

// TestRenderYAMLMatchesGoldenFiles renders every platform and profile pair.
// Pairs the platform cannot run, such as a three-zone profile on kind, must
// be rejected; the others must match testdata/<platform>-<profile>.golden.yaml.
func TestRenderYAMLMatchesGoldenFiles(t *testing.T) {
	for _, platform := range sortedValues(Platforms) {
		for _, profile := range sortedValues(Profiles) {
			t.Run(platform.Name+"/"+profile.Name, func(t *testing.T) {
				// Production customers are protected, as the CLI and the
				// console create them by default.
				testGoldenFile(t, Spec{
					CustomerID: "acme", Profile: profile, Platform: platform,
					DeletionProtection: profile.Name == ProductionProfile.Name,
				})
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
		t.Fatalf("%v (run just golden to create it)", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("rendered YAML differs from %s; if the change is intended, run just golden\n--- got ---\n%s", golden, got)
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
	// The tiered storage keys and the certificate come before the Kafka
	// cluster that uses them.
	wantKinds := []string{"Namespace", "Secret", "Certificate", "KafkaNodePool", "Kafka", "KafkaUser", "KafkaUser", "KafkaUser", "Role", "RoleBinding", "Deployment"}
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
	for _, i := range []int{3, 5, 6, 7} {
		if got := objects[i].GetLabels()["strimzi.io/cluster"]; got != objects[4].GetName() {
			t.Errorf("%s belongs to cluster %q, want %q", objects[i].GetKind(), got, objects[4].GetName())
		}
	}
}

// The platform's own users can never read or write customers' messages: only
// the customer's admin user may.
func TestRenderPlatformUsersCannotTouchMessages(t *testing.T) {
	objects, err := Render(Spec{CustomerID: "acme", Profile: DevProfile, Platform: KindPlatform})
	if err != nil {
		t.Fatal(err)
	}
	platformUsers := map[string]bool{AgentUser: true, ConsoleUser: true}
	checked := 0
	for _, obj := range objects {
		if obj.GetKind() != "KafkaUser" || !platformUsers[obj.GetName()] {
			continue
		}
		checked++
		acls, _, err := unstructured.NestedSlice(obj.Object, "spec", "authorization", "acls")
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range acls {
			acl, _ := a.(map[string]any)
			operations, _, _ := unstructured.NestedStringSlice(acl, "operations")
			for _, operation := range operations {
				switch operation {
				case "Read", "Write", "All", "IdempotentWrite", "Alter", "AlterConfigs":
					resource, _, _ := unstructured.NestedString(acl, "resource", "type")
					if operation == "AlterConfigs" && obj.GetName() == AgentUser && (resource == "topic" || resource == "cluster") {
						// The agent turns on tiered storage, a topic setting,
						// and keeps the client metrics subscription in place,
						// which Kafka authorizes as a cluster setting.
						continue
					}
					t.Errorf("KafkaUser %s may %s %ss", obj.GetName(), operation, resource)
				}
			}
		}
	}
	if checked != len(platformUsers) {
		t.Errorf("checked %d platform users, want %d", checked, len(platformUsers))
	}
}

// The console may read exactly the Secrets it needs, by name, and the pods.
func TestRenderConsoleSecretAccess(t *testing.T) {
	spec := Spec{CustomerID: "acme", Profile: DevProfile, Platform: KindPlatform}
	objects, err := Render(spec)
	if err != nil {
		t.Fatal(err)
	}
	for _, obj := range objects {
		switch obj.GetKind() {
		case "Role":
			rules, _, _ := unstructured.NestedSlice(obj.Object, "rules")
			if len(rules) != 2 {
				t.Fatalf("Role %s has %d rules, want 2", obj.GetName(), len(rules))
			}
			rule := func(i int) (resources, names, verbs string) {
				r, _ := rules[i].(map[string]any)
				res, _, _ := unstructured.NestedStringSlice(r, "resources")
				n, _, _ := unstructured.NestedStringSlice(r, "resourceNames")
				v, _, _ := unstructured.NestedStringSlice(r, "verbs")
				return strings.Join(res, ","), strings.Join(n, ","), strings.Join(v, ",")
			}
			if res, names, verbs := rule(0); res != "secrets" || verbs != "get" || names != "changeport-console,acme-cluster-ca-cert,admin,external-listener-tls" {
				t.Errorf("Role %s allows %s on %s named %s, want get on the console's secrets by name", obj.GetName(), verbs, res, names)
			}
			if res, names, verbs := rule(1); res != "pods" || verbs != "get,list" || names != "" {
				t.Errorf("Role %s allows %s on %s named %q, want get and list on pods", obj.GetName(), verbs, res, names)
			}
		case "RoleBinding":
			subjects, _, _ := unstructured.NestedSlice(obj.Object, "subjects")
			subject, _ := subjects[0].(map[string]any)
			if len(subjects) != 1 || subject["name"] != ConsoleServiceAccount || subject["namespace"] != ConsoleNamespace {
				t.Errorf("RoleBinding %s binds %v, want only %s/%s", obj.GetName(), subjects, ConsoleNamespace, ConsoleServiceAccount)
			}
		}
	}
}

// TestRenderPublicListener checks the names, certificate and load balancer
// settings that customers depend on, beyond the golden files.
func TestRenderPublicListener(t *testing.T) {
	spec := Spec{CustomerID: "acme", Profile: ProductionProfile, Platform: EKSProdPlatform}
	objects, err := Render(spec)
	if err != nil {
		t.Fatal(err)
	}
	certificate, kafka := objectOfKind(t, objects, "Certificate"), objectOfKind(t, objects, "Kafka")
	dnsNames, _, _ := unstructured.NestedStringSlice(certificate.Object, "spec", "dnsNames")
	if len(dnsNames) != 1 || dnsNames[0] != "*.acme.kafka.changeport.invalid" {
		t.Errorf("certificate covers %q, want the customer's wildcard name", dnsNames)
	}
	if encoding, _, _ := unstructured.NestedString(certificate.Object, "spec", "privateKey", "encoding"); encoding != "PKCS8" {
		t.Errorf("certificate key encoding is %q; Kafka only loads PKCS8 keys", encoding)
	}
	listeners, _, _ := unstructured.NestedSlice(kafka.Object, "spec", "kafka", "listeners")
	var external map[string]any
	for _, l := range listeners {
		listener := l.(map[string]any)
		if listener["name"] == "external" {
			external = listener
		}
		if listener["tls"] != true {
			t.Errorf("listener %v does not use TLS", listener["name"])
		}
	}
	if external == nil {
		t.Fatal("no external listener")
	}
	for _, field := range []struct {
		path []string
		want string
	}{
		{[]string{"authentication", "type"}, "scram-sha-512"},
		{[]string{"configuration", "class"}, "eks.amazonaws.com/nlb"},
		{[]string{"configuration", "brokerCertChainAndKey", "secretName"}, ExternalListenerSecret},
		{[]string{"configuration", "advertisedHostTemplate"}, "b{nodeId}.acme.kafka.changeport.invalid"},
		{[]string{"configuration", "bootstrap", "annotations", "external-dns.alpha.kubernetes.io/hostname"}, "bootstrap.acme.kafka.changeport.invalid"},
		{[]string{"configuration", "perBrokerAnnotationsTemplate", "external-dns.alpha.kubernetes.io/hostname"}, "b{nodeId}.acme.kafka.changeport.invalid"},
		{[]string{"configuration", "perBrokerAnnotationsTemplate", "service.beta.kubernetes.io/aws-load-balancer-scheme"}, "internet-facing"},
	} {
		if got, _, _ := unstructured.NestedString(external, field.path...); got != field.want {
			t.Errorf("external listener %v = %q, want %q", field.path, got, field.want)
		}
	}
}

func TestRenderRejectsInvalidSpecs(t *testing.T) {
	for _, spec := range []Spec{
		{CustomerID: "Bad_ID", Profile: DevProfile, Platform: KindPlatform},
		{CustomerID: "acme", Profile: Profile{}, Platform: KindPlatform},
		{CustomerID: "acme", Profile: DevProfile, Platform: Platform{}},
		{CustomerID: "acme", Profile: ProductionProfile, Platform: oneZonePlatform},
	} {
		if _, err := Render(spec); err == nil {
			t.Errorf("Render(%+v) = nil error, want an error", spec)
		}
	}
}

// TestRenderKeepsCustomerIDsStrings renders customer IDs that YAML would
// otherwise read as booleans, nulls or numbers.
func TestRenderKeepsCustomerIDsStrings(t *testing.T) {
	for _, id := range []string{"true", "false", "yes", "no", "on", "off", "y", "n", "null", "nan", "inf", "e3"} {
		for _, platform := range Platforms {
			objects, err := Render(Spec{CustomerID: id, Profile: DevProfile, Platform: platform})
			if err != nil {
				t.Fatalf("Render(%s on %s): %v", id, platform.Name, err)
			}
			for _, obj := range objects {
				if got := obj.GetLabels()[TenantLabel]; got != id {
					t.Errorf("%s on %s: %s has tenant label %q", id, platform.Name, obj.GetKind(), got)
				}
				name, _, err := unstructured.NestedFieldNoCopy(obj.Object, "metadata", "name")
				if s, ok := name.(string); err != nil || !ok || s == "" {
					t.Errorf("%s on %s: %s has name %#v", id, platform.Name, obj.GetKind(), name)
				}
			}
		}
	}
}

// objectOfKind returns the one rendered object of kind.
func objectOfKind(t *testing.T, objects []*unstructured.Unstructured, kind string) *unstructured.Unstructured {
	t.Helper()
	var found *unstructured.Unstructured
	for _, obj := range objects {
		if obj.GetKind() == kind {
			if found != nil {
				t.Fatalf("more than one %s", kind)
			}
			found = obj
		}
	}
	if found == nil {
		t.Fatalf("no %s", kind)
	}
	return found
}

// TestRenderDedicatedNodes checks that on EKS each customer's pods run on
// nodes of their own, and that kind renders none of it.
func TestRenderDedicatedNodes(t *testing.T) {
	spec := Spec{CustomerID: "acme", Profile: ProductionProfile, Platform: EKSProdPlatform}
	objects, err := Render(spec)
	if err != nil {
		t.Fatal(err)
	}
	nodeClass, nodePool := objectOfKind(t, objects, "NodeClass"), objectOfKind(t, objects, "NodePool")
	if role, _, _ := unstructured.NestedString(nodeClass.Object, "spec", "role"); role != "changeport-prod-customer-node" {
		t.Errorf("NodeClass role = %q", role)
	}
	if tag, _, _ := unstructured.NestedString(nodeClass.Object, "spec", "tags", TenantLabel); tag != "acme" {
		t.Errorf("NodeClass tags its nodes with tenant %q, want acme", tag)
	}
	if label, _, _ := unstructured.NestedString(nodePool.Object, "spec", "template", "metadata", "labels", TenantLabel); label != "acme" {
		t.Errorf("NodePool labels its nodes with tenant %q, want acme", label)
	}
	taints, _, _ := unstructured.NestedSlice(nodePool.Object, "spec", "template", "spec", "taints")
	if len(taints) != 1 || taints[0].(map[string]any)["value"] != "acme" || taints[0].(map[string]any)["effect"] != "NoSchedule" {
		t.Errorf("NodePool taints = %v, want the tenant's NoSchedule taint", taints)
	}
	if expire, _, _ := unstructured.NestedString(nodePool.Object, "spec", "template", "spec", "expireAfter"); expire != "504h" {
		t.Errorf("NodePool expireAfter = %q, want the 21-day maximum", expire)
	}
	if limit, _, _ := unstructured.NestedString(nodePool.Object, "spec", "limits", "cpu"); limit != "8" {
		t.Errorf("NodePool CPU limit = %q, want 8: three 2-vCPU brokers plus one replacement node", limit)
	}

	for _, path := range [][]string{
		{"spec", "template", "pod"},                   // KafkaNodePool
		{"spec", "entityOperator", "template", "pod"}, // Kafka
	} {
		obj := objectOfKind(t, objects, "KafkaNodePool")
		if path[1] == "entityOperator" {
			obj = objectOfKind(t, objects, "Kafka")
		}
		pod, _, _ := unstructured.NestedMap(obj.Object, path...)
		tolerations, _, _ := unstructured.NestedSlice(pod, "tolerations")
		if len(tolerations) != 1 || tolerations[0].(map[string]any)["value"] != "acme" {
			t.Errorf("%s %v tolerations = %v, want the tenant's taint", obj.GetKind(), path, tolerations)
		}
		terms, _, _ := unstructured.NestedSlice(pod, "affinity", "nodeAffinity", "requiredDuringSchedulingIgnoredDuringExecution", "nodeSelectorTerms")
		if len(terms) != 1 {
			t.Errorf("%s %v requires no tenant nodes: %v", obj.GetKind(), path, terms)
		}
	}

	kindObjects, err := Render(Spec{CustomerID: "acme", Profile: DevHAProfile, Platform: KindPlatform})
	if err != nil {
		t.Fatal(err)
	}
	for _, obj := range kindObjects {
		if obj.GetKind() == "NodeClass" || obj.GetKind() == "NodePool" {
			t.Errorf("kind renders a %s", obj.GetKind())
		}
	}
	pod, _, _ := unstructured.NestedMap(objectOfKind(t, kindObjects, "KafkaNodePool").Object, "spec", "template", "pod")
	if _, found, _ := unstructured.NestedSlice(pod, "tolerations"); found {
		t.Error("kind brokers tolerate a tenant taint")
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(pod, "affinity", "podAntiAffinity"); !found {
		t.Error("kind brokers may share a node")
	}
	if spread, _, _ := unstructured.NestedSlice(pod, "topologySpreadConstraints"); len(spread) != 1 {
		t.Errorf("kind dev-ha brokers spread = %v, want one zone constraint", spread)
	}
}
