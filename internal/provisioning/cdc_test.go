package provisioning

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

func renderSpec(t *testing.T, spec Spec) []*unstructured.Unstructured {
	t.Helper()
	objects, err := Render(spec)
	if err != nil {
		t.Fatal(err)
	}
	return objects
}

func cdcObjects(objects []*unstructured.Unstructured) []*unstructured.Unstructured {
	_, cdc := splitCDC(objects)
	return cdc
}

func TestRenderCDCRuntimeOnlyWithSources(t *testing.T) {
	if cdc := cdcObjects(renderSpec(t, acme)); len(cdc) != 0 {
		t.Errorf("a customer without sources renders %d CDC objects", len(cdc))
	}
	// Without a published Connect image, the platform runs no sources.
	eks := Spec{CustomerID: "acme", Profile: ProductionProfile, Platform: EKSProdPlatform, Sources: sources}
	eks.Platform.ConnectImage = ""
	if cdc := cdcObjects(renderSpec(t, eks)); len(cdc) != 0 {
		t.Errorf("a platform without a Connect image renders %d CDC objects", len(cdc))
	}

	withSources := acme
	withSources.Sources = sources
	var kinds []string
	for _, obj := range cdcObjects(renderSpec(t, withSources)) {
		kinds = append(kinds, obj.GetKind()+" "+obj.GetName())
		if obj.GetNamespace() != withSources.Namespace() || obj.GetLabels()[TenantLabel] != "acme" {
			t.Errorf("%s %s is in namespace %q with tenant %q", obj.GetKind(), obj.GetName(), obj.GetNamespace(), obj.GetLabels()[TenantLabel])
		}
		// Reconcile deletes them once they are no longer rendered.
		if !slices.Contains(prunedKinds, obj.GroupVersionKind()) {
			t.Errorf("%s is rendered as part of CDC, but the provisioner does not prune its kind", obj.GetKind())
		}
	}
	want := []string{
		"KafkaUser changeport-connect", "KafkaUser changeport-registry", "KafkaConnect changeport-connect",
		"Deployment changeport-registry", "Service changeport-registry",
		"NetworkPolicy changeport-connect-egress", "NetworkPolicy changeport-registry",
	}
	if !slices.Equal(kinds, want) {
		t.Errorf("CDC objects = %v, want %v", kinds, want)
	}
}

func TestSpecValidatesSources(t *testing.T) {
	for _, list := range [][]Source{{{Name: "App"}}, {{Name: "app"}, {Name: "app"}}, {{Name: ""}}, {{Name: "app.db"}}, {{Name: strings.Repeat("a", 41)}}} {
		spec := acme
		spec.Sources = list
		if spec.Validate() == nil {
			t.Errorf("Validate() accepted sources %v", list)
		}
	}
}

// acls returns a KafkaUser's ACLs as "operation resource-type pattern name".
func acls(t *testing.T, user *unstructured.Unstructured) []string {
	t.Helper()
	list, _, err := unstructured.NestedSlice(user.Object, "spec", "authorization", "acls")
	if err != nil {
		t.Fatal(err)
	}
	var result []string
	for _, a := range list {
		acl, _ := a.(map[string]any)
		resource, _, _ := unstructured.NestedString(acl, "resource", "type")
		name, _, _ := unstructured.NestedString(acl, "resource", "name")
		pattern, _, _ := unstructured.NestedString(acl, "resource", "patternType")
		operations, _, _ := unstructured.NestedStringSlice(acl, "operations")
		for _, operation := range operations {
			result = append(result, strings.TrimSpace(operation+" "+resource+" "+pattern+" "+name))
		}
	}
	return result
}

func kafkaUser(t *testing.T, objects []*unstructured.Unstructured, name string) *unstructured.Unstructured {
	t.Helper()
	for _, obj := range objects {
		if obj.GetKind() == "KafkaUser" && obj.GetName() == name {
			return obj
		}
	}
	t.Fatalf("no KafkaUser %s", name)
	return nil
}

// The Connect worker creates and writes only its own topics and the
// sources' topics, and reads only its own; the registry touches only its
// own. Neither can read a customer's topic.
func TestRenderCDCUsersCannotReadCustomerTopics(t *testing.T) {
	spec := acme
	spec.Sources = sources
	objects := renderSpec(t, spec)

	connect := acls(t, kafkaUser(t, objects, ConnectUser))
	want := []string{
		"Create topic prefix changeport.connect.", "Describe topic prefix changeport.connect.", "DescribeConfigs topic prefix changeport.connect.",
		"Read topic prefix changeport.connect.", "Write topic prefix changeport.connect.",
		"Describe group literal changeport.connect", "Read group literal changeport.connect",
		"Create topic prefix changeport.cdc.", "Describe topic prefix changeport.cdc.", "DescribeConfigs topic prefix changeport.cdc.",
		"Read topic prefix changeport.cdc.", "Write topic prefix changeport.cdc.",
		"Describe group prefix changeport.cdc.", "Read group prefix changeport.cdc.",
		"Create topic prefix app.", "Describe topic prefix app.", "DescribeConfigs topic prefix app.", "Write topic prefix app.",
		"Create topic prefix billing.", "Describe topic prefix billing.", "DescribeConfigs topic prefix billing.", "Write topic prefix billing.",
		"Describe cluster", "DescribeConfigs cluster",
	}
	if !slices.Equal(connect, want) {
		t.Errorf("Connect ACLs =\n%s\nwant\n%s", strings.Join(connect, "\n"), strings.Join(want, "\n"))
	}

	registry := acls(t, kafkaUser(t, objects, RegistryUser))
	for _, acl := range registry {
		if !strings.HasSuffix(acl, " cluster") && !strings.HasSuffix(acl, " prefix changeport.registry.") {
			t.Errorf("the registry may %s", acl)
		}
	}

	// Of all users but the customer's own, only these read topics, all of
	// them the platform's.
	for _, obj := range objects {
		if obj.GetKind() != "KafkaUser" || obj.GetName() == AdminUser {
			continue
		}
		for _, acl := range acls(t, obj) {
			if (strings.HasPrefix(acl, "Read topic") || strings.HasPrefix(acl, "All ")) && !strings.HasPrefix(acl, "Read topic prefix changeport.") {
				t.Errorf("KafkaUser %s may %s", obj.GetName(), acl)
			}
		}
	}
}

func TestRenderCDCPlacementAndNetwork(t *testing.T) {
	eks := Spec{CustomerID: "acme", Profile: ProductionProfile, Platform: EKSProdPlatform, Sources: sources, KubernetesAPI: eksAPIServer}
	eks.Platform.ConnectImage = publishedConnectImage
	objects := renderSpec(t, eks)
	if limit, _, _ := unstructured.NestedString(objectOfKind(t, objects, "NodePool").Object, "spec", "limits", "cpu"); limit != "10" {
		t.Errorf("NodePool CPU limit = %q, want 10: three brokers, the CDC node and a replacement", limit)
	}
	connect := objectOfKind(t, objects, "KafkaConnect")
	registry := objectOfKind(t, cdcObjects(objects), "Deployment")
	for name, pod := range map[string][]string{
		"KafkaConnect": {"spec", "template", "pod"},
		"Deployment":   {"spec", "template", "spec"},
	} {
		obj := connect
		if name == "Deployment" {
			obj = registry
		}
		spec, _, _ := unstructured.NestedMap(obj.Object, pod...)
		tolerations, _, _ := unstructured.NestedSlice(spec, "tolerations")
		if len(tolerations) != 1 {
			t.Errorf("%s tolerations = %v, want the tenant's taint", name, tolerations)
		}
		anti, _, _ := unstructured.NestedSlice(spec, "affinity", "podAntiAffinity", "requiredDuringSchedulingIgnoredDuringExecution")
		if len(anti) != 1 || !strings.Contains(strings.Join(mapStrings(anti[0]), " "), "dual-role") {
			t.Errorf("%s may run on a broker's node: %v", name, anti)
		}
	}

	// The worker reaches the internet but not the cluster's networks or the
	// metadata service; on kind also the stand-in database.
	policy := func(objects []*unstructured.Unstructured) string {
		for _, obj := range objects {
			if obj.GetKind() == "NetworkPolicy" && obj.GetName() == ConnectCluster+"-egress" {
				egress, _, _ := unstructured.NestedSlice(obj.Object, "spec", "egress")
				return strings.Join(mapStrings(egress...), " ")
			}
		}
		t.Fatal("no egress NetworkPolicy for the Connect worker")
		return ""
	}
	onEKS := policy(objects)
	for _, want := range []string{"0.0.0.0/0", "10.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10"} {
		if !strings.Contains(onEKS, want) {
			t.Errorf("the Connect worker's egress does not mention %s: %s", want, onEKS)
		}
	}
	if strings.Contains(onEKS, "source-postgres") {
		t.Error("on EKS the Connect worker may reach the source-postgres namespace")
	}
	onKind := acme
	onKind.Sources = sources
	if !strings.Contains(policy(renderSpec(t, onKind)), "source-postgres") {
		t.Error("on kind the Connect worker may not reach the stand-in database")
	}
}

// mapStrings flattens values into their strings, for searching.
func mapStrings(values ...any) []string {
	var result []string
	for _, value := range values {
		switch v := value.(type) {
		case map[string]any:
			for key, inner := range v {
				result = append(result, key)
				result = append(result, mapStrings(inner)...)
			}
		case []any:
			result = append(result, mapStrings(v...)...)
		case string:
			result = append(result, v)
		}
	}
	return result
}

// The provisioner's ClusterRole lets it delete what it prunes.
func TestProvisionerMayPruneTheCDCRuntime(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "deploy", "base", "provisioner", "provisioner.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string][]string{}
	decoder := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(string(data)), 4096)
	for {
		var obj unstructured.Unstructured
		if err := decoder.Decode(&obj.Object); err != nil {
			break
		}
		if obj.GetKind() != "ClusterRole" || obj.GetName() != "changeport-provisioner" {
			continue
		}
		rules, _, _ := unstructured.NestedSlice(obj.Object, "rules")
		for _, r := range rules {
			rule, _ := r.(map[string]any)
			groups, _, _ := unstructured.NestedStringSlice(rule, "apiGroups")
			resources, _, _ := unstructured.NestedStringSlice(rule, "resources")
			verbs, _, _ := unstructured.NestedStringSlice(rule, "verbs")
			for _, group := range groups {
				for _, resource := range resources {
					allowed[group+"/"+resource] = append(allowed[group+"/"+resource], verbs...)
				}
			}
		}
	}
	resources := map[string]string{"KafkaUser": "kafkausers", "KafkaConnect": "kafkaconnects", "Deployment": "deployments", "Service": "services", "NetworkPolicy": "networkpolicies"}
	for _, kind := range prunedKinds {
		resource, ok := resources[kind.Kind]
		if !ok {
			t.Fatalf("no resource name for %s", kind.Kind)
		}
		verbs := allowed[kind.Group+"/"+resource]
		for _, verb := range []string{"get", "create", "patch", "list", "delete"} {
			if !slices.Contains(verbs, verb) {
				t.Errorf("the provisioner may not %s %s, which it prunes", verb, resource)
			}
		}
	}
}

// addKubernetesAPI gives the fake cluster the EndpointSlice of the API
// server's Service, as every cluster has.
func addKubernetesAPI(cluster *fakeCluster, addresses ...string) {
	slice := &unstructured.Unstructured{Object: map[string]any{
		"ports":     []any{map[string]any{"name": "https", "port": int64(6443), "protocol": "TCP"}},
		"endpoints": []any{map[string]any{"addresses": toAny(addresses)}},
	}}
	slice.SetGroupVersionKind(sliceGVK)
	slice.SetNamespace("default")
	slice.SetName("kubernetes")
	slice.SetLabels(map[string]string{"kubernetes.io/service-name": "kubernetes"})
	cluster.objects[fakeKey(sliceGVK, "default", "kubernetes")] = slice
}

func toAny(values []string) []any {
	result := make([]any, len(values))
	for i, v := range values {
		result[i] = v
	}
	return result
}

// The Connect worker may reach the API server's addresses, which the
// provisioner looks up.
func TestReconcileLetsConnectReachTheKubernetesAPI(t *testing.T) {
	cluster := newFakeCluster()
	p := testProvisioner(cluster)
	spec := acme
	spec.Sources = sources
	if _, err := p.Reconcile(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "has no addresses") {
		t.Errorf("Reconcile() without the API server's addresses = %v", err)
	}
	addKubernetesAPI(cluster, "172.19.0.6", "172.19.0.5")
	provision(t, p, spec)
	policy := cluster.objects[fakeKey(policyGVK, spec.Namespace(), ConnectCluster+"-egress")]
	egress := strings.Join(mapStrings(policy.Object["spec"]), " ")
	if !strings.Contains(egress, "172.19.0.5/32") || !strings.Contains(egress, "172.19.0.6/32") {
		t.Errorf("the Connect worker's egress does not allow the API server: %s", egress)
	}
	// Without sources, it is not looked up.
	if _, err := testProvisioner(newFakeCluster()).Reconcile(t.Context(), acme); err != nil {
		t.Errorf("Reconcile() without sources = %v", err)
	}
}

func TestReconcileReportsTheCDCRuntimeApart(t *testing.T) {
	cluster := newFakeCluster()
	addKubernetesAPI(cluster, "172.19.0.5")
	p := testProvisioner(cluster)
	spec := acme
	spec.Sources = sources
	provision(t, p, spec)
	cluster.setStatus(kafkaGVK, spec.Namespace(), spec.ClusterName(), readyTrue)
	cluster.setUsersReady(spec)

	// The customer's cluster is Ready while the CDC runtime is not.
	progress, err := p.Reconcile(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if !progress.Ready() || progress.Users.String() != "Ready" {
		t.Errorf("progress = %+v, want the cluster ready without the CDC runtime's users", progress)
	}
	if progress.CDC.Ready || progress.CDC.Object != "KafkaUser customer-acme/changeport-connect" {
		t.Errorf("CDC = %+v, want it waiting for the Connect user", progress.CDC)
	}

	for _, user := range []string{ConnectUser, RegistryUser} {
		cluster.setStatus(kafkaUserGVK, spec.Namespace(), user, readyTrue)
	}
	if progress, _ = p.Reconcile(t.Context(), spec); progress.CDC.Object != "KafkaConnect customer-acme/changeport-connect" {
		t.Errorf("CDC = %+v, want it waiting for Connect", progress.CDC)
	}
	cluster.setStatus(connectGVK, spec.Namespace(), ConnectCluster, readyTrue)
	if progress, _ = p.Reconcile(t.Context(), spec); progress.CDC.Object != "Deployment customer-acme/changeport-registry" {
		t.Errorf("CDC = %+v, want it waiting for the registry", progress.CDC)
	}
	registry := cluster.objects[fakeKey(deploymentGVK, spec.Namespace(), Registry)]
	registry.Object["status"] = map[string]any{"observedGeneration": int64(0), "updatedReplicas": int64(1), "availableReplicas": int64(1)}
	if progress, _ = p.Reconcile(t.Context(), spec); !progress.CDC.Ready || progress.CDC.Status != "Kafka Connect and the schema registry run for 2 sources" {
		t.Errorf("CDC = %+v, want it ready", progress.CDC)
	}
}

func TestReconcileWithSourcesOnAPlatformWithoutConnect(t *testing.T) {
	spec := Spec{CustomerID: "acme", Profile: DevProfile, Platform: KindPlatform, Sources: sources}
	spec.Platform.ConnectImage = ""
	progress, err := testProvisioner(newFakeCluster()).Reconcile(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if progress.CDC.Ready || !strings.Contains(progress.CDC.Status, "do not run on platform kind yet") {
		t.Errorf("CDC = %+v", progress.CDC)
	}
}

// Once the last source is gone, the CDC runtime goes; the customer's other
// objects, and objects of other customers, stay.
func TestReconcilePrunesTheCDCRuntime(t *testing.T) {
	cluster := newFakeCluster()
	addKubernetesAPI(cluster, "172.19.0.5")
	p := testProvisioner(cluster)
	spec := acme
	spec.Sources = sources
	provision(t, p, spec)
	other := Spec{CustomerID: "globex", Profile: DevProfile, Platform: KindPlatform, Sources: sources}
	provision(t, p, other)

	// Strimzi copies the KafkaConnect's labels onto the Services and the
	// NetworkPolicy it makes for it, naming itself their manager and the
	// KafkaConnect their owner; those are its own to delete.
	for _, kind := range []string{"Service", "NetworkPolicy"} {
		made := &unstructured.Unstructured{}
		made.SetGroupVersionKind(serviceGVK)
		if kind == "NetworkPolicy" {
			made.SetGroupVersionKind(policyGVK)
		}
		made.SetNamespace(spec.Namespace())
		made.SetName("changeport-connect-connect")
		made.SetLabels(map[string]string{PartLabel: PartCDC, TenantLabel: "acme", "app.kubernetes.io/managed-by": "strimzi-cluster-operator"})
		made.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "kafka.strimzi.io/v1", Kind: "KafkaConnect", Name: ConnectCluster, UID: "1"}})
		cluster.objects[fakeKey(made.GroupVersionKind(), made.GetNamespace(), made.GetName())] = made
	}
	// An object of the provisioner's own that a person gave an owner is left
	// alone too.
	owned := cluster.objects[fakeKey(serviceGVK, spec.Namespace(), Registry)].DeepCopy()
	owned.SetName("changeport-registry-owned")
	owned.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: "someone", UID: "2"}})
	cluster.objects[fakeKey(serviceGVK, spec.Namespace(), owned.GetName())] = owned

	// Removing one source keeps the runtime, without that source's ACL.
	spec.Sources = sources[:1]
	provision(t, p, spec)
	if len(cluster.deleted) != 0 {
		t.Errorf("removing one of two sources deleted %v", cluster.deleted)
	}
	connect := acls(t, cluster.objects[fakeKey(kafkaUserGVK, spec.Namespace(), ConnectUser)])
	if slices.Contains(connect, "Write topic prefix billing.") {
		t.Error("the Connect user may still write the removed source's topics")
	}

	spec.Sources = nil
	provision(t, p, spec)
	slices.Sort(cluster.deleted)
	want := []string{
		"Deployment:customer-acme/changeport-registry", "KafkaConnect:customer-acme/changeport-connect",
		"KafkaUser:customer-acme/changeport-connect", "KafkaUser:customer-acme/changeport-registry",
		"NetworkPolicy:customer-acme/changeport-connect-egress", "NetworkPolicy:customer-acme/changeport-registry",
		"Service:customer-acme/changeport-registry",
	}
	if !slices.Equal(cluster.deleted, want) {
		t.Errorf("deleted %v, want %v", cluster.deleted, want)
	}
	for _, key := range []string{
		fakeKey(kafkaUserGVK, spec.Namespace(), AdminUser), fakeKey(deploymentGVK, spec.Namespace(), "changeport-agent"),
		fakeKey(connectGVK, other.Namespace(), ConnectCluster), fakeKey(serviceGVK, spec.Namespace(), "changeport-connect-connect"),
		fakeKey(policyGVK, spec.Namespace(), "changeport-connect-connect"), fakeKey(serviceGVK, spec.Namespace(), "changeport-registry-owned"),
	} {
		if cluster.objects[key] == nil {
			t.Errorf("%s was deleted", key)
		}
	}

	// Nothing is left to prune.
	cluster.deleted = nil
	provision(t, p, spec)
	if len(cluster.deleted) != 0 {
		t.Errorf("a second reconcile deleted %v", cluster.deleted)
	}
}
