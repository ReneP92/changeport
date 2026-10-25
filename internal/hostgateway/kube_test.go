package hostgateway

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// kafkaService is a load balancer of a customer's public listener, as
// Strimzi creates it from the template.
func kafkaService(customer, name, hostname, ip string) *corev1.Service {
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "customer-" + customer,
			Name:        name,
			Labels:      map[string]string{"strimzi.io/cluster": customer},
			Annotations: map[string]string{hostnameAnnotation: hostname},
		},
		Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer, Ports: []corev1.ServicePort{{Port: 9094}}},
	}
	if ip != "" {
		service.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: ip}}
	}
	return service
}

func TestKubeResolverFindsLoadBalancersByName(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	internal := kafkaService("acme", "acme-kafka-bootstrap", "", "")
	internal.Spec.Type = corev1.ServiceTypeClusterIP
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		kafkaService("acme", "acme-kafka-external-bootstrap", "bootstrap.acme.kafka.changeport.test", "172.19.0.10"),
		kafkaService("acme", "acme-dual-role-0", "b0.acme.kafka.changeport.test", "172.19.0.11"),
		// Not given an address yet.
		kafkaService("globex", "globex-kafka-external-bootstrap", "bootstrap.globex.kafka.changeport.test", ""),
		internal,
	).Build()
	now := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
	resolver := NewKubeResolver(c)
	resolver.Now = func() time.Time { return now }

	for name, want := range map[string]string{
		"bootstrap.acme.kafka.changeport.test": "172.19.0.10:9094",
		"b0.acme.kafka.changeport.test":        "172.19.0.11:9094",
	} {
		if got, err := resolver.Resolve(t.Context(), name); err != nil || got != want {
			t.Errorf("Resolve(%s) = %q, %v; want %s", name, got, err, want)
		}
	}
	if _, err := resolver.Resolve(t.Context(), "bootstrap.globex.kafka.changeport.test"); !errors.Is(err, ErrUnknownName) {
		t.Errorf("Resolve() of a load balancer without an address = %v, want ErrUnknownName", err)
	}

	// A new customer is found on the next listing, at most a second later.
	if err := c.Create(t.Context(), kafkaService("initech", "initech-kafka-external-bootstrap", "bootstrap.initech.kafka.changeport.test", "172.19.0.20")); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve(t.Context(), "bootstrap.initech.kafka.changeport.test"); !errors.Is(err, ErrUnknownName) {
		t.Errorf("Resolve() listed again within a second: %v", err)
	}
	now = now.Add(time.Second)
	if got, err := resolver.Resolve(t.Context(), "bootstrap.initech.kafka.changeport.test"); err != nil || got != "172.19.0.20:9094" {
		t.Errorf("Resolve() of a new customer = %q, %v", got, err)
	}

	// A load balancer's new address is used once the listing is old.
	moved := &corev1.Service{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "customer-acme", Name: "acme-dual-role-0"}, moved); err != nil {
		t.Fatal(err)
	}
	moved.Status.LoadBalancer.Ingress[0].IP = "172.19.0.12"
	if err := c.Status().Update(t.Context(), moved); err != nil {
		t.Fatal(err)
	}
	now = now.Add(resolver.MaxAge)
	if got, _ := resolver.Resolve(t.Context(), "b0.acme.kafka.changeport.test"); got != "172.19.0.12:9094" {
		t.Errorf("Resolve() after the address changed = %q", got)
	}
}

// failingReader fails every listing.
type failingReader struct{ client.Reader }

func (failingReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("the API server is down")
}

func TestKubeResolverReportsListingErrors(t *testing.T) {
	resolver := NewKubeResolver(failingReader{})
	if _, err := resolver.Resolve(t.Context(), "bootstrap.acme.kafka.changeport.test"); err == nil || errors.Is(err, ErrUnknownName) {
		t.Errorf("Resolve() with the API server down = %v, want its error", err)
	}
}
