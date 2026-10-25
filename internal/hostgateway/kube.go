package hostgateway

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// hostnameAnnotation is where the template puts each load balancer's public
// name, which external-dns publishes on EKS.
const hostnameAnnotation = "external-dns.alpha.kubernetes.io/hostname"

// KubeResolver finds load balancers among the LoadBalancer Services of the
// customers' Kafka clusters, by the public name in their annotation. It
// keeps what it found and lists the Services again for a name it does not
// know, or when what it knows is older than MaxAge, so that new customers
// are reachable at once.
type KubeResolver struct {
	Client client.Reader
	// MaxAge is how long a listing is used for known names.
	MaxAge time.Duration
	// MinInterval is how often at most unknown names list the Services.
	MinInterval time.Duration
	Now         func() time.Time

	mu        sync.Mutex
	addresses map[string]string
	listed    time.Time
}

// NewKubeResolver returns a KubeResolver reading Services through c.
func NewKubeResolver(c client.Reader) *KubeResolver {
	return &KubeResolver{Client: c, MaxAge: 30 * time.Second, MinInterval: time.Second, Now: time.Now}
}

// Resolve implements Resolver.
func (r *KubeResolver) Resolve(ctx context.Context, name string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.Now()
	address, known := r.addresses[name]
	if known && now.Sub(r.listed) < r.MaxAge {
		return address, nil
	}
	if known || now.Sub(r.listed) >= r.MinInterval {
		if err := r.list(ctx); err != nil {
			return "", err
		}
		r.listed = now
		address, known = r.addresses[name]
	}
	if !known {
		return "", fmt.Errorf("%s: %w", name, ErrUnknownName)
	}
	return address, nil
}

// list reads the public names and addresses of every Kafka cluster's load
// balancers.
func (r *KubeResolver) list(ctx context.Context) error {
	var services corev1.ServiceList
	if err := r.Client.List(ctx, &services, client.HasLabels{"strimzi.io/cluster"}); err != nil {
		return fmt.Errorf("list the Kafka clusters' Services: %w", err)
	}
	addresses := map[string]string{}
	for _, service := range services.Items {
		name := strings.ToLower(service.Annotations[hostnameAnnotation])
		if service.Spec.Type != corev1.ServiceTypeLoadBalancer || name == "" ||
			len(service.Status.LoadBalancer.Ingress) == 0 || len(service.Spec.Ports) == 0 {
			continue
		}
		ip := service.Status.LoadBalancer.Ingress[0].IP
		if ip == "" {
			continue
		}
		addresses[name] = net.JoinHostPort(ip, strconv.Itoa(int(service.Spec.Ports[0].Port)))
	}
	r.addresses = addresses
	return nil
}
