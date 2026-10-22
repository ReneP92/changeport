// Package clusters gives the console a Kafka admin client for each customer's
// cluster, connected on demand over the internal mTLS listener as the
// changeport-console Kafka user, which may manage topics but never read or
// write messages.
package clusters

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ReneP92/changeport/internal/provisioning"
)

// ErrNotReady means the customer's cluster does not have the console's
// credentials yet, for example while it is being created.
var ErrNotReady = errors.New("the cluster is not ready for the console yet")

// Secrets reads Secrets in customers' namespaces. The console implements it
// with the Kubernetes API, as a service account that may read only the
// Secrets in provisioning.Spec.ConsoleSecrets.
type Secrets interface {
	// Get returns the Secret's data and resource version, or an error that
	// satisfies errors.Is(err, ErrNotFound) if it does not exist.
	Get(ctx context.Context, namespace, name string) (data map[string][]byte, version string, err error)
}

// ErrNotFound is what Secrets returns for a missing Secret.
var ErrNotFound = errors.New("secret not found")

// Pool keeps one admin client per customer. Clients connect on demand and
// are closed after a while without use. Strimzi renews the console user's
// certificate and the cluster CA from time to time, so the pool reads them
// again every RefreshInterval, and every new connection uses the latest.
type Pool struct {
	secrets Secrets
	logger  *slog.Logger

	// RefreshInterval is how often a client's certificates are read again.
	RefreshInterval time.Duration
	// IdleTimeout is how long a client may go unused before it is closed.
	IdleTimeout time.Duration
	// Bootstrap is the address a customer's client connects to; the default
	// is the internal mTLS listener.
	Bootstrap func(provisioning.Spec) string

	now     func() time.Time
	mu      sync.Mutex
	clients map[string]*pooled
}

// pooled is one customer's client and the certificates its connections use.
type pooled struct {
	client *kgo.Client
	admin  *kadm.Client
	tls    atomic.Pointer[tls.Config]
	// versions identifies the Secrets the certificates came from.
	versions string
	checked  time.Time
	used     time.Time
}

// NewPool returns a Pool that reads certificates through secrets.
func NewPool(secrets Secrets, logger *slog.Logger) *Pool {
	return &Pool{
		secrets:         secrets,
		logger:          logger,
		RefreshInterval: 5 * time.Minute,
		IdleTimeout:     10 * time.Minute,
		Bootstrap:       provisioning.Spec.InternalBootstrapAddress,
		now:             time.Now,
		clients:         map[string]*pooled{},
	}
}

// Admin returns the admin client of the customer's cluster, connecting it
// first if needed. Callers must not close it; the pool does when it has
// been idle for IdleTimeout, so a caller should use it for one request.
func (p *Pool) Admin(ctx context.Context, customerID string) (*kadm.Client, error) {
	if err := provisioning.ValidateCustomerID(customerID); err != nil {
		return nil, err
	}
	spec := provisioning.Spec{CustomerID: customerID}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	p.closeIdle(now)

	entry, ok := p.clients[customerID]
	if ok && now.Sub(entry.checked) < p.RefreshInterval {
		entry.used = now
		return entry.admin, nil
	}
	config, versions, err := p.loadTLS(ctx, spec)
	if err != nil {
		return nil, err
	}
	if ok {
		if versions != entry.versions {
			entry.tls.Store(config)
			entry.versions = versions
			p.logger.Info("reloaded the console's certificates", "tenant", customerID)
		}
		entry.checked, entry.used = now, now
		return entry.admin, nil
	}

	entry = &pooled{versions: versions, checked: now, used: now}
	entry.tls.Store(config)
	client, err := kgo.NewClient(
		kgo.SeedBrokers(p.Bootstrap(spec)),
		kgo.Dialer(entry.dial),
		kgo.ClientID("changeport-console"),
	)
	if err != nil {
		return nil, fmt.Errorf("create the Kafka client of customer %s: %w", customerID, err)
	}
	entry.client, entry.admin = client, kadm.NewClient(client)
	p.clients[customerID] = entry
	return entry.admin, nil
}

// dial opens a TLS connection with the latest certificates.
func (e *pooled) dial(ctx context.Context, network, address string) (net.Conn, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	config := e.tls.Load().Clone()
	config.ServerName = host
	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: config}
	return dialer.DialContext(ctx, network, address)
}

// loadTLS reads the console user's certificate and the cluster CA, and
// returns a TLS configuration for them with the Secrets' versions.
func (p *Pool) loadTLS(ctx context.Context, spec provisioning.Spec) (*tls.Config, string, error) {
	user, userVersion, err := p.secret(ctx, spec, provisioning.ConsoleUser)
	if err != nil {
		return nil, "", err
	}
	ca, caVersion, err := p.secret(ctx, spec, spec.ClusterCASecret())
	if err != nil {
		return nil, "", err
	}
	certificate, err := tls.X509KeyPair(user["user.crt"], user["user.key"])
	if err != nil {
		return nil, "", fmt.Errorf("the console's certificate for customer %s: %w", spec.CustomerID, err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca["ca.crt"]) {
		return nil, "", fmt.Errorf("the cluster CA of customer %s holds no certificate", spec.CustomerID)
	}
	config := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
		RootCAs:      roots,
	}
	return config, userVersion + "/" + caVersion, nil
}

func (p *Pool) secret(ctx context.Context, spec provisioning.Spec, name string) (map[string][]byte, string, error) {
	data, version, err := p.secrets.Get(ctx, spec.Namespace(), name)
	if errors.Is(err, ErrNotFound) {
		return nil, "", fmt.Errorf("customer %s: secret %s is missing: %w", spec.CustomerID, name, ErrNotReady)
	}
	if err != nil {
		return nil, "", fmt.Errorf("customer %s: read secret %s: %w", spec.CustomerID, name, err)
	}
	return data, version, nil
}

// closeIdle closes the clients that have not been used for IdleTimeout.
func (p *Pool) closeIdle(now time.Time) {
	for id, entry := range p.clients {
		if now.Sub(entry.used) >= p.IdleTimeout {
			entry.client.Close()
			delete(p.clients, id)
		}
	}
}

// Close closes every client.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, entry := range p.clients {
		entry.client.Close()
		delete(p.clients, id)
	}
}
