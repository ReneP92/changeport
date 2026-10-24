package clusters

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/ReneP92/changeport/internal/provisioning"
)

// authority is a test CA that issues certificates.
type authority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

var serial int64

func newAuthority(t *testing.T, name string) *authority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial++
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &authority{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issue returns a certificate and key in PEM for name, valid for the
// loopback address when it is a server certificate.
func (a *authority) issue(t *testing.T, name string, server bool) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial++
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	if server {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		template.IPAddresses = []net.IP{net.IPv4(127, 0, 0, 1)}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, a.cert, &key.PublicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

// broker is a fake Kafka cluster on a TLS listener that requires client
// certificates from the clients CA and records the names they present.
type broker struct {
	cluster *kfake.Cluster
	mu      sync.Mutex
	clients []string
}

func newBroker(t *testing.T, clusterCA, clientsCA *authority) *broker {
	t.Helper()
	certPEM, keyPEM := clusterCA.issue(t, "broker", true)
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	clientRoots := x509.NewCertPool()
	clientRoots.AddCert(clientsCA.cert)
	b := &broker{}
	config := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientRoots,
		VerifyConnection: func(state tls.ConnectionState) error {
			b.mu.Lock()
			defer b.mu.Unlock()
			b.clients = append(b.clients, state.PeerCertificates[0].Subject.CommonName)
			return nil
		},
	}
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.TLS(config), kfake.SeedTopics(1, "orders"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	b.cluster = cluster
	return b
}

func (b *broker) lastClient() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.clients) == 0 {
		return ""
	}
	return b.clients[len(b.clients)-1]
}

// fakeSecrets holds Secrets by namespace and name.
type fakeSecrets struct {
	mu      sync.Mutex
	secrets map[string]map[string][]byte
	version map[string]int
	reads   int
}

func (f *fakeSecrets) Get(_ context.Context, namespace, name string) (map[string][]byte, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	data, ok := f.secrets[namespace+"/"+name]
	if !ok {
		return nil, "", ErrNotFound
	}
	return data, strconv.Itoa(f.version[namespace+"/"+name]), nil
}

func (f *fakeSecrets) set(namespace, name string, data map[string][]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.secrets[namespace+"/"+name] = data
	f.version[namespace+"/"+name]++
}

// fixture is a customer acme whose cluster is the broker.
type fixture struct {
	broker    *broker
	secrets   *fakeSecrets
	pool      *Pool
	now       time.Time
	clientsCA *authority
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	clusterCA, clientsCA := newAuthority(t, "cluster-ca"), newAuthority(t, "clients-ca")
	f := &fixture{
		broker:    newBroker(t, clusterCA, clientsCA),
		secrets:   &fakeSecrets{secrets: map[string]map[string][]byte{}, version: map[string]int{}},
		now:       time.Now(),
		clientsCA: clientsCA,
	}
	f.rotateUser(t, "console-1")
	f.secrets.set("customer-acme", "acme-cluster-ca-cert", map[string][]byte{"ca.crt": clusterCA.pem})
	f.pool = NewPool(f.secrets, slog.New(slog.NewTextHandler(io.Discard, nil)))
	f.pool.now = func() time.Time { return f.now }
	f.pool.Bootstrap = func(provisioning.Spec) string { return f.broker.cluster.ListenAddrs()[0] }
	t.Cleanup(f.pool.Close)
	return f
}

// rotateUser gives the console user a new certificate named name, as the
// Strimzi User Operator does when it renews it.
func (f *fixture) rotateUser(t *testing.T, name string) {
	t.Helper()
	certPEM, keyPEM := f.clientsCA.issue(t, name, false)
	f.secrets.set("customer-acme", "changeport-console", map[string][]byte{"user.crt": certPEM, "user.key": keyPEM})
}

func TestAdminConnectsWithTheConsoleCertificate(t *testing.T) {
	f := newFixture(t)
	admin, err := f.pool.Admin(t.Context(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	topics, err := admin.ListTopics(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !topics.Has("orders") {
		t.Errorf("topics = %v, want orders", topics.Names())
	}
	if got := f.broker.lastClient(); got != "console-1" {
		t.Errorf("the broker saw client certificate %q, want console-1", got)
	}

	again, err := f.pool.Admin(t.Context(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	if again != admin {
		t.Error("a second Admin call made a new client")
	}
}

func TestMetadataSeesTopicsAtOnce(t *testing.T) {
	f := newFixture(t)
	admin, err := f.pool.Admin(t.Context(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	// Listing fills the client's metadata cache, which would hide a topic
	// created next for up to kgo.MetadataMinAge.
	if _, err := admin.ListTopics(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.CreateTopic(t.Context(), 1, 1, nil, "invoices"); err != nil {
		t.Fatal(err)
	}
	metadata, err := f.pool.Metadata(t.Context(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, topic := range metadata.Topics {
		names = append(names, *topic.Topic)
	}
	if !slices.Contains(names, "invoices") || !slices.Contains(names, "orders") {
		t.Errorf("Metadata() topics = %v, want invoices and orders", names)
	}
	if len(metadata.Brokers) != 1 {
		t.Errorf("Metadata() brokers = %d, want 1", len(metadata.Brokers))
	}
}

func TestAdminUsesRenewedCertificates(t *testing.T) {
	f := newFixture(t)
	if _, err := f.pool.Admin(t.Context(), "acme"); err != nil {
		t.Fatal(err)
	}
	f.rotateUser(t, "console-2")

	// Within the refresh interval the pool keeps what it has.
	reads := f.secrets.reads
	if _, err := f.pool.Admin(t.Context(), "acme"); err != nil {
		t.Fatal(err)
	}
	if f.secrets.reads != reads {
		t.Errorf("read %d Secrets within the refresh interval", f.secrets.reads-reads)
	}

	f.now = f.now.Add(f.pool.RefreshInterval)
	if _, err := f.pool.Admin(t.Context(), "acme"); err != nil {
		t.Fatal(err)
	}
	// The next connection presents the renewed certificate.
	conn, err := f.pool.clients["acme"].dial(t.Context(), "tcp", f.broker.cluster.ListenAddrs()[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.(*tls.Conn).HandshakeContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	// With TLS 1.3 the server checks the client certificate after the
	// client's handshake is done.
	deadline := time.Now().Add(5 * time.Second)
	for f.broker.lastClient() != "console-2" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	_ = conn.Close()
	if got := f.broker.lastClient(); got != "console-2" {
		t.Errorf("a new connection presented %q, want the renewed console-2", got)
	}
}

func TestAdminWithoutCredentialsIsNotReady(t *testing.T) {
	f := newFixture(t)
	if _, err := f.pool.Admin(t.Context(), "globex"); !errors.Is(err, ErrNotReady) {
		t.Errorf("Admin() for a customer without the console's Secrets = %v, want ErrNotReady", err)
	}
}

func TestAdminRefusesInvalidCustomerIDs(t *testing.T) {
	f := newFixture(t)
	if _, err := f.pool.Admin(t.Context(), "../kube-system"); err == nil {
		t.Error("Admin() accepted an invalid customer ID")
	}
	if f.secrets.reads != 0 {
		t.Errorf("read %d Secrets for an invalid customer ID", f.secrets.reads)
	}
}

func TestAdminClosesIdleClients(t *testing.T) {
	f := newFixture(t)
	if _, err := f.pool.Admin(t.Context(), "acme"); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(f.pool.IdleTimeout)
	// Any call closes the clients that have been idle for long enough.
	if _, err := f.pool.Admin(t.Context(), "globex"); !errors.Is(err, ErrNotReady) {
		t.Fatal(err)
	}
	if _, ok := f.pool.clients["acme"]; ok {
		t.Error("the idle client of acme is still open")
	}
}
