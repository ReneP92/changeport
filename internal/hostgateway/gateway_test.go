package hostgateway

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"log/slog"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

const domain = "kafka.changeport.test"

// loadBalancer is a TLS server for one public name, standing in for a
// Kafka listener behind its load balancer: it answers each line with its
// name and the line.
type loadBalancer struct {
	name    string
	address string
}

// newLoadBalancers starts a TLS server per name, with certificates of one
// CA, and returns them and a pool trusting the CA.
func newLoadBalancers(t *testing.T, names ...string) ([]loadBalancer, *x509.CertPool) {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, IsCA: true,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(caDER)
	pool := x509.NewCertPool()
	pool.AddCert(ca)

	var balancers []loadBalancer
	for i, name := range names {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		template := &x509.Certificate{
			SerialNumber: big.NewInt(int64(i + 2)), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
			Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { listener.Close() })
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				go func() {
					defer conn.Close()
					lines := bufio.NewScanner(conn)
					for lines.Scan() {
						if _, err := io.WriteString(conn, name+": "+lines.Text()+"\n"); err != nil {
							return
						}
					}
				}()
			}
		}()
		balancers = append(balancers, loadBalancer{name: name, address: listener.Addr().String()})
	}
	return balancers, pool
}

// mapResolver resolves names from a map.
type mapResolver map[string]string

func (m mapResolver) Resolve(_ context.Context, name string) (string, error) {
	if address, ok := m[name]; ok {
		return address, nil
	}
	return "", ErrUnknownName
}

// startGateway serves a gateway for the load balancers and returns its
// address.
func startGateway(t *testing.T, balancers []loadBalancer) string {
	t.Helper()
	resolver := mapResolver{}
	for _, b := range balancers {
		resolver[b.name] = b.address
	}
	gateway := New(domain, resolver, slog.New(slog.NewTextHandler(io.Discard, nil)))
	gateway.HandshakeTimeout = time.Second
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gateway.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve() = %v", err)
		}
	})
	return listener.Addr().String()
}

// ask connects to the gateway as name, as a Kafka client does, and returns
// the answer to one line.
func ask(t *testing.T, gateway, name string, pool *x509.CertPool) (string, error) {
	t.Helper()
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", gateway,
		&tls.Config{ServerName: name, RootCAs: pool, MinVersion: tls.VersionTLS12})
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "hello\n"); err != nil {
		return "", err
	}
	return bufio.NewReader(conn).ReadString('\n')
}

func TestGatewayForwardsByServerName(t *testing.T) {
	names := []string{"bootstrap.acme.kafka.changeport.test", "b0.acme.kafka.changeport.test", "b1.globex.kafka.changeport.test"}
	balancers, pool := newLoadBalancers(t, names...)
	gateway := startGateway(t, balancers)
	for _, name := range names {
		// The client verifies the load balancer's own certificate: TLS
		// runs end to end, through the gateway.
		got, err := ask(t, gateway, name, pool)
		if err != nil {
			t.Fatalf("connecting as %s: %v", name, err)
		}
		if want := name + ": hello\n"; got != want {
			t.Errorf("connecting as %s got %q, want %q", name, got, want)
		}
	}
	// Names are matched regardless of case.
	if got, err := ask(t, gateway, "B0.ACME.kafka.changeport.test", pool); err != nil || !strings.HasPrefix(got, "b0.acme") {
		t.Errorf("connecting in upper case = %q, %v", got, err)
	}
}

func TestGatewayRefusesWhatItCannotRoute(t *testing.T) {
	balancers, pool := newLoadBalancers(t, "bootstrap.acme.kafka.changeport.test", "bootstrap.acme.example.com")
	gateway := startGateway(t, balancers)
	for _, name := range []string{
		"bootstrap.initech.kafka.changeport.test", // no such customer
		"bootstrap.acme.example.com",              // outside the Kafka domain
	} {
		if got, err := ask(t, gateway, name, pool); err == nil {
			t.Errorf("connecting as %s got %q, want the connection closed", name, got)
		}
	}

	// A client that speaks no TLS gets nothing, and the gateway goes on.
	conn, err := net.Dial("tcp", gateway)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, err := conn.Read(make([]byte, 1)); n != 0 || err == nil {
		t.Errorf("a plaintext client read %d bytes, %v; want the connection closed", n, err)
	}
	conn.Close()
	if _, err := ask(t, gateway, "bootstrap.acme.kafka.changeport.test", pool); err != nil {
		t.Errorf("after a plaintext client: %v", err)
	}
}
