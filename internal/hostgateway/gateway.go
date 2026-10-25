// Package hostgateway lets clients on the developer's machine reach the
// customers' Kafka clusters on kind. Docker Desktop does not route the
// addresses of kind's load balancers to the host, so the gateway runs as a
// container on the kind network with one port published on the host. Every
// Kafka client names the broker it connects to in its TLS handshake (SNI);
// the gateway reads that name and forwards the connection, still encrypted
// end to end, to the load balancer that serves it. It also answers DNS for
// the Kafka domain with the host's loopback address, so that every public
// name of every customer leads to the gateway.
package hostgateway

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"
)

// Resolver finds the address of the load balancer serving a public name,
// such as bootstrap.acme.kafka.changeport.test, as host:port.
type Resolver interface {
	Resolve(ctx context.Context, name string) (string, error)
}

// ErrUnknownName is returned for a name no load balancer serves.
var ErrUnknownName = errors.New("no load balancer serves this name")

// Gateway forwards TLS connections to the load balancer named in their
// handshake, for names under Domain.
type Gateway struct {
	// Domain is the Kafka domain, such as kafka.changeport.test; names
	// outside it are refused.
	Domain   string
	Resolver Resolver
	Logger   *slog.Logger
	// HandshakeTimeout bounds how long a client may take to send its
	// handshake's first message.
	HandshakeTimeout time.Duration
	// Dial connects to a load balancer.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)
}

// New returns a Gateway for domain.
func New(domain string, resolver Resolver, logger *slog.Logger) *Gateway {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	return &Gateway{Domain: domain, Resolver: resolver, Logger: logger, HandshakeTimeout: 10 * time.Second, Dial: dialer.DialContext}
}

// Serve forwards the connections listener accepts until ctx is done.
func (g *Gateway) Serve(ctx context.Context, listener net.Listener) error {
	go func() {
		<-ctx.Done()
		// Closing ends Accept below; there is nothing to do on an error.
		_ = listener.Close()
	}()
	var connections sync.WaitGroup
	defer connections.Wait()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		connections.Go(func() { g.forward(ctx, conn) })
	}
}

// forward reads the server name of client's handshake and pipes the
// connection to the load balancer serving it.
func (g *Gateway) forward(ctx context.Context, client net.Conn) {
	defer client.Close()
	if err := client.SetReadDeadline(time.Now().Add(g.HandshakeTimeout)); err != nil {
		return
	}
	name, hello, err := serverName(client)
	if err != nil {
		g.Logger.Warn("refused a connection without a TLS server name", "client", client.RemoteAddr(), "error", err)
		return
	}
	if err := client.SetReadDeadline(time.Time{}); err != nil {
		return
	}
	name = strings.ToLower(strings.TrimSuffix(name, "."))
	if !strings.HasSuffix(name, "."+g.Domain) {
		g.Logger.Warn("refused a connection for a name outside the Kafka domain", "name", name, "domain", g.Domain)
		return
	}
	address, err := g.Resolver.Resolve(ctx, name)
	if err != nil {
		g.Logger.Warn("refused a connection", "name", name, "error", err)
		return
	}
	upstream, err := g.Dial(ctx, "tcp", address)
	if err != nil {
		g.Logger.Warn("could not reach the load balancer", "name", name, "address", address, "error", err)
		return
	}
	defer upstream.Close()
	if _, err := upstream.Write(hello); err != nil {
		return
	}
	g.Logger.Debug("forwarding", "name", name, "address", address)
	pipe(client, upstream)
}

// pipe copies both ways until either side is done, then closes both.
func pipe(a, b net.Conn) {
	done := make(chan struct{}, 2)
	copyTo := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		// Half-close so that the other side sees the end, where possible.
		if tcp, ok := dst.(interface{ CloseWrite() error }); ok {
			_ = tcp.CloseWrite()
		}
		done <- struct{}{}
	}
	go copyTo(a, b)
	go copyTo(b, a)
	<-done
	// Closing both ends the other copy; their errors change nothing.
	_ = a.Close()
	_ = b.Close()
	<-done
}

// errHelloRead stops the handshake once the ClientHello has been read.
var errHelloRead = errors.New("client hello read")

// serverName reads the TLS ClientHello from conn and returns the server name
// it asks for, and the bytes read, which the load balancer must get first.
// It lets crypto/tls parse the handshake, and stops it before answering.
func serverName(conn net.Conn) (string, []byte, error) {
	var read bytes.Buffer
	var name string
	err := tls.Server(readOnly{io.TeeReader(conn, &read)}, &tls.Config{
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			name = hello.ServerName
			return nil, errHelloRead
		},
	}).Handshake()
	if !errors.Is(err, errHelloRead) {
		return "", nil, fmt.Errorf("read the TLS client hello: %w", err)
	}
	if name == "" {
		return "", nil, errors.New("the client sent no server name")
	}
	return name, read.Bytes(), nil
}

// readOnly is a connection that can only be read: crypto/tls must not answer
// the client, which the load balancer does.
type readOnly struct{ r io.Reader }

func (c readOnly) Read(p []byte) (int, error)     { return c.r.Read(p) }
func (readOnly) Write([]byte) (int, error)        { return 0, io.ErrClosedPipe }
func (readOnly) Close() error                     { return nil }
func (readOnly) LocalAddr() net.Addr              { return nil }
func (readOnly) RemoteAddr() net.Addr             { return nil }
func (readOnly) SetDeadline(time.Time) error      { return nil }
func (readOnly) SetReadDeadline(time.Time) error  { return nil }
func (readOnly) SetWriteDeadline(time.Time) error { return nil }
