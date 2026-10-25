// Command agent keeps the settings the platform owns in place on one
// customer's Kafka cluster, turning on tiered storage for every topic that
// can use it and setting up client telemetry (KIP-714), and measures each
// consumer group's lag in seconds. It runs next to the cluster, connects to
// the internal mTLS listener as the changeport-agent KafkaUser, and serves its
// metrics and health on -listen.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ReneP92/changeport/internal/agent"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stderr)
	stop()
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type options struct {
	bootstrap string
	cert, key string
	ca        string
	interval  time.Duration
	listen    string
	// clientMetricsInterval is how often clients push their metrics.
	clientMetricsInterval time.Duration
}

func run(ctx context.Context, args []string, stderr io.Writer) error {
	fs := flag.NewFlagSet("agent", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o options
	fs.StringVar(&o.bootstrap, "bootstrap", "", "bootstrap address of the internal mTLS listener (required)")
	fs.StringVar(&o.cert, "cert", "/etc/changeport/agent-user/user.crt", "client certificate of the agent's KafkaUser")
	fs.StringVar(&o.key, "key", "/etc/changeport/agent-user/user.key", "private key of the agent's KafkaUser")
	fs.StringVar(&o.ca, "ca", "/etc/changeport/cluster-ca/ca.crt", "the Kafka cluster's CA certificate")
	fs.DurationVar(&o.interval, "interval", 30*time.Second, "how often to reconcile")
	fs.DurationVar(&o.clientMetricsInterval, "client-metrics-interval", time.Minute, "how often clients push their metrics to the brokers")
	fs.StringVar(&o.listen, "listen", ":8080", "address to serve /metrics, /healthz and /readyz on")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if o.bootstrap == "" {
		return errors.New("-bootstrap is required")
	}
	logger := slog.New(slog.NewJSONHandler(stderr, nil))

	tlsConfig, err := clientTLS(o.cert, o.key, o.ca)
	if err != nil {
		return err
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(o.bootstrap), kgo.DialTLSConfig(tlsConfig))
	if err != nil {
		return fmt.Errorf("create Kafka client: %w", err)
	}
	defer client.Close()

	registry := prometheus.NewRegistry()
	admin := kadm.NewClient(client)
	reconciler := &agent.Reconciler{Admin: admin, Logger: logger, Metrics: agent.NewMetrics(registry)}
	lag := agent.NewLagCollector(admin, registry)
	telemetry := agent.NewTelemetry(client, o.clientMetricsInterval, logger, registry)
	health := &health{started: time.Now(), stale: 10 * o.interval}
	server := &http.Server{Addr: o.listen, Handler: health.routes(registry), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serving metrics and health failed", "error", err)
		}
	}()

	logger.Info("agent started", "bootstrap", o.bootstrap, "interval", o.interval.String())
	// Lag is measured in its own loop, so that a slow measurement never delays
	// the tiering reconciliation; its failures do not affect health.
	go func() {
		ticker := time.NewTicker(o.interval)
		defer ticker.Stop()
		for {
			if err := lag.Measure(ctx); err != nil && ctx.Err() == nil {
				logger.Error("measuring consumer lag failed", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()
	for {
		// Client telemetry only adds observability, so its failures do not
		// affect health either.
		if err := telemetry.Reconcile(ctx); err != nil && ctx.Err() == nil {
			logger.Error("keeping client telemetry in place failed", "error", err)
		}
		if err := reconciler.Reconcile(ctx); err == nil {
			health.succeeded()
		} else if ctx.Err() == nil {
			logger.Error("reconcile failed", "error", err)
		}
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return server.Shutdown(shutdown)
		case <-ticker.C:
		}
	}
}

// clientTLS trusts the cluster CA and presents the agent's client
// certificate, read again on every handshake so that certificates the
// Strimzi User Operator renews take effect without a restart.
func clientTLS(certFile, keyFile, caFile string) (*tls.Config, error) {
	caPEM, err := os.ReadFile(filepath.Clean(caFile))
	if err != nil {
		return nil, fmt.Errorf("read cluster CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("no certificates in %s", caFile)
	}
	if _, err := tls.LoadX509KeyPair(certFile, keyFile); err != nil {
		return nil, fmt.Errorf("load client certificate: %w", err)
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			return &cert, err
		},
	}, nil
}

// health reports the agent ready once a reconciliation has succeeded, and
// unhealthy when none has for stale, so that Kubernetes restarts an agent
// that is stuck, for example with a CA it no longer trusts.
type health struct {
	started     time.Time
	stale       time.Duration
	lastSuccess atomic.Int64
}

func (h *health) succeeded() { h.lastSuccess.Store(time.Now().UnixNano()) }

func (h *health) routes(registry *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if h.lastSuccess.Load() == 0 {
			http.Error(w, "no reconciliation has succeeded yet", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		last := h.started
		if ns := h.lastSuccess.Load(); ns != 0 {
			last = time.Unix(0, ns)
		}
		if time.Since(last) > h.stale {
			http.Error(w, "no reconciliation has succeeded since "+last.UTC().Format(time.RFC3339), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	return mux
}
