// Command registry-gateway serves a customer's schema registry to the
// customer's clients on the internet, over TLS on the cluster's certificate,
// for the customer's Kafka credentials (internal/registrygateway).
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ReneP92/changeport/internal/registrygateway"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], logger)
	stop()
	if err != nil {
		logger.Error("registry gateway failed", "error", err.Error())
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, logger *slog.Logger) error {
	fs := flag.NewFlagSet("registry-gateway", flag.ContinueOnError)
	listen := fs.String("listen", ":8443", "address of the TLS listener customers reach")
	healthListen := fs.String("health-listen", ":8080", "address of /healthz, for the kubelet")
	upstream := fs.String("upstream", "", "the registry's Confluent-compatible API, such as http://changeport-registry:8080/apis/ccompat/v7 (required)")
	username := fs.String("username", "admin", "the customer's Kafka user")
	passwordFile := fs.String("password-file", "/etc/changeport/kafka-user/password", "the file holding the Kafka user's password")
	certFile := fs.String("cert-file", "/etc/changeport/tls/tls.crt", "the TLS certificate")
	keyFile := fs.String("key-file", "/etc/changeport/tls/tls.key", "the TLS certificate's key")
	sources := fs.String("sources", "", "comma-separated names of the customer's Postgres sources, whose schemas customers may not change")
	if err := fs.Parse(args); err != nil {
		return err
	}
	upstreamURL, err := url.Parse(*upstream)
	if err != nil || upstreamURL.Scheme == "" || upstreamURL.Host == "" {
		return fmt.Errorf("-upstream %q is not a URL", *upstream)
	}
	var sourceNames []string
	for name := range strings.SplitSeq(*sources, ",") {
		if name != "" {
			sourceNames = append(sourceNames, name)
		}
	}
	gateway := registrygateway.New(registrygateway.Config{
		Upstream: upstreamURL, Username: *username, PasswordFile: *passwordFile, Sources: sourceNames, Logger: logger,
	})
	certificate := &registrygateway.Certificate{CertFile: *certFile, KeyFile: *keyFile}
	if _, err := certificate.Get(nil); err != nil {
		return err
	}
	server := &http.Server{
		Addr:              *listen,
		Handler:           gateway,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: certificate.Get},
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	health := &http.Server{
		Addr: *healthListen,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/healthz" {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write([]byte("ok\n"))
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errs := make(chan error, 2)
	go func() { errs <- server.ListenAndServeTLS("", "") }()
	go func() { errs <- health.ListenAndServe() }()
	logger.Info("registry gateway listening", "address", *listen, "upstream", upstreamURL.String(), "sources", sourceNames)
	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	err = errors.Join(server.Shutdown(shutdown), health.Shutdown(shutdown))
	return err
}
