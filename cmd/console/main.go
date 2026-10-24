// Command console serves the Changeport console: the API of
// api/openapi.yaml behind Clerk sign-in, and the web app.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/clusters"
	"github.com/ReneP92/changeport/internal/console"
	"github.com/ReneP92/changeport/internal/console/ui"
	"github.com/ReneP92/changeport/internal/metrics"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:])
	stop()
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("console", flag.ContinueOnError)
	listen := fs.String("listen", ":8080", "address to serve on")
	metricsURL := fs.String("metrics-url", envOr("CHANGEPORT_METRICS_URL", "http://victoria-metrics.metrics.svc:8428"),
		"address of the metrics store (VictoriaMetrics)")
	kubeContext := fs.String("context", "", "kubeconfig context to use (default: the current context, or the pod's service account in a cluster)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{CurrentContext: *kubeContext},
	).ClientConfig()
	if err != nil {
		return fmt.Errorf("load kubeconfig: %w", err)
	}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return err
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		return err
	}
	kube, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("connect to the cluster: %w", err)
	}

	metricsClient, err := metrics.NewClient(*metricsURL)
	if err != nil {
		return err
	}
	kafka := clusters.NewPool(console.KubeSecrets{Client: kube}, logger)
	defer kafka.Close()

	server, err := console.New(console.Config{
		PublishableKey:      os.Getenv("CLERK_PUBLISHABLE_KEY"),
		SecretKey:           os.Getenv("CLERK_SECRET_KEY"),
		StaffOrganizationID: os.Getenv("CHANGEPORT_STAFF_ORG_ID"),
		Origins:             splitList(os.Getenv("CHANGEPORT_CONSOLE_ORIGINS")),
	}, console.Options{
		Customers: console.KubeCustomers{Client: kube},
		Clusters:  console.KubeClusters{Client: kube, Kafka: kafka},
		Metrics:   metricsClient,
		UI:        ui.FS(),
		Logger:    logger,
	})
	if err != nil {
		return err
	}
	go server.WarmUp(ctx)
	httpServer := &http.Server{
		Addr:              *listen,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	served := make(chan error, 1)
	go func() { served <- httpServer.ListenAndServe() }()
	logger.Info("console serving", "address", *listen)
	select {
	case err := <-served:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return httpServer.Shutdown(shutdown)
}

// envOr returns the environment variable name, or fallback when it is unset.
func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// splitList splits a comma-separated list, dropping empty items.
func splitList(s string) []string {
	var items []string
	for item := range strings.SplitSeq(s, ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	return items
}
