// Command hostgateway lets clients on the developer's machine reach the
// customers' Kafka clusters on kind (internal/hostgateway). hack/lib.sh runs
// it as a container on the kind network, with its ports published on the
// host's loopback address: Kafka on 9094, and DNS for the Kafka domain on
// 15353, which the host's resolver uses after just host-dns.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"syscall"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ReneP92/changeport/internal/hostgateway"
	"github.com/ReneP92/changeport/internal/provisioning"
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
	fs := flag.NewFlagSet("hostgateway", flag.ContinueOnError)
	listen := fs.String("listen", ":9094", "address to accept Kafka clients on")
	dnsListen := fs.String("dns", ":15353", "UDP address to answer DNS queries on")
	domain := fs.String("domain", provisioning.KindPlatform.KafkaDomain, "the Kafka domain whose names the gateway serves")
	address := fs.String("address", "127.0.0.1", "the IPv4 address DNS answers with: where the host reaches the gateway")
	if err := fs.Parse(args); err != nil {
		return err
	}
	answer, err := netip.ParseAddr(*address)
	if err != nil || !answer.Is4() {
		return fmt.Errorf("-address %q is not an IPv4 address", *address)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return fmt.Errorf("load kubeconfig: %w", err)
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		return err
	}
	kube, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("connect to the cluster: %w", err)
	}

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	packets, err := net.ListenPacket("udp", *dnsListen)
	if err != nil {
		return errors.Join(err, listener.Close())
	}
	gateway := hostgateway.New(*domain, hostgateway.NewKubeResolver(kube), logger)
	dns := &hostgateway.DNS{Domain: *domain, Address: answer.As4(), Logger: logger}
	logger.Info("serving", "kafka", *listen, "dns", *dnsListen, "domain", *domain)
	// Either server stopping stops the other: each closes its listener once
	// the context is done.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	served := make(chan error, 2)
	go func() { served <- gateway.Serve(ctx, listener) }()
	go func() { served <- dns.Serve(ctx, packets) }()
	err = <-served
	cancel()
	return errors.Join(err, <-served)
}
