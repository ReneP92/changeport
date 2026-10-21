// Command provisioner creates and removes dedicated customer Kafka clusters.
//
// It is the stub of the provisioning service: today a CLI, later driven by
// the Changeport API.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/tools/clientcmd"

	"github.com/ReneP92/changeport/internal/provisioning"
)

const usage = `Usage: provisioner <command> [flags]

Commands:
  provision    create or update a customer's Kafka cluster
  deprovision  delete a customer's Kafka cluster and namespace
  render       print the resources for a customer without applying them

Run "provisioner <command> -h" for the flags of a command.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return flag.ErrHelp
	}
	logger := slog.New(slog.NewTextHandler(stderr, nil))
	command, args := args[0], args[1:]
	switch command {
	case "provision":
		return runProvision(ctx, args, logger, stderr)
	case "deprovision":
		return runDeprovision(ctx, args, logger, stderr)
	case "render":
		return runRender(args, stdout, stderr)
	case "-h", "-help", "--help", "help":
		fmt.Fprint(stdout, usage)
		return nil
	default:
		fmt.Fprint(stderr, usage)
		return fmt.Errorf("unknown command %q", command)
	}
}

type commonFlags struct {
	customer    string
	kubeContext string
	wait        bool
	timeout     time.Duration
}

func (c *commonFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&c.customer, "customer", "", "customer ID, for example acme (required)")
	fs.StringVar(&c.kubeContext, "context", "", "kubeconfig context to use (default: the current context)")
	fs.BoolVar(&c.wait, "wait", false, "wait until the change has completed")
	fs.DurationVar(&c.timeout, "timeout", 10*time.Minute, "how long -wait waits")
}

// clusterFlags select the profile and platform of a customer's cluster.
type clusterFlags struct {
	profile  string
	platform string
}

func (c *clusterFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&c.profile, "profile", provisioning.DevProfile.Name, "cluster size profile: dev or production")
	fs.StringVar(&c.platform, "platform", provisioning.KindPlatform.Name, "where the cluster runs: kind, eks-dev or eks-prod")
}

func (c *clusterFlags) spec(customer string) (provisioning.Spec, error) {
	profile, err := provisioning.ProfileByName(c.profile)
	if err != nil {
		return provisioning.Spec{}, err
	}
	platform, err := provisioning.PlatformByName(c.platform)
	if err != nil {
		return provisioning.Spec{}, err
	}
	return provisioning.Spec{CustomerID: customer, Profile: profile, Platform: platform}, nil
}

func runProvision(ctx context.Context, args []string, logger *slog.Logger, stderr io.Writer) error {
	fs := flag.NewFlagSet("provision", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var common commonFlags
	common.register(fs)
	var cluster clusterFlags
	cluster.register(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	spec, err := cluster.spec(common.customer)
	if err != nil {
		return err
	}
	if err := spec.Validate(); err != nil {
		return err
	}
	provisioner, err := newProvisioner(common.kubeContext, logger)
	if err != nil {
		return err
	}
	if err := provisioner.Provision(ctx, spec); err != nil {
		return err
	}
	if !common.wait {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, common.timeout)
	defer cancel()
	return provisioner.WaitReady(ctx, spec)
}

func runDeprovision(ctx context.Context, args []string, logger *slog.Logger, stderr io.Writer) error {
	fs := flag.NewFlagSet("deprovision", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var common commonFlags
	common.register(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := provisioning.ValidateCustomerID(common.customer); err != nil {
		return err
	}
	provisioner, err := newProvisioner(common.kubeContext, logger)
	if err != nil {
		return err
	}
	if err := provisioner.Deprovision(ctx, common.customer); err != nil {
		return err
	}
	if !common.wait {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, common.timeout)
	defer cancel()
	return provisioner.WaitDeleted(ctx, common.customer)
}

func runRender(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("render", flag.ContinueOnError)
	fs.SetOutput(stderr)
	customer := fs.String("customer", "", "customer ID, for example acme (required)")
	var cluster clusterFlags
	cluster.register(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	spec, err := cluster.spec(*customer)
	if err != nil {
		return err
	}
	rendered, err := provisioning.RenderYAML(spec)
	if err != nil {
		return err
	}
	_, err = stdout.Write(rendered)
	return err
}

func newProvisioner(kubeContext string, logger *slog.Logger) (*provisioning.Provisioner, error) {
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{CurrentContext: kubeContext},
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	cluster, err := provisioning.NewKubeCluster(config)
	if err != nil {
		return nil, err
	}
	return provisioning.New(cluster, logger), nil
}
