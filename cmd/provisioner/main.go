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
  credentials  print a client.properties for connecting as the customer's admin user
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
	case "credentials":
		return runCredentials(ctx, args, logger, stdout, stderr)
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
	fs.DurationVar(&c.timeout, "timeout", 10*time.Minute, "how long the command and -wait may take")
}

// clusterFlags select the profile and platform of a customer's cluster, and
// optionally other images than the platform's, such as the content-tagged
// local builds the dev scripts pass so that rebuilt images roll the pods.
type clusterFlags struct {
	profile    string
	platform   string
	kafkaImage string
	agentImage string
}

func (c *clusterFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&c.profile, "profile", provisioning.DevProfile.Name, "cluster size profile: dev, dev-ha or production")
	fs.StringVar(&c.platform, "platform", provisioning.KindPlatform.Name, "where the cluster runs: kind, eks-dev or eks-prod")
	fs.StringVar(&c.kafkaImage, "kafka-image", "", "broker image to use instead of the platform's")
	fs.StringVar(&c.agentImage, "agent-image", "", "Kafka agent image to use instead of the platform's")
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
	if c.kafkaImage != "" {
		platform.KafkaImage = c.kafkaImage
	}
	if c.agentImage != "" {
		platform.AgentImage = c.agentImage
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
	if err := connectAWS(ctx, provisioner, spec); err != nil {
		return err
	}
	// Provision itself waits for the listener certificate.
	ctx, cancel := context.WithTimeout(ctx, common.timeout)
	defer cancel()
	if err := provisioner.Provision(ctx, spec); err != nil {
		return err
	}
	if !common.wait {
		return nil
	}
	return provisioner.WaitReady(ctx, spec)
}

func runDeprovision(ctx context.Context, args []string, logger *slog.Logger, stderr io.Writer) error {
	fs := flag.NewFlagSet("deprovision", flag.ContinueOnError)
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
	if err := connectAWS(ctx, provisioner, spec); err != nil {
		return err
	}
	if err := provisioner.Deprovision(ctx, spec); err != nil {
		return err
	}
	if !common.wait {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, common.timeout)
	defer cancel()
	return provisioner.WaitDeleted(ctx, spec)
}

func runCredentials(ctx context.Context, args []string, logger *slog.Logger, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("credentials", flag.ContinueOnError)
	fs.SetOutput(stderr)
	customer := fs.String("customer", "", "customer ID, for example acme (required)")
	kubeContext := fs.String("context", "", "kubeconfig context to use (default: the current context)")
	var cluster clusterFlags
	cluster.register(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	spec, err := cluster.spec(*customer)
	if err != nil {
		return err
	}
	if err := spec.Validate(); err != nil {
		return err
	}
	provisioner, err := newProvisioner(*kubeContext, logger)
	if err != nil {
		return err
	}
	properties, err := provisioner.ClientProperties(ctx, spec)
	if err != nil {
		return err
	}
	_, err = io.WriteString(stdout, properties)
	return err
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

// connectAWS gives the provisioner access to EKS Pod Identity when the
// platform's brokers get their credentials from it. The EKS cluster runs in
// the same region as its tiered storage bucket.
func connectAWS(ctx context.Context, provisioner *provisioning.Provisioner, spec provisioning.Spec) error {
	if spec.Platform.TieredStorage.PodIdentity == nil {
		return nil
	}
	podIdentities, err := provisioning.NewAWSPodIdentities(ctx, spec.Platform.TieredStorage.Region)
	if err != nil {
		return err
	}
	provisioner.PodIdentities = podIdentities
	return nil
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
