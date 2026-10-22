// Command provisioner creates and removes dedicated customer Kafka clusters:
// as a CLI, and as the controller that turns every Customer resource into its
// cluster.
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

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/controller"
	"github.com/ReneP92/changeport/internal/provisioning"
)

const usage = `Usage: provisioner <command> [flags]

Commands:
  controller   run the controller that provisions every Customer resource
  provision    create or update a Customer, whose Kafka cluster the controller provisions
  deprovision  delete a Customer, whose Kafka cluster the controller deletes first
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
	case "controller":
		return runController(ctx, args, logger, stderr)
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

// platformFlags select the platform clusters run on, and optionally other
// images than the platform's, such as locally built ones.
type platformFlags struct {
	platform   string
	kafkaImage string
	agentImage string
}

func (p *platformFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&p.platform, "platform", provisioning.KindPlatform.Name, "where the clusters run: kind, eks-dev or eks-prod")
	fs.StringVar(&p.kafkaImage, "kafka-image", "", "broker image to use instead of the platform's")
	fs.StringVar(&p.agentImage, "agent-image", "", "Kafka agent image to use instead of the platform's")
}

func (p *platformFlags) resolve() (provisioning.Platform, error) {
	platform, err := provisioning.PlatformByName(p.platform)
	if err != nil {
		return provisioning.Platform{}, err
	}
	if p.kafkaImage != "" {
		platform.KafkaImage = p.kafkaImage
	}
	if p.agentImage != "" {
		platform.AgentImage = p.agentImage
	}
	return platform, nil
}

// clusterFlags select the profile and platform of a customer's cluster.
type clusterFlags struct {
	profile string
	platformFlags
}

func (c *clusterFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&c.profile, "profile", provisioning.DevProfile.Name, "cluster size profile: dev, dev-ha or production")
	c.platformFlags.register(fs)
}

func (c *clusterFlags) spec(customer string) (provisioning.Spec, error) {
	profile, err := provisioning.ProfileByName(c.profile)
	if err != nil {
		return provisioning.Spec{}, err
	}
	platform, err := c.resolve()
	if err != nil {
		return provisioning.Spec{}, err
	}
	return provisioning.Spec{CustomerID: customer, Profile: profile, Platform: platform}, nil
}

// runProvision creates or updates a Customer; the controller provisions its
// cluster.
func runProvision(ctx context.Context, args []string, logger *slog.Logger, stderr io.Writer) error {
	fs := flag.NewFlagSet("provision", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var common commonFlags
	common.register(fs)
	profile := fs.String("profile", provisioning.DevProfile.Name, "cluster size profile: dev, dev-ha or production")
	displayName := fs.String("display-name", "", "the customer's name as people know it (default: the customer ID)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := provisioning.ValidateCustomerID(common.customer); err != nil {
		return err
	}
	if _, err := provisioning.ProfileByName(*profile); err != nil {
		return err
	}
	if *displayName == "" {
		*displayName = common.customer
	}
	c, err := newCustomerClient(common.kubeContext)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, common.timeout)
	defer cancel()
	if err := applyCustomer(ctx, c, common.customer, *displayName, *profile); err != nil {
		return err
	}
	logger.Info("applied the Customer", "tenant", common.customer, "profile", *profile)
	if !common.wait {
		return nil
	}
	return waitReady(ctx, c, common.customer, logger)
}

// runDeprovision deletes a Customer; the controller deletes its cluster
// first.
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
	c, err := newCustomerClient(common.kubeContext)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, common.timeout)
	defer cancel()
	if err := deleteCustomer(ctx, c, common.customer); err != nil {
		return err
	}
	if !common.wait {
		return nil
	}
	return waitGone(ctx, c, common.customer, logger)
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

func runController(ctx context.Context, args []string, logger *slog.Logger, stderr io.Writer) error {
	fs := flag.NewFlagSet("controller", flag.ContinueOnError)
	fs.SetOutput(stderr)
	kubeContext := fs.String("context", "", "kubeconfig context to use (default: the current context, or the pod's service account in a cluster)")
	var platformFlags platformFlags
	platformFlags.register(fs)
	metricsAddress := fs.String("metrics-address", ":8080", `address of the metrics endpoint, or "0" for none`)
	probeAddress := fs.String("probe-address", ":8081", "address of the /healthz and /readyz endpoints")
	leaderElection := fs.Bool("leader-elect", false, "elect a leader, so that only one of several replicas reconciles")
	leaderNamespace := fs.String("leader-election-namespace", "", "namespace of the leader election lease (default: the pod's own)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	platform, err := platformFlags.resolve()
	if err != nil {
		return err
	}
	config, err := restConfig(*kubeContext)
	if err != nil {
		return err
	}
	cluster, err := provisioning.NewKubeCluster(config)
	if err != nil {
		return err
	}
	provisioner := provisioning.New(cluster, logger)
	if identity := platform.TieredStorage.PodIdentity; identity != nil {
		if provisioner.PodIdentities, err = provisioning.NewAWSPodIdentities(ctx, platform.TieredStorage.Region); err != nil {
			return err
		}
	}

	ctrl.SetLogger(logr.FromSlogHandler(logger.Handler()))
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return err
	}
	mgr, err := ctrl.NewManager(config, ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: *metricsAddress},
		HealthProbeBindAddress:  *probeAddress,
		LeaderElection:          *leaderElection,
		LeaderElectionID:        "changeport-provisioner",
		LeaderElectionNamespace: *leaderNamespace,
		// The new leader starts right away when the old one shuts down.
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		return fmt.Errorf("create the controller manager: %w", err)
	}
	reconciler := &controller.CustomerReconciler{
		Client:         mgr.GetClient(),
		Provisioner:    provisioner,
		Platform:       platform,
		PollInterval:   10 * time.Second,
		ResyncInterval: 10 * time.Minute,
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		return err
	}
	if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("ping", healthz.Ping); err != nil {
		return err
	}
	logger.Info("controller starting", "platform", platform.Name, "kafkaImage", platform.KafkaImage, "agentImage", platform.AgentImage)
	return mgr.Start(ctx)
}

func newProvisioner(kubeContext string, logger *slog.Logger) (*provisioning.Provisioner, error) {
	config, err := restConfig(kubeContext)
	if err != nil {
		return nil, err
	}
	cluster, err := provisioning.NewKubeCluster(config)
	if err != nil {
		return nil, err
	}
	return provisioning.New(cluster, logger), nil
}

// restConfig connects to the cluster of kubeContext in the kubeconfig, or,
// without a kubeconfig in a pod, to the pod's own cluster.
func restConfig(kubeContext string) (*rest.Config, error) {
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{CurrentContext: kubeContext},
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	return config, nil
}
