// Command devuser makes sure a developer can sign in to the console on kind
// without manual steps (internal/devuser). hack/dev-user.sh runs it from
// just dev-up with the Clerk keys of .env.
//
// Usage: CLERK_SECRET_KEY=sk_test_... devuser -developer rene -staff-org org_... [-demo-org "Demo (rene)"]
//
// It prints what it did, then the demo organization's ID as
// demo_organization=<id> for the script.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ReneP92/changeport/internal/devuser"
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
	fs := flag.NewFlagSet("devuser", flag.ContinueOnError)
	developer := fs.String("developer", "", "the developer's name, such as rene (required)")
	staff := fs.String("staff-org", os.Getenv("CHANGEPORT_STAFF_ORG_ID"), "the staff organization's ID")
	demo := fs.String("demo-org", "", "the name of the developer's organization for the local demo customer, if any")
	if err := fs.Parse(args); err != nil {
		return err
	}
	secretKey := os.Getenv("CLERK_SECRET_KEY")
	if err := devuser.CheckSecretKey(secretKey); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	result, err := devuser.Ensure(ctx, devuser.NewAPI(secretKey), devuser.Options{
		Developer:           *developer,
		StaffOrganizationID: *staff,
		DemoOrganization:    *demo,
	})
	if err != nil {
		return err
	}
	if len(result.Changes) == 0 {
		fmt.Println("everything was already set up")
	}
	for _, change := range result.Changes {
		fmt.Println(change)
	}
	if result.DemoOrganizationID != "" {
		fmt.Printf("demo_organization=%s\n", result.DemoOrganizationID)
	}
	return nil
}
