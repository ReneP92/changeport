// Package devuser makes sure a developer can sign in to the console on kind
// without manual steps: a Clerk test user, <developer>+clerk_test@example.com
// (which Clerk development instances verify with the code 424242), that is an
// admin of the staff organization, so it sees the operator console, and of
// an organization of its own for the local demo customer, so it sees a
// customer's side too. Ensure creates what is missing and changes nothing
// that is already right.
package devuser

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// AdminRole is Clerk's role of organization admins.
const AdminRole = "org:admin"

// Clerk is what Ensure needs of Clerk's Backend API.
type Clerk interface {
	// FindUser returns the ID of the user with email, or "".
	FindUser(ctx context.Context, email string) (string, error)
	// CreateUser creates a user with email and no password.
	CreateUser(ctx context.Context, email, firstName, lastName string) (string, error)
	// FindOrganization returns the ID of the organization named exactly
	// name, or "".
	FindOrganization(ctx context.Context, name string) (string, error)
	// CreateOrganization creates an organization without members.
	CreateOrganization(ctx context.Context, name string) (string, error)
	// Role returns the user's role in the organization, or "" if the user is
	// no member.
	Role(ctx context.Context, organizationID, userID string) (string, error)
	// AddMember makes the user a member of the organization with role.
	AddMember(ctx context.Context, organizationID, userID, role string) error
	// SetRole changes a member's role.
	SetRole(ctx context.Context, organizationID, userID, role string) error
}

// Options say whom to set up.
type Options struct {
	// Developer names the developer, such as rene; the email address and
	// the demo organization's name come from it.
	Developer string
	// StaffOrganizationID is the organization of Changeport's operators.
	StaffOrganizationID string
	// DemoOrganization, when set, is the name of the developer's
	// organization for the local demo customer.
	DemoOrganization string
}

// Result says what Ensure found and what it created or changed.
type Result struct {
	Email  string
	UserID string
	// DemoOrganizationID is the developer's demo organization, if asked for.
	DemoOrganizationID string
	// Changes lists what Ensure did, such as "created the user"; empty when
	// everything was already in place.
	Changes []string
}

// ErrNotDevelopment means the secret key is not of a development instance,
// whose test addresses alone take the fixed code.
var ErrNotDevelopment = errors.New("not a Clerk development instance")

var developerName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

// Email is the developer's test address.
func Email(developer string) string { return developer + "+clerk_test@example.com" }

// CheckSecretKey refuses keys of production instances: dev users belong in
// development instances only.
func CheckSecretKey(secretKey string) error {
	if !strings.HasPrefix(secretKey, "sk_test_") {
		return fmt.Errorf("the secret key is %w: dev users go to development instances (sk_test_...) only", ErrNotDevelopment)
	}
	return nil
}

// Ensure makes sure the developer's test user exists and is an admin of the
// staff organization and, if asked for, of the developer's demo
// organization, which it creates if missing.
func Ensure(ctx context.Context, clerk Clerk, opts Options) (Result, error) {
	if !developerName.MatchString(opts.Developer) {
		return Result{}, fmt.Errorf("developer name %q must be lowercase letters, digits, dots, underscores and dashes", opts.Developer)
	}
	if opts.StaffOrganizationID == "" {
		return Result{}, errors.New("no staff organization")
	}
	result := Result{Email: Email(opts.Developer)}

	userID, err := clerk.FindUser(ctx, result.Email)
	if err != nil {
		return result, fmt.Errorf("find user %s: %w", result.Email, err)
	}
	if userID == "" {
		if userID, err = clerk.CreateUser(ctx, result.Email, "Dev", opts.Developer); err != nil {
			return result, fmt.Errorf("create user %s: %w", result.Email, err)
		}
		result.Changes = append(result.Changes, "created the user")
	}
	result.UserID = userID

	if err := ensureAdmin(ctx, clerk, opts.StaffOrganizationID, "the staff organization", userID, &result); err != nil {
		return result, err
	}

	if opts.DemoOrganization != "" {
		organizationID, err := clerk.FindOrganization(ctx, opts.DemoOrganization)
		if err != nil {
			return result, fmt.Errorf("find organization %q: %w", opts.DemoOrganization, err)
		}
		if organizationID == "" {
			if organizationID, err = clerk.CreateOrganization(ctx, opts.DemoOrganization); err != nil {
				return result, fmt.Errorf("create organization %q: %w", opts.DemoOrganization, err)
			}
			result.Changes = append(result.Changes, fmt.Sprintf("created the organization %q", opts.DemoOrganization))
		}
		result.DemoOrganizationID = organizationID
		if err := ensureAdmin(ctx, clerk, organizationID, fmt.Sprintf("%q", opts.DemoOrganization), userID, &result); err != nil {
			return result, err
		}
	}
	return result, nil
}

// ensureAdmin makes the user an admin of the organization, adding them or
// raising their role as needed.
func ensureAdmin(ctx context.Context, clerk Clerk, organizationID, organization, userID string, result *Result) error {
	role, err := clerk.Role(ctx, organizationID, userID)
	if err != nil {
		return fmt.Errorf("look up the user's role in %s: %w", organization, err)
	}
	switch role {
	case AdminRole:
		return nil
	case "":
		if err := clerk.AddMember(ctx, organizationID, userID, AdminRole); err != nil {
			return fmt.Errorf("add the user to %s: %w", organization, err)
		}
		result.Changes = append(result.Changes, "made the user an admin of "+organization)
	default:
		if err := clerk.SetRole(ctx, organizationID, userID, AdminRole); err != nil {
			return fmt.Errorf("make the user an admin of %s: %w", organization, err)
		}
		result.Changes = append(result.Changes, fmt.Sprintf("raised the user's role in %s from %s to admin", organization, role))
	}
	return nil
}
