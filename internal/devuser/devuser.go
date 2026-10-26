// Package devuser makes sure a developer can sign in to the console on kind
// without manual steps, as either side of it, with Clerk test addresses,
// which Clerk development instances verify with the code 424242:
//
//   - the operator, <developer>+clerk_test@example.com, an admin of the staff
//     organization, who sees the operator console; and
//   - the demo customer's admin, <developer>-demo+clerk_test@example.com, an
//     admin of an organization of the developer's own that is linked to the
//     local demo customer, who sees exactly what a customer's admin sees.
//
// Each is a member of its own organization only. Like customers, neither may
// create organizations: organizations are customers, which operators create
// in the operator console. Ensure creates what is missing and changes nothing
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
	// FindUser returns the user with email, or nil.
	FindUser(ctx context.Context, email string) (*User, error)
	// CreateUser creates a user with email and no password, who may not
	// create organizations.
	CreateUser(ctx context.Context, email, firstName, lastName string) (string, error)
	// DisallowOrganizations stops the user from creating organizations.
	DisallowOrganizations(ctx context.Context, userID string) error
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
	// RemoveMember takes the user out of the organization.
	RemoveMember(ctx context.Context, organizationID, userID string) error
}

// User is a Clerk user.
type User struct {
	ID string
	// CanCreateOrganizations says whether the user may create organizations,
	// which Clerk then offers in the organization switcher.
	CanCreateOrganizations bool
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

// Account is a user Ensure set up.
type Account struct {
	Email  string
	UserID string
}

// Result says what Ensure found and what it created or changed.
type Result struct {
	// Operator is the developer's operator.
	Operator Account
	// Customer is the demo customer's admin, if a demo organization was
	// asked for.
	Customer *Account
	// DemoOrganizationID is the developer's demo organization, if asked for.
	DemoOrganizationID string
	// Changes lists what Ensure did, each with the address of the user it
	// concerns, such as "rene+clerk_test@example.com: created the user";
	// empty when everything was already in place.
	Changes []string
}

// ErrNotDevelopment means the secret key is not of a development instance,
// whose test addresses alone take the fixed code.
var ErrNotDevelopment = errors.New("not a Clerk development instance")

var developerName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

// Email is the test address of the developer's operator.
func Email(developer string) string { return developer + "+clerk_test@example.com" }

// CustomerEmail is the test address of the admin of the developer's demo
// customer.
func CustomerEmail(developer string) string { return developer + "-demo+clerk_test@example.com" }

// CheckSecretKey refuses keys of production instances: dev users belong in
// development instances only.
func CheckSecretKey(secretKey string) error {
	if !strings.HasPrefix(secretKey, "sk_test_") {
		return fmt.Errorf("the secret key is %w: dev users go to development instances (sk_test_...) only", ErrNotDevelopment)
	}
	return nil
}

// Ensure makes sure the developer's operator exists and is an admin of the
// staff organization and, if a demo organization is asked for, that it
// exists, which it creates if missing, with the demo customer's admin as its
// admin. Each user is taken out of the other's organization.
func Ensure(ctx context.Context, clerk Clerk, opts Options) (Result, error) {
	if !developerName.MatchString(opts.Developer) {
		return Result{}, fmt.Errorf("developer name %q must be lowercase letters, digits, dots, underscores and dashes", opts.Developer)
	}
	if opts.StaffOrganizationID == "" {
		return Result{}, errors.New("no staff organization")
	}
	var result Result
	e := ensurer{clerk: clerk, result: &result}

	operator, err := e.user(ctx, Email(opts.Developer), "Dev", opts.Developer)
	if err != nil {
		return result, err
	}
	result.Operator = operator
	if err := e.admin(ctx, opts.StaffOrganizationID, "the staff organization", operator); err != nil {
		return result, err
	}
	if opts.DemoOrganization == "" {
		return result, nil
	}

	demo := fmt.Sprintf("%q", opts.DemoOrganization)
	organizationID, err := clerk.FindOrganization(ctx, opts.DemoOrganization)
	if err != nil {
		return result, fmt.Errorf("find organization %s: %w", demo, err)
	}
	if organizationID == "" {
		if organizationID, err = clerk.CreateOrganization(ctx, opts.DemoOrganization); err != nil {
			return result, fmt.Errorf("create organization %s: %w", demo, err)
		}
		result.Changes = append(result.Changes, "created the organization "+demo)
	}
	result.DemoOrganizationID = organizationID

	customer, err := e.user(ctx, CustomerEmail(opts.Developer), "Demo", opts.Developer)
	if err != nil {
		return result, err
	}
	result.Customer = &customer
	if err := e.admin(ctx, organizationID, demo, customer); err != nil {
		return result, err
	}
	if err := e.notMember(ctx, organizationID, demo, operator); err != nil {
		return result, err
	}
	if err := e.notMember(ctx, opts.StaffOrganizationID, "the staff organization", customer); err != nil {
		return result, err
	}
	return result, nil
}

// ensurer makes changes through Clerk and records them in the result.
type ensurer struct {
	clerk  Clerk
	result *Result
}

func (e ensurer) changed(account Account, change string) {
	e.result.Changes = append(e.result.Changes, account.Email+": "+change)
}

// user makes sure the user with email exists and may not create
// organizations.
func (e ensurer) user(ctx context.Context, email, firstName, lastName string) (Account, error) {
	account := Account{Email: email}
	user, err := e.clerk.FindUser(ctx, email)
	if err != nil {
		return account, fmt.Errorf("find user %s: %w", email, err)
	}
	switch {
	case user == nil:
		if account.UserID, err = e.clerk.CreateUser(ctx, email, firstName, lastName); err != nil {
			return account, fmt.Errorf("create user %s: %w", email, err)
		}
		e.changed(account, "created the user")
	case user.CanCreateOrganizations:
		account.UserID = user.ID
		if err := e.clerk.DisallowOrganizations(ctx, user.ID); err != nil {
			return account, fmt.Errorf("stop user %s from creating organizations: %w", email, err)
		}
		e.changed(account, "stopped the user from creating organizations")
	default:
		account.UserID = user.ID
	}
	return account, nil
}

// admin makes the user an admin of the organization, adding them or raising
// their role as needed.
func (e ensurer) admin(ctx context.Context, organizationID, organization string, account Account) error {
	role, err := e.clerk.Role(ctx, organizationID, account.UserID)
	if err != nil {
		return fmt.Errorf("look up the role of %s in %s: %w", account.Email, organization, err)
	}
	switch role {
	case AdminRole:
		return nil
	case "":
		if err := e.clerk.AddMember(ctx, organizationID, account.UserID, AdminRole); err != nil {
			return fmt.Errorf("add %s to %s: %w", account.Email, organization, err)
		}
		e.changed(account, "made the user an admin of "+organization)
	default:
		if err := e.clerk.SetRole(ctx, organizationID, account.UserID, AdminRole); err != nil {
			return fmt.Errorf("make %s an admin of %s: %w", account.Email, organization, err)
		}
		e.changed(account, fmt.Sprintf("raised the user's role in %s from %s to admin", organization, role))
	}
	return nil
}

// notMember takes the user out of the organization if they are in it.
func (e ensurer) notMember(ctx context.Context, organizationID, organization string, account Account) error {
	role, err := e.clerk.Role(ctx, organizationID, account.UserID)
	if err != nil {
		return fmt.Errorf("look up the role of %s in %s: %w", account.Email, organization, err)
	}
	if role == "" {
		return nil
	}
	if err := e.clerk.RemoveMember(ctx, organizationID, account.UserID); err != nil {
		return fmt.Errorf("take %s out of %s: %w", account.Email, organization, err)
	}
	e.changed(account, "took the user out of "+organization)
	return nil
}
