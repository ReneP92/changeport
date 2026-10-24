package console

import (
	"context"
	"errors"
	"fmt"

	"github.com/clerk/clerk-sdk-go/v2"
	"github.com/clerk/clerk-sdk-go/v2/organization"
	"github.com/clerk/clerk-sdk-go/v2/organizationinvitation"
)

// Organizations manages customers' Clerk organizations, whose members sign
// in to the console as the customer.
type Organizations interface {
	// Create creates an organization named name, without members, and
	// returns its ID.
	Create(ctx context.Context, name string) (string, error)
	// InviteAdmin invites email to join the organization as an admin, or
	// returns an error satisfying errors.Is(err, ErrInvitationRefused) with
	// Clerk's reason.
	InviteAdmin(ctx context.Context, organizationID, email string) error
	// Delete deletes an organization.
	Delete(ctx context.Context, organizationID string) error
}

// ErrInvitationRefused means Clerk refused to invite an address, such as one
// it considers invalid.
var ErrInvitationRefused = errors.New("clerk refused the invitation")

// adminRole is Clerk's role of organization admins.
const adminRole = "org:admin"

// ClerkOrganizations manages organizations through Clerk's Backend API.
type ClerkOrganizations struct {
	organizations *organization.Client
	invitations   *organizationinvitation.Client
	// redirectURL is where an invitation's link leads once accepted: the
	// console.
	redirectURL string
}

var _ Organizations = ClerkOrganizations{}

// NewClerkOrganizations returns Organizations that call Clerk's Backend API
// with secretKey, whose invitations lead to the console at redirectURL.
func NewClerkOrganizations(secretKey, redirectURL string) ClerkOrganizations {
	config := &clerk.ClientConfig{BackendConfig: clerk.BackendConfig{Key: &secretKey}}
	return ClerkOrganizations{
		organizations: organization.NewClient(config),
		invitations:   organizationinvitation.NewClient(config),
		redirectURL:   redirectURL,
	}
}

// Create implements Organizations.
func (c ClerkOrganizations) Create(ctx context.Context, name string) (string, error) {
	created, err := c.organizations.Create(ctx, &organization.CreateParams{Name: &name})
	if err != nil {
		return "", fmt.Errorf("create the Clerk organization %q: %w", name, err)
	}
	return created.ID, nil
}

// InviteAdmin implements Organizations.
func (c ClerkOrganizations) InviteAdmin(ctx context.Context, organizationID, email string) error {
	params := &organizationinvitation.CreateParams{
		OrganizationID: organizationID,
		EmailAddress:   &email,
		Role:           clerk.String(adminRole),
	}
	if c.redirectURL != "" {
		params.RedirectURL = &c.redirectURL
	}
	_, err := c.invitations.Create(ctx, params)
	var refused *clerk.APIErrorResponse
	if errors.As(err, &refused) && refused.HTTPStatusCode >= 400 && refused.HTTPStatusCode < 500 {
		reason := "Clerk refused the address"
		if len(refused.Errors) > 0 {
			reason = refused.Errors[0].Message
			if refused.Errors[0].LongMessage != "" {
				reason = refused.Errors[0].LongMessage
			}
		}
		return fmt.Errorf("%w: %s", ErrInvitationRefused, reason)
	}
	if err != nil {
		return fmt.Errorf("invite %s to Clerk organization %s: %w", email, organizationID, err)
	}
	return nil
}

// Delete implements Organizations.
func (c ClerkOrganizations) Delete(ctx context.Context, organizationID string) error {
	if _, err := c.organizations.Delete(ctx, organizationID); err != nil {
		return fmt.Errorf("delete Clerk organization %s: %w", organizationID, err)
	}
	return nil
}
