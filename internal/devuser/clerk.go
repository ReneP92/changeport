package devuser

import (
	"context"

	"github.com/clerk/clerk-sdk-go/v2"
	"github.com/clerk/clerk-sdk-go/v2/organization"
	"github.com/clerk/clerk-sdk-go/v2/organizationmembership"
	"github.com/clerk/clerk-sdk-go/v2/user"
)

// API implements Clerk with Clerk's Backend API.
type API struct {
	users         *user.Client
	organizations *organization.Client
	memberships   *organizationmembership.Client
}

var _ Clerk = API{}

// NewAPI returns an API that calls Clerk with secretKey.
func NewAPI(secretKey string) API {
	config := &clerk.ClientConfig{BackendConfig: clerk.BackendConfig{Key: &secretKey}}
	return API{
		users:         user.NewClient(config),
		organizations: organization.NewClient(config),
		memberships:   organizationmembership.NewClient(config),
	}
}

// FindUser implements Clerk.
func (a API) FindUser(ctx context.Context, email string) (*User, error) {
	list, err := a.users.List(ctx, &user.ListParams{EmailAddresses: []string{email}})
	if err != nil || len(list.Users) == 0 {
		return nil, err
	}
	found := list.Users[0]
	return &User{ID: found.ID, CanCreateOrganizations: found.CreateOrganizationEnabled}, nil
}

// CreateUser implements Clerk.
func (a API) CreateUser(ctx context.Context, email, firstName, lastName string) (string, error) {
	created, err := a.users.Create(ctx, &user.CreateParams{
		EmailAddresses:            &[]string{email},
		FirstName:                 &firstName,
		LastName:                  &lastName,
		SkipPasswordRequirement:   clerk.Bool(true),
		CreateOrganizationEnabled: clerk.Bool(false),
	})
	if err != nil {
		return "", err
	}
	return created.ID, nil
}

// DisallowOrganizations implements Clerk.
func (a API) DisallowOrganizations(ctx context.Context, userID string) error {
	_, err := a.users.Update(ctx, userID, &user.UpdateParams{CreateOrganizationEnabled: clerk.Bool(false)})
	return err
}

// FindOrganization implements Clerk. Clerk's query matches parts of names,
// so the exact name is checked here.
func (a API) FindOrganization(ctx context.Context, name string) (string, error) {
	list, err := a.organizations.List(ctx, &organization.ListParams{Query: &name, ListParams: clerk.ListParams{Limit: clerk.Int64(100)}})
	if err != nil {
		return "", err
	}
	for _, o := range list.Organizations {
		if o.Name == name {
			return o.ID, nil
		}
	}
	return "", nil
}

// CreateOrganization implements Clerk.
func (a API) CreateOrganization(ctx context.Context, name string) (string, error) {
	created, err := a.organizations.Create(ctx, &organization.CreateParams{Name: &name})
	if err != nil {
		return "", err
	}
	return created.ID, nil
}

// Role implements Clerk.
func (a API) Role(ctx context.Context, organizationID, userID string) (string, error) {
	list, err := a.memberships.List(ctx, &organizationmembership.ListParams{OrganizationID: organizationID, UserIDs: []string{userID}})
	if err != nil || len(list.OrganizationMemberships) == 0 {
		return "", err
	}
	return list.OrganizationMemberships[0].Role, nil
}

// AddMember implements Clerk.
func (a API) AddMember(ctx context.Context, organizationID, userID, role string) error {
	_, err := a.memberships.Create(ctx, &organizationmembership.CreateParams{OrganizationID: organizationID, UserID: &userID, Role: &role})
	return err
}

// SetRole implements Clerk.
func (a API) SetRole(ctx context.Context, organizationID, userID, role string) error {
	_, err := a.memberships.Update(ctx, &organizationmembership.UpdateParams{OrganizationID: organizationID, UserID: userID, Role: &role})
	return err
}

// RemoveMember implements Clerk.
func (a API) RemoveMember(ctx context.Context, organizationID, userID string) error {
	_, err := a.memberships.Delete(ctx, &organizationmembership.DeleteParams{OrganizationID: organizationID, UserID: userID})
	return err
}
