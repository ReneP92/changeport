package devuser

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// fakeClerk keeps users, organizations and memberships in memory.
type fakeClerk struct {
	users         map[string]string // email to ID
	creators      map[string]bool   // IDs of users who may create organizations
	organizations map[string]string // name to ID
	roles         map[string]string // organization/user to role
	calls         []string          // the changing calls, in order
}

func newFakeClerk() *fakeClerk {
	return &fakeClerk{
		users:         map[string]string{},
		creators:      map[string]bool{},
		organizations: map[string]string{"ChangePort": "org_staff", "Demo (renee)": "org_other"},
		roles:         map[string]string{},
	}
}

func (f *fakeClerk) FindUser(_ context.Context, email string) (*User, error) {
	id, ok := f.users[email]
	if !ok {
		return nil, nil
	}
	return &User{ID: id, CanCreateOrganizations: f.creators[id]}, nil
}

func (f *fakeClerk) DisallowOrganizations(_ context.Context, userID string) error {
	f.creators[userID] = false
	f.calls = append(f.calls, "disallow organizations for "+userID)
	return nil
}

func (f *fakeClerk) CreateUser(_ context.Context, email, firstName, lastName string) (string, error) {
	id := fmt.Sprintf("user_%d", len(f.users)+1)
	f.users[email] = id
	f.calls = append(f.calls, "create user "+email+" "+firstName+" "+lastName)
	return id, nil
}

func (f *fakeClerk) FindOrganization(_ context.Context, name string) (string, error) {
	return f.organizations[name], nil
}

func (f *fakeClerk) CreateOrganization(_ context.Context, name string) (string, error) {
	id := fmt.Sprintf("org_%d", len(f.organizations)+1)
	f.organizations[name] = id
	f.calls = append(f.calls, "create organization "+name)
	return id, nil
}

func (f *fakeClerk) Role(_ context.Context, organizationID, userID string) (string, error) {
	return f.roles[organizationID+"/"+userID], nil
}

func (f *fakeClerk) AddMember(_ context.Context, organizationID, userID, role string) error {
	f.roles[organizationID+"/"+userID] = role
	f.calls = append(f.calls, "add "+userID+" to "+organizationID+" as "+role)
	return nil
}

func (f *fakeClerk) RemoveMember(_ context.Context, organizationID, userID string) error {
	delete(f.roles, organizationID+"/"+userID)
	f.calls = append(f.calls, "remove "+userID+" from "+organizationID)
	return nil
}

func (f *fakeClerk) SetRole(_ context.Context, organizationID, userID, role string) error {
	f.roles[organizationID+"/"+userID] = role
	f.calls = append(f.calls, "set "+userID+" in "+organizationID+" to "+role)
	return nil
}

var rene = Options{Developer: "rene", StaffOrganizationID: "org_staff", DemoOrganization: "Demo (rene)"}

func TestEnsureCreatesWhatIsMissing(t *testing.T) {
	clerk := newFakeClerk()
	result, err := Ensure(t.Context(), clerk, rene)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"create user rene+clerk_test@example.com Dev rene",
		"add user_1 to org_staff as org:admin",
		"create organization Demo (rene)",
		"create user rene-demo+clerk_test@example.com Demo rene",
		"add user_2 to org_3 as org:admin",
	}
	if !slices.Equal(clerk.calls, want) {
		t.Errorf("calls =\n%s\nwant\n%s", strings.Join(clerk.calls, "\n"), strings.Join(want, "\n"))
	}
	if result.Operator != (Account{Email: "rene+clerk_test@example.com", UserID: "user_1"}) {
		t.Errorf("operator = %+v", result.Operator)
	}
	if result.Customer == nil || *result.Customer != (Account{Email: "rene-demo+clerk_test@example.com", UserID: "user_2"}) {
		t.Errorf("customer = %+v", result.Customer)
	}
	if result.DemoOrganizationID != "org_3" || len(result.Changes) != 5 {
		t.Errorf("result = %+v", result)
	}
	if result.Changes[0] != "rene+clerk_test@example.com: created the user" {
		t.Errorf("changes = %q, want each with the user's address", result.Changes)
	}

	// A second run finds everything in place.
	clerk.calls = nil
	again, err := Ensure(t.Context(), clerk, rene)
	if err != nil {
		t.Fatal(err)
	}
	if len(clerk.calls) != 0 || len(again.Changes) != 0 || again.DemoOrganizationID != "org_3" {
		t.Errorf("a second run made calls %v and changes %v", clerk.calls, again.Changes)
	}
}

// Before the split, one user was the operator and the demo customer's admin;
// it now leaves the demo organization to the customer's admin, and neither is
// a member of the other's organization.
func TestEnsureSeparatesTheOperatorAndTheCustomer(t *testing.T) {
	clerk := newFakeClerk()
	clerk.users["rene+clerk_test@example.com"] = "user_7"
	clerk.users["rene-demo+clerk_test@example.com"] = "user_8"
	clerk.organizations["Demo (rene)"] = "org_demo"
	clerk.roles["org_staff/user_7"] = AdminRole
	clerk.roles["org_demo/user_7"] = AdminRole
	clerk.roles["org_staff/user_8"] = "org:member"
	result, err := Ensure(t.Context(), clerk, rene)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"add user_8 to org_demo as org:admin",
		"remove user_7 from org_demo",
		"remove user_8 from org_staff",
	}
	if !slices.Equal(clerk.calls, want) {
		t.Errorf("calls =\n%s\nwant\n%s", strings.Join(clerk.calls, "\n"), strings.Join(want, "\n"))
	}
	if want := "rene+clerk_test@example.com: took the user out of \"Demo (rene)\""; !slices.Contains(result.Changes, want) {
		t.Errorf("changes = %q, want %q among them", result.Changes, want)
	}
}

func TestEnsureRaisesAMemberToAdmin(t *testing.T) {
	clerk := newFakeClerk()
	clerk.users["rene+clerk_test@example.com"] = "user_7"
	clerk.roles["org_staff/user_7"] = "org:member"
	result, err := Ensure(t.Context(), clerk, Options{Developer: "rene", StaffOrganizationID: "org_staff"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(clerk.calls, []string{"set user_7 in org_staff to org:admin"}) {
		t.Errorf("calls = %v", clerk.calls)
	}
	// Without a demo organization asked for, neither it nor its admin is made.
	if result.DemoOrganizationID != "" || result.Customer != nil {
		t.Errorf("demo organization %q and customer %+v, want none", result.DemoOrganizationID, result.Customer)
	}
}

func TestEnsureRefusesBadInput(t *testing.T) {
	for _, developer := range []string{"", "Rene", "rene smith", "-rene", "rene@x"} {
		if _, err := Ensure(t.Context(), newFakeClerk(), Options{Developer: developer, StaffOrganizationID: "org_staff"}); err == nil {
			t.Errorf("Ensure() accepted the developer name %q", developer)
		}
	}
	if _, err := Ensure(t.Context(), newFakeClerk(), Options{Developer: "rene"}); err == nil {
		t.Error("Ensure() accepted no staff organization")
	}
}

func TestCheckSecretKeyRefusesProduction(t *testing.T) {
	if err := CheckSecretKey("sk_test_abc"); err != nil {
		t.Errorf("CheckSecretKey(development) = %v", err)
	}
	for _, key := range []string{"sk_live_abc", "", "pk_test_abc"} {
		if err := CheckSecretKey(key); !errors.Is(err, ErrNotDevelopment) {
			t.Errorf("CheckSecretKey(%q) = %v, want ErrNotDevelopment", key, err)
		}
	}
}

// failingClerk fails at one call.
type failingClerk struct {
	*fakeClerk
	failAt string
}

func (f failingClerk) AddMember(ctx context.Context, organizationID, userID, role string) error {
	if f.failAt == "add" {
		return errors.New("clerk is down")
	}
	return f.fakeClerk.AddMember(ctx, organizationID, userID, role)
}

func TestEnsureReportsWhatFailed(t *testing.T) {
	_, err := Ensure(t.Context(), failingClerk{newFakeClerk(), "add"}, rene)
	if err == nil || !strings.Contains(err.Error(), "add rene+clerk_test@example.com to the staff organization: clerk is down") {
		t.Errorf("Ensure() = %v, want the failed step", err)
	}
}

// A user Clerk lets create organizations, as Clerk's instance setting may
// still allow for new users, is stopped from it; one already stopped is left
// alone.
func TestEnsureStopsUsersFromCreatingOrganizations(t *testing.T) {
	clerk := newFakeClerk()
	clerk.users["rene+clerk_test@example.com"] = "user_7"
	clerk.creators["user_7"] = true
	clerk.roles["org_staff/user_7"] = AdminRole
	result, err := Ensure(t.Context(), clerk, Options{Developer: "rene", StaffOrganizationID: "org_staff"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"disallow organizations for user_7"}; !slices.Equal(clerk.calls, want) {
		t.Errorf("calls = %q, want %q", clerk.calls, want)
	}
	if want := []string{"rene+clerk_test@example.com: stopped the user from creating organizations"}; !slices.Equal(result.Changes, want) {
		t.Errorf("changes = %q, want %q", result.Changes, want)
	}

	clerk.calls = nil
	if _, err := Ensure(t.Context(), clerk, Options{Developer: "rene", StaffOrganizationID: "org_staff"}); err != nil {
		t.Fatal(err)
	}
	if len(clerk.calls) != 0 {
		t.Errorf("a second run made calls %q, want none", clerk.calls)
	}
}
