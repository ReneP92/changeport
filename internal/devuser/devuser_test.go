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
	organizations map[string]string // name to ID
	roles         map[string]string // organization/user to role
	calls         []string          // the changing calls, in order
}

func newFakeClerk() *fakeClerk {
	return &fakeClerk{
		users:         map[string]string{},
		organizations: map[string]string{"ChangePort": "org_staff", "Demo (renee)": "org_other"},
		roles:         map[string]string{},
	}
}

func (f *fakeClerk) FindUser(_ context.Context, email string) (string, error) {
	return f.users[email], nil
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
		"add user_1 to org_3 as org:admin",
	}
	if !slices.Equal(clerk.calls, want) {
		t.Errorf("calls =\n%s\nwant\n%s", strings.Join(clerk.calls, "\n"), strings.Join(want, "\n"))
	}
	if result.Email != "rene+clerk_test@example.com" || result.UserID != "user_1" || result.DemoOrganizationID != "org_3" || len(result.Changes) != 4 {
		t.Errorf("result = %+v", result)
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
	// Without a demo organization asked for, none is made.
	if result.DemoOrganizationID != "" {
		t.Errorf("demo organization %q, want none", result.DemoOrganizationID)
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
	if err == nil || !strings.Contains(err.Error(), "add the user to the staff organization: clerk is down") {
		t.Errorf("Ensure() = %v, want the failed step", err)
	}
}
