package console

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/clerk/clerk-sdk-go/v2"
	"github.com/go-jose/go-jose/v3"
	josejwt "github.com/go-jose/go-jose/v3/jwt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/console/api"
)

const (
	frontendAPI = "test-instance.clerk.accounts.dev"
	origin      = "http://localhost:8088"
	staffOrg    = "org_staff"
)

var publishableKey = "pk_test_" + base64.StdEncoding.EncodeToString([]byte(frontendAPI+"$"))

// fakeCustomers holds acme, of organization org_acme, and globex, of
// org_globex.
type fakeCustomers struct{ customers []v1alpha1.Customer }

func newFakeCustomers() *fakeCustomers {
	customer := func(id, org string) v1alpha1.Customer {
		c := v1alpha1.Customer{
			ObjectMeta: metav1.ObjectMeta{
				Name:              id,
				Generation:        1,
				Labels:            map[string]string{v1alpha1.ClerkOrganizationLabel: org},
				CreationTimestamp: metav1.NewTime(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)),
			},
			Spec: v1alpha1.CustomerSpec{DisplayName: strings.ToUpper(id[:1]) + id[1:], Profile: "dev", ClerkOrganizationID: org},
		}
		c.Status.BootstrapAddress = "bootstrap." + id + ".kafka.changeport.test:9094"
		c.Status.Conditions = []metav1.Condition{
			{Type: v1alpha1.ConditionCertificateIssued, Status: "True", Reason: "Done", Message: "Issued", ObservedGeneration: 1},
			{Type: v1alpha1.ConditionReady, Status: "True", Reason: "Done", Message: "Clients can connect", ObservedGeneration: 1},
		}
		return c
	}
	return &fakeCustomers{customers: []v1alpha1.Customer{customer("globex", "org_globex"), customer("acme", "org_acme")}}
}

func (f *fakeCustomers) ByOrganization(_ context.Context, organizationID string) (*v1alpha1.Customer, error) {
	for i := range f.customers {
		if f.customers[i].Spec.ClerkOrganizationID == organizationID {
			return &f.customers[i], nil
		}
	}
	return nil, nil
}

func (f *fakeCustomers) List(context.Context) ([]v1alpha1.Customer, error) {
	return append([]v1alpha1.Customer(nil), f.customers...), nil
}

func (f *fakeCustomers) Get(_ context.Context, id string) (*v1alpha1.Customer, error) {
	for i := range f.customers {
		if f.customers[i].Name == id {
			return &f.customers[i], nil
		}
	}
	return nil, nil
}

// signer signs session tokens as the test Clerk instance.
type signer struct {
	key   *rsa.PrivateKey
	keyID string
}

func newSigner(t *testing.T, keyID string) signer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return signer{key: key, keyID: keyID}
}

func (s signer) jwk() *clerk.JSONWebKey {
	return &clerk.JSONWebKey{Key: &s.key.PublicKey, KeyID: s.keyID, Algorithm: "RS256", Use: "sig"}
}

// session describes a token's claims.
type session struct {
	user, org, role string
	issuer, party   string
	expires         time.Time
}

// token signs a version 2 session token, as Clerk issues them.
func (s signer) token(t *testing.T, c session) string {
	t.Helper()
	signing, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: s.key},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", s.keyID))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	claims := map[string]any{
		"sub": c.user, "sid": "sess_1", "v": 2,
		"iss": cmpOr(c.issuer, "https://"+frontendAPI), "azp": cmpOr(c.party, origin),
		"iat": now.Unix(), "nbf": now.Add(-time.Minute).Unix(), "exp": cmpOrTime(c.expires, now.Add(time.Minute)).Unix(),
	}
	if c.org != "" {
		claims["o"] = map[string]any{"id": c.org, "rol": c.role, "slg": c.org}
	}
	token, err := josejwt.Signed(signing).Claims(claims).CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func cmpOrTime(a, b time.Time) time.Time {
	if !a.IsZero() {
		return a
	}
	return b
}

// fixture is a configured console with the customers acme and globex.
type fixture struct {
	handler http.Handler
	signer  signer
	fetches int
}

var ui = fstest.MapFS{
	"index.html":         {Data: []byte("<!doctype html><title>Changeport</title>")},
	"assets/app-1a2b.js": {Data: []byte("console.log(1)")},
	"favicon.svg":        {Data: []byte("<svg/>")},
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{signer: newSigner(t, "key-1")}
	server, err := New(Config{
		PublishableKey:      publishableKey,
		SecretKey:           "sk_test_unused",
		StaffOrganizationID: staffOrg,
		Origins:             []string{origin, "http://localhost:5173"},
	}, Options{
		Customers: newFakeCustomers(),
		UI:        ui,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Keys: func(context.Context) ([]*clerk.JSONWebKey, error) {
			f.fetches++
			return []*clerk.JSONWebKey{f.signer.jwk()}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	f.handler = server.Handler()
	return f
}

// get requests path with token, if any, and returns the status and body.
func get(t *testing.T, handler http.Handler, path, token string) (int, string, http.Header) {
	t.Helper()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody)
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder.Code, recorder.Body.String(), recorder.Header()
}

func TestAuthorizationMatrix(t *testing.T) {
	f := newFixture(t)
	principals := map[string]string{
		"anonymous":            "",
		"no organization":      f.signer.token(t, session{user: "user_1"}),
		"acme member":          f.signer.token(t, session{user: "user_2", org: "org_acme", role: "member"}),
		"acme admin":           f.signer.token(t, session{user: "user_3", org: "org_acme", role: "admin"}),
		"globex member":        f.signer.token(t, session{user: "user_4", org: "org_globex", role: "member"}),
		"organization without": f.signer.token(t, session{user: "user_5", org: "org_new", role: "admin"}),
		"staff":                f.signer.token(t, session{user: "user_6", org: staffOrg, role: "member"}),
	}
	// want is the status, and a string the body must contain, of each route
	// for each principal.
	type want struct {
		status int
		body   string
	}
	routes := map[string]map[string]want{
		"/api/config": {
			"anonymous": {200, publishableKey}, "no organization": {200, publishableKey}, "acme member": {200, publishableKey},
			"acme admin": {200, publishableKey}, "globex member": {200, publishableKey}, "organization without": {200, publishableKey},
			"staff": {200, publishableKey},
		},
		"/api/me": {
			"anonymous":       {401, "Sign in first"},
			"no organization": {200, `"access":"none"`},
			"acme member":     {200, `"access":"customer","admin":false,"customerID":"acme"`},
			"acme admin":      {200, `"access":"customer","admin":true,"customerID":"acme"`},
			"globex member":   {200, `"customerID":"globex"`},
			// An organization that is no customer yet sees nothing.
			"organization without": {200, `"access":"none","admin":true`},
			"staff":                {200, `"access":"operator"`},
		},
		"/api/customer": {
			"anonymous":       {401, "Sign in first"},
			"no organization": {403, "Choose an organization"},
			"acme member":     {200, `"id":"acme"`},
			"acme admin":      {200, `"id":"acme"`},
			// The customer comes from the token, so globex can only see globex.
			"globex member":        {200, `"id":"globex"`},
			"organization without": {404, "No Kafka cluster yet"},
			"staff":                {403, "not a customer"},
		},
		"/api/operator/customers": {
			"anonymous": {401, "Sign in first"}, "no organization": {403, "Operators only"}, "acme member": {403, "Operators only"},
			"acme admin": {403, "Operators only"}, "globex member": {403, "Operators only"}, "organization without": {403, "Operators only"},
			"staff": {200, `{"customers":[{`},
		},
		"/api/operator/customers/acme": {
			"anonymous": {401, "Sign in first"}, "no organization": {403, "Operators only"}, "acme member": {403, "Operators only"},
			"acme admin": {403, "Operators only"}, "globex member": {403, "Operators only"}, "organization without": {403, "Operators only"},
			"staff": {200, `"id":"acme"`},
		},
	}
	for route, byPrincipal := range routes {
		for principal, token := range principals {
			w, ok := byPrincipal[principal]
			if !ok {
				t.Fatalf("no expectation for %s on %s", principal, route)
			}
			status, body, _ := get(t, f.handler, route, token)
			if status != w.status || !strings.Contains(body, w.body) {
				t.Errorf("%s GET %s = %d %s, want %d containing %s", principal, route, status, body, w.status, w.body)
			}
			if principal == "globex member" && strings.Contains(body, "acme") {
				t.Errorf("globex member GET %s sees acme: %s", route, body)
			}
		}
	}
	if f.fetches != 1 {
		t.Errorf("fetched the signing keys %d times, want once", f.fetches)
	}
}

func TestOperatorsSeeEveryCustomer(t *testing.T) {
	f := newFixture(t)
	staff := f.signer.token(t, session{user: "user_6", org: staffOrg, role: "member"})
	status, body, _ := get(t, f.handler, "/api/operator/customers", staff)
	if status != 200 {
		t.Fatalf("GET /api/operator/customers = %d %s", status, body)
	}
	var list api.CustomerList
	if err := json.Unmarshal([]byte(body), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Customers) != 2 || list.Customers[0].Id != "acme" || list.Customers[1].Id != "globex" {
		t.Errorf("customers = %+v, want acme and globex by ID", list.Customers)
	}
	acme := list.Customers[0]
	if !acme.Ready || acme.Status.Reason != "Done" || len(acme.Conditions) != 2 || acme.Conditions[0].Type != api.CertificateIssued {
		t.Errorf("acme = %+v", acme)
	}
	if status, body, _ := get(t, f.handler, "/api/operator/customers/initech", staff); status != 404 {
		t.Errorf("GET an unknown customer = %d %s, want 404", status, body)
	}
	if status, _, _ := get(t, f.handler, "/api/operator/customers/Not_An_ID", staff); status != 404 {
		t.Errorf("GET an invalid customer ID = %d, want 404: no customer has it", status)
	}
}

func TestInvalidTokensAreRefused(t *testing.T) {
	f := newFixture(t)
	impostor := newSigner(t, "key-1")
	stranger := newSigner(t, "key-2")
	tokens := map[string]string{
		"signed by another key":       impostor.token(t, session{user: "user_1", org: "org_acme", role: "admin"}),
		"an unknown key":              stranger.token(t, session{user: "user_1", org: "org_acme", role: "admin"}),
		"for another origin":          f.signer.token(t, session{user: "user_1", org: "org_acme", party: "https://evil.example"}),
		"from another Clerk instance": f.signer.token(t, session{user: "user_1", org: "org_acme", issuer: "https://other.clerk.accounts.dev"}),
		"expired":                     f.signer.token(t, session{user: "user_1", org: "org_acme", expires: time.Now().Add(-time.Hour)}),
		"without a user":              f.signer.token(t, session{org: "org_acme"}),
		"not a JWT":                   "garbage",
	}
	for name, token := range tokens {
		status, body, header := get(t, f.handler, "/api/customer", token)
		if status != 401 || header.Get("WWW-Authenticate") != "Bearer" {
			t.Errorf("a token %s: GET /api/customer = %d %s, want 401", name, status, body)
		}
	}
	// The unknown key made the console fetch the keys once more at most.
	if f.fetches > 2 {
		t.Errorf("fetched the signing keys %d times", f.fetches)
	}
}

func TestUnknownKeysDoNotFloodClerk(t *testing.T) {
	now := time.Now()
	fetches := 0
	keys := &keySet{now: func() time.Time { return now }, fetch: func(context.Context) ([]*clerk.JSONWebKey, error) {
		fetches++
		return nil, nil
	}}
	for range 10 {
		if _, err := keys.key(t.Context(), "forged"); err == nil {
			t.Fatal("found a key that does not exist")
		}
	}
	if fetches != 1 {
		t.Errorf("fetched %d times for 10 unknown keys within a minute, want 1", fetches)
	}
	now = now.Add(refetchInterval)
	_, _ = keys.key(t.Context(), "forged")
	if fetches != 2 {
		t.Errorf("fetched %d times after a minute, want 2", fetches)
	}
}

func TestFailedKeyFetchesAreRetriedSoon(t *testing.T) {
	now := time.Now()
	keys := &keySet{now: func() time.Time { return now }, fetch: func(context.Context) ([]*clerk.JSONWebKey, error) {
		return nil, errors.New("clerk is down")
	}}
	_, _ = keys.key(t.Context(), "key-1")
	signer := newSigner(t, "key-1")
	keys.fetch = func(context.Context) ([]*clerk.JSONWebKey, error) { return []*clerk.JSONWebKey{signer.jwk()}, nil }
	if _, err := keys.key(t.Context(), "key-1"); err == nil {
		t.Fatal("refetched right after a failure")
	}
	now = now.Add(retryInterval)
	if _, err := keys.key(t.Context(), "key-1"); err != nil {
		t.Errorf("did not refetch %v after a failure: %v", retryInterval, err)
	}
}

func TestUnconfiguredConsoleFailsClosed(t *testing.T) {
	signer := newSigner(t, "key-1")
	for name, config := range map[string]Config{
		"nothing":           {},
		"no secret key":     {PublishableKey: publishableKey, StaffOrganizationID: staffOrg, Origins: []string{origin}},
		"no staff":          {PublishableKey: publishableKey, SecretKey: "sk_test_x", Origins: []string{origin}},
		"no publishable":    {SecretKey: "sk_test_x", StaffOrganizationID: staffOrg, Origins: []string{origin}},
		"no origins at all": {PublishableKey: publishableKey, SecretKey: "sk_test_x", StaffOrganizationID: staffOrg},
	} {
		server, err := New(config, Options{Customers: newFakeCustomers(), UI: ui, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		handler := server.Handler()
		token := signer.token(t, session{user: "user_1", org: staffOrg})
		for _, route := range []string{"/api/config", "/api/me", "/api/customer", "/api/operator/customers"} {
			if status, body, _ := get(t, handler, route, token); status != 503 || !strings.Contains(body, "not configured") {
				t.Errorf("%s: GET %s = %d %s, want 503", name, route, status, body)
			}
		}
		// The web app still loads, to say so.
		if status, _, _ := get(t, handler, "/", ""); status != 200 {
			t.Errorf("%s: GET / = %d, want 200", name, status)
		}
	}
}

func TestInvalidPublishableKey(t *testing.T) {
	_, err := New(Config{PublishableKey: "pk_test_!!!", SecretKey: "sk", StaffOrganizationID: staffOrg, Origins: []string{origin}},
		Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err == nil {
		t.Error("New() accepted a publishable key without a Frontend API")
	}
}

func TestFrontendAPI(t *testing.T) {
	if got, err := FrontendAPI(publishableKey); err != nil || got != frontendAPI {
		t.Errorf("FrontendAPI() = %q, %v, want %s", got, err, frontendAPI)
	}
	live := "pk_live_" + base64.StdEncoding.EncodeToString([]byte("clerk.changeport.io$"))
	if got, err := FrontendAPI(live); err != nil || got != "clerk.changeport.io" {
		t.Errorf("FrontendAPI(live) = %q, %v", got, err)
	}
	for _, key := range []string{"", "sk_test_abc", "pk_test_" + base64.StdEncoding.EncodeToString([]byte("no-dollar")), "pk_test_" + base64.StdEncoding.EncodeToString([]byte("a/b$"))} {
		if got, err := FrontendAPI(key); err == nil {
			t.Errorf("FrontendAPI(%q) = %q, want an error", key, got)
		}
	}
}

func TestWebApp(t *testing.T) {
	f := newFixture(t)
	status, body, header := get(t, f.handler, "/", "")
	if status != 200 || !strings.Contains(body, "<title>Changeport</title>") {
		t.Fatalf("GET / = %d %s", status, body)
	}
	csp := header.Get("Content-Security-Policy")
	for _, want := range []string{
		"script-src 'self' https://" + frontendAPI + " https://cdn.protect.clerk.com ",
		"connect-src 'self' https://" + frontendAPI + " https://*.protect.clerk.com:*;",
		"frame-ancestors 'none'",
	} {
		if !strings.Contains(csp, want) {
			t.Errorf("Content-Security-Policy %q lacks %q", csp, want)
		}
	}
	if header.Get("Cache-Control") != "no-cache" || header.Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("GET / headers = %v", header)
	}
	// The app's own routes load index.html too.
	if status, body, _ := get(t, f.handler, "/operator/customers/acme", ""); status != 200 || !strings.Contains(body, "<title>Changeport</title>") {
		t.Errorf("GET an app route = %d %s, want index.html", status, body)
	}
	if status, _, header := get(t, f.handler, "/assets/app-1a2b.js", ""); status != 200 || !strings.Contains(header.Get("Cache-Control"), "immutable") {
		t.Errorf("GET an asset = %d %v, want cached for good", status, header)
	}
	if status, body, _ := get(t, f.handler, "/api/nothing", ""); status != 401 && status != 404 {
		t.Errorf("GET an unknown API route = %d %s", status, body)
	}
	staff := f.signer.token(t, session{user: "user_6", org: staffOrg})
	if status, body, header := get(t, f.handler, "/api/nothing", staff); status != 404 || header.Get("Content-Type") != "application/problem+json" {
		t.Errorf("GET an unknown API route = %d %s, want a 404 problem", status, body)
	}
	if status, _, _ := get(t, f.handler, "/healthz", ""); status != 200 {
		t.Errorf("GET /healthz = %d", status)
	}
}
