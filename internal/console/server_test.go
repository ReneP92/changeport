package console

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/ReneP92/changeport/internal/clusters"
	"github.com/ReneP92/changeport/internal/console/api"
	"github.com/ReneP92/changeport/internal/metrics"
	"github.com/ReneP92/changeport/internal/topics"
)

const (
	frontendAPI = "test-instance.clerk.accounts.dev"
	origin      = "http://localhost:8088"
	staffOrg    = "org_staff"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

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
	handler  http.Handler
	signer   signer
	clusters *fakeClusters
	// log is what the server logged, as JSON lines.
	log     *bytes.Buffer
	fetches int
}

var ui = fstest.MapFS{
	"index.html":         {Data: []byte("<!doctype html><title>Changeport</title>")},
	"assets/app-1a2b.js": {Data: []byte("console.log(1)")},
	"favicon.svg":        {Data: []byte("<svg/>")},
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{signer: newSigner(t, "key-1"), clusters: newFakeClusters(), log: &bytes.Buffer{}}
	server, err := New(Config{
		PublishableKey:      publishableKey,
		SecretKey:           "sk_test_unused",
		StaffOrganizationID: staffOrg,
		Origins:             []string{origin, "http://localhost:5173"},
	}, Options{
		Customers: newFakeCustomers(),
		Clusters:  f.clusters,
		Metrics:   fakeMetrics{},
		UI:        ui,
		Logger:    slog.New(slog.NewJSONHandler(f.log, nil)),
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
	// Every customer route acts on the active organization's customer.
	customerRoute := func(member, admin, globex string) map[string]want {
		return map[string]want{
			"anonymous": {401, "Sign in first"}, "no organization": {403, "Choose an organization"},
			"acme member": {200, member}, "acme admin": {200, admin}, "globex member": {200, globex},
			"organization without": {404, "No Kafka cluster yet"}, "staff": {403, "not a customer"},
		}
	}
	routes["/api/customer/connection"] = customerRoute(
		`"bootstrapAddress":"bootstrap.acme.kafka.changeport.test:9094"`,
		`"caCertificate":"-----BEGIN CERTIFICATE-----`,
		`"bootstrapAddress":"bootstrap.globex.kafka.changeport.test:9094"`)
	routes["/api/customer/health"] = customerRoute(`"name":"acme-dual-role-0"`, `"underReplicatedPartitions":0`, `"name":"globex-dual-role-0"`)
	routes["/api/customer/topics"] = customerRoute(`"name":"acme-orders"`, `"tiered":true`, `"name":"globex-orders"`)
	routes["/api/customer/throughput"] = customerRoute(`"bytesIn":[{`, `"range":"1h"`, `"stepSeconds":60`)
	routes["/api/customer/topic-policy"] = customerRoute(`"max":12`, `"replicationFactor":1`, `"maxNameLength":200`)
	routes["/api/customer/topics/acme-orders/consumer-groups"] = customerRoute(`{"groups":[]}`, `{"groups":[]}`, `{"groups":[]}`)
	// Only an organization's admins may download its credentials.
	clientProperties := customerRoute("", "sasl.jaas.config=", "")
	clientProperties["acme member"] = want{403, "Admins only"}
	clientProperties["globex member"] = want{403, "Admins only"}
	routes["/api/customer/client-properties"] = clientProperties
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
	keys := &keySet{now: func() time.Time { return now }, logger: discard, fetch: func(context.Context) ([]*clerk.JSONWebKey, error) {
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

func TestFailedKeyFetchesAreRetriedSoonThenLessOften(t *testing.T) {
	now := time.Now()
	down := func(context.Context) ([]*clerk.JSONWebKey, error) { return nil, errors.New("clerk is down") }
	keys := &keySet{now: func() time.Time { return now }, logger: discard, fetch: down}
	// Each failure doubles the wait, up to lastRetry.
	for _, wait := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second} {
		if _, err := keys.key(t.Context(), "key-1"); err == nil {
			t.Fatal("found a key while Clerk is down")
		}
		if got := keys.nextFetch.Sub(now); got != wait {
			t.Errorf("waits %v after %d failures, want %v", got, keys.failures, wait)
		}
		now = now.Add(wait)
	}
	signer := newSigner(t, "key-1")
	keys.fetch = func(context.Context) ([]*clerk.JSONWebKey, error) { return []*clerk.JSONWebKey{signer.jwk()}, nil }
	if _, err := keys.key(t.Context(), "key-1"); err != nil {
		t.Errorf("did not fetch once Clerk was back: %v", err)
	}
	if keys.failures != 0 || keys.nextFetch.Sub(now) != refetchInterval {
		t.Errorf("after a success: %d failures, next fetch in %v", keys.failures, keys.nextFetch.Sub(now))
	}
}

func TestWarmFetchesTheKeysUntilItHasThem(t *testing.T) {
	signer := newSigner(t, "key-1")
	attempts := 0
	keys := &keySet{now: time.Now, logger: discard, fetch: func(context.Context) ([]*clerk.JSONWebKey, error) {
		attempts++
		if attempts < 3 {
			return nil, errors.New("clerk is down")
		}
		return []*clerk.JSONWebKey{signer.jwk()}, nil
	}}
	keys.warm(t.Context())
	if attempts != 3 || len(keys.keys) != 1 {
		t.Errorf("warm made %d attempts and has %d keys, want 3 and 1", attempts, len(keys.keys))
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

// fakeClusters has a broker, a topic and credentials for every customer.
type fakeClusters struct {
	// notReady lists customers whose credentials and topics are not there yet.
	notReady map[string]bool
	// topics holds the topics created by the tests, by customer.
	topics map[string][]topics.Topic
	// groups are the consumer groups reading each topic.
	groups map[string][]string
	// deleted records the topics deleted by the tests, as customer/topic.
	deleted []string
}

func newFakeClusters() *fakeClusters {
	return &fakeClusters{notReady: map[string]bool{}, topics: map[string][]topics.Topic{}, groups: map[string][]string{}}
}

func (f *fakeClusters) Load(_ context.Context, id string) (topics.Cluster, error) {
	if f.notReady[id] {
		return topics.Cluster{}, fmt.Errorf("no certificate yet: %w", clusters.ErrNotReady)
	}
	load := topics.Cluster{Topics: []string{"__consumer_offsets", id + "-orders", id + ".events"}, Replicas: 52, Brokers: 1}
	for _, t := range f.topics[id] {
		load.Topics = append(load.Topics, t.Name)
		load.Replicas += int(t.Partitions) * int(t.ReplicationFactor)
	}
	return load, nil
}

func (f *fakeClusters) CreateTopic(_ context.Context, id string, topic topics.Topic) error {
	f.topics[id] = append(f.topics[id], topic)
	return nil
}

func (f *fakeClusters) DeleteTopic(_ context.Context, id, name string) error {
	if name != id+"-orders" {
		return ErrNoSuchTopic
	}
	f.deleted = append(f.deleted, id+"/"+name)
	return nil
}

func (f *fakeClusters) TopicConsumerGroups(_ context.Context, _, topic string) ([]string, error) {
	return append([]string{}, f.groups[topic]...), nil
}

func (f *fakeClusters) Brokers(_ context.Context, id string) ([]Broker, error) {
	return []Broker{{Name: id + "-dual-role-0", Ready: true, Zone: "zone-a"}}, nil
}

func (f *fakeClusters) Topics(_ context.Context, id string) ([]TopicSettings, error) {
	if f.notReady[id] {
		return nil, fmt.Errorf("no certificate yet: %w", clusters.ErrNotReady)
	}
	week := int64(7 * 24 * time.Hour / time.Millisecond)
	return []TopicSettings{{Name: id + "-orders", Partitions: 3, ReplicationFactor: 1, Tiered: true, CleanupPolicy: "delete", RetentionMs: &week}}, nil
}

func (f *fakeClusters) AdminCredentials(_ context.Context, id string) (string, error) {
	if f.notReady[id] {
		return "", fmt.Errorf("no password yet: %w", clusters.ErrNotReady)
	}
	return `org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="` + id + `-secret";`, nil
}

func (f *fakeClusters) ListenerCA(context.Context, string) (string, error) {
	return "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----", nil
}

// fakeMetrics answers every query of a tenant with one point per series:
// a topic series named after the tenant, or a cluster series of 0.
type fakeMetrics struct{}

func (fakeMetrics) Instant(_ context.Context, tenant string, q metrics.Query, _ metrics.Filter, at time.Time) ([]metrics.Series, error) {
	labels := map[string]string{"topic": tenant + "-orders"}
	value := 1024.0
	if expr, _ := metrics.Expr(q, tenant, metrics.Filter{}); !strings.Contains(expr, "topic") {
		labels, value = map[string]string{}, 0
	}
	return []metrics.Series{{Labels: labels, Points: []metrics.Point{{Time: at, Value: value}}}}, nil
}

func (fakeMetrics) Range(_ context.Context, tenant string, _ metrics.Query, _ metrics.Filter, start, end time.Time, step time.Duration) ([]metrics.Series, error) {
	one := metrics.Series{Labels: map[string]string{"topic": tenant + "-orders"}}
	for t := start; !t.After(end); t = t.Add(step) {
		one.Points = append(one.Points, metrics.Point{Time: t, Value: 10})
	}
	// A second topic, so that the series must be added up.
	two := metrics.Series{Labels: map[string]string{"topic": tenant + "-payments"}, Points: one.Points}
	return []metrics.Series{one, two}, nil
}

func TestClientPropertiesDownload(t *testing.T) {
	f := newFixture(t)
	admin := f.signer.token(t, session{user: "user_3", org: "org_acme", role: "admin"})
	status, body, header := get(t, f.handler, "/api/customer/client-properties", admin)
	if status != 200 {
		t.Fatalf("GET client.properties = %d %s", status, body)
	}
	for _, want := range []string{
		"bootstrap.servers=bootstrap.acme.kafka.changeport.test:9094\n",
		"security.protocol=SASL_SSL\n",
		`password="acme-secret"`,
		"ssl.truststore.certificates=-----BEGIN CERTIFICATE-----",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("client.properties lacks %q:\n%s", want, body)
		}
	}
	if got := header.Get("Content-Disposition"); got != `attachment; filename="acme-client.properties"` {
		t.Errorf("Content-Disposition = %q", got)
	}
	if header.Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store for a file with a password", header.Get("Cache-Control"))
	}
}

func TestNotReadyClusters(t *testing.T) {
	f := newFixture(t)
	f.clusters.notReady["acme"] = true
	admin := f.signer.token(t, session{user: "user_3", org: "org_acme", role: "admin"})
	for _, route := range []string{"/api/customer/client-properties", "/api/customer/topics"} {
		if status, body, _ := get(t, f.handler, route, admin); status != 409 || !strings.Contains(body, "not ready yet") {
			t.Errorf("GET %s of a cluster without credentials = %d %s, want 409", route, status, body)
		}
	}
}

func TestThroughput(t *testing.T) {
	f := newFixture(t)
	member := f.signer.token(t, session{user: "user_2", org: "org_acme", role: "member"})
	status, body, _ := get(t, f.handler, "/api/customer/throughput?range=6h", member)
	if status != 200 {
		t.Fatalf("GET throughput = %d %s", status, body)
	}
	var throughput api.Throughput
	if err := json.Unmarshal([]byte(body), &throughput); err != nil {
		t.Fatal(err)
	}
	if throughput.Range != "6h" || throughput.StepSeconds != 300 || len(throughput.BytesIn) != 73 {
		t.Errorf("throughput = %s with %d points every %ds, want 6h with 73 points every 300s", throughput.Range, len(throughput.BytesIn), throughput.StepSeconds)
	}
	// Two topics of 10 bytes per second each.
	if throughput.BytesIn[0].Value != 20 || !throughput.BytesIn[0].Time.Before(throughput.BytesIn[1].Time) {
		t.Errorf("the first points are %+v and %+v, want 20 in time order", throughput.BytesIn[0], throughput.BytesIn[1])
	}
	if status, _, _ := get(t, f.handler, "/api/customer/throughput?range=1y", member); status != 400 {
		t.Errorf("GET throughput for a year = %d, want 400", status)
	}
}

// send makes a request with a method and a JSON body, if any.
func send(t *testing.T, handler http.Handler, method, path, token, body string) (int, string) {
	t.Helper()
	var reader io.Reader = http.NoBody
	if body != "" {
		reader = strings.NewReader(body)
	}
	request := httptest.NewRequestWithContext(t.Context(), method, path, reader)
	request.Header.Set("Authorization", "Bearer "+token)
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder.Code, recorder.Body.String()
}

const newTopic = `{"name":"invoices","partitions":3,"retentionMs":3600000,"cleanupPolicy":"delete"}`

func TestOnlyAdminsChangeTopics(t *testing.T) {
	f := newFixture(t)
	member := f.signer.token(t, session{user: "user_2", org: "org_acme", role: "member"})
	staff := f.signer.token(t, session{user: "user_6", org: staffOrg, role: "admin"})
	for name, token := range map[string]string{"a member": member, "staff": staff} {
		if status, body := send(t, f.handler, http.MethodPost, "/api/customer/topics", token, newTopic); status != 403 {
			t.Errorf("%s creating a topic = %d %s, want 403", name, status, body)
		}
		if status, body := send(t, f.handler, http.MethodDelete, "/api/customer/topics/acme-orders", token, ""); status != 403 {
			t.Errorf("%s deleting a topic = %d %s, want 403", name, status, body)
		}
	}
	if len(f.clusters.topics) != 0 || len(f.clusters.deleted) != 0 {
		t.Errorf("refused requests changed topics: created %v, deleted %v", f.clusters.topics, f.clusters.deleted)
	}
}

func TestAdminsCreateTopicsWithinTheGuardrails(t *testing.T) {
	f := newFixture(t)
	admin := f.signer.token(t, session{user: "user_3", org: "org_acme", role: "admin"})
	status, body := send(t, f.handler, http.MethodPost, "/api/customer/topics", admin, newTopic)
	if status != 201 || !strings.Contains(body, `"tiered":true`) || !strings.Contains(body, `"retentionMs":3600000`) {
		t.Fatalf("creating a topic = %d %s", status, body)
	}
	created := f.clusters.topics["acme"]
	if len(created) != 1 || created[0].Configs["local.retention.ms"] != "3600000" || created[0].ReplicationFactor != 1 {
		t.Errorf("created %+v, want one tiered topic keeping an hour locally", created)
	}
	if !strings.Contains(f.log.String(), `"msg":"topic created","audit":true,"tenant":"acme","user":"user_3"`) {
		t.Errorf("no audit line for the creation in %s", f.log.String())
	}

	tests := []struct {
		name, body, field string
		status            int
	}{
		{"a reserved name", `{"name":"_secret","partitions":1,"retentionMs":3600000,"cleanupPolicy":"delete"}`, "name", 400},
		{"a taken name", newTopic, "name", 409},
		{"a colliding name", `{"name":"acme_events","partitions":1,"retentionMs":3600000,"cleanupPolicy":"delete"}`, "name", 409},
		{"too many partitions", `{"name":"big","partitions":13,"retentionMs":3600000,"cleanupPolicy":"delete"}`, "partitions", 400},
		{"a retention that is no preset", `{"name":"odd","partitions":1,"retentionMs":1234,"cleanupPolicy":"delete"}`, "retention", 400},
		{"another cleanup policy", `{"name":"odd","partitions":1,"retentionMs":3600000,"cleanupPolicy":"compact,delete"}`, "", 400},
	}
	for _, tt := range tests {
		status, body := send(t, f.handler, http.MethodPost, "/api/customer/topics", admin, tt.body)
		if status != tt.status || (tt.field != "" && !strings.Contains(body, `"field":"`+tt.field+`"`)) {
			t.Errorf("creating a topic with %s = %d %s, want %d about %q", tt.name, status, body, tt.status, tt.field)
		}
	}
	if len(f.clusters.topics["acme"]) != 1 {
		t.Errorf("refused topics were created: %+v", f.clusters.topics["acme"])
	}
}

func TestAdminsDeleteOnlyTheirOwnTopics(t *testing.T) {
	f := newFixture(t)
	admin := f.signer.token(t, session{user: "user_3", org: "org_acme", role: "admin"})
	for _, name := range []string{"__consumer_offsets", "changeport-audit", "unknown"} {
		if status, body := send(t, f.handler, http.MethodDelete, "/api/customer/topics/"+name, admin, ""); status != 404 {
			t.Errorf("deleting %s = %d %s, want 404", name, status, body)
		}
	}
	if status, body := send(t, f.handler, http.MethodDelete, "/api/customer/topics/acme-orders", admin, ""); status != 204 {
		t.Fatalf("deleting acme-orders = %d %s, want 204", status, body)
	}
	if strings.Join(f.clusters.deleted, ",") != "acme/acme-orders" {
		t.Errorf("deleted %v, want acme/acme-orders", f.clusters.deleted)
	}
	if !strings.Contains(f.log.String(), `"msg":"topic deleted","audit":true,"tenant":"acme","user":"user_3","organization":"org_acme","topic":"acme-orders"`) {
		t.Errorf("no audit line for the deletion in %s", f.log.String())
	}
}

func TestTopicConsumerGroups(t *testing.T) {
	f := newFixture(t)
	f.clusters.groups["acme-orders"] = []string{"billing", "search"}
	member := f.signer.token(t, session{user: "user_2", org: "org_acme", role: "member"})
	if status, body := send(t, f.handler, http.MethodGet, "/api/customer/topics/acme-orders/consumer-groups", member, ""); status != 200 || body != `{"groups":["billing","search"]}`+"\n" {
		t.Errorf("consumer groups = %d %q", status, body)
	}
}
