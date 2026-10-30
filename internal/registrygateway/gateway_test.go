package registrygateway

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// registry is a stand-in for Apicurio's Confluent-compatible API, which
// records the requests it gets.
type registry struct {
	server   *httptest.Server
	requests []string
	headers  []http.Header
}

func newRegistry(t *testing.T) *registry {
	t.Helper()
	r := &registry{}
	r.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		r.requests = append(r.requests, req.Method+" "+req.URL.RequestURI()+" "+string(body))
		r.headers = append(r.headers, req.Header.Clone())
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		if req.URL.Path == "/apis/ccompat/v7/subjects" {
			_, _ = w.Write([]byte(`["orders-value","app.public.orders-key","app.public.orders-value","changeport.cdc.heartbeat.app-value"]`))
			return
		}
		_, _ = w.Write([]byte(`{"id":7}`))
	}))
	t.Cleanup(r.server.Close)
	return r
}

type fixture struct {
	gateway  *Gateway
	registry *registry
	now      time.Time
	password string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{registry: newRegistry(t), now: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)}
	passwordFile := filepath.Join(t.TempDir(), "password")
	f.password = "Kafka-p4ssword"
	if err := os.WriteFile(passwordFile, []byte(f.password), 0o600); err != nil {
		t.Fatal(err)
	}
	upstream, _ := url.Parse(f.registry.server.URL + "/apis/ccompat/v7")
	f.gateway = New(Config{
		Upstream: upstream, Username: "admin", PasswordFile: passwordFile,
		Sources: []string{"app", "shop-db"},
		Limiter: NewLimiter(3, 5*time.Minute, func() time.Time { return f.now }),
	})
	return f
}

// do sends a request as the client at 203.0.113.7, with credentials unless
// username is empty.
func (f *fixture) do(t *testing.T, method, path, username, password, body string) (int, string) {
	t.Helper()
	request := httptest.NewRequest(method, "https://registry.acme.kafka.changeport.test"+path, strings.NewReader(body))
	request.RemoteAddr = "203.0.113.7:51234"
	if username != "" {
		request.SetBasicAuth(username, password)
	}
	recorder := httptest.NewRecorder()
	f.gateway.ServeHTTP(recorder, request)
	return recorder.Code, recorder.Body.String()
}

func TestGatewayProxiesTheConfluentAPI(t *testing.T) {
	f := newFixture(t)
	status, body := f.do(t, http.MethodGet, "/schemas/ids/7", "admin", f.password, "")
	if status != http.StatusOK || body != `{"id":7}` {
		t.Errorf("GET /schemas/ids/7 = %d %s", status, body)
	}
	status, _ = f.do(t, http.MethodPost, "/subjects/orders-value/versions", "admin", f.password, `{"schema":"\"string\""}`)
	if status != http.StatusOK {
		t.Errorf("registering a schema of the customer's own topic = %d", status)
	}
	want := []string{"GET /apis/ccompat/v7/schemas/ids/7 ", `POST /apis/ccompat/v7/subjects/orders-value/versions {"schema":"\"string\""}`}
	if strings.Join(f.registry.requests, "\n") != strings.Join(want, "\n") {
		t.Errorf("the registry got\n%s\nwant\n%s", strings.Join(f.registry.requests, "\n"), strings.Join(want, "\n"))
	}
	// The registry never sees the customer's credentials.
	for _, header := range f.registry.headers {
		if header.Get("Authorization") != "" {
			t.Error("the registry got the customer's credentials")
		}
	}
}

func TestGatewayHidesThePlatformsSubjects(t *testing.T) {
	f := newFixture(t)
	status, body := f.do(t, http.MethodGet, "/subjects", "admin", f.password, "")
	var subjects []string
	if err := json.Unmarshal([]byte(body), &subjects); status != http.StatusOK || err != nil {
		t.Fatalf("GET /subjects = %d %s", status, body)
	}
	if strings.Join(subjects, ",") != "orders-value,app.public.orders-key,app.public.orders-value" {
		t.Errorf("subjects = %v, want the platform's left out", subjects)
	}
}

func TestGatewayRefusesWrongCredentials(t *testing.T) {
	f := newFixture(t)
	for _, c := range []struct{ username, password string }{{"", ""}, {"admin", "wrong"}, {"other", "Kafka-p4ssword"}} {
		status, body := f.do(t, http.MethodGet, "/subjects", c.username, c.password, "")
		if status != http.StatusUnauthorized || !strings.Contains(body, "Use your Kafka credentials") {
			t.Errorf("%q/%q: %d %s, want 401", c.username, c.password, status, body)
		}
	}
	if len(f.registry.requests) != 0 {
		t.Errorf("the registry got %v", f.registry.requests)
	}
}

func TestGatewayLimitsFailedAttempts(t *testing.T) {
	f := newFixture(t)
	for range 3 {
		f.do(t, http.MethodGet, "/subjects", "admin", "wrong", "")
	}
	// After three failures, even the right password waits.
	status, body := f.do(t, http.MethodGet, "/subjects", "admin", f.password, "")
	if status != http.StatusTooManyRequests || !strings.Contains(body, "Too many failed attempts") {
		t.Errorf("after 3 failures: %d %s, want 429", status, body)
	}
	// Once the window has passed, it may try again.
	f.now = f.now.Add(5 * time.Minute)
	if status, _ := f.do(t, http.MethodGet, "/subjects", "admin", f.password, ""); status != http.StatusOK {
		t.Errorf("after the window: %d, want 200", status)
	}
}

func TestGatewayRefusesChangesToSourcesSchemas(t *testing.T) {
	f := newFixture(t)
	for _, c := range []struct {
		method, path string
		refused      bool
	}{
		{http.MethodDelete, "/subjects/app.public.orders-value", true},
		{http.MethodDelete, "/subjects/app.public.orders-value/versions/1", true},
		{http.MethodPost, "/subjects/app.public.orders-value/versions", true},
		{http.MethodPut, "/config/app.public.orders-value", true},
		{http.MethodDelete, "/config/app.public.orders-value", true},
		{http.MethodPut, "/config", true},
		{http.MethodPut, "/mode", true},
		{http.MethodPut, "/mode/orders-value", true},
		// The records' own subjects, named with underscores for dashes.
		{http.MethodDelete, "/subjects/shop_db.public.orders.Value", true},
		{http.MethodDelete, "/subjects/shop-db.public.orders-key", true},
		{http.MethodDelete, "/subjects/changeport.cdc.heartbeat.app-value", true},
		// An escaped slash does not hide a subject.
		{http.MethodDelete, "/subjects/app.public%2Forders-value", true},
		// Reading, and looking a schema up, change nothing.
		{http.MethodGet, "/subjects/app.public.orders-value/versions/latest", false},
		{http.MethodPost, "/subjects/app.public.orders-value", false},
		{http.MethodPost, "/compatibility/subjects/app.public.orders-value/versions/latest", false},
		// The customer's own subjects are theirs.
		{http.MethodDelete, "/subjects/orders-value", false},
		{http.MethodPut, "/config/orders-value", false},
		{http.MethodDelete, "/subjects/apples.public.orders-value", false},
	} {
		status, body := f.do(t, c.method, c.path, "admin", f.password, `{}`)
		if refused := status == http.StatusForbidden; refused != c.refused {
			t.Errorf("%s %s = %d %s, want refused %t", c.method, c.path, status, body, c.refused)
		}
	}
}

func TestGatewayReadsANewPassword(t *testing.T) {
	f := newFixture(t)
	if err := os.WriteFile(f.gateway.config.PasswordFile, []byte("rotated\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if status, _ := f.do(t, http.MethodGet, "/subjects", "admin", "rotated", ""); status != http.StatusOK {
		t.Errorf("with the new password: %d", status)
	}
}

func TestGatewayWithoutTheRegistry(t *testing.T) {
	f := newFixture(t)
	f.registry.server.Close()
	status, body := f.do(t, http.MethodGet, "/subjects", "admin", f.password, "")
	if status != http.StatusBadGateway || !strings.Contains(body, "not reachable right now") {
		t.Errorf("without the registry: %d %s", status, body)
	}
}

func TestCertificateReloads(t *testing.T) {
	dir := t.TempDir()
	certificate := &Certificate{CertFile: filepath.Join(dir, "tls.crt"), KeyFile: filepath.Join(dir, "tls.key")}
	if _, err := certificate.Get(nil); err == nil {
		t.Fatal("Get() without files succeeded")
	}
	writePair := func(name string) {
		cert, key := selfSigned(t, name)
		if err := os.WriteFile(certificate.CertFile, cert, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(certificate.KeyFile, key, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writePair("first.example")
	first, err := certificate.Get(nil)
	if err != nil || first.Leaf.DNSNames[0] != "first.example" {
		t.Fatalf("Get() = %v, %v", first, err)
	}
	// cert-manager renews it.
	later := time.Now().Add(time.Minute)
	writePair("second.example")
	for _, file := range []string{certificate.CertFile, certificate.KeyFile} {
		_ = os.Chtimes(file, later, later)
	}
	second, err := certificate.Get(nil)
	if err != nil || second.Leaf.DNSNames[0] != "second.example" {
		t.Errorf("after renewal, Get() = %v, %v", second.Leaf.DNSNames, err)
	}
	// A half-written pair keeps the last one.
	_ = os.WriteFile(certificate.KeyFile, []byte("garbage"), 0o600)
	_ = os.Chtimes(certificate.KeyFile, later.Add(time.Minute), later.Add(time.Minute))
	if kept, err := certificate.Get(nil); err != nil || kept != second {
		t.Errorf("with a broken key, Get() = %v, %v; want the last pair", kept, err)
	}
}
