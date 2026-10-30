// Package registrygateway is the gateway customers reach a customer's schema
// registry through, from the internet (docs/cdc.md). It serves the registry's
// Confluent-compatible API at its root, so that a Confluent client's
// schema.registry.url is the gateway's address. It lets in only the
// customer's Kafka user, with its password, and limits failed attempts. It
// refuses changes to the schemas of the customer's Postgres sources and of
// the platform, and hides the platform's subjects.
package registrygateway

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// platformPrefix starts the subjects of the platform's own topics, such as
// the sources' heartbeats and notifications.
const platformPrefix = "changeport."

// maxBody caps a request's body: a schema, or a schema to check.
const maxBody = 8 << 20

// Config configures a Gateway.
type Config struct {
	// Upstream is the registry's Confluent-compatible API, such as
	// http://changeport-registry.customer-acme.svc:8080/apis/ccompat/v7.
	Upstream *url.URL
	// Username is the customer's Kafka user, whose password is in
	// PasswordFile. The file is read for each check, so that a new password
	// takes effect at once.
	Username     string
	PasswordFile string
	// Sources are the names of the customer's Postgres sources, whose
	// subjects customers may read but not change.
	Sources []string
	// Limiter limits failed attempts; nil uses NewLimiter's defaults.
	Limiter *Limiter
	Logger  *slog.Logger
}

// Gateway is the http.Handler of the registry gateway.
type Gateway struct {
	config Config
	proxy  *httputil.ReverseProxy
}

// New returns a Gateway.
func New(config Config) *Gateway {
	if config.Limiter == nil {
		config.Limiter = NewLimiter(10, 5*time.Minute, time.Now)
	}
	if config.Logger == nil {
		config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	upstream := config.Upstream
	g := &Gateway{config: config}
	g.proxy = &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(upstream)
			// The registry inside the cluster takes no credentials, and
			// should not see the customer's.
			r.Out.Header.Del("Authorization")
			r.Out.Header.Del("Cookie")
		},
		ModifyResponse: g.hidePlatformSubjects,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			config.Logger.Warn("the registry did not answer", "path", r.URL.Path, "error", err.Error())
			writeError(w, http.StatusBadGateway, 50201, "The schema registry is not reachable right now. Try again shortly.")
		},
	}
	return g
}

// ServeHTTP implements http.Handler.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	client := clientAddress(r)
	if !g.config.Limiter.Allowed(client) {
		w.Header().Set("Retry-After", strconv.Itoa(int(g.config.Limiter.window.Seconds())))
		writeError(w, http.StatusTooManyRequests, 42901, "Too many failed attempts. Try again later.")
		return
	}
	if !g.authenticated(r) {
		g.config.Limiter.Fail(client)
		g.config.Logger.Info("refused credentials", "client", client)
		w.Header().Set("WWW-Authenticate", `Basic realm="Changeport schema registry", charset="UTF-8"`)
		writeError(w, http.StatusUnauthorized, 40101, "Use your Kafka credentials: the username and password of your client.properties.")
		return
	}
	if reason := g.refusal(r); reason != "" {
		g.config.Logger.Info("refused a change", "client", client, "method", r.Method, "path", r.URL.Path)
		writeError(w, http.StatusForbidden, 40301, reason)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	g.proxy.ServeHTTP(w, r)
}

// authenticated reports whether the request carries the customer's Kafka
// user and its password. Both are compared as digests in constant time, so
// that neither their content nor their length shows in the time it takes.
func (g *Gateway) authenticated(r *http.Request) bool {
	username, password, ok := r.BasicAuth()
	if !ok {
		return false
	}
	want, err := os.ReadFile(g.config.PasswordFile)
	if err != nil || len(want) == 0 {
		g.config.Logger.Error("cannot read the password", "error", fmt.Sprint(err))
		return false
	}
	usernameDigest, wantUsername := sha256.Sum256([]byte(username)), sha256.Sum256([]byte(g.config.Username))
	passwordDigest, wantPassword := sha256.Sum256([]byte(password)), sha256.Sum256(bytes.TrimRight(want, "\n"))
	usernameOK := subtle.ConstantTimeCompare(usernameDigest[:], wantUsername[:])
	passwordOK := subtle.ConstantTimeCompare(passwordDigest[:], wantPassword[:])
	return usernameOK&passwordOK == 1
}

// refusal says why a request may not change the registry, or is empty. The
// schemas of the customer's sources, and of the platform's topics, belong to
// the platform: their versions, compatibility and mode stay as the
// connectors need them. So do the registry's global settings.
func (g *Gateway) refusal(r *http.Request) string {
	segments := pathSegments(r.URL)
	if len(segments) == 0 || r.Method == http.MethodGet || r.Method == http.MethodHead {
		return ""
	}
	switch {
	case segments[0] == "mode":
		return "The registry's mode is the platform's to set."
	case segments[0] == "config" && len(segments) == 1:
		return "The registry's global compatibility is the platform's to set."
	case segments[0] == "config" && g.protected(segments[1]):
		return fmt.Sprintf("The compatibility of %s belongs to its Postgres source, and cannot change.", segments[1])
	case segments[0] == "subjects" && len(segments) >= 2 && g.protected(segments[1]):
		// Looking a schema up under a subject is a POST that changes nothing.
		if r.Method == http.MethodPost && len(segments) == 2 {
			return ""
		}
		return fmt.Sprintf("The schemas of %s belong to its Postgres source, and cannot change.", segments[1])
	}
	return ""
}

// protected reports whether a subject belongs to one of the customer's
// sources or to the platform. A source's subjects are named after its
// topics, <source>.<schema>.<table>-key and -value, and after the records
// of its schemas, whose names have the source's dashes as underscores.
func (g *Gateway) protected(subject string) bool {
	if strings.HasPrefix(subject, platformPrefix) {
		return true
	}
	return slices.ContainsFunc(g.config.Sources, func(source string) bool {
		return strings.HasPrefix(subject, source+".") || strings.HasPrefix(subject, strings.ReplaceAll(source, "-", "_")+".")
	})
}

// hidePlatformSubjects leaves the platform's subjects out of the list of
// subjects; customers' clients never need them.
func (g *Gateway) hidePlatformSubjects(response *http.Response) error {
	// The request is the one to the registry, under the upstream's path.
	request := response.Request
	path := strings.TrimPrefix(request.URL.Path, strings.TrimSuffix(g.config.Upstream.Path, "/"))
	if request.Method != http.MethodGet || strings.Trim(path, "/") != "subjects" || response.StatusCode != http.StatusOK {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxBody))
	_ = response.Body.Close()
	if err != nil {
		return err
	}
	var subjects []string
	if err := json.Unmarshal(body, &subjects); err == nil {
		subjects = slices.DeleteFunc(subjects, func(s string) bool { return strings.HasPrefix(s, platformPrefix) })
		if subjects == nil {
			subjects = []string{}
		}
		body, err = json.Marshal(subjects)
		if err != nil {
			return err
		}
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	response.ContentLength = int64(len(body))
	response.Header.Set("Content-Length", strconv.Itoa(len(body)))
	return nil
}

// pathSegments splits the request's path, unescaping each segment: a
// subject may hold a slash, escaped as %2F.
func pathSegments(u *url.URL) []string {
	var segments []string
	for segment := range strings.SplitSeq(strings.Trim(u.EscapedPath(), "/"), "/") {
		if segment == "" {
			continue
		}
		unescaped, err := url.PathUnescape(segment)
		if err != nil {
			unescaped = segment
		}
		segments = append(segments, unescaped)
	}
	return segments
}

// clientAddress is the client's IP address, from the connection: the load
// balancer keeps clients' addresses, and headers from clients are not
// trusted.
func clientAddress(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// writeError writes an error in the Confluent API's form.
func writeError(w http.ResponseWriter, status, code int, message string) {
	w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error_code": code, "message": message})
}

// Limiter counts failed attempts per client address, and refuses a client
// that failed max times within window until the window has passed. It keeps
// at most maxClients addresses; beyond them, every new client is refused, so
// that many addresses cannot exhaust its memory.
type Limiter struct {
	max    int
	window time.Duration
	now    func() time.Time

	mu       sync.Mutex
	failures map[string][]time.Time
}

// maxClients caps the addresses a Limiter remembers.
const maxClients = 100_000

// NewLimiter returns a Limiter.
func NewLimiter(maxFailures int, window time.Duration, now func() time.Time) *Limiter {
	return &Limiter{max: maxFailures, window: window, now: now, failures: map[string][]time.Time{}}
}

// Allowed reports whether a client may try.
func (l *Limiter) Allowed(client string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.forget(client)
	return len(l.failures[client]) < l.max
}

// Fail records a failed attempt of a client.
func (l *Limiter) Fail(client string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, known := l.failures[client]; !known && len(l.failures) >= maxClients {
		l.prune()
		if len(l.failures) >= maxClients {
			return
		}
	}
	l.failures[client] = append(l.failures[client], l.now())
}

// forget drops a client's failures older than the window.
func (l *Limiter) forget(client string) {
	recent := slices.DeleteFunc(l.failures[client], func(t time.Time) bool { return l.now().Sub(t) >= l.window })
	if len(recent) == 0 {
		delete(l.failures, client)
		return
	}
	l.failures[client] = recent
}

// prune forgets the failures older than the window of every client.
func (l *Limiter) prune() {
	for client := range l.failures {
		l.forget(client)
	}
}
