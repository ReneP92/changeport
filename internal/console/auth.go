package console

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/clerk/clerk-sdk-go/v2"
	"github.com/clerk/clerk-sdk-go/v2/jwt"
)

// Principal is the signed-in user of a request.
type Principal struct {
	UserID string
	// OrganizationID is the user's active Clerk organization, if any.
	OrganizationID string
	// OrganizationRole is the user's role in it, such as org:admin.
	OrganizationRole string
}

// Admin reports whether the user is an admin of the active organization.
func (p Principal) Admin() bool { return p.OrganizationRole == "org:admin" }

// FrontendAPI returns the host of the Clerk instance's Frontend API, which
// its publishable key encodes: pk_test_ or pk_live_, then the host and a
// dollar sign in base64.
func FrontendAPI(publishableKey string) (string, error) {
	encoded, ok := strings.CutPrefix(publishableKey, "pk_test_")
	if !ok {
		encoded, ok = strings.CutPrefix(publishableKey, "pk_live_")
	}
	if !ok {
		return "", errors.New("the Clerk publishable key does not start with pk_test_ or pk_live_")
	}
	decoded, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(encoded, "="))
	if err != nil {
		return "", fmt.Errorf("the Clerk publishable key is not valid base64: %w", err)
	}
	host, ok := strings.CutSuffix(string(decoded), "$")
	if !ok || host == "" || strings.ContainsAny(host, "/ ") {
		return "", errors.New("the Clerk publishable key does not hold a Frontend API host")
	}
	return host, nil
}

// KeySource fetches the Clerk instance's JSON Web Key Set.
type KeySource func(ctx context.Context) ([]*clerk.JSONWebKey, error)

// keySet caches the instance's signing keys. The console fetches them when
// it starts (warm), and again for a key it has not seen, at most once a
// minute, which covers Clerk's key rotation without a request to Clerk per
// token. A failed fetch is retried soon, then less and less often.
type keySet struct {
	fetch  KeySource
	now    func() time.Time
	logger *slog.Logger

	mu   sync.Mutex
	keys map[string]*clerk.JSONWebKey
	// nextFetch is when an unknown key ID may make the console ask Clerk
	// again, so that forged tokens cannot flood Clerk.
	nextFetch time.Time
	// failures counts the fetches that failed in a row.
	failures int
}

const (
	// refetchInterval spaces out fetches after a successful one.
	refetchInterval = time.Minute
	// firstRetry and lastRetry bound the wait after failed fetches, which
	// doubles with each one, so that a blip at Clerk costs a second and an
	// outage does not become a flood.
	firstRetry = time.Second
	lastRetry  = 30 * time.Second
	// fetchTimeout bounds one fetch, so that a slow Clerk cannot hold up
	// requests for long.
	fetchTimeout = 10 * time.Second
)

// key returns the signing key keyID, fetching the keys when it is unknown
// and the last fetch is long enough ago.
func (s *keySet) key(ctx context.Context, keyID string) (*clerk.JSONWebKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key, ok := s.keys[keyID]; ok {
		return key, nil
	}
	if s.now().Before(s.nextFetch) {
		return nil, fmt.Errorf("unknown signing key %q", keyID)
	}
	if err := s.refreshLocked(ctx); err != nil {
		return nil, err
	}
	if key, ok := s.keys[keyID]; ok {
		return key, nil
	}
	return nil, fmt.Errorf("unknown signing key %q", keyID)
}

// refreshLocked fetches the keys and schedules the next fetch. The caller
// holds mu.
func (s *keySet) refreshLocked(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	keys, err := s.fetch(ctx)
	if err != nil {
		s.failures++
		wait := min(firstRetry<<(s.failures-1), lastRetry)
		s.nextFetch = s.now().Add(wait)
		s.logger.Warn("could not fetch Clerk's signing keys", "error", err, "failures", s.failures, "retry", wait.String())
		return fmt.Errorf("fetch Clerk's signing keys: %w", err)
	}
	s.failures = 0
	s.nextFetch = s.now().Add(refetchInterval)
	s.keys = map[string]*clerk.JSONWebKey{}
	for _, key := range keys {
		if key != nil {
			s.keys[key.KeyID] = key
		}
	}
	return nil
}

// warm fetches the keys until it has them or ctx is done, so that the first
// requests do not wait on Clerk.
func (s *keySet) warm(ctx context.Context) {
	for {
		s.mu.Lock()
		if len(s.keys) > 0 {
			s.mu.Unlock()
			return
		}
		wait := s.nextFetch.Sub(s.now())
		if wait <= 0 {
			err := s.refreshLocked(ctx)
			wait = s.nextFetch.Sub(s.now())
			if err == nil {
				s.mu.Unlock()
				return
			}
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// authenticator verifies Clerk session tokens.
type authenticator struct {
	keys *keySet
	// issuer is the instance's Frontend API, which issues its tokens.
	issuer string
	// origins are the console's own origins, the only authorized parties.
	origins []string
	clock   clerk.Clock
}

// errUnauthenticated means the request has no valid session token.
var errUnauthenticated = errors.New("unauthenticated")

// authenticate returns the principal of the request's Bearer token.
func (a *authenticator) authenticate(r *http.Request) (Principal, error) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return Principal{}, fmt.Errorf("%w: no Bearer token", errUnauthenticated)
	}
	unverified, err := jwt.Decode(r.Context(), &jwt.DecodeParams{Token: token})
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %w", errUnauthenticated, err)
	}
	key, err := a.keys.key(r.Context(), unverified.KeyID)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %w", errUnauthenticated, err)
	}
	claims, err := jwt.Verify(r.Context(), &jwt.VerifyParams{
		Token: token,
		JWK:   key,
		Clock: a.clock,
		// Allow for a little clock skew between Clerk and the cluster.
		Leeway: 5 * time.Second,
		AuthorizedPartyHandler: func(party string) bool {
			return slices.Contains(a.origins, party)
		},
	})
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %w", errUnauthenticated, err)
	}
	// The SDK accepts any Clerk issuer; only this instance's tokens count.
	if claims.Issuer != a.issuer {
		return Principal{}, fmt.Errorf("%w: issuer %q is not this console's Clerk instance", errUnauthenticated, claims.Issuer)
	}
	if claims.Subject == "" {
		return Principal{}, fmt.Errorf("%w: the token names no user", errUnauthenticated)
	}
	return Principal{
		UserID:           claims.Subject,
		OrganizationID:   claims.ActiveOrganizationID,
		OrganizationRole: claims.ActiveOrganizationRole,
	}, nil
}

type principalKey struct{}

// principalFrom returns the request's principal, which the authentication
// middleware put in its context.
func principalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}
