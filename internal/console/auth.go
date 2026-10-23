package console

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
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

// keySet caches the instance's signing keys: it fetches them for the first
// request and again for a key it has not seen, at most once a minute, which
// covers Clerk's key rotation without a request to Clerk per token.
type keySet struct {
	fetch KeySource
	now   func() time.Time

	mu   sync.Mutex
	keys map[string]*clerk.JSONWebKey
	// nextFetch is when an unknown key ID may make the console ask Clerk
	// again, so that forged tokens cannot flood Clerk.
	nextFetch time.Time
}

const (
	// refetchInterval spaces out fetches after a successful one.
	refetchInterval = time.Minute
	// retryInterval spaces out fetches after a failed one, so that a short
	// outage at Clerk does not lock everyone out for long.
	retryInterval = 10 * time.Second
)

func (s *keySet) key(ctx context.Context, keyID string) (*clerk.JSONWebKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key, ok := s.keys[keyID]; ok {
		return key, nil
	}
	if s.now().Before(s.nextFetch) {
		return nil, fmt.Errorf("unknown signing key %q", keyID)
	}
	keys, err := s.fetch(ctx)
	if err != nil {
		s.nextFetch = s.now().Add(retryInterval)
		return nil, fmt.Errorf("fetch Clerk's signing keys: %w", err)
	}
	s.nextFetch = s.now().Add(refetchInterval)
	s.keys = map[string]*clerk.JSONWebKey{}
	for _, key := range keys {
		if key != nil {
			s.keys[key.KeyID] = key
		}
	}
	if key, ok := s.keys[keyID]; ok {
		return key, nil
	}
	return nil, fmt.Errorf("unknown signing key %q", keyID)
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
