// Package console is the Changeport console's server: the API of
// api/openapi.yaml behind Clerk sign-in, and the web app.
package console

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/clerk/clerk-sdk-go/v2"
	"github.com/clerk/clerk-sdk-go/v2/jwks"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/utils/ptr"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/console/api"
	"github.com/ReneP92/changeport/internal/provisioning"
)

// Config is the console's Clerk configuration. Without all of it the
// console fails closed: every API route answers 503.
type Config struct {
	PublishableKey string
	SecretKey      string
	// StaffOrganizationID is the Clerk organization of Changeport's
	// operators.
	StaffOrganizationID string
	// Origins are the console's origins, such as http://localhost:8088:
	// the authorized parties session tokens must name.
	Origins []string
}

// missing lists the settings the configuration lacks.
func (c Config) missing() []string {
	var missing []string
	if c.PublishableKey == "" {
		missing = append(missing, "the Clerk publishable key")
	}
	if c.SecretKey == "" {
		missing = append(missing, "the Clerk secret key")
	}
	if c.StaffOrganizationID == "" {
		missing = append(missing, "the staff organization ID")
	}
	if len(c.Origins) == 0 {
		missing = append(missing, "the console's origins")
	}
	return missing
}

// Server serves the console.
type Server struct {
	config    Config
	auth      *authenticator
	customers Customers
	ui        fs.FS
	logger    *slog.Logger
	// csp is the Content-Security-Policy of the web app's pages.
	csp string
}

// Options holds what New needs besides the configuration.
type Options struct {
	Customers Customers
	// UI is the built web app, with index.html at its root.
	UI     fs.FS
	Logger *slog.Logger
	// Keys fetches Clerk's signing keys; the default asks Clerk's Backend
	// API with the secret key.
	Keys KeySource
	// Clock replaces the time for verifying tokens, in tests.
	Clock clerk.Clock
}

// New returns a console server. An incomplete configuration is not an
// error: the console then serves the web app and refuses every API call.
func New(config Config, options Options) (*Server, error) {
	s := &Server{config: config, customers: options.Customers, ui: options.UI, logger: options.Logger}
	if missing := config.missing(); len(missing) > 0 {
		s.logger.Warn("the console is not configured and refuses every API call", "missing", strings.Join(missing, ", "))
		s.csp = contentSecurityPolicy("")
		return s, nil
	}
	frontendAPI, err := FrontendAPI(config.PublishableKey)
	if err != nil {
		return nil, err
	}
	keys := options.Keys
	if keys == nil {
		keys = clerkKeys(config.SecretKey)
	}
	s.auth = &authenticator{
		keys:    &keySet{fetch: keys, now: time.Now},
		issuer:  "https://" + frontendAPI,
		origins: config.Origins,
		clock:   options.Clock,
	}
	s.csp = contentSecurityPolicy(frontendAPI)
	return s, nil
}

// clerkKeys fetches the signing keys from Clerk's Backend API.
func clerkKeys(secretKey string) KeySource {
	client := jwks.NewClient(&clerk.ClientConfig{BackendConfig: clerk.BackendConfig{Key: &secretKey}})
	return func(ctx context.Context) ([]*clerk.JSONWebKey, error) {
		set, err := client.Get(ctx, &jwks.GetParams{})
		if err != nil {
			return nil, err
		}
		return set.Keys, nil
	}
}

// contentSecurityPolicy allows the web app's own files and, with a Clerk
// Frontend API, what Clerk's sign-in components load: its script and API,
// its bot protection (a script, and probes of its servers on several ports),
// user images and the Cloudflare bot check.
func contentSecurityPolicy(frontendAPI string) string {
	scripts, connections := "", ""
	if frontendAPI != "" {
		scripts = " https://" + frontendAPI + " https://cdn.protect.clerk.com"
		connections = " https://" + frontendAPI + " https://*.protect.clerk.com:*"
	}
	return strings.Join([]string{
		"default-src 'self'",
		"script-src 'self'" + scripts + " https://challenges.cloudflare.com",
		"connect-src 'self'" + connections,
		"img-src 'self' data: https://img.clerk.com",
		"style-src 'self' 'unsafe-inline'",
		"frame-src https://challenges.cloudflare.com",
		"worker-src 'self' blob:",
		"form-action 'self'",
		"frame-ancestors 'none'",
		"base-uri 'self'",
		"object-src 'none'",
	}, "; ")
}

// Handler returns the console's HTTP handler.
func (s *Server) Handler() http.Handler {
	routes := http.NewServeMux()
	routes.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	routes.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	apiRoutes := http.NewServeMux()
	apiRoutes.HandleFunc("/api/", func(w http.ResponseWriter, _ *http.Request) {
		writeProblem(w, http.StatusNotFound, "No such API route", "")
	})
	strict := api.NewStrictHandlerWithOptions(s, nil, api.StrictHTTPServerOptions{
		RequestErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, err error) {
			writeProblem(w, http.StatusBadRequest, "Bad request", err.Error())
		},
		ResponseErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			s.logger.Error("API request failed", "method", r.Method, "path", r.URL.Path, "error", err)
			writeProblem(w, http.StatusInternalServerError, "Something went wrong", "")
		},
	})
	api.HandlerWithOptions(strict, api.StdHTTPServerOptions{
		BaseRouter: apiRoutes,
		ErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, err error) {
			writeProblem(w, http.StatusBadRequest, "Bad request", err.Error())
		},
	})
	routes.Handle("/api/", s.authenticate(apiRoutes))
	routes.Handle("/", s.serveUI())
	return securityHeaders(routes)
}

// authenticate refuses every API call while the console is not configured,
// and every call but GET /api/config without a valid session token.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.auth == nil {
			writeProblem(w, http.StatusServiceUnavailable, "The console is not configured", "It has no Clerk configuration, so it refuses every request.")
			return
		}
		if r.URL.Path == "/api/config" {
			next.ServeHTTP(w, r)
			return
		}
		principal, err := s.auth.authenticate(r)
		if err != nil {
			s.logger.Debug("refused a request", "path", r.URL.Path, "error", err)
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeProblem(w, http.StatusUnauthorized, "Sign in first", "")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, principal)))
	})
}

// securityHeaders sets the headers every response carries.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		next.ServeHTTP(w, r)
	})
}

// serveUI serves the web app's files, and index.html for every other path,
// so that the app's own routes load it. Hashed assets are cached for good;
// index.html never, so a new release shows at once.
func (s *Server) serveUI() http.Handler {
	files := http.FileServerFS(s.ui)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if name != "" && name != "index.html" {
			if info, err := fs.Stat(s.ui, name); err == nil && !info.IsDir() {
				if strings.HasPrefix(name, "assets/") {
					w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
				}
				files.ServeHTTP(w, r)
				return
			}
		}
		index, err := fs.ReadFile(s.ui, "index.html")
		if err != nil {
			http.Error(w, "the web app is not built into this console; run make web-dev", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Content-Security-Policy", s.csp)
		_, _ = w.Write(index)
	})
}

func writeProblem(w http.ResponseWriter, status int, title, detail string) {
	problem := api.Problem{Status: status, Title: title}
	if detail != "" {
		problem.Detail = &detail
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problem)
}

func problem(status int, title, detail string) api.Problem {
	p := api.Problem{Status: status, Title: title}
	if detail != "" {
		p.Detail = &detail
	}
	return p
}

var _ api.StrictServerInterface = (*Server)(nil)

// GetConfig implements api.StrictServerInterface.
func (s *Server) GetConfig(context.Context, api.GetConfigRequestObject) (api.GetConfigResponseObject, error) {
	return api.GetConfig200JSONResponse{ClerkPublishableKey: s.config.PublishableKey}, nil
}

// GetMe implements api.StrictServerInterface.
func (s *Server) GetMe(ctx context.Context, _ api.GetMeRequestObject) (api.GetMeResponseObject, error) {
	principal, err := mustPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	me := api.GetMe200JSONResponse{UserID: principal.UserID, Access: api.MeAccessNone}
	if principal.OrganizationID == "" {
		return me, nil
	}
	me.OrganizationID = ptr.To(principal.OrganizationID)
	me.Admin = ptr.To(principal.Admin())
	if s.operator(principal) {
		me.Access = api.MeAccessOperator
		return me, nil
	}
	customer, err := s.customers.ByOrganization(ctx, principal.OrganizationID)
	if err != nil {
		return nil, err
	}
	if customer != nil {
		me.Access = api.MeAccessCustomer
		me.CustomerID = ptr.To(customer.Name)
	}
	return me, nil
}

// GetCustomer implements api.StrictServerInterface.
func (s *Server) GetCustomer(ctx context.Context, _ api.GetCustomerRequestObject) (api.GetCustomerResponseObject, error) {
	principal, err := mustPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	switch {
	case principal.OrganizationID == "":
		return api.GetCustomer403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(problem(http.StatusForbidden,
			"Choose an organization", "Customers are Clerk organizations; switch to yours to see its cluster."))}, nil
	case s.operator(principal):
		return api.GetCustomer403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(problem(http.StatusForbidden,
			"The staff organization is not a customer", "Operators see customers in the operator console."))}, nil
	}
	customer, err := s.customers.ByOrganization(ctx, principal.OrganizationID)
	if err != nil {
		return nil, err
	}
	if customer == nil {
		return api.GetCustomer404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(problem(http.StatusNotFound,
			"No Kafka cluster yet", "Your organization has no Changeport cluster yet."))}, nil
	}
	return api.GetCustomer200JSONResponse(toAPI(customer)), nil
}

// ListCustomers implements api.StrictServerInterface.
func (s *Server) ListCustomers(ctx context.Context, _ api.ListCustomersRequestObject) (api.ListCustomersResponseObject, error) {
	principal, err := mustPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if !s.operator(principal) {
		return api.ListCustomers403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(operatorsOnly)}, nil
	}
	customers, err := s.customers.List(ctx)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(customers, func(a, b v1alpha1.Customer) int { return cmp.Compare(a.Name, b.Name) })
	list := api.ListCustomers200JSONResponse{Customers: make([]api.Customer, 0, len(customers))}
	for i := range customers {
		list.Customers = append(list.Customers, toAPI(&customers[i]))
	}
	return list, nil
}

// GetOperatorCustomer implements api.StrictServerInterface.
func (s *Server) GetOperatorCustomer(ctx context.Context, request api.GetOperatorCustomerRequestObject) (api.GetOperatorCustomerResponseObject, error) {
	principal, err := mustPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if !s.operator(principal) {
		return api.GetOperatorCustomer403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(operatorsOnly)}, nil
	}
	var customer *v1alpha1.Customer
	if provisioning.ValidateCustomerID(request.CustomerID) == nil {
		if customer, err = s.customers.Get(ctx, request.CustomerID); err != nil {
			return nil, err
		}
	}
	if customer == nil {
		return api.GetOperatorCustomer404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(problem(http.StatusNotFound,
			"No such customer", fmt.Sprintf("There is no customer %s.", request.CustomerID)))}, nil
	}
	return api.GetOperatorCustomer200JSONResponse(toAPI(customer)), nil
}

var operatorsOnly = problem(http.StatusForbidden, "Operators only", "Switch to the Changeport staff organization.")

// operator reports whether the principal acts for the staff organization.
func (s *Server) operator(p Principal) bool {
	return p.OrganizationID != "" && p.OrganizationID == s.config.StaffOrganizationID
}

// mustPrincipal returns the request's principal. Every route but
// GET /api/config is behind authentication, so a missing one is a bug.
func mustPrincipal(ctx context.Context) (Principal, error) {
	p, ok := principalFrom(ctx)
	if !ok {
		return Principal{}, errors.New("no principal in an authenticated request")
	}
	return p, nil
}

// conditionTypes are a Customer's conditions in the order its cluster gets
// ready.
var conditionTypes = []string{
	v1alpha1.ConditionCertificateIssued,
	v1alpha1.ConditionKafkaReady,
	v1alpha1.ConditionUsersReady,
	v1alpha1.ConditionAgentAvailable,
	v1alpha1.ConditionReady,
}

// toAPI describes a Customer for the API.
func toAPI(c *v1alpha1.Customer) api.Customer {
	out := api.Customer{
		Id:                 c.Name,
		DisplayName:        c.Spec.DisplayName,
		Profile:            api.CustomerProfile(c.Spec.Profile),
		DeletionProtection: c.Spec.DeletionProtection,
		CreatedAt:          c.CreationTimestamp.UTC(),
		Conditions:         []api.Condition{},
		Status:             api.Status{Reason: v1alpha1.ReasonPending, Message: "Waiting for the provisioning controller"},
	}
	if c.Status.BootstrapAddress != "" {
		out.BootstrapAddress = ptr.To(c.Status.BootstrapAddress)
	}
	if c.DeletionTimestamp != nil {
		out.Deleting = ptr.To(true)
	}
	for _, conditionType := range conditionTypes {
		condition := meta.FindStatusCondition(c.Status.Conditions, conditionType)
		if condition == nil {
			continue
		}
		ready := condition.Status == "True"
		out.Conditions = append(out.Conditions, api.Condition{
			Type:    api.ConditionType(conditionType),
			Ready:   ready,
			Reason:  condition.Reason,
			Message: condition.Message,
			Since:   ptr.To(condition.LastTransitionTime.UTC()),
		})
		if conditionType == v1alpha1.ConditionReady {
			// A condition about an older spec does not describe the cluster.
			out.Ready = ready && condition.ObservedGeneration >= c.Generation
			out.Status = api.Status{Reason: condition.Reason, Message: condition.Message}
		}
	}
	return out
}
