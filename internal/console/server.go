// Package console is the Changeport console's server: the API of
// api/openapi.yaml behind Clerk sign-in, and the web app.
package console

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/mail"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/clerk/clerk-sdk-go/v2"
	"github.com/clerk/clerk-sdk-go/v2/jwks"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/utils/ptr"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/clusters"
	"github.com/ReneP92/changeport/internal/console/api"
	"github.com/ReneP92/changeport/internal/metrics"
	"github.com/ReneP92/changeport/internal/provisioning"
	"github.com/ReneP92/changeport/internal/topics"
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
	config        Config
	auth          *authenticator
	customers     Customers
	organizations Organizations
	clusters      Clusters
	metrics       Metrics
	ui            fs.FS
	logger        *slog.Logger
	// csp is the Content-Security-Policy of the web app's pages.
	csp string
}

// Options holds what New needs besides the configuration.
type Options struct {
	Customers Customers
	// Organizations manages customers' Clerk organizations; the default
	// calls Clerk's Backend API with the secret key.
	Organizations Organizations
	// Clusters and Metrics read the customers' clusters and their metrics.
	Clusters Clusters
	Metrics  Metrics
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
	s := &Server{
		config:        config,
		customers:     options.Customers,
		organizations: options.Organizations,
		clusters:      options.Clusters,
		metrics:       options.Metrics,
		ui:            options.UI,
		logger:        options.Logger,
	}
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
	if s.organizations == nil {
		// Invitations lead to the console's first origin.
		s.organizations = NewClerkOrganizations(config.SecretKey, config.Origins[0])
	}
	s.auth = &authenticator{
		keys:    &keySet{fetch: keys, now: time.Now, logger: options.Logger},
		issuer:  "https://" + frontendAPI,
		origins: config.Origins,
		clock:   options.Clock,
	}
	s.csp = contentSecurityPolicy(frontendAPI)
	return s, nil
}

// WarmUp fetches Clerk's signing keys before the first request needs them,
// retrying until it has them or ctx is done.
func (s *Server) WarmUp(ctx context.Context) {
	if s.auth != nil {
		s.auth.keys.warm(ctx)
	}
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
			if r.Context().Err() != nil {
				// The client went away; there is nobody to answer.
				s.logger.Debug("API request abandoned", "method", r.Method, "path", r.URL.Path, "error", err)
				return
			}
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
		return api.GetOperatorCustomer404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(noSuchCustomer(request.CustomerID))}, nil
	}
	return api.GetOperatorCustomer200JSONResponse(toAPI(customer)), nil
}

// ProvisionCustomer implements api.StrictServerInterface: it creates the
// customer's Clerk organization, invites its first admin if asked, and
// creates the customer, undoing the organization if a later step fails.
func (s *Server) ProvisionCustomer(ctx context.Context, request api.ProvisionCustomerRequestObject) (api.ProvisionCustomerResponseObject, error) {
	principal, err := mustPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if !s.operator(principal) {
		return api.ProvisionCustomer403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(operatorsOnly)}, nil
	}
	invalid := func(field, detail string) api.ProvisionCustomerResponseObject {
		p := problem(http.StatusBadRequest, "Invalid customer", detail)
		p.Field = &field
		return api.ProvisionCustomer400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(p)}
	}
	body := request.Body
	displayName := strings.TrimSpace(body.DisplayName)
	email := ""
	if body.InviteEmail != nil {
		email = strings.TrimSpace(*body.InviteEmail)
	}
	switch {
	case displayName == "" || len(displayName) > maxDisplayNameLength:
		return invalid("displayName", fmt.Sprintf("Name the customer in 1 to %d characters.", maxDisplayNameLength)), nil
	case !validProfile(string(body.Profile)):
		return invalid("profile", "Choose dev, dev-ha or production."), nil
	case email != "" && !validEmail(email):
		return invalid("inviteEmail", "Enter an email address, such as ada@example.com, or leave it empty."), nil
	}

	organizationID, err := s.organizations.Create(ctx, displayName)
	if err != nil {
		return nil, err
	}
	// Undoes the organization when the customer cannot be provisioned, even
	// when the request is gone.
	undo := func() {
		if err := s.organizations.Delete(context.WithoutCancel(ctx), organizationID); err != nil {
			s.logger.ErrorContext(ctx, "could not delete the organization of a failed provisioning", "organization", organizationID, "error", err)
		}
	}
	if email != "" {
		if err := s.organizations.InviteAdmin(ctx, organizationID, email); err != nil {
			undo()
			if errors.Is(err, ErrInvitationRefused) {
				return invalid("inviteEmail", err.Error()), nil
			}
			return nil, err
		}
	}
	customer := &v1alpha1.Customer{
		Spec: v1alpha1.CustomerSpec{
			DisplayName:         displayName,
			Profile:             string(body.Profile),
			ClerkOrganizationID: organizationID,
			DeletionProtection:  body.DeletionProtection,
		},
	}
	// The controller keeps the label in sync; set now, the organization's
	// members find their customer at once.
	customer.Labels = map[string]string{v1alpha1.ClerkOrganizationLabel: organizationID}
	for attempt := 0; ; attempt++ {
		customer.Name, err = newCustomerID()
		if err == nil {
			err = s.customers.Create(ctx, customer)
		}
		if !errors.Is(err, ErrCustomerExists) || attempt == 4 {
			break
		}
	}
	if err != nil {
		undo()
		return nil, err
	}
	s.audit(ctx, principal, customer.Name, "customer provisioned", "display_name", displayName, "profile", customer.Spec.Profile,
		"customer_organization", organizationID, "deletion_protection", customer.Spec.DeletionProtection, "invited", email != "")
	return api.ProvisionCustomer201JSONResponse(toAPI(customer)), nil
}

// DeprovisionCustomer implements api.StrictServerInterface. The customer's
// Clerk organization is kept, no longer linked to a cluster.
func (s *Server) DeprovisionCustomer(ctx context.Context, request api.DeprovisionCustomerRequestObject) (api.DeprovisionCustomerResponseObject, error) {
	principal, err := mustPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if !s.operator(principal) {
		return api.DeprovisionCustomer403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(operatorsOnly)}, nil
	}
	customer, err := s.customers.Get(ctx, request.CustomerID)
	if err != nil {
		return nil, err
	}
	if customer == nil {
		return api.DeprovisionCustomer404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(noSuchCustomer(request.CustomerID))}, nil
	}
	if customer.Spec.DeletionProtection {
		return api.DeprovisionCustomer409JSONResponse{ConflictJSONResponse: api.ConflictJSONResponse(problem(http.StatusConflict,
			"Deletion protection is on", fmt.Sprintf("Customer %[1]s is protected; an operator turns protection off first with provisioner unprotect -customer %[1]s -confirm %[1]s.", customer.Name)))}, nil
	}
	id, organizationID := customer.Name, customer.Spec.ClerkOrganizationID
	err = s.customers.Delete(ctx, id)
	if errors.Is(err, ErrNoSuchCustomer) {
		return api.DeprovisionCustomer404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(noSuchCustomer(id))}, nil
	}
	if err != nil {
		return nil, err
	}
	s.audit(ctx, principal, id, "customer deprovisioned", "customer_organization", organizationID)
	return api.DeprovisionCustomer204Response{}, nil
}

// maxDisplayNameLength is the Customer resource's limit.
const maxDisplayNameLength = 100

func validProfile(name string) bool {
	_, err := provisioning.ProfileByName(name)
	return err == nil
}

// validEmail reports whether s is a bare email address, such as
// ada@example.com, without a name.
func validEmail(s string) bool {
	address, err := mail.ParseAddress(s)
	return err == nil && address.Address == s && address.Name == ""
}

// customerIDAlphabet leaves out letters and digits that look alike, such as
// l and 1, so that people can read IDs out.
const customerIDAlphabet = "abcdefghijkmnpqrstuvwxyz23456789"

// newCustomerID returns a new opaque customer ID, such as c-7f3k2q: about a
// billion possibilities, so that collisions are rare and retried.
func newCustomerID() (string, error) {
	random := make([]byte, 6)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}
	id := []byte("c-")
	for _, b := range random {
		id = append(id, customerIDAlphabet[int(b)%len(customerIDAlphabet)])
	}
	return string(id), nil
}

func noSuchCustomer(id string) api.Problem {
	return problem(http.StatusNotFound, "No such customer", fmt.Sprintf("There is no customer %s.", id))
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
		OrganizationID:     ptr.To(c.Spec.ClerkOrganizationID),
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
	if c.DeletionTimestamp != nil && out.Status.Reason != v1alpha1.ReasonDeprovisioning && out.Status.Reason != v1alpha1.ReasonDeletionProtected {
		// Until the controller sees the deletion, the status still says how
		// the cluster was.
		out.Ready = false
		out.Status = api.Status{Reason: v1alpha1.ReasonDeprovisioning, Message: "Waiting for the provisioning controller to delete the cluster"}
	}
	return out
}

// customerOf returns the customer of the principal's active organization
// for the customer routes, or the problem why there is none to act on.
func (s *Server) customerOf(ctx context.Context) (*v1alpha1.Customer, Principal, *api.Problem, error) {
	principal, err := mustPrincipal(ctx)
	if err != nil {
		return nil, principal, nil, err
	}
	switch {
	case principal.OrganizationID == "":
		p := problem(http.StatusForbidden, "Choose an organization", "Customers are Clerk organizations; switch to yours to see its cluster.")
		return nil, principal, &p, nil
	case s.operator(principal):
		p := problem(http.StatusForbidden, "The staff organization is not a customer", "Operators see customers in the operator console.")
		return nil, principal, &p, nil
	}
	customer, err := s.customers.ByOrganization(ctx, principal.OrganizationID)
	if err != nil {
		return nil, principal, nil, err
	}
	if customer == nil {
		p := problem(http.StatusNotFound, "No Kafka cluster yet", "Your organization has no Changeport cluster yet.")
		return nil, principal, &p, nil
	}
	return customer, principal, nil, nil
}

// GetConnection implements api.StrictServerInterface.
func (s *Server) GetConnection(ctx context.Context, _ api.GetConnectionRequestObject) (api.GetConnectionResponseObject, error) {
	customer, _, refused, err := s.customerOf(ctx)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && refused.Status == http.StatusNotFound:
		return api.GetConnection404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	case refused != nil:
		return api.GetConnection403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	}
	connection := api.GetConnection200JSONResponse{
		Username:         provisioning.AdminUser,
		SecurityProtocol: api.SASLSSL,
		SaslMechanism:    api.SCRAMSHA512,
	}
	if customer.Status.BootstrapAddress != "" {
		connection.BootstrapAddress = ptr.To(customer.Status.BootstrapAddress)
	}
	ca, err := s.clusters.ListenerCA(ctx, customer.Name)
	if err != nil {
		return nil, err
	}
	if ca != "" {
		connection.CaCertificate = ptr.To(ca)
	}
	return connection, nil
}

// clientPropertiesDownload is a client.properties sent as a file to save.
type clientPropertiesDownload struct {
	customerID string
	content    string
}

// VisitGetClientPropertiesResponse implements api.GetClientPropertiesResponseObject.
func (d clientPropertiesDownload) VisitGetClientPropertiesResponse(w http.ResponseWriter) error {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s-client.properties"`, d.customerID))
	// The file holds the admin user's password.
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte(d.content))
	return err
}

// GetClientProperties implements api.StrictServerInterface. Only the
// organization's admins may download the admin user's credentials.
func (s *Server) GetClientProperties(ctx context.Context, _ api.GetClientPropertiesRequestObject) (api.GetClientPropertiesResponseObject, error) {
	customer, principal, refused, err := s.customerOf(ctx)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && refused.Status == http.StatusNotFound:
		return api.GetClientProperties404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	case refused != nil:
		return api.GetClientProperties403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	case !principal.Admin():
		return api.GetClientProperties403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(problem(http.StatusForbidden,
			"Admins only", "Ask an admin of your organization for the cluster's credentials."))}, nil
	case customer.Status.BootstrapAddress == "":
		return api.GetClientProperties409JSONResponse{NotReadyJSONResponse: api.NotReadyJSONResponse(notReadyYet)}, nil
	}
	jaas, err := s.clusters.AdminCredentials(ctx, customer.Name)
	if errors.Is(err, clusters.ErrNotReady) {
		return api.GetClientProperties409JSONResponse{NotReadyJSONResponse: api.NotReadyJSONResponse(notReadyYet)}, nil
	}
	if err != nil {
		return nil, err
	}
	ca, err := s.clusters.ListenerCA(ctx, customer.Name)
	if err != nil {
		return nil, err
	}
	s.audit(ctx, principal, customer.Name, "credentials downloaded")
	return clientPropertiesDownload{
		customerID: customer.Name,
		content:    provisioning.ClientPropertiesFile(customer.Name, customer.Status.BootstrapAddress, jaas, ca),
	}, nil
}

var notReadyYet = problem(http.StatusConflict, "The cluster is not ready yet", "Try again once it is ready.")

// GetHealth implements api.StrictServerInterface. The partitions' health
// comes from the metrics store and is left out while it has none.
func (s *Server) GetHealth(ctx context.Context, _ api.GetHealthRequestObject) (api.GetHealthResponseObject, error) {
	customer, _, refused, err := s.customerOf(ctx)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && refused.Status == http.StatusNotFound:
		return api.GetHealth404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	case refused != nil:
		return api.GetHealth403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	}
	brokers, err := s.clusters.Brokers(ctx, customer.Name)
	if err != nil {
		return nil, err
	}
	health := api.GetHealth200JSONResponse{Brokers: make([]api.Broker, 0, len(brokers))}
	for _, b := range brokers {
		broker := api.Broker{Name: b.Name, Ready: b.Ready, Restarts: b.Restarts}
		if b.Zone != "" {
			broker.Zone = ptr.To(b.Zone)
		}
		health.Brokers = append(health.Brokers, broker)
	}
	now := time.Now()
	for q, field := range map[metrics.Query]**int{
		metrics.UnderReplicated:   &health.UnderReplicatedPartitions,
		metrics.UnderMinISR:       &health.UnderMinIsrPartitions,
		metrics.OfflinePartitions: &health.OfflinePartitions,
		metrics.ActiveControllers: &health.ActiveControllers,
		metrics.ActiveBrokers:     &health.ActiveBrokers,
		metrics.FencedBrokers:     &health.FencedBrokers,
	} {
		series, err := s.metrics.Instant(ctx, customer.Name, q, metrics.Filter{}, now)
		if err != nil {
			s.logger.Warn("the metrics store did not answer", "query", q, "tenant", customer.Name, "error", err)
			continue
		}
		if len(series) == 1 && len(series[0].Points) == 1 {
			*field = ptr.To(int(series[0].Points[0].Value))
		}
	}
	return health, nil
}

// ListTopics implements api.StrictServerInterface. Sizes and throughput
// come from the metrics store and are left out while it has none.
func (s *Server) ListTopics(ctx context.Context, _ api.ListTopicsRequestObject) (api.ListTopicsResponseObject, error) {
	customer, _, refused, err := s.customerOf(ctx)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && refused.Status == http.StatusNotFound:
		return api.ListTopics404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	case refused != nil:
		return api.ListTopics403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	}
	settings, err := s.clusters.Topics(ctx, customer.Name)
	if errors.Is(err, clusters.ErrNotReady) {
		return api.ListTopics409JSONResponse{NotReadyJSONResponse: api.NotReadyJSONResponse(notReadyYet)}, nil
	}
	if err != nil {
		return nil, err
	}
	now := time.Now()
	perTopic := map[metrics.Query]map[string]float64{}
	for _, q := range []metrics.Query{metrics.LocalSize, metrics.RemoteSize, metrics.BytesIn, metrics.BytesOut} {
		series, err := s.metrics.Instant(ctx, customer.Name, q, metrics.Filter{}, now)
		if err != nil {
			s.logger.Warn("the metrics store did not answer", "query", q, "tenant", customer.Name, "error", err)
			continue
		}
		perTopic[q] = map[string]float64{}
		for _, one := range series {
			if len(one.Points) == 1 {
				perTopic[q][one.Labels["topic"]] = one.Points[0].Value
			}
		}
	}
	value := func(q metrics.Query, topic string) *float32 {
		if v, ok := perTopic[q][topic]; ok {
			return ptr.To(float32(v))
		}
		return nil
	}
	list := api.ListTopics200JSONResponse{Topics: make([]api.Topic, 0, len(settings))}
	for _, t := range settings {
		list.Topics = append(list.Topics, api.Topic{
			Name:              t.Name,
			Partitions:        t.Partitions,
			ReplicationFactor: t.ReplicationFactor,
			Tiered:            t.Tiered,
			CleanupPolicy:     t.CleanupPolicy,
			RetentionMs:       t.RetentionMs,
			LocalBytes:        value(metrics.LocalSize, t.Name),
			RemoteBytes:       value(metrics.RemoteSize, t.Name),
			BytesInPerSecond:  value(metrics.BytesIn, t.Name),
			BytesOutPerSecond: value(metrics.BytesOut, t.Name),
		})
	}
	return list, nil
}

// throughputRanges are the ranges the throughput chart offers, each with a
// step that keeps it to a few hundred points.
var throughputRanges = map[api.GetThroughputParamsRange]time.Duration{
	api.N1h:  time.Minute,
	api.N6h:  5 * time.Minute,
	api.N24h: 15 * time.Minute,
	api.N7d:  time.Hour,
}

var rangeDurations = map[api.GetThroughputParamsRange]time.Duration{
	api.N1h:  time.Hour,
	api.N6h:  6 * time.Hour,
	api.N24h: 24 * time.Hour,
	api.N7d:  7 * 24 * time.Hour,
}

// GetThroughput implements api.StrictServerInterface: the bytes in and out
// per second over all topics.
func (s *Server) GetThroughput(ctx context.Context, request api.GetThroughputRequestObject) (api.GetThroughputResponseObject, error) {
	window := api.N1h
	if request.Params.Range != nil {
		window = *request.Params.Range
	}
	step, ok := throughputRanges[window]
	if !ok {
		return api.GetThroughput400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(problem(http.StatusBadRequest,
			"Unknown range", "Choose 1h, 6h, 24h or 7d."))}, nil
	}
	customer, _, refused, err := s.customerOf(ctx)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && refused.Status == http.StatusNotFound:
		return api.GetThroughput404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	case refused != nil:
		return api.GetThroughput403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	}
	end := time.Now().Truncate(step)
	start := end.Add(-rangeDurations[window])
	throughput := api.GetThroughput200JSONResponse{Range: string(window), StepSeconds: int(step.Seconds())}
	for q, field := range map[metrics.Query]*[]api.Point{metrics.BytesIn: &throughput.BytesIn, metrics.BytesOut: &throughput.BytesOut} {
		series, err := s.metrics.Range(ctx, customer.Name, q, metrics.Filter{}, start, end, step)
		if err != nil {
			return nil, err
		}
		*field = sumSeries(series)
	}
	return throughput, nil
}

// sumSeries adds up per-topic series point by point, into one series in
// time order.
func sumSeries(series []metrics.Series) []api.Point {
	totals := map[time.Time]float64{}
	for _, one := range series {
		for _, point := range one.Points {
			totals[point.Time] += point.Value
		}
	}
	points := make([]api.Point, 0, len(totals))
	for t, v := range totals {
		points = append(points, api.Point{Time: t.UTC(), Value: float32(v)})
	}
	slices.SortFunc(points, func(a, b api.Point) int { return a.Time.Compare(b.Time) })
	return points
}

// lagHistory is how far back the consumer groups' lag charts go, and their
// step.
const (
	lagHistory     = time.Hour
	lagHistoryStep = time.Minute
)

// groupTopic is a consumer group reading a topic: one row of lag.
type groupTopic struct{ group, topic string }

func groupTopicOf(series metrics.Series) groupTopic {
	return groupTopic{group: series.Labels["consumer_group"], topic: series.Labels["topic"]}
}

// ListConsumerGroups implements api.StrictServerInterface.
func (s *Server) ListConsumerGroups(ctx context.Context, _ api.ListConsumerGroupsRequestObject) (api.ListConsumerGroupsResponseObject, error) {
	customer, _, refused, err := s.customerOf(ctx)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && refused.Status == http.StatusNotFound:
		return api.ListConsumerGroups404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	case refused != nil:
		return api.ListConsumerGroups403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	}
	now := time.Now()
	latest := map[metrics.Query]map[groupTopic]float64{}
	for _, q := range []metrics.Query{metrics.LagSeconds, metrics.LagMessages, metrics.LagCapped} {
		series, err := s.metrics.Instant(ctx, customer.Name, q, metrics.Filter{}, now)
		if err != nil {
			return nil, err
		}
		latest[q] = map[groupTopic]float64{}
		for _, one := range series {
			if len(one.Points) == 1 {
				latest[q][groupTopicOf(one)] = one.Points[0].Value
			}
		}
	}
	end := now.Truncate(lagHistoryStep)
	history, err := s.metrics.Range(ctx, customer.Name, metrics.LagSeconds, metrics.Filter{}, end.Add(-lagHistory), end, lagHistoryStep)
	if err != nil {
		return nil, err
	}
	histories := map[groupTopic][]api.Point{}
	for _, one := range history {
		points := make([]api.Point, 0, len(one.Points))
		for _, point := range one.Points {
			points = append(points, api.Point{Time: point.Time.UTC(), Value: float32(point.Value)})
		}
		histories[groupTopicOf(one)] = points
	}

	// A row for every group and topic the agent measures now; the history
	// of groups that are gone is left out.
	list := api.ListConsumerGroups200JSONResponse{Groups: []api.ConsumerGroupLag{}}
	for key, seconds := range latest[metrics.LagSeconds] {
		messages, ok := latest[metrics.LagMessages][key]
		if !ok {
			continue
		}
		points := histories[key]
		if points == nil {
			points = []api.Point{}
		}
		list.Groups = append(list.Groups, api.ConsumerGroupLag{
			Group:       key.group,
			Topic:       key.topic,
			LagSeconds:  float32(seconds),
			LagMessages: int64(messages),
			Capped:      latest[metrics.LagCapped][key] == 1,
			History:     points,
		})
	}
	slices.SortFunc(list.Groups, func(a, b api.ConsumerGroupLag) int {
		return cmp.Or(
			cmp.Compare(b.LagSeconds, a.LagSeconds),
			cmp.Compare(b.LagMessages, a.LagMessages),
			strings.Compare(a.Group, b.Group),
			strings.Compare(a.Topic, b.Topic),
		)
	})
	return list, nil
}

// topicPolicy returns the topic guardrails of a customer's cluster.
func topicPolicy(customer *v1alpha1.Customer) (topics.Policy, error) {
	profile, err := provisioning.ProfileByName(customer.Spec.Profile)
	if err != nil {
		return topics.Policy{}, err
	}
	return topics.ForProfile(profile)
}

// GetTopicPolicy implements api.StrictServerInterface.
func (s *Server) GetTopicPolicy(ctx context.Context, _ api.GetTopicPolicyRequestObject) (api.GetTopicPolicyResponseObject, error) {
	customer, _, refused, err := s.customerOf(ctx)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && refused.Status == http.StatusNotFound:
		return api.GetTopicPolicy404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	case refused != nil:
		return api.GetTopicPolicy403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	}
	policy, err := topicPolicy(customer)
	if err != nil {
		return nil, err
	}
	response := api.GetTopicPolicy200JSONResponse{
		MaxNameLength:      topics.MaxNameLength,
		ReservedPrefixes:   topics.ReservedPrefixes,
		Partitions:         api.PartitionRange{Min: 1, Max: policy.MaxPartitions, Default: topics.DefaultPartitions},
		ReplicationFactor:  policy.ReplicationFactor,
		DefaultRetentionMs: topics.DefaultRetention.Milliseconds(),
		CleanupPolicies: []api.CleanupPolicyOption{
			{Name: api.CleanupPolicyOptionNameDelete, Tiered: true},
			{Name: api.CleanupPolicyOptionNameCompact, Tiered: false},
		},
	}
	for _, preset := range topics.RetentionPresets {
		response.RetentionPresets = append(response.RetentionPresets, api.RetentionPreset{Label: preset.Label, Ms: preset.Duration.Milliseconds()})
	}
	return response, nil
}

var adminsOnly = problem(http.StatusForbidden, "Admins only", "Ask an admin of your organization to change topics.")

// CreateTopic implements api.StrictServerInterface: organization admins
// create topics within the guardrails, and every creation is audited.
func (s *Server) CreateTopic(ctx context.Context, request api.CreateTopicRequestObject) (api.CreateTopicResponseObject, error) {
	customer, principal, refused, err := s.customerOf(ctx)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && refused.Status == http.StatusNotFound:
		return api.CreateTopic404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	case refused != nil:
		return api.CreateTopic403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	case !principal.Admin():
		return api.CreateTopic403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(adminsOnly)}, nil
	case request.Body == nil:
		return api.CreateTopic400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(problem(http.StatusBadRequest, "Describe the topic", ""))}, nil
	}
	policy, err := topicPolicy(customer)
	if err != nil {
		return nil, err
	}
	load, err := s.clusters.Load(ctx, customer.Name)
	if errors.Is(err, clusters.ErrNotReady) {
		return api.CreateTopic409JSONResponse{ConflictJSONResponse: api.ConflictJSONResponse(notReadyYet)}, nil
	}
	if err != nil {
		return nil, err
	}
	body := *request.Body
	topic, err := policy.Check(topics.Request{
		Name:          body.Name,
		Partitions:    body.Partitions,
		Retention:     time.Duration(body.RetentionMs) * time.Millisecond,
		CleanupPolicy: string(body.CleanupPolicy),
	}, load)
	var fieldErr *topics.FieldError
	if errors.As(err, &fieldErr) {
		p := problem(http.StatusBadRequest, "The topic breaks a guardrail", fieldErr.Message)
		p.Field = ptr.To(fieldErr.Field)
		if errors.Is(err, topics.ErrConflict) {
			p.Status, p.Title = http.StatusConflict, "The name is taken"
			return api.CreateTopic409JSONResponse{ConflictJSONResponse: api.ConflictJSONResponse(p)}, nil
		}
		return api.CreateTopic400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(p)}, nil
	}
	if err != nil {
		return nil, err
	}
	if err := s.clusters.CreateTopic(ctx, customer.Name, topic); errors.Is(err, ErrTopicExists) {
		p := problem(http.StatusConflict, "The name is taken", fmt.Sprintf("topic %s exists", topic.Name))
		p.Field = ptr.To("name")
		return api.CreateTopic409JSONResponse{ConflictJSONResponse: api.ConflictJSONResponse(p)}, nil
	} else if err != nil {
		return nil, err
	}
	s.audit(ctx, principal, customer.Name, "topic created", "topic", topic.Name,
		"partitions", topic.Partitions, "retention_ms", body.RetentionMs, "cleanup_policy", body.CleanupPolicy)
	created := api.CreateTopic201JSONResponse{
		Name:              topic.Name,
		Partitions:        int(topic.Partitions),
		ReplicationFactor: int(topic.ReplicationFactor),
		Tiered:            topic.Configs["remote.storage.enable"] == "true",
		CleanupPolicy:     topic.Configs["cleanup.policy"],
		RetentionMs:       ptr.To(body.RetentionMs),
	}
	return created, nil
}

// DeleteTopic implements api.StrictServerInterface: organization admins
// delete their own topics, never Kafka's or the platform's, and every
// deletion is audited.
func (s *Server) DeleteTopic(ctx context.Context, request api.DeleteTopicRequestObject) (api.DeleteTopicResponseObject, error) {
	customer, principal, refused, err := s.customerOf(ctx)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && refused.Status == http.StatusNotFound:
		return api.DeleteTopic404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	case refused != nil:
		return api.DeleteTopic403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	case !principal.Admin():
		return api.DeleteTopic403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(adminsOnly)}, nil
	case topics.Reserved(request.Topic):
		return api.DeleteTopic404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(noSuchTopic(request.Topic))}, nil
	}
	err = s.clusters.DeleteTopic(ctx, customer.Name, request.Topic)
	switch {
	case errors.Is(err, ErrNoSuchTopic):
		return api.DeleteTopic404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(noSuchTopic(request.Topic))}, nil
	case errors.Is(err, clusters.ErrNotReady):
		return api.DeleteTopic409JSONResponse{NotReadyJSONResponse: api.NotReadyJSONResponse(notReadyYet)}, nil
	case err != nil:
		return nil, err
	}
	s.audit(ctx, principal, customer.Name, "topic deleted", "topic", request.Topic)
	return api.DeleteTopic204Response{}, nil
}

func noSuchTopic(name string) api.Problem {
	return problem(http.StatusNotFound, "No such topic", fmt.Sprintf("There is no topic %s.", name))
}

// ListTopicConsumerGroups implements api.StrictServerInterface.
func (s *Server) ListTopicConsumerGroups(ctx context.Context, request api.ListTopicConsumerGroupsRequestObject) (api.ListTopicConsumerGroupsResponseObject, error) {
	customer, _, refused, err := s.customerOf(ctx)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && refused.Status == http.StatusNotFound:
		return api.ListTopicConsumerGroups404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	case refused != nil:
		return api.ListTopicConsumerGroups403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	case topics.Reserved(request.Topic):
		return api.ListTopicConsumerGroups404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(noSuchTopic(request.Topic))}, nil
	}
	groups, err := s.clusters.TopicConsumerGroups(ctx, customer.Name, request.Topic)
	if errors.Is(err, clusters.ErrNotReady) {
		return api.ListTopicConsumerGroups409JSONResponse{NotReadyJSONResponse: api.NotReadyJSONResponse(notReadyYet)}, nil
	}
	if err != nil {
		return nil, err
	}
	return api.ListTopicConsumerGroups200JSONResponse{Groups: groups}, nil
}

// audit logs a change a user made to a customer's cluster, with who made it.
func (s *Server) audit(ctx context.Context, principal Principal, tenant, change string, attributes ...any) {
	s.logger.InfoContext(ctx, change, append([]any{
		"audit", true, "tenant", tenant, "user", principal.UserID, "organization", principal.OrganizationID,
	}, attributes...)...)
}
