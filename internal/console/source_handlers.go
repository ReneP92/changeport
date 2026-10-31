package console

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/cdc"
	"github.com/ReneP92/changeport/internal/clusters"
	"github.com/ReneP92/changeport/internal/console/api"
	"github.com/ReneP92/changeport/internal/topics"
)

// Databases checks customers' databases for sources; cdc.Checker does, from
// the console's own addresses, which are Connect's.
type Databases interface {
	Check(ctx context.Context, target cdc.Target) cdc.Report
	ListTables(ctx context.Context, connection cdc.Connection) ([]cdc.TableInfo, error)
}

var _ Databases = cdc.Checker{}

var sourceAdminsOnly = problem(http.StatusForbidden, "Admins only", "Ask an admin of your organization to change Postgres sources.")

// sourceCaller returns the caller's customer for the sources' routes, or the
// problem why there is none; admin asks for an organization admin.
func (s *Server) sourceCaller(ctx context.Context, admin bool) (*customerContext, *api.Problem, error) {
	customer, principal, refused, err := s.customerOf(ctx)
	if err != nil || refused != nil {
		return nil, refused, err
	}
	if admin && !principal.Admin() {
		return nil, &sourceAdminsOnly, nil
	}
	return &customerContext{customer: customer, principal: principal}, nil, nil
}

// customerContext is the customer a request acts on, and who acts.
type customerContext struct {
	customer  *v1alpha1.Customer
	principal Principal
}

func (c *customerContext) id() string { return c.customer.Name }

// forbidden reports whether a refusal is a 403, rather than a 404.
func forbidden(p *api.Problem) bool { return p.Status == http.StatusForbidden }

// GetSourceOptions implements api.StrictServerInterface.
func (s *Server) GetSourceOptions(ctx context.Context, _ api.GetSourceOptionsRequestObject) (api.GetSourceOptionsResponseObject, error) {
	caller, refused, err := s.sourceCaller(ctx, false)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && forbidden(refused):
		return api.GetSourceOptions403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	case refused != nil:
		return api.GetSourceOptions404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	}
	options := api.GetSourceOptions200JSONResponse{
		EgressAddresses:    append([]string{}, s.egress...),
		MaxTables:          cdc.MaxTables,
		MaxNameLength:      cdc.MaxSourceNameLength,
		DefaultRetentionMs: topics.DefaultRetention.Milliseconds(),
	}
	for _, provider := range cdc.Providers {
		options.Providers = append(options.Providers, api.ProviderOption{
			Id: api.Provider(provider), Title: provider.Title(), PasswordFromCustomer: provider.PasswordFromCustomer(),
		})
	}
	for _, preset := range topics.RetentionPresets {
		options.RetentionPresets = append(options.RetentionPresets, api.RetentionPreset{Label: preset.Label, Ms: preset.Duration.Milliseconds()})
	}
	if url := registryURL(caller.customer); url != "" {
		options.RegistryURL = ptr.To(url)
	}
	return options, nil
}

// registryURL is where the customer's clients reach the schema registry:
// the registry gateway, at registry.<customer>.<domain>, next to the
// bootstrap name.
func registryURL(customer *v1alpha1.Customer) string {
	host, _, _ := strings.Cut(customer.Status.BootstrapAddress, ":")
	rest, ok := strings.CutPrefix(host, "bootstrap.")
	if !ok {
		return ""
	}
	return "https://registry." + rest
}

// ListSources implements api.StrictServerInterface.
func (s *Server) ListSources(ctx context.Context, _ api.ListSourcesRequestObject) (api.ListSourcesResponseObject, error) {
	caller, refused, err := s.sourceCaller(ctx, false)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && forbidden(refused):
		return api.ListSources403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	case refused != nil:
		return api.ListSources404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	}
	sources, err := s.sources.List(ctx, caller.id())
	if err != nil {
		return nil, err
	}
	list := api.ListSources200JSONResponse{Sources: make([]api.Source, 0, len(sources))}
	for i := range sources {
		list.Sources = append(list.Sources, toAPISource(&sources[i]))
	}
	return list, nil
}

// GetSource implements api.StrictServerInterface.
func (s *Server) GetSource(ctx context.Context, request api.GetSourceRequestObject) (api.GetSourceResponseObject, error) {
	caller, refused, err := s.sourceCaller(ctx, false)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && forbidden(refused):
		return api.GetSource403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	case refused != nil:
		return api.GetSource404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	}
	source, err := s.sources.Get(ctx, caller.id(), request.Source)
	if err != nil {
		return nil, err
	}
	if source == nil {
		return api.GetSource404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(noSuchSource(request.Source))}, nil
	}
	return api.GetSource200JSONResponse(toAPISource(source)), nil
}

func noSuchSource(name string) api.Problem {
	return problem(http.StatusNotFound, "No such source", fmt.Sprintf("There is no Postgres source %s.", name))
}

func invalidSource(field, detail string) api.Problem {
	p := problem(http.StatusBadRequest, "Invalid source", detail)
	p.Field = &field
	return p
}

// GetSetupSQL implements api.StrictServerInterface: the SQL of a new
// source's first and second steps. The first chooses the source's ID and its
// role's password, which only this answer holds.
func (s *Server) GetSetupSQL(ctx context.Context, request api.GetSetupSQLRequestObject) (api.GetSetupSQLResponseObject, error) {
	caller, refused, err := s.sourceCaller(ctx, true)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && forbidden(refused):
		return api.GetSetupSQL403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	case refused != nil:
		return api.GetSetupSQL404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	case request.Body == nil:
		return api.GetSetupSQL400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(invalidSource("step", "Describe the step."))}, nil
	}
	body := *request.Body
	bad := func(p api.Problem) (api.GetSetupSQLResponseObject, error) {
		return api.GetSetupSQL400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(p)}, nil
	}
	if p, err := s.newSourceName(ctx, caller, body.Name); err != nil {
		return nil, err
	} else if p != nil {
		return bad(*p)
	}
	provider := cdc.Provider(body.Provider)
	if !provider.Valid() {
		return bad(invalidSource("provider", "Choose where the database runs."))
	}
	id := ""
	if body.Id != nil {
		id = *body.Id
	}
	var script cdc.Script
	var password string
	switch body.Step {
	case api.Role:
		id = cdc.NewID()
		names, _ := cdc.NamesFor(body.Name, id)
		if !provider.PasswordFromCustomer() {
			password = cdc.GeneratePassword()
		}
		script, err = cdc.RoleSQL(provider, names, password)
	case api.Tables:
		if cdc.ValidateID(id) != nil {
			return bad(invalidSource("id", "Give the source's ID from the first step."))
		}
		names, _ := cdc.NamesFor(body.Name, id)
		tables := toTables(body.Tables)
		if err := cdc.ValidateTables(tables); err != nil {
			return bad(invalidSource("tables", capitalizeFirst(err.Error())+"."))
		}
		script, err = cdc.TablesSQL(names, tables)
	default:
		return bad(invalidSource("step", "Choose the role or the tables step."))
	}
	if err != nil {
		return nil, err
	}
	names, _ := cdc.NamesFor(body.Name, id)
	return api.GetSetupSQL200JSONResponse(toAPISQL(names, script, password)), nil
}

// newSourceName checks the name of a new source: valid, not another
// source's, and no existing topic under its prefix.
func (s *Server) newSourceName(ctx context.Context, caller *customerContext, name string) (*api.Problem, error) {
	if err := cdc.ValidateName(name); err != nil {
		p := invalidSource("name", capitalizeFirst(err.Error())+".")
		return &p, nil
	}
	existing, err := s.sources.Get(ctx, caller.id(), name)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		p := invalidSource("name", fmt.Sprintf("A source named %s exists.", name))
		return &p, nil
	}
	names, err := s.topicNames(ctx, caller.id())
	if err != nil {
		return nil, err
	}
	if i := slices.IndexFunc(names, func(topic string) bool { return strings.HasPrefix(topic, name+".") }); i >= 0 {
		p := invalidSource("name", fmt.Sprintf("Topic %s exists, and the source's topics would start with %s. like it. Choose another name.", names[i], name))
		return &p, nil
	}
	return nil, nil
}

// topicNames are the names of the customer's topics, or none while the
// cluster is not ready.
func (s *Server) topicNames(ctx context.Context, customerID string) ([]string, error) {
	load, err := s.clusters.Load(ctx, customerID)
	if errors.Is(err, clusters.ErrNotReady) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return load.Topics, nil
}

func toTables(refs *[]api.TableRef) []cdc.Table {
	if refs == nil {
		return nil
	}
	tables := make([]cdc.Table, len(*refs))
	for i, ref := range *refs {
		tables[i] = cdc.Table{Schema: ref.Schema, Name: ref.Name}
	}
	return tables
}

func toAPISQL(names cdc.Names, script cdc.Script, password string) api.SourceSQL {
	out := api.SourceSQL{Id: strings.TrimPrefix(names.Connector, names.Source+"-"), Role: names.Role, Steps: script.Steps, Sql: script.SQL}
	if out.Steps == nil {
		out.Steps = []string{}
	}
	if password != "" {
		out.Password = ptr.To(password)
	}
	return out
}

func capitalizeFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// checkTarget is the target of a check request, or the problem with it.
func checkTarget(body api.SourceCheckRequest) (cdc.Target, *api.Problem) {
	names, err := cdc.NamesFor(body.Name, body.Id)
	if err != nil {
		p := invalidSource("name", capitalizeFirst(err.Error())+".")
		return cdc.Target{}, &p
	}
	provider := cdc.Provider(body.Provider)
	if !provider.Valid() {
		p := invalidSource("provider", "Choose where the database runs.")
		return cdc.Target{}, &p
	}
	c := body.Connection
	connection := cdc.Connection{
		Host: strings.TrimSpace(c.Host), Port: c.Port, Database: c.Database, User: names.Role,
		Password: c.Password, TLSMode: cdc.TLSMode(c.TlsMode), CA: ptr.Deref(c.Ca, ""),
	}
	if err := connection.Validate(); err != nil {
		p := invalidSource("connection", capitalizeFirst(err.Error())+".")
		return cdc.Target{}, &p
	}
	return cdc.Target{Provider: provider, Connection: connection, Names: names, Tables: toTables(body.Tables)}, nil
}

func toAPIReport(report cdc.Report) api.CheckReport {
	out := api.CheckReport{Ok: report.OK(), Results: make([]api.CheckResult, 0, len(report.Results))}
	for _, r := range report.Results {
		out.Results = append(out.Results, api.CheckResult{Check: string(r.Check), Status: api.CheckResultStatus(r.Status), Message: r.Message})
	}
	return out
}

// CheckSource implements api.StrictServerInterface.
func (s *Server) CheckSource(ctx context.Context, request api.CheckSourceRequestObject) (api.CheckSourceResponseObject, error) {
	_, refused, err := s.sourceCaller(ctx, true)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && forbidden(refused):
		return api.CheckSource403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	case refused != nil:
		return api.CheckSource404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	case request.Body == nil:
		return api.CheckSource400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(invalidSource("connection", "Describe the database."))}, nil
	}
	target, p := checkTarget(*request.Body)
	if p != nil {
		return api.CheckSource400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(*p)}, nil
	}
	return api.CheckSource200JSONResponse(toAPIReport(s.databases.Check(ctx, target))), nil
}

// ListSourceTables implements api.StrictServerInterface: the table picker's
// tables, each with whether it can be synced, its topic, and what stops the
// topic.
func (s *Server) ListSourceTables(ctx context.Context, request api.ListSourceTablesRequestObject) (api.ListSourceTablesResponseObject, error) {
	caller, refused, err := s.sourceCaller(ctx, true)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && forbidden(refused):
		return api.ListSourceTables403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	case refused != nil:
		return api.ListSourceTables404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	case request.Body == nil:
		return api.ListSourceTables400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(invalidSource("connection", "Describe the database."))}, nil
	}
	target, p := checkTarget(*request.Body)
	if p != nil {
		return api.ListSourceTables400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(*p)}, nil
	}
	tables, err := s.databases.ListTables(ctx, target.Connection)
	if err != nil {
		return api.ListSourceTables409JSONResponse{ConflictJSONResponse: api.ConflictJSONResponse(problem(http.StatusConflict,
			"Changeport cannot list the tables", err.Error()))}, nil
	}
	existing, err := s.topicNames(ctx, caller.id())
	if err != nil {
		return nil, err
	}
	refs := make([]cdc.Table, len(tables))
	for i, t := range tables {
		refs[i] = t.Table
	}
	topicOf := cdc.Topics(target.Names.Source, refs, existing)
	list := api.ListSourceTables200JSONResponse{Tables: make([]api.SourceTable, 0, len(tables))}
	for i, t := range tables {
		table := api.SourceTable{
			Schema: t.Schema, Name: t.Name, Partitioned: t.Partitioned, EstimatedRows: t.EstimatedRows,
			Readable: t.Readable, Keyed: t.Keyed, Topic: topicOf[i].Topic,
		}
		if t.Reason != "" {
			table.Reason = ptr.To(t.Reason)
		}
		if topicOf[i].Problem != "" {
			table.TopicProblem = ptr.To(capitalizeFirst(topicOf[i].Problem) + ".")
		}
		list.Tables = append(list.Tables, table)
	}
	return list, nil
}

// tablesProblem checks the tables of a source, whose topics must not
// collide with the customer's other topics and must fit the cluster's
// replica budget. Current are the source's tables before the change.
func (s *Server) tablesProblem(ctx context.Context, caller *customerContext, source string, tables, current []cdc.Table) (*api.Problem, error) {
	if err := cdc.ValidateTables(tables); err != nil {
		p := invalidSource("tables", capitalizeFirst(err.Error())+".")
		return &p, nil
	}
	policy, err := topicPolicy(caller.customer)
	if err != nil {
		return nil, err
	}
	load, err := s.clusters.Load(ctx, caller.id())
	if errors.Is(err, clusters.ErrNotReady) {
		p := problem(http.StatusConflict, "The cluster is not ready yet", "Add sources once it is ready.")
		return &p, nil
	}
	if err != nil {
		return nil, err
	}
	owned, _, err := s.sourceTopics(ctx, caller.id())
	if err != nil {
		return nil, err
	}
	own := map[string]bool{}
	for _, t := range current {
		own[cdc.TopicName(source, t)] = true
	}
	others := slices.DeleteFunc(slices.Clone(load.Topics), func(topic string) bool { return own[topic] })
	exists := map[string]bool{}
	for _, topic := range load.Topics {
		exists[topic] = true
	}
	// Connect creates a table's topic with the table's first record, so the
	// budget counts each table's topic, of this source and the others, that
	// does not exist yet; each has one partition.
	pending := 0
	for topic, owner := range owned {
		if owner != source && !exists[topic] {
			pending++
		}
	}
	for _, topic := range cdc.Topics(source, tables, others) {
		if topic.Problem != "" {
			p := invalidSource("tables", fmt.Sprintf("Table %s cannot be synced: %s.", topic.Table, topic.Problem))
			return &p, nil
		}
		if !exists[topic.Topic] {
			pending++
		}
	}
	if budget := load.Brokers * topics.ReplicaBudgetPerBroker; load.Replicas+pending*policy.ReplicationFactor > budget {
		p := problem(http.StatusConflict, "The cluster has no room for the tables", fmt.Sprintf(
			"The cluster holds %d of its %d partition replicas; the sources' %d tables without a topic yet would exceed them.", load.Replicas, budget, pending))
		p.Field = ptr.To("tables")
		return &p, nil
	}
	return nil, nil
}

// CreateSource implements api.StrictServerInterface: it creates a source
// once its database passes every check with its tables.
func (s *Server) CreateSource(ctx context.Context, request api.CreateSourceRequestObject) (api.CreateSourceResponseObject, error) {
	caller, refused, err := s.sourceCaller(ctx, true)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && forbidden(refused):
		return api.CreateSource403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	case refused != nil:
		return api.CreateSource404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	case request.Body == nil:
		return api.CreateSource400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(invalidSource("name", "Describe the source."))}, nil
	}
	body := *request.Body
	answer := func(p api.Problem) (api.CreateSourceResponseObject, error) {
		if p.Status == http.StatusConflict {
			return api.CreateSource409JSONResponse{ConflictJSONResponse: api.ConflictJSONResponse(p)}, nil
		}
		return api.CreateSource400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(p)}, nil
	}
	if p, err := s.newSourceName(ctx, caller, body.Name); err != nil {
		return nil, err
	} else if p != nil {
		return answer(*p)
	}
	target, p := checkTarget(api.SourceCheckRequest{Name: body.Name, Id: body.Id, Provider: body.Provider, Connection: body.Connection, Tables: &body.Tables})
	if p != nil {
		return answer(*p)
	}
	if !slices.ContainsFunc(topics.RetentionPresets, func(preset topics.RetentionPreset) bool { return preset.Duration.Milliseconds() == body.RetentionMs }) {
		return answer(invalidSource("retentionMs", "Choose one of the retention presets, from 1 hour to 30 days."))
	}
	if p, err := s.tablesProblem(ctx, caller, body.Name, target.Tables, nil); err != nil {
		return nil, err
	} else if p != nil {
		return answer(*p)
	}
	report := s.databases.Check(ctx, target)
	if !report.OK() {
		return answer(problem(http.StatusConflict, "The database is not ready for the source", firstFailure(report)))
	}

	source := &v1alpha1.PostgresSource{
		ObjectMeta: metav1.ObjectMeta{Name: body.Name},
		Spec: v1alpha1.PostgresSourceSpec{
			ID:       body.Id,
			Provider: string(body.Provider),
			Connection: v1alpha1.PostgresConnection{
				Host: target.Connection.Host, Port: int32(target.Connection.Port), Database: target.Connection.Database, //nolint:gosec // G115: a port, validated
				TLS: v1alpha1.PostgresTLS{Mode: string(target.Connection.TLSMode), CA: target.Connection.CA},
			},
			Tables:      toSpecTables(target.Tables),
			RetentionMs: body.RetentionMs,
			State:       v1alpha1.StateRunning,
		},
	}
	if err := s.sources.Create(ctx, caller.id(), source, target.Connection.Password); errors.Is(err, ErrSourceExists) {
		return answer(invalidSource("name", fmt.Sprintf("A source named %s exists.", body.Name)))
	} else if err != nil {
		return nil, err
	}
	s.audit(ctx, caller.principal, caller.id(), "source created", "source", source.Name, "provider", source.Spec.Provider,
		"host", source.Spec.Connection.Host, "database", source.Spec.Connection.Database, "tables", len(source.Spec.Tables),
		"retention_ms", body.RetentionMs)
	return api.CreateSource201JSONResponse(toAPISource(source)), nil
}

// firstFailure is the message of a report's first failed check.
func firstFailure(report cdc.Report) string {
	for _, r := range report.Results {
		if r.Status == cdc.Fail {
			return r.Message
		}
	}
	return "The checks did not finish."
}

func toSpecTables(tables []cdc.Table) []v1alpha1.PostgresTable {
	out := make([]v1alpha1.PostgresTable, len(tables))
	for i, t := range tables {
		out[i] = v1alpha1.PostgresTable{Schema: t.Schema, Name: t.Name}
	}
	return out
}

func specTables(source *v1alpha1.PostgresSource) []cdc.Table {
	out := make([]cdc.Table, len(source.Spec.Tables))
	for i, t := range source.Spec.Tables {
		out[i] = cdc.Table{Schema: t.Schema, Name: t.Name}
	}
	return out
}

// existingSource returns the request's source, or the problem why there is
// none to act on.
func (s *Server) existingSource(ctx context.Context, caller *customerContext, name string) (*v1alpha1.PostgresSource, *api.Problem, error) {
	source, err := s.sources.Get(ctx, caller.id(), name)
	if err != nil {
		return nil, nil, err
	}
	if source == nil {
		p := noSuchSource(name)
		return nil, &p, nil
	}
	return source, nil, nil
}

var sourceDeleting = problem(http.StatusConflict, "The source is being deleted", "A deleted source cannot change.")

// ChangeSource implements api.StrictServerInterface.
func (s *Server) ChangeSource(ctx context.Context, request api.ChangeSourceRequestObject) (api.ChangeSourceResponseObject, error) {
	caller, refused, err := s.sourceCaller(ctx, true)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && forbidden(refused):
		return api.ChangeSource403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	case refused != nil:
		return api.ChangeSource404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	case request.Body == nil:
		return api.ChangeSource400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(invalidSource("tables", "Describe the change."))}, nil
	}
	source, p, err := s.existingSource(ctx, caller, request.Source)
	switch {
	case err != nil:
		return nil, err
	case p != nil:
		return api.ChangeSource404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*p)}, nil
	case source.DeletionTimestamp != nil:
		return api.ChangeSource409JSONResponse{ConflictJSONResponse: api.ConflictJSONResponse(sourceDeleting)}, nil
	}
	answer := func(p api.Problem) (api.ChangeSourceResponseObject, error) {
		if p.Status == http.StatusConflict {
			return api.ChangeSource409JSONResponse{ConflictJSONResponse: api.ConflictJSONResponse(p)}, nil
		}
		return api.ChangeSource400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(p)}, nil
	}
	body := *request.Body
	var changes []any
	if body.Tables != nil {
		tables := toTables(body.Tables)
		if p, err := s.tablesProblem(ctx, caller, source.Name, tables, specTables(source)); err != nil {
			return nil, err
		} else if p != nil {
			return answer(*p)
		}
		source.Spec.Tables = toSpecTables(tables)
		changes = append(changes, "tables", len(tables))
	}
	if body.RetentionMs != nil {
		if !slices.ContainsFunc(topics.RetentionPresets, func(preset topics.RetentionPreset) bool { return preset.Duration.Milliseconds() == *body.RetentionMs }) {
			return answer(invalidSource("retentionMs", "Choose one of the retention presets, from 1 hour to 30 days."))
		}
		source.Spec.RetentionMs = *body.RetentionMs
		changes = append(changes, "retention_ms", *body.RetentionMs)
	}
	if body.State != nil {
		if !body.State.Valid() {
			return answer(invalidSource("state", "Choose Running or Stopped."))
		}
		source.Spec.State = string(*body.State)
		changes = append(changes, "state", source.Spec.State)
	}
	if err := s.sources.Update(ctx, source); errors.Is(err, ErrSourceChanged) {
		return answer(problem(http.StatusConflict, "The source changed meanwhile", "Look at it again, and change it then."))
	} else if err != nil {
		return nil, err
	}
	s.audit(ctx, caller.principal, caller.id(), "source changed", append([]any{"source", source.Name}, changes...)...)
	return api.ChangeSource200JSONResponse(toAPISource(source)), nil
}

// DeleteSource implements api.StrictServerInterface.
func (s *Server) DeleteSource(ctx context.Context, request api.DeleteSourceRequestObject) (api.DeleteSourceResponseObject, error) {
	caller, refused, err := s.sourceCaller(ctx, true)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && forbidden(refused):
		return api.DeleteSource403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	case refused != nil:
		return api.DeleteSource404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	case request.Params.Confirm != request.Source:
		return api.DeleteSource400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(invalidSource("confirm",
			"Type the source's name to delete it."))}, nil
	}
	source, p, err := s.existingSource(ctx, caller, request.Source)
	switch {
	case err != nil:
		return nil, err
	case p != nil:
		return api.DeleteSource404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*p)}, nil
	}
	if err := s.sources.Delete(ctx, source); err != nil {
		return nil, err
	}
	s.audit(ctx, caller.principal, caller.id(), "source deleted", "source", source.Name)
	return api.DeleteSource204Response{}, nil
}

// GetSourceSQL implements api.StrictServerInterface: the SQL of a change to
// an existing source's database. A new password step chooses the password,
// which only this answer holds.
func (s *Server) GetSourceSQL(ctx context.Context, request api.GetSourceSQLRequestObject) (api.GetSourceSQLResponseObject, error) {
	caller, refused, err := s.sourceCaller(ctx, true)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && forbidden(refused):
		return api.GetSourceSQL403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	case refused != nil:
		return api.GetSourceSQL404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	case request.Body == nil:
		return api.GetSourceSQL400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(invalidSource("step", "Describe the step."))}, nil
	}
	source, p, err := s.existingSource(ctx, caller, request.Source)
	switch {
	case err != nil:
		return nil, err
	case p != nil:
		return api.GetSourceSQL404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*p)}, nil
	}
	names, err := cdc.NamesFor(source.Name, source.Spec.ID)
	if err != nil {
		return nil, err
	}
	provider := cdc.Provider(source.Spec.Provider)
	tables := toTables(request.Body.Tables)
	var script cdc.Script
	var password string
	switch request.Body.Step {
	case api.Add:
		script, err = cdc.AddTablesSQL(names, tables)
	case api.Remove:
		remaining := slices.DeleteFunc(specTables(source), func(t cdc.Table) bool { return slices.Contains(tables, t) })
		script, err = cdc.RemoveTablesSQL(names, tables, remaining)
	case api.Password:
		if !provider.PasswordFromCustomer() {
			password = cdc.GeneratePassword()
		}
		script, err = cdc.PasswordSQL(provider, names, password)
	case api.Cleanup:
		script, err = cdc.CleanupSQL(provider, names, specTables(source))
	case api.DropSlot:
		script = cdc.DropSlotSQL(names)
	default:
		return api.GetSourceSQL400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(invalidSource("step", "Choose add, remove, password, cleanup or dropSlot."))}, nil
	}
	if err != nil {
		return api.GetSourceSQL400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(invalidSource("tables", capitalizeFirst(err.Error())+"."))}, nil
	}
	return api.GetSourceSQL200JSONResponse(toAPISQL(names, script, password)), nil
}

// SetSourcePassword implements api.StrictServerInterface: once the new
// password logs in, the source uses it.
func (s *Server) SetSourcePassword(ctx context.Context, request api.SetSourcePasswordRequestObject) (api.SetSourcePasswordResponseObject, error) {
	caller, refused, err := s.sourceCaller(ctx, true)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && forbidden(refused):
		return api.SetSourcePassword403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	case refused != nil:
		return api.SetSourcePassword404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	case request.Body == nil || request.Body.Password == "":
		return api.SetSourcePassword400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse(invalidSource("password", "Enter the new password."))}, nil
	}
	source, p, err := s.existingSource(ctx, caller, request.Source)
	switch {
	case err != nil:
		return nil, err
	case p != nil:
		return api.SetSourcePassword404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*p)}, nil
	case source.DeletionTimestamp != nil:
		return api.SetSourcePassword409JSONResponse{ConflictJSONResponse: api.ConflictJSONResponse(sourceDeleting)}, nil
	}
	names, err := cdc.NamesFor(source.Name, source.Spec.ID)
	if err != nil {
		return nil, err
	}
	c := source.Spec.Connection
	report := s.databases.Check(ctx, cdc.Target{
		Provider: cdc.Provider(source.Spec.Provider), Names: names,
		Connection: cdc.Connection{
			Host: c.Host, Port: int(c.Port), Database: c.Database, User: names.Role, Password: request.Body.Password,
			TLSMode: cdc.TLSMode(cmp.Or(c.TLS.Mode, v1alpha1.TLSVerifyFull)), CA: c.TLS.CA,
		},
	})
	if result, ok := report.Result(cdc.Authentication); !ok || result.Status != cdc.Pass {
		return api.SetSourcePassword409JSONResponse{ConflictJSONResponse: api.ConflictJSONResponse(problem(http.StatusConflict,
			"The new password does not log in", firstFailure(report)))}, nil
	}
	if err := s.sources.SetPassword(ctx, source, request.Body.Password); errors.Is(err, ErrSourceChanged) {
		return api.SetSourcePassword409JSONResponse{ConflictJSONResponse: api.ConflictJSONResponse(problem(http.StatusConflict,
			"The source changed meanwhile", "Try again."))}, nil
	} else if err != nil {
		return nil, err
	}
	s.audit(ctx, caller.principal, caller.id(), "source password changed", "source", source.Name)
	return api.SetSourcePassword204Response{}, nil
}

// ConfirmSlotDropped implements api.StrictServerInterface.
func (s *Server) ConfirmSlotDropped(ctx context.Context, request api.ConfirmSlotDroppedRequestObject) (api.ConfirmSlotDroppedResponseObject, error) {
	caller, refused, err := s.sourceCaller(ctx, true)
	switch {
	case err != nil:
		return nil, err
	case refused != nil && forbidden(refused):
		return api.ConfirmSlotDropped403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse(*refused)}, nil
	case refused != nil:
		return api.ConfirmSlotDropped404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*refused)}, nil
	}
	source, p, err := s.existingSource(ctx, caller, request.Source)
	switch {
	case err != nil:
		return nil, err
	case p != nil:
		return api.ConfirmSlotDropped404JSONResponse{NotFoundJSONResponse: api.NotFoundJSONResponse(*p)}, nil
	case source.DeletionTimestamp == nil:
		return api.ConfirmSlotDropped409JSONResponse{ConflictJSONResponse: api.ConflictJSONResponse(problem(http.StatusConflict,
			"The source is not deleted", "Only a deleted source's slot is dropped by hand."))}, nil
	}
	if err := s.sources.ConfirmSlotDropped(ctx, source); err != nil {
		return nil, err
	}
	s.audit(ctx, caller.principal, caller.id(), "source slot confirmed dropped", "source", source.Name)
	return api.ConfirmSlotDropped204Response{}, nil
}

// toAPISource describes a source for the API.
func toAPISource(source *v1alpha1.PostgresSource) api.Source {
	names, _ := cdc.NamesFor(source.Name, source.Spec.ID)
	c := source.Spec.Connection
	out := api.Source{
		Name: source.Name, Id: source.Spec.ID, Provider: api.Provider(source.Spec.Provider), Role: names.Role,
		Host: c.Host, Port: int(c.Port), Database: c.Database, TlsMode: cmp.Or(c.TLS.Mode, v1alpha1.TLSVerifyFull),
		RetentionMs: source.Spec.RetentionMs, State: api.SourceState(cmp.Or(source.Spec.State, v1alpha1.StateRunning)),
		Phase: cmp.Or(source.Status.Phase, v1alpha1.PhasePending), Reason: v1alpha1.PhasePending,
		Message: "Waiting for the source controller.", CreatedAt: source.CreationTimestamp.UTC(),
		Tables: make([]api.SourceTableStatus, 0, len(source.Spec.Tables)),
	}
	if ready := meta.FindStatusCondition(source.Status.Conditions, v1alpha1.ConditionReady); ready != nil {
		out.Ready = ready.Status == metav1.ConditionTrue && ready.ObservedGeneration >= source.Generation
		out.Reason, out.Message = ready.Reason, ready.Message
		if ready.Reason == v1alpha1.SourceReasonDatabaseUnreachable {
			out.DropSlotSQL = ptr.To(strings.TrimSpace(cdc.DropSlotSQL(names).SQL))
		}
	}
	if source.DeletionTimestamp != nil {
		out.Deleting = ptr.To(true)
		out.Ready = false
	}
	// The spec's tables, with the status's progress where it has them.
	for _, t := range source.Spec.Tables {
		table := api.SourceTableStatus{
			Schema: t.Schema, Name: t.Name, Snapshot: api.SourceTableStatusSnapshot(v1alpha1.SnapshotPending),
			Topic: cdc.TopicName(source.Name, cdc.Table{Schema: t.Schema, Name: t.Name}),
		}
		if i := slices.IndexFunc(source.Status.Tables, func(s v1alpha1.TableStatus) bool { return s.Schema == t.Schema && s.Name == t.Name }); i >= 0 {
			status := source.Status.Tables[i]
			table.Snapshot = api.SourceTableStatusSnapshot(status.Snapshot)
			table.RowsCopied, table.RowsEstimated = ptr.To(status.RowsCopied), ptr.To(status.RowsEstimated)
			if status.Message != "" {
				table.Message = ptr.To(status.Message)
			}
		}
		out.Tables = append(out.Tables, table)
	}
	if slot := source.Status.Slot; slot != nil {
		out.Slot = &api.SlotHealth{
			Name: slot.Name, Active: slot.Active, RetainedWALBytes: slot.RetainedWALBytes,
			SafeWALSizeBytes: slot.SafeWALSizeBytes, CheckedAt: slot.CheckedAt.UTC(),
		}
		if slot.WALStatus != "" {
			out.Slot.WalStatus = ptr.To(slot.WALStatus)
		}
	}
	return out
}

// sourceTopics maps each topic of the customer's sources' tables to its
// source.
func (s *Server) sourceTopics(ctx context.Context, customerID string) (map[string]string, []v1alpha1.PostgresSource, error) {
	if s.sources == nil {
		return map[string]string{}, nil, nil
	}
	sources, err := s.sources.List(ctx, customerID)
	if err != nil {
		return nil, nil, err
	}
	owned := map[string]string{}
	for i := range sources {
		for _, t := range specTables(&sources[i]) {
			owned[cdc.TopicName(sources[i].Name, t)] = sources[i].Name
		}
	}
	return owned, sources, nil
}

// platformGroup reports whether a consumer group is the platform's own,
// such as Kafka Connect's or the registry's, which customers do not see.
func platformGroup(group string) bool { return strings.HasPrefix(group, "changeport") }
