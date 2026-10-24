// Package metrics reads customers' metrics from VictoriaMetrics for the
// console. Callers choose among named queries; the package writes the PromQL
// itself and always selects the tenant, so no request can read another
// customer's series or run arbitrary queries.
package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ReneP92/changeport/internal/provisioning"
)

// Query names one of the console's charts or tables.
type Query string

// The named queries. Rates are per second over 5 minutes.
const (
	// BytesIn is the bytes per second producers write, per topic.
	BytesIn Query = "bytes_in"
	// BytesOut is the bytes per second consumers read, per topic.
	BytesOut Query = "bytes_out"
	// MessagesIn is the messages per second producers write, per topic.
	MessagesIn Query = "messages_in"
	// LocalSize is the bytes each topic keeps on the brokers' disks, once,
	// not counting replicas.
	LocalSize Query = "local_size"
	// RemoteSize is the bytes each topic keeps in tiered storage.
	RemoteSize Query = "remote_size"
	// LagSeconds is the age of each consumer group's oldest unconsumed
	// message, per group and topic.
	LagSeconds Query = "lag_seconds"
	// LagMessages is the messages each consumer group has not consumed yet,
	// per group and topic.
	LagMessages Query = "lag_messages"
	// LagCapped is 1 where a consumer group's lag is older than the agent
	// looks up (23 hours), so LagSeconds is a lower bound, per group and
	// topic.
	LagCapped Query = "lag_capped"

	// UnderReplicated is the partitions with fewer in-sync replicas than
	// replicas, over all brokers.
	UnderReplicated Query = "under_replicated"
	// UnderMinISR is the partitions with fewer in-sync replicas than
	// min.insync.replicas, which refuse writes with acks=all.
	UnderMinISR Query = "under_min_isr"
	// OfflinePartitions is the partitions without a leader, which refuse
	// reads and writes, as the active controller counts them.
	OfflinePartitions Query = "offline_partitions"
	// ActiveControllers is the controllers acting as the active one: 1 in a
	// healthy cluster.
	ActiveControllers Query = "active_controllers"
	// ActiveBrokers is the brokers the active controller sees registered.
	ActiveBrokers Query = "active_brokers"
	// FencedBrokers is the brokers the active controller has fenced.
	FencedBrokers Query = "fenced_brokers"
)

// scope is what a query's series are about, which decides its filters.
type scope int

const (
	// perTopic queries have a series per topic.
	perTopic scope = iota
	// perGroup queries have a series per consumer group and topic.
	perGroup
	// perCluster queries have one series for the whole cluster.
	perCluster
)

// definition is a query's PromQL, where %s is the series selector, and its
// scope.
type definition struct {
	expression string
	scope      scope
}

var definitions = map[Query]definition{
	BytesIn:     {`sum by (topic) (rate(kafka_server_brokertopicmetrics_bytesinpersec_total{%s}[5m]))`, perTopic},
	BytesOut:    {`sum by (topic) (rate(kafka_server_brokertopicmetrics_bytesoutpersec_total{%s}[5m]))`, perTopic},
	MessagesIn:  {`sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesinpersec_total{%s}[5m]))`, perTopic},
	LocalSize:   {`sum by (topic) (max by (topic, partition) (kafka_log_log_size{%s}))`, perTopic},
	RemoteSize:  {`sum by (topic) (kafka_server_brokertopicmetrics_remotelogsizebytes{%s})`, perTopic},
	LagSeconds:  {`max by (consumer_group, topic) (changeport_consumergroup_lag_seconds{%s})`, perGroup},
	LagMessages: {`sum by (consumer_group, topic) (changeport_consumergroup_lag_messages{%s})`, perGroup},
	LagCapped:   {`max by (consumer_group, topic) (changeport_consumergroup_lag_capped{%s})`, perGroup},
	// Every broker reports its own partitions; every controller reports the
	// controller's counts, but only the active one knows them.
	UnderReplicated:   {`sum(kafka_server_replicamanager_underreplicatedpartitions{%s})`, perCluster},
	UnderMinISR:       {`sum(kafka_server_replicamanager_underminisrpartitioncount{%s})`, perCluster},
	OfflinePartitions: {`max(kafka_controller_kafkacontroller_offlinepartitionscount{%s})`, perCluster},
	ActiveControllers: {`sum(kafka_controller_kafkacontroller_activecontrollercount{%s})`, perCluster},
	ActiveBrokers:     {`max(kafka_controller_kafkacontroller_activebrokercount{%s})`, perCluster},
	FencedBrokers:     {`max(kafka_controller_kafkacontroller_fencedbrokercount{%s})`, perCluster},
}

// Queries lists every named query.
func Queries() []Query {
	return []Query{
		BytesIn, BytesOut, MessagesIn, LocalSize, RemoteSize, LagSeconds, LagMessages, LagCapped,
		UnderReplicated, UnderMinISR, OfflinePartitions, ActiveControllers, ActiveBrokers, FencedBrokers,
	}
}

// ErrUnknownQuery is returned for a query name that is not one of Queries.
var ErrUnknownQuery = errors.New("unknown query")

// ParseQuery returns the named query, or ErrUnknownQuery.
func ParseQuery(name string) (Query, error) {
	if _, ok := definitions[Query(name)]; !ok {
		return "", fmt.Errorf("%w %q", ErrUnknownQuery, name)
	}
	return Query(name), nil
}

// Filter narrows a query to one topic or consumer group; empty fields match
// everything.
type Filter struct {
	Topic         string
	ConsumerGroup string
}

// Expr returns the PromQL of q for tenant's series that match filter.
func Expr(q Query, tenant string, filter Filter) (string, error) {
	d, ok := definitions[q]
	if !ok {
		return "", fmt.Errorf("%w %q", ErrUnknownQuery, q)
	}
	if err := provisioning.ValidateCustomerID(tenant); err != nil {
		return "", err
	}
	selector := []string{`tenant=` + quote(tenant)}
	switch {
	case d.scope == perCluster && (filter.Topic != "" || filter.ConsumerGroup != ""):
		return "", fmt.Errorf("query %s is about the whole cluster", q)
	case d.scope == perTopic && filter.ConsumerGroup != "":
		return "", fmt.Errorf("query %s has no consumer groups", q)
	case filter.Topic != "":
		selector = append(selector, `topic=`+quote(filter.Topic))
	case d.scope == perTopic:
		// Broker metrics without a topic label are totals over all topics.
		selector = append(selector, `topic!=""`)
	}
	if filter.ConsumerGroup != "" {
		selector = append(selector, `consumer_group=`+quote(filter.ConsumerGroup))
	}
	return fmt.Sprintf(d.expression, strings.Join(selector, ", ")), nil
}

// quote writes s as a PromQL string literal.
func quote(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s) + `"`
}

// Point is one value of a series.
type Point struct {
	Time  time.Time
	Value float64
}

// Series is one result series: its labels, such as topic, and its points.
type Series struct {
	Labels map[string]string
	Points []Point
}

// Client queries VictoriaMetrics' Prometheus API.
type Client struct {
	base *url.URL
	http *http.Client
}

// NewClient returns a client for the VictoriaMetrics at baseURL, such as
// http://victoria-metrics.metrics.svc:8428, which comes from the console's
// configuration.
func NewClient(baseURL string) (*Client, error) {
	base, err := url.Parse(strings.TrimSuffix(baseURL, "/"))
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, fmt.Errorf("VictoriaMetrics' address %q is not an http(s) URL", baseURL)
	}
	return &Client{base: base, http: &http.Client{Timeout: 30 * time.Second}}, nil
}

// MaxPoints caps the points of one series in a range query.
const MaxPoints = 1000

// Range returns q for tenant from start to end, one point per step.
func (c *Client) Range(ctx context.Context, tenant string, q Query, filter Filter, start, end time.Time, step time.Duration) ([]Series, error) {
	expression, err := Expr(q, tenant, filter)
	if err != nil {
		return nil, err
	}
	switch {
	case step < time.Second:
		return nil, fmt.Errorf("step %v is shorter than a second", step)
	case !end.After(start):
		return nil, errors.New("the range ends before it starts")
	case end.Sub(start)/step > MaxPoints:
		return nil, fmt.Errorf("the range has more than %d points; use a longer step", MaxPoints)
	}
	params := url.Values{
		"query": {expression},
		"start": {unix(start)},
		"end":   {unix(end)},
		"step":  {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)},
	}
	return c.get(ctx, "/api/v1/query_range", params)
}

// Instant returns q for tenant at one moment: one point per series.
func (c *Client) Instant(ctx context.Context, tenant string, q Query, filter Filter, at time.Time) ([]Series, error) {
	expression, err := Expr(q, tenant, filter)
	if err != nil {
		return nil, err
	}
	return c.get(ctx, "/api/v1/query", url.Values{"query": {expression}, "time": {unix(at)}})
}

func unix(t time.Time) string {
	return strconv.FormatFloat(float64(t.UnixMilli())/1000, 'f', -1, 64)
}

// response is the Prometheus API's response to a query.
type response struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
	Data      struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []any             `json:"value"`
			Values [][]any           `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

// get queries one of the API's fixed paths on the configured server; only
// the query parameters come from the request.
func (c *Client) get(ctx context.Context, path string, params url.Values) ([]Series, error) {
	target := *c.base
	target.Path += path
	target.RawQuery = params.Encode()
	// The server is the configured one; requests only choose the query, which
	// Expr builds with the tenant and quoted label values.
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), http.NoBody) //nolint:gosec // G704: fixed server, see above
	if err != nil {
		return nil, err
	}
	reply, err := c.http.Do(request) //nolint:gosec // G704: fixed server, see above
	if err != nil {
		return nil, fmt.Errorf("query VictoriaMetrics: %w", err)
	}
	defer reply.Body.Close()
	body, err := io.ReadAll(io.LimitReader(reply.Body, 32<<20))
	if err != nil {
		return nil, fmt.Errorf("read VictoriaMetrics' response: %w", err)
	}
	var decoded response
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, fmt.Errorf("VictoriaMetrics answered %s: %.200s", reply.Status, body)
	}
	if decoded.Status != "success" {
		return nil, fmt.Errorf("VictoriaMetrics refused the query (%s): %s", decoded.ErrorType, decoded.Error)
	}
	series := make([]Series, 0, len(decoded.Data.Result))
	for _, result := range decoded.Data.Result {
		s := Series{Labels: result.Metric}
		samples := result.Values
		if result.Value != nil {
			samples = [][]any{result.Value}
		}
		for _, sample := range samples {
			point, err := parsePoint(sample)
			if err != nil {
				return nil, err
			}
			s.Points = append(s.Points, point)
		}
		series = append(series, s)
	}
	return series, nil
}

// parsePoint reads a [unix seconds, "value"] pair.
func parsePoint(sample []any) (Point, error) {
	if len(sample) != 2 {
		return Point{}, fmt.Errorf("a sample has %d fields, want 2", len(sample))
	}
	seconds, ok := sample[0].(float64)
	text, ok2 := sample[1].(string)
	if !ok || !ok2 {
		return Point{}, fmt.Errorf("malformed sample %v", sample)
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return Point{}, fmt.Errorf("sample value %q: %w", text, err)
	}
	return Point{Time: time.UnixMilli(int64(seconds * 1000)), Value: value}, nil
}
