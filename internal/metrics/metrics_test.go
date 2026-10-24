package metrics

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestExpr(t *testing.T) {
	tests := []struct {
		name   string
		query  Query
		filter Filter
		want   string
	}{
		{
			"every topic", BytesIn,
			Filter{},
			`sum by (topic) (rate(kafka_server_brokertopicmetrics_bytesinpersec_total{tenant="acme", topic!=""}[5m]))`,
		},
		{
			"one topic", LocalSize,
			Filter{Topic: "orders"},
			`sum by (topic) (max by (topic, partition) (kafka_log_log_size{tenant="acme", topic="orders"}))`,
		},
		{
			"one group", LagSeconds,
			Filter{ConsumerGroup: "billing"},
			`max by (consumer_group, topic) (changeport_consumergroup_lag_seconds{tenant="acme", consumer_group="billing"})`,
		},
		{
			"the whole cluster", OfflinePartitions,
			Filter{},
			`max(kafka_controller_kafkacontroller_offlinepartitionscount{tenant="acme"})`,
		},
		// Consumer group names are free text; quoting keeps them inside the
		// label value.
		{
			"a hostile group name", LagMessages,
			Filter{ConsumerGroup: `x"} or vector(1) or {a="\`},
			`sum by (consumer_group, topic) (changeport_consumergroup_lag_messages{tenant="acme", consumer_group="x\"} or vector(1) or {a=\"\\"})`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Expr(tt.query, "acme", tt.filter)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("Expr() =\n%s\nwant\n%s", got, tt.want)
			}
		})
	}
}

func TestExprRefuses(t *testing.T) {
	tests := []struct {
		name   string
		query  Query
		tenant string
		filter Filter
	}{
		{"an unknown query", Query("up"), "acme", Filter{}},
		{"no tenant", BytesIn, "", Filter{}},
		{"a tenant that is not a customer ID", BytesIn, `acme"}`, Filter{}},
		{"a consumer group on a broker metric", BytesIn, "acme", Filter{ConsumerGroup: "billing"}},
		{"a topic on a cluster metric", UnderReplicated, "acme", Filter{Topic: "orders"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := Expr(tt.query, tt.tenant, tt.filter); err == nil {
				t.Errorf("Expr() = %s, want an error", got)
			}
		})
	}
}

func TestParseQuery(t *testing.T) {
	for _, q := range Queries() {
		if got, err := ParseQuery(string(q)); err != nil || got != q {
			t.Errorf("ParseQuery(%q) = %q, %v", q, got, err)
		}
	}
	if _, err := ParseQuery("sum(up)"); !errors.Is(err, ErrUnknownQuery) {
		t.Errorf("ParseQuery(sum(up)) = %v, want ErrUnknownQuery", err)
	}
}

// fakeVictoriaMetrics answers every request with body and records the
// last request's parameters.
func fakeVictoriaMetrics(t *testing.T, body string) (*Client, *url.Values, *string) {
	t.Helper()
	var params url.Values
	var path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params, path = r.URL.Query(), r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	c, err := NewClient(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	return c, &params, &path
}

func TestNewClientRefusesOtherAddresses(t *testing.T) {
	for _, address := range []string{"", "victoria-metrics:8428", "file:///etc/passwd", "http://"} {
		if _, err := NewClient(address); err == nil {
			t.Errorf("NewClient(%q) accepted it", address)
		}
	}
}

func TestRange(t *testing.T) {
	c, params, path := fakeVictoriaMetrics(t, `{"status":"success","data":{"resultType":"matrix","result":[
		{"metric":{"topic":"orders"},"values":[[1792630000,"1.5"],[1792630060,"2"]]},
		{"metric":{"topic":"payments"},"values":[[1792630000,"NaN"]]}]}}`)
	start := time.Unix(1792630000, 0)
	series, err := c.Range(t.Context(), "acme", BytesIn, Filter{}, start, start.Add(time.Hour), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if *path != "/api/v1/query_range" || params.Get("start") != "1792630000" || params.Get("end") != "1792633600" || params.Get("step") != "60" {
		t.Errorf("requested %s with %v", *path, *params)
	}
	if !strings.Contains(params.Get("query"), `tenant="acme"`) {
		t.Errorf("query %s does not select the tenant", params.Get("query"))
	}
	if len(series) != 2 || series[0].Labels["topic"] != "orders" || len(series[0].Points) != 2 {
		t.Fatalf("series = %+v", series)
	}
	if p := series[0].Points[1]; !p.Time.Equal(time.Unix(1792630060, 0)) || p.Value != 2 {
		t.Errorf("second point = %+v", p)
	}
	if v := series[1].Points[0].Value; v == v {
		t.Errorf("NaN parsed as %v", v)
	}
}

func TestRangeRefusesTooManyPoints(t *testing.T) {
	c, _, _ := fakeVictoriaMetrics(t, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	start := time.Unix(1792630000, 0)
	if _, err := c.Range(t.Context(), "acme", BytesIn, Filter{}, start, start.Add(30*24*time.Hour), time.Minute); err == nil {
		t.Error("Range() accepted 43,200 points")
	}
	if _, err := c.Range(t.Context(), "acme", BytesIn, Filter{}, start, start, time.Minute); err == nil {
		t.Error("Range() accepted an empty range")
	}
}

func TestInstant(t *testing.T) {
	c, params, path := fakeVictoriaMetrics(t, `{"status":"success","data":{"resultType":"vector","result":[
		{"metric":{"consumer_group":"billing","topic":"orders"},"value":[1792630000.5,"42"]}]}}`)
	series, err := c.Instant(t.Context(), "acme", LagSeconds, Filter{}, time.UnixMilli(1792630000500))
	if err != nil {
		t.Fatal(err)
	}
	if *path != "/api/v1/query" || params.Get("time") != "1792630000.5" {
		t.Errorf("requested %s with %v", *path, *params)
	}
	if len(series) != 1 || series[0].Labels["consumer_group"] != "billing" || series[0].Points[0].Value != 42 {
		t.Errorf("series = %+v", series)
	}
}

func TestQueryErrors(t *testing.T) {
	c, _, _ := fakeVictoriaMetrics(t, `{"status":"error","errorType":"bad_data","error":"cannot parse"}`)
	if _, err := c.Instant(t.Context(), "acme", BytesIn, Filter{}, time.Now()); err == nil || !strings.Contains(err.Error(), "cannot parse") {
		t.Errorf("Instant() = %v, want VictoriaMetrics' error", err)
	}
	c, _, _ = fakeVictoriaMetrics(t, `<html>bad gateway</html>`)
	if _, err := c.Instant(t.Context(), "acme", BytesIn, Filter{}, time.Now()); err == nil {
		t.Error("Instant() accepted a non-JSON answer")
	}
}

// TestQueriesAgainstVictoriaMetrics runs every named query against a real
// VictoriaMetrics, which the end-to-end test points it at (check_named_queries
// in hack/lib.sh). The queries in CHANGEPORT_METRICS_EXPECT must return
// series, such as bytes_in after traffic on the tenant's cluster.
func TestQueriesAgainstVictoriaMetrics(t *testing.T) {
	address, tenant := os.Getenv("CHANGEPORT_METRICS_URL"), os.Getenv("CHANGEPORT_METRICS_TENANT")
	if address == "" || tenant == "" {
		t.Skip("CHANGEPORT_METRICS_URL and CHANGEPORT_METRICS_TENANT are not set; the end-to-end test sets them")
	}
	expected := map[Query]bool{}
	for _, name := range strings.Split(os.Getenv("CHANGEPORT_METRICS_EXPECT"), ",") {
		if name == "" {
			continue
		}
		q, err := ParseQuery(name)
		if err != nil {
			t.Fatal(err)
		}
		expected[q] = true
	}
	c, err := NewClient(address)
	if err != nil {
		t.Fatal(err)
	}
	end := time.Now()
	for _, q := range Queries() {
		series, err := c.Range(t.Context(), tenant, q, Filter{}, end.Add(-30*time.Minute), end, time.Minute)
		if err != nil {
			t.Errorf("%s: %v", q, err)
			continue
		}
		for _, s := range series {
			if definitions[q].scope != perCluster && s.Labels["topic"] == "" {
				t.Errorf("%s returned a series without a topic: %v", q, s.Labels)
			}
		}
		t.Logf("%s: %d series", q, len(series))
		if expected[q] && len(series) == 0 {
			t.Errorf("%s returned no series for tenant %s", q, tenant)
		}
	}
}
