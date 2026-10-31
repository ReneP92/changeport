package alerts

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

var root = filepath.Join("..", "..")

func load(t *testing.T) []Rule {
	t.Helper()
	rules, err := Load(filepath.Join(root, "deploy", "base", "victoria-metrics", "alerts"))
	if err != nil {
		t.Fatal(err)
	}
	if len(rules) == 0 {
		t.Fatal("no alerting rules")
	}
	return rules
}

// runbookAnchors are the anchors of docs/runbook.md's sections.
func runbookAnchors(t *testing.T) []string {
	t.Helper()
	file, err := os.Open(filepath.Join(root, "docs", "runbook.md"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	var anchors []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if heading, ok := strings.CutPrefix(scanner.Text(), "## "); ok {
			anchors = append(anchors, Anchor(heading))
		}
	}
	return anchors
}

const runbookURL = "https://github.com/ReneP92/changeport/blob/main/docs/runbook.md#"

var metricName = regexp.MustCompile(`\b(changeport|debezium|kafka_connect)_[a-z_]+\b`)

func TestRules(t *testing.T) {
	anchors := runbookAnchors(t)
	documented, err := os.ReadFile(filepath.Join(root, "docs", "metrics-labels.md"))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, rule := range load(t) {
		if !regexp.MustCompile(`^[A-Z][A-Za-z]+$`).MatchString(rule.Alert) || seen[rule.Alert] {
			t.Errorf("alert %q is not a unique CamelCase name", rule.Alert)
		}
		seen[rule.Alert] = true
		if rule.Expr == "" {
			t.Errorf("%s has no expression", rule.Alert)
		}
		if severity := rule.Labels["severity"]; severity != "page" && severity != "warn" {
			t.Errorf("%s has severity %q, want page or warn", rule.Alert, severity)
		}
		if audience := rule.Labels["audience"]; audience != "operator" && audience != "customer" {
			t.Errorf("%s has audience %q, want operator or customer", rule.Alert, audience)
		}
		for _, annotation := range []string{"summary", "description", "runbook"} {
			if rule.Annotations[annotation] == "" {
				t.Errorf("%s has no %s", rule.Alert, annotation)
			}
		}
		if summary := rule.Annotations["summary"]; !strings.Contains(summary, "$labels.tenant") || !strings.Contains(summary, "$labels.source") {
			t.Errorf("%s's summary does not name the tenant and the source: %q", rule.Alert, summary)
		}
		anchor, ok := strings.CutPrefix(rule.Annotations["runbook"], runbookURL)
		if !ok || !slices.Contains(anchors, anchor) {
			t.Errorf("%s's runbook %q is no section of docs/runbook.md", rule.Alert, rule.Annotations["runbook"])
		}
		for _, name := range metricName.FindAllString(rule.Expr, -1) {
			if !strings.Contains(string(documented), "`"+name+"`") {
				t.Errorf("%s uses %s, which docs/metrics-labels.md does not describe", rule.Alert, name)
			}
		}
	}
}

func TestAnchor(t *testing.T) {
	for heading, want := range map[string]string{
		"A lost replication slot":            "a-lost-replication-slot",
		"A source's database is unreachable": "a-sources-database-is-unreachable",
		"Upgrading Kafka":                    "upgrading-kafka",
	} {
		if got := Anchor(heading); got != want {
			t.Errorf("Anchor(%q) = %q, want %q", heading, got, want)
		}
	}
}

// TestRulesAgainstVictoriaMetrics evaluates every rule's expression for one
// Postgres source, as the end-to-end tests run it (hack/lib.sh): each
// expression must work, and exactly the alerts of CHANGEPORT_ALERTS_FIRING
// must have a series for the source, ignoring how long they must hold.
func TestRulesAgainstVictoriaMetrics(t *testing.T) {
	address, tenant, source := os.Getenv("CHANGEPORT_METRICS_URL"), os.Getenv("CHANGEPORT_ALERTS_TENANT"), os.Getenv("CHANGEPORT_ALERTS_SOURCE")
	if address == "" || tenant == "" || source == "" {
		t.Skip("CHANGEPORT_METRICS_URL, CHANGEPORT_ALERTS_TENANT and CHANGEPORT_ALERTS_SOURCE are not set; the end-to-end test sets them")
	}
	var firing []string
	for alert := range strings.SplitSeq(os.Getenv("CHANGEPORT_ALERTS_FIRING"), ",") {
		if alert != "" {
			firing = append(firing, alert)
		}
	}
	for _, rule := range load(t) {
		series := query(t, address, rule.Expr)
		var mine []map[string]string
		for _, s := range series {
			if s["tenant"] == tenant && s["source"] == source {
				mine = append(mine, s)
			}
		}
		for _, s := range mine {
			for _, label := range []string{"tenant", "cluster", "env", "source"} {
				if s[label] == "" {
					t.Errorf("%s has a series without %s: %v", rule.Alert, label, s)
				}
			}
		}
		if want := slices.Contains(firing, rule.Alert); want != (len(mine) > 0) {
			t.Errorf("%s fires for source %s of %s: %t, want %t", rule.Alert, source, tenant, len(mine) > 0, want)
		}
	}
}

func query(t *testing.T, address, expr string) []map[string]string {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, address+"/api/v1/query?query="+url.QueryEscape(expr), http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil || result.Status != "success" {
		t.Fatalf("%s: %s %s", expr, result.Error, body)
	}
	series := make([]map[string]string, len(result.Data.Result))
	for i, r := range result.Data.Result {
		series[i] = r.Metric
	}
	return series
}
