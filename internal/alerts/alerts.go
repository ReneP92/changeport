// Package alerts reads the alerting rules of deploy/base/victoria-metrics/alerts,
// which vmalert evaluates once it runs (#96). Its tests check the rules
// against the runbook and the documented metrics, and evaluate them against
// VictoriaMetrics in the end-to-end tests.
package alerts

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"sigs.k8s.io/yaml"
)

// Rule is an alerting rule.
type Rule struct {
	Alert       string            `json:"alert"`
	Expr        string            `json:"expr"`
	For         string            `json:"for,omitempty"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
}

type ruleFile struct {
	Groups []struct {
		Name  string `json:"name"`
		Rules []Rule `json:"rules"`
	} `json:"groups"`
}

// Load reads the rules of every YAML file in dir, by file name.
func Load(dir string) ([]Rule, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	slices.Sort(files)
	var rules []Rule
	for _, path := range files {
		data, err := os.ReadFile(path) //nolint:gosec // G304: the repository's own rules
		if err != nil {
			return nil, err
		}
		var file ruleFile
		if err := yaml.UnmarshalStrict(data, &file); err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Base(path), err)
		}
		for _, group := range file.Groups {
			rules = append(rules, group.Rules...)
		}
	}
	return rules, nil
}

// Anchor is the anchor GitHub gives a Markdown heading: lowercase, spaces
// as dashes, and nothing else but letters, digits, dashes and underscores.
func Anchor(heading string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(heading)) {
		switch {
		case r == ' ':
			b.WriteRune('-')
		case r == '-' || r == '_' || ('a' <= r && r <= 'z') || ('0' <= r && r <= '9'):
			b.WriteRune(r)
		}
	}
	return b.String()
}
