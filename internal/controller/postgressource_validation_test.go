package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/cdc"
)

// sourcesNamespace is a customer's namespace for the sources of the tests.
func sourcesNamespace(t *testing.T, c client.Client) string {
	t.Helper()
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "customer-sources-"}}
	if err := c.Create(t.Context(), namespace); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Delete(context.Background(), namespace) })
	return namespace.Name
}

func validSource(namespace, name string) *v1alpha1.PostgresSource {
	return &v1alpha1.PostgresSource{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: v1alpha1.PostgresSourceSpec{
			ID:       "x1y2z3",
			Provider: v1alpha1.ProviderRDS,
			Connection: v1alpha1.PostgresConnection{
				Host:           "app.abc123.us-east-1.rds.amazonaws.com",
				Database:       "app",
				PasswordSecret: "postgres-password-app-1",
			},
			Tables: []v1alpha1.PostgresTable{{Schema: "public", Name: "orders"}, {Schema: "Sales", Name: "Order Lines"}},
		},
	}
}

func TestPostgresSourceDefaults(t *testing.T) {
	requireEnv(t)
	c, err := client.New(testEnv, client.Options{Scheme: testScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	source := validSource(sourcesNamespace(t, c), "app")
	if err := c.Create(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	spec := source.Spec
	if spec.Connection.Port != 5432 || spec.Connection.TLS.Mode != v1alpha1.TLSVerifyFull || spec.RetentionMs != 604800000 || spec.State != v1alpha1.StateRunning {
		t.Errorf("defaults: port %d, TLS mode %q, retention %d, state %q; want 5432, verify-full, 7 days and Running",
			spec.Connection.Port, spec.Connection.TLS.Mode, spec.RetentionMs, spec.State)
	}
}

// The API server accepts exactly the names internal/cdc does, which the
// console checks first.
func TestPostgresSourceNamesMatchInternalCDC(t *testing.T) {
	requireEnv(t)
	c, err := client.New(testEnv, client.Options{Scheme: testScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	namespace := sourcesNamespace(t, c)
	for _, name := range []string{
		"app", "app-db", "a1", strings.Repeat("a", 40), strings.Repeat("a", 41), "1app", "app-", "changeport", "changeports",
		"strimzi-x", "connect-app", "connect", "connector", "app.db",
	} {
		t.Run(name, func(t *testing.T) {
			err := c.Create(t.Context(), validSource(namespace, name))
			// Names Kubernetes itself refuses never reach the CRD's rules.
			wantValid := cdc.ValidateName(name) == nil
			if wantValid && err != nil {
				t.Errorf("Create(%q) = %v, but internal/cdc accepts the name", name, err)
			}
			if !wantValid && !apierrors.IsInvalid(err) {
				t.Errorf("Create(%q) = %v, but internal/cdc refuses the name", name, err)
			}
		})
	}
}

func TestPostgresSourceValidation(t *testing.T) {
	requireEnv(t)
	c, err := client.New(testEnv, client.Options{Scheme: testScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	namespace := sourcesNamespace(t, c)
	tooMany := make([]v1alpha1.PostgresTable, cdc.MaxTables+1)
	for i := range tooMany {
		tooMany[i] = v1alpha1.PostgresTable{Schema: "public", Name: fmt.Sprintf("t%d", i)}
	}
	tests := []struct {
		name   string
		change func(*v1alpha1.PostgresSource)
		want   string
	}{
		{"an ID that is not one", func(s *v1alpha1.PostgresSource) { s.Spec.ID = "X1Y2Z3" }, "spec.id"},
		{"an unknown provider", func(s *v1alpha1.PostgresSource) { s.Spec.Provider = "oracle" }, `spec.provider: Unsupported value: "oracle"`},
		{"a host with a port", func(s *v1alpha1.PostgresSource) { s.Spec.Connection.Host = "db.example.com:5432/app" }, "spec.connection.host"},
		{"two hosts", func(s *v1alpha1.PostgresSource) { s.Spec.Connection.Host = "a.example.com,b.example.com" }, "spec.connection.host"},
		{"a port too high", func(s *v1alpha1.PostgresSource) { s.Spec.Connection.Port = 65536 }, "spec.connection.port"},
		{"no database", func(s *v1alpha1.PostgresSource) { s.Spec.Connection.Database = "" }, "spec.connection.database"},
		{"no password Secret", func(s *v1alpha1.PostgresSource) { s.Spec.Connection.PasswordSecret = "" }, "spec.connection.passwordSecret"},
		{"an unknown TLS mode", func(s *v1alpha1.PostgresSource) { s.Spec.Connection.TLS.Mode = "require" }, `spec.connection.tls.mode: Unsupported value: "require"`},
		{"verify-ca without a CA", func(s *v1alpha1.PostgresSource) { s.Spec.Connection.TLS.Mode = v1alpha1.TLSVerifyCA }, "verify-ca needs the database's CA"},
		{"a CA that is not PEM", func(s *v1alpha1.PostgresSource) { s.Spec.Connection.TLS.CA = "MIIB..." }, "the CA is PEM certificates"},
		{"no tables", func(s *v1alpha1.PostgresSource) { s.Spec.Tables = nil }, "spec.tables"},
		{"too many tables", func(s *v1alpha1.PostgresSource) { s.Spec.Tables = tooMany }, "spec.tables: Too many: 201: must have at most 200 items"},
		{"a table twice", func(s *v1alpha1.PostgresSource) {
			s.Spec.Tables = append(s.Spec.Tables, v1alpha1.PostgresTable{Schema: "public", Name: "orders"})
		}, "spec.tables[2]: Duplicate value"},
		{"a table without a schema", func(s *v1alpha1.PostgresSource) { s.Spec.Tables[0].Schema = "" }, "spec.tables[0].schema"},
		{"a retention that is no preset", func(s *v1alpha1.PostgresSource) { s.Spec.RetentionMs = 1234 }, "spec.retentionMs: Unsupported value: 1234"},
		{"an unknown state", func(s *v1alpha1.PostgresSource) { s.Spec.State = "Paused" }, `spec.state: Unsupported value: "Paused"`},
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := validSource(namespace, fmt.Sprintf("invalid-%d", i))
			test.change(source)
			if err := c.Create(t.Context(), source); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), test.want) {
				t.Errorf("Create() error = %v, want it invalid with %q", err, test.want)
			}
		})
	}

	t.Run("changes", func(t *testing.T) {
		source := validSource(namespace, "changes")
		source.Spec.Connection.TLS = v1alpha1.PostgresTLS{Mode: v1alpha1.TLSVerifyCA, CA: "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"}
		if err := c.Create(t.Context(), source); err != nil {
			t.Fatal(err)
		}
		// Tables, the host, the password, the retention and the state may
		// change.
		source.Spec.Tables = append(source.Spec.Tables, v1alpha1.PostgresTable{Schema: "billing", Name: "invoices"})
		source.Spec.Connection.Host = "app-new.abc123.us-east-1.rds.amazonaws.com"
		source.Spec.Connection.PasswordSecret = "postgres-password-app-2"
		source.Spec.RetentionMs = 86400000
		source.Spec.State = v1alpha1.StateStopped
		if err := c.Update(t.Context(), source); err != nil {
			t.Fatalf("Update() = %v, want allowed", err)
		}
		for name, change := range map[string]func(*v1alpha1.PostgresSource){
			"the ID cannot change":       func(s *v1alpha1.PostgresSource) { s.Spec.ID = "a1b2c3" },
			"the database cannot change": func(s *v1alpha1.PostgresSource) { s.Spec.Connection.Database = "other" },
		} {
			changed := source.DeepCopy()
			change(changed)
			if err := c.Update(t.Context(), changed); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), name) {
				t.Errorf("Update() error = %v, want %q", err, name)
			}
		}
	})
}
