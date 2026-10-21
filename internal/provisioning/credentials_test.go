package provisioning

import (
	"encoding/base64"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func addSecret(f *fakeCluster, namespace, name string, data map[string]string) {
	encoded := map[string]any{}
	for key, value := range data {
		encoded[key] = base64.StdEncoding.EncodeToString([]byte(value))
	}
	obj := &unstructured.Unstructured{Object: map[string]any{"data": encoded}}
	obj.SetGroupVersionKind(secretGVK)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	f.objects[fakeKey(secretGVK, namespace, name)] = obj
}

const jaas = `org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="s3cret";`

func TestClientPropertiesWithPrivateCA(t *testing.T) {
	cluster := newFakeCluster()
	addSecret(cluster, acme.Namespace(), AdminUser, map[string]string{"sasl.jaas.config": jaas, "password": "s3cret"})
	addSecret(cluster, acme.Namespace(), ExternalListenerSecret, map[string]string{
		"ca.crt": "-----BEGIN CERTIFICATE-----\nMIIB\nAAAA\n-----END CERTIFICATE-----\n",
	})
	got, err := testProvisioner(cluster).ClientProperties(t.Context(), acme)
	if err != nil {
		t.Fatal(err)
	}
	want := `# Kafka cluster of customer acme, as user admin
bootstrap.servers=bootstrap.acme.kafka.changeport.test:9094
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=` + jaas + `
# The listener certificate comes from a CA clients do not trust by default.
ssl.truststore.type=PEM
ssl.truststore.certificates=-----BEGIN CERTIFICATE----- \
  MIIB \
  AAAA \
  -----END CERTIFICATE-----
`
	if got != want {
		t.Errorf("ClientProperties() =\n%s\nwant\n%s", got, want)
	}
}

func TestClientPropertiesWithPublicCA(t *testing.T) {
	cluster := newFakeCluster()
	addSecret(cluster, acme.Namespace(), AdminUser, map[string]string{"sasl.jaas.config": jaas})
	// ACME issuers such as Let's Encrypt leave ca.crt out.
	addSecret(cluster, acme.Namespace(), ExternalListenerSecret, map[string]string{"tls.crt": "x"})
	got, err := testProvisioner(cluster).ClientProperties(t.Context(), acme)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, "truststore") {
		t.Errorf("ClientProperties() sets a truststore for a publicly trusted certificate:\n%s", got)
	}
}

func TestClientPropertiesNeedsTheUser(t *testing.T) {
	cluster := newFakeCluster()
	if _, err := testProvisioner(cluster).ClientProperties(t.Context(), acme); err == nil || !strings.Contains(err.Error(), "customer-acme/admin") {
		t.Errorf("ClientProperties() error = %v, want one naming the missing user secret", err)
	}
	addSecret(cluster, acme.Namespace(), AdminUser, map[string]string{"password": "s3cret"})
	if _, err := testProvisioner(cluster).ClientProperties(t.Context(), acme); err == nil || !strings.Contains(err.Error(), "sasl.jaas.config") {
		t.Errorf("ClientProperties() error = %v, want one about the missing sasl.jaas.config", err)
	}
}
