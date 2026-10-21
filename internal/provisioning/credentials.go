package provisioning

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// ClientProperties returns a Java client.properties for the customer's admin
// user on the public listener: the bootstrap address, SASL/SCRAM over TLS and,
// when the listener certificate's issuer is not publicly trusted (the local CA
// on kind), that CA as an inline PEM truststore.
func (p *Provisioner) ClientProperties(ctx context.Context, spec Spec) (string, error) {
	if err := spec.Validate(); err != nil {
		return "", err
	}
	user, err := p.secretValue(ctx, spec.Namespace(), AdminUser, "sasl.jaas.config")
	if err != nil {
		return "", err
	}
	if user == "" {
		return "", fmt.Errorf("secret %s/%s has no sasl.jaas.config; is the KafkaUser ready?", spec.Namespace(), AdminUser)
	}
	ca, err := p.secretValue(ctx, spec.Namespace(), ExternalListenerSecret, "ca.crt")
	if err != nil {
		return "", err
	}

	var b strings.Builder
	fmt.Fprintf(&b, "# Kafka cluster of customer %s, as user %s\n", spec.CustomerID, AdminUser)
	fmt.Fprintf(&b, "bootstrap.servers=%s\n", spec.BootstrapAddress())
	b.WriteString("security.protocol=SASL_SSL\n")
	b.WriteString("sasl.mechanism=SCRAM-SHA-512\n")
	fmt.Fprintf(&b, "sasl.jaas.config=%s\n", strings.TrimSpace(user))
	if ca = strings.TrimSpace(ca); ca != "" {
		b.WriteString("# The listener certificate comes from a CA clients do not trust by default.\n")
		b.WriteString("ssl.truststore.type=PEM\n")
		b.WriteString("ssl.truststore.certificates=")
		lines := strings.Split(ca, "\n")
		for i, line := range lines {
			if i < len(lines)-1 {
				fmt.Fprintf(&b, "%s \\\n  ", line)
			} else {
				fmt.Fprintf(&b, "%s\n", line)
			}
		}
	}
	return b.String(), nil
}

// secretValue returns one decoded key of a Secret, or "" if the key is
// missing or empty.
func (p *Provisioner) secretValue(ctx context.Context, namespace, name, key string) (string, error) {
	secret, err := p.Cluster.Get(ctx, secretGVK, namespace, name)
	if err != nil {
		return "", fmt.Errorf("get secret %s/%s: %w", namespace, name, err)
	}
	encoded, _, err := unstructured.NestedString(secret.Object, "data", key)
	if err != nil {
		return "", fmt.Errorf("secret %s/%s: %w", namespace, name, err)
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("secret %s/%s key %s: %w", namespace, name, key, err)
	}
	return string(decoded), nil
}
