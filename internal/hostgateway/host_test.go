package hostgateway

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// TestProduceAndConsumeFromTheHost connects to a customer's cluster on kind
// as an application on the developer's machine would, with the
// client.properties of CHANGEPORT_CLIENT_PROPERTIES, through the host
// gateway and the host's DNS (just host-dns). hack/host-access-test.sh runs
// it; without the variable it is skipped.
func TestProduceAndConsumeFromTheHost(t *testing.T) {
	path := os.Getenv("CHANGEPORT_CLIENT_PROPERTIES")
	if path == "" {
		t.Skip("CHANGEPORT_CLIENT_PROPERTIES is not set; hack/host-access-test.sh sets it")
	}
	properties := readProperties(t, path)
	opts := clientOptions(t, properties)
	topic := fmt.Sprintf("host-%d", time.Now().UnixNano())

	admin, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	adm := kadm.NewClient(admin)
	if _, err := adm.CreateTopic(t.Context(), 3, -1, nil, topic); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	defer func() { _, _ = adm.DeleteTopics(t.Context(), topic) }()

	// Producing to three partitions reaches every broker that leads one.
	producer, err := kgo.NewClient(append(opts, kgo.DefaultProduceTopic(topic))...)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	const messages = 30
	for i := range messages {
		if err := producer.ProduceSync(t.Context(), &kgo.Record{Value: fmt.Appendf(nil, "message %d", i)}).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	consumer, err := kgo.NewClient(append(opts, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	consumed := 0
	deadline := time.Now().Add(time.Minute)
	for consumed < messages && time.Now().Before(deadline) {
		fetches := consumer.PollFetches(t.Context())
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("consume: %v", errs[0].Err)
		}
		consumed += fetches.NumRecords()
	}
	if consumed != messages {
		t.Fatalf("consumed %d of %d messages", consumed, messages)
	}
	t.Logf("produced and consumed %d messages on %s through %s", messages, topic, properties["bootstrap.servers"])
}

// readProperties reads a Java properties file with the continuation lines
// the console writes for the CA certificate.
func readProperties(t *testing.T, path string) map[string]string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	properties := map[string]string{}
	var line strings.Builder
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		text := strings.TrimLeft(scanner.Text(), " \t")
		if line.Len() == 0 && (text == "" || strings.HasPrefix(text, "#")) {
			continue
		}
		if continued, ok := strings.CutSuffix(text, `\`); ok {
			line.WriteString(continued)
			continue
		}
		line.WriteString(text)
		key, value, _ := strings.Cut(line.String(), "=")
		properties[strings.TrimSpace(key)] = strings.TrimSpace(value)
		line.Reset()
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return properties
}

var jaasCredentials = regexp.MustCompile(`username="([^"]*)" password="([^"]*)"`)

// clientOptions are the franz-go options for the properties: SASL/SCRAM
// over TLS, trusting the certificate in the file.
func clientOptions(t *testing.T, properties map[string]string) []kgo.Opt {
	t.Helper()
	if properties["security.protocol"] != "SASL_SSL" || properties["sasl.mechanism"] != "SCRAM-SHA-512" {
		t.Fatalf("unexpected security settings: %v %v", properties["security.protocol"], properties["sasl.mechanism"])
	}
	credentials := jaasCredentials.FindStringSubmatch(properties["sasl.jaas.config"])
	if credentials == nil {
		t.Fatal("client.properties has no username and password")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	if certificates := properties["ssl.truststore.certificates"]; certificates != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(normalizePEM(certificates)) {
			t.Fatal("client.properties has a malformed CA certificate")
		}
		config.RootCAs = pool
	}
	return []kgo.Opt{
		kgo.SeedBrokers(strings.Split(properties["bootstrap.servers"], ",")...),
		kgo.DialTLSConfig(config),
		kgo.SASL(scram.Auth{User: credentials[1], Pass: credentials[2]}.AsSha512Mechanism()),
	}
}

var pemBlock = regexp.MustCompile(`-----BEGIN CERTIFICATE-----([A-Za-z0-9+/=\s]+)-----END CERTIFICATE-----`)

// normalizePEM turns certificates whose lines a properties file joined with
// spaces back into PEM.
func normalizePEM(joined string) []byte {
	var out []byte
	for _, match := range pemBlock.FindAllStringSubmatch(joined, -1) {
		der, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(match[1]), ""))
		if err != nil {
			continue
		}
		out = append(out, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}
	return out
}
