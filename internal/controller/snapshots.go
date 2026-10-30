package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/cdc"
	"github.com/ReneP92/changeport/internal/provisioning"
)

// Notification is one of Debezium's notifications about a source's
// incremental snapshots, such as IN_PROGRESS with its chunk.
type Notification struct {
	Type          string
	AggregateType string
	Data          map[string]string
}

// SnapshotChannel asks a source's connector for snapshots, through its signal
// topic, and reads their progress from its notification topic.
type SnapshotChannel interface {
	// Signal asks for an incremental snapshot of tables.
	Signal(ctx context.Context, source *v1alpha1.PostgresSource, names cdc.Names, tables []cdc.Table) error
	// Notifications returns the notifications from offset from on, and the
	// offset to read from next.
	Notifications(ctx context.Context, source *v1alpha1.PostgresSource, names cdc.Names, from int64) ([]Notification, int64, error)
}

// KafkaSnapshots is the SnapshotChannel of the customer's Kafka cluster: it
// connects to the internal mTLS listener as the Connect worker's user, which
// may write signals and read notifications, and decodes the notifications'
// Avro with their schemas from the customer's registry.
type KafkaSnapshots struct {
	Client client.Client
	// HTTP fetches schemas; nil uses a client with a 10 second timeout.
	HTTP *http.Client
	// Bootstrap and RegistryURL locate a customer's cluster and registry;
	// nil uses their addresses inside the Kubernetes cluster.
	Bootstrap   func(spec provisioning.Spec) string
	RegistryURL func(spec provisioning.Spec) string

	schemas sync.Map // registry URL and ID to avro.Schema
}

var _ SnapshotChannel = (*KafkaSnapshots)(nil)

// signal is Debezium's signal for an incremental snapshot.
type signal struct {
	Type string     `json:"type"`
	Data signalData `json:"data"`
}

type signalData struct {
	DataCollections []string `json:"data-collections"`
	Type            string   `json:"type"`
}

// Signal implements SnapshotChannel. The record's key is the source's topic
// prefix, which Debezium requires of signals on Kafka.
func (k *KafkaSnapshots) Signal(ctx context.Context, source *v1alpha1.PostgresSource, names cdc.Names, tables []cdc.Table) error {
	collections := make([]string, len(tables))
	for i, table := range tables {
		collections[i] = cdc.TablePattern(table)
	}
	value, err := json.Marshal(signal{Type: "execute-snapshot", Data: signalData{DataCollections: collections, Type: "INCREMENTAL"}})
	if err != nil {
		return err
	}
	kafka, err := k.kafka(ctx, source)
	if err != nil {
		return err
	}
	defer kafka.Close()
	record := &kgo.Record{Topic: names.SignalTopic, Key: []byte(source.Name), Value: value}
	if err := kafka.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("send the snapshot signal of source %s: %w", source.Name, err)
	}
	return nil
}

// Notifications implements SnapshotChannel.
func (k *KafkaSnapshots) Notifications(ctx context.Context, source *v1alpha1.PostgresSource, names cdc.Names, from int64) ([]Notification, int64, error) {
	kafka, err := k.kafka(ctx, source, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		names.NotificationTopic: {0: kgo.NewOffset().At(from)},
	}))
	if err != nil {
		return nil, from, err
	}
	defer kafka.Close()
	ends, err := kadm.NewClient(kafka).ListEndOffsets(ctx, names.NotificationTopic)
	if err != nil {
		return nil, from, fmt.Errorf("read the end of source %s's notifications: %w", source.Name, err)
	}
	end, ok := ends.Lookup(names.NotificationTopic, 0)
	if !ok {
		return nil, from, fmt.Errorf("source %s's notification topic %s has no partition 0", source.Name, names.NotificationTopic)
	}
	if end.Err != nil {
		return nil, from, fmt.Errorf("read the end of source %s's notifications: %w", source.Name, end.Err)
	}
	var notifications []Notification
	next := from
	for next < end.Offset {
		fetches := kafka.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return notifications, next, err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return notifications, next, fmt.Errorf("read source %s's notifications: %w", source.Name, errs[0].Err)
		}
		for _, record := range fetches.Records() {
			next = record.Offset + 1
			notification, err := k.decode(ctx, source, record.Value)
			if err != nil {
				return notifications, record.Offset, err
			}
			if notification != nil {
				notifications = append(notifications, *notification)
			}
		}
	}
	return notifications, next, nil
}

// notificationRecord is Debezium's Notification record.
type notificationRecord struct {
	ID            string `avro:"id"`
	Type          string `avro:"type"`
	AggregateType string `avro:"aggregate_type"`
	// AdditionalData is a nullable union, which hamba/avro decodes into a
	// pointer.
	AdditionalData *map[string]string `avro:"additional_data"`
	Timestamp      int64              `avro:"timestamp"`
}

// decode reads a notification in Confluent's wire format: a zero byte, the
// schema's ID in four bytes, then Avro. A record without a value, or not in
// that format, is no notification.
func (k *KafkaSnapshots) decode(ctx context.Context, source *v1alpha1.PostgresSource, value []byte) (*Notification, error) {
	if len(value) < 5 || value[0] != 0 {
		return nil, nil
	}
	schema, err := k.schema(ctx, source, binary.BigEndian.Uint32(value[1:5]))
	if err != nil {
		return nil, err
	}
	var record notificationRecord
	if err := avro.Unmarshal(schema, value[5:], &record); err != nil {
		return nil, fmt.Errorf("decode a notification of source %s: %w", source.Name, err)
	}
	notification := &Notification{Type: record.Type, AggregateType: record.AggregateType}
	if record.AdditionalData != nil {
		notification.Data = *record.AdditionalData
	}
	return notification, nil
}

// schema fetches a schema by ID from the customer's registry, through its
// Confluent-compatible API, once.
func (k *KafkaSnapshots) schema(ctx context.Context, source *v1alpha1.PostgresSource, id uint32) (avro.Schema, error) {
	base := k.registryURL(source)
	key := fmt.Sprintf("%s#%d", base, id)
	if schema, ok := k.schemas.Load(key); ok {
		return schema.(avro.Schema), nil
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/apis/ccompat/v7/schemas/ids/%d", base, id), nil)
	if err != nil {
		return nil, err
	}
	httpClient := k.HTTP
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch schema %d of source %s: %w", id, source.Name, err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch schema %d of source %s: %s", id, source.Name, response.Status)
	}
	var schemaResponse struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(body, &schemaResponse); err != nil {
		return nil, fmt.Errorf("read schema %d of source %s: %w", id, source.Name, err)
	}
	schema, err := avro.Parse(schemaResponse.Schema)
	if err != nil {
		return nil, fmt.Errorf("parse schema %d of source %s: %w", id, source.Name, err)
	}
	k.schemas.Store(key, schema)
	return schema, nil
}

func (k *KafkaSnapshots) spec(source *v1alpha1.PostgresSource) provisioning.Spec {
	return provisioning.Spec{CustomerID: strings.TrimPrefix(source.Namespace, provisioning.NamespacePrefix)}
}

func (k *KafkaSnapshots) registryURL(source *v1alpha1.PostgresSource) string {
	if k.RegistryURL != nil {
		return k.RegistryURL(k.spec(source))
	}
	return fmt.Sprintf("http://%s.%s.svc:%d", provisioning.Registry, source.Namespace, provisioning.RegistryPort)
}

// kafka connects to the source's customer's cluster as the Connect worker's
// user.
func (k *KafkaSnapshots) kafka(ctx context.Context, source *v1alpha1.PostgresSource, options ...kgo.Opt) (*kgo.Client, error) {
	spec := k.spec(source)
	var user, ca corev1.Secret
	if err := k.Client.Get(ctx, client.ObjectKey{Namespace: source.Namespace, Name: provisioning.ConnectUser}, &user); err != nil {
		return nil, fmt.Errorf("get the Connect worker's certificate: %w", err)
	}
	if err := k.Client.Get(ctx, client.ObjectKey{Namespace: source.Namespace, Name: spec.ClusterCASecret()}, &ca); err != nil {
		return nil, fmt.Errorf("get the cluster CA: %w", err)
	}
	certificate, err := tls.X509KeyPair(user.Data["user.crt"], user.Data["user.key"])
	if err != nil {
		return nil, fmt.Errorf("the Connect worker's certificate: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca.Data["ca.crt"]) {
		return nil, errors.New("the cluster CA holds no certificate")
	}
	bootstrap := spec.InternalBootstrapAddress()
	if k.Bootstrap != nil {
		bootstrap = k.Bootstrap(spec)
	}
	options = append([]kgo.Opt{
		kgo.SeedBrokers(bootstrap),
		kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}, RootCAs: roots}),
		kgo.ClientID("changeport-source-controller"),
		kgo.RequestTimeoutOverhead(10 * time.Second),
	}, options...)
	return kgo.NewClient(options...)
}
