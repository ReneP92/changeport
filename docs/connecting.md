# Connecting to your Kafka cluster

Your Changeport cluster speaks the Kafka protocol, so any Kafka client works.
It accepts TLS connections with a user name and password (SASL_SSL with
SCRAM-SHA-512) on port 9094, from anywhere on the internet.

## Your connection details

In the Changeport console, an admin of your organization opens **Overview** and
downloads `client.properties` from the **Connection** card. It holds everything
a client needs:

```properties
bootstrap.servers=bootstrap.<your-cluster-id>.kafka.changeport.<tld>:9094
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="...";
```

- **Bootstrap address:** clients connect to it first and then to each broker
  by its own name. Use the name, never an IP address: the brokers' TLS
  certificate is for the names.
- **User:** `admin` may create, read, write and delete every topic and
  consumer group of your cluster, and use transactions. Keep the password
  out of source control, for example in your platform's secret store.
- **TLS certificate:** issued by Let's Encrypt, which every mainstream
  operating system and language runtime trusts, so clients need no
  certificate configuration.

## Settings that matter

### Keep the default retries

Leave your producers' retry settings at their defaults: Kafka clients retry a
failed send until `delivery.timeout.ms` (two minutes in Java, five in
librdkafka-based clients) has passed. That is what carries your producers
through an availability zone outage or a broker restart without losing or
duplicating messages: with the default `acks=all` and idempotence, the retried
sends land exactly once.

Do not set `retries` to a small number. Our failover tests show the
difference: Kafka's own console producer retries only 3 times by default,
and that turns a zone outage into failed sends, while producers with the
default settings deliver every message.

### Set `client.rack` on consumers

Clusters with three availability zones (the production profile) keep a copy
of every partition in each zone. A consumer that sets `client.rack` to the
availability zone it runs in reads from the copy in its own zone, which is
faster and avoids AWS's cross-zone transfer charges.

Use the **zone ID**, such as `use1-az1`, not the zone name such as
`us-east-1a`: zone names are mapped differently in every AWS account, so
your `us-east-1a` may be a different zone from ours, while zone IDs are the
same everywhere. To see how your account maps them:

```bash
aws ec2 describe-availability-zones --region us-east-1 \
  --query 'AvailabilityZones[].[ZoneName,ZoneId]' --output table
```

On EC2, an instance can read its own zone ID from the instance metadata
service:

```bash
token=$(curl -sX PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')
curl -s -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/placement/availability-zone-id
```

Outside AWS, or if you are unsure, leave `client.rack` unset: consumers then
read from each partition's leader, which works everywhere.

## Examples

Each example produces a message and consumes it back as a consumer group.
They take the connection details from environment variables:
`KAFKA_BOOTSTRAP`, `KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_CLIENT_RACK`
and `TOPIC`.

### Java

The Java client reads `client.properties` as it is.

```java
import java.io.FileInputStream;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Example {
    public static void main(String[] args) throws Exception {
        Properties config = new Properties();
        try (var in = new FileInputStream("client.properties")) {
            config.load(in);
        }
        String topic = System.getenv("TOPIC");

        Properties producerConfig = new Properties();
        producerConfig.putAll(config);
        producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try (var producer = new KafkaProducer<String, String>(producerConfig)) {
            producer.send(new ProducerRecord<>(topic, "order-1", "hello from Java")).get();
        }

        Properties consumerConfig = new Properties();
        consumerConfig.putAll(config);
        consumerConfig.put("group.id", "example-java");
        consumerConfig.put("auto.offset.reset", "earliest");
        // The availability zone ID your application runs in, such as use1-az1.
        consumerConfig.put("client.rack", System.getenv("KAFKA_CLIENT_RACK"));
        consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        try (var consumer = new KafkaConsumer<String, String>(consumerConfig)) {
            consumer.subscribe(List.of(topic));
            for (int i = 0; i < 30; i++) {
                for (var record : consumer.poll(Duration.ofSeconds(1))) {
                    System.out.println("java consumed: " + record.value());
                    return;
                }
            }
            throw new IllegalStateException("nothing consumed");
        }
    }
}
```

Spring Boot takes the same properties under `spring.kafka.properties.*`, for
example `spring.kafka.properties.sasl.mechanism=SCRAM-SHA-512`.

### Python

With [confluent-kafka](https://pypi.org/project/confluent-kafka/)
(`pip install confluent-kafka`). Note that librdkafka spells the mechanism
setting `sasl.mechanisms`.

```python
import os

from confluent_kafka import Consumer, Producer

config = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "SCRAM-SHA-512",
    "sasl.username": os.environ["KAFKA_USERNAME"],
    "sasl.password": os.environ["KAFKA_PASSWORD"],
}
topic = os.environ["TOPIC"]

producer = Producer(config)
producer.produce(topic, key="order-1", value="hello from Python")
if producer.flush(30) != 0:
    raise SystemExit("not delivered")

consumer = Consumer({
    **config,
    "group.id": "example-python",
    "auto.offset.reset": "earliest",
    # The availability zone ID your application runs in, such as use1-az1.
    "client.rack": os.environ["KAFKA_CLIENT_RACK"],
})
consumer.subscribe([topic])
for _ in range(30):
    message = consumer.poll(1.0)
    if message is not None and message.error() is None:
        print("python consumed:", message.value().decode())
        break
else:
    raise SystemExit("nothing consumed")
consumer.close()
```

### Go

With [franz-go](https://github.com/twmb/franz-go)
(`go get github.com/twmb/franz-go`).

```go
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	topic := os.Getenv("TOPIC")
	opts := []kgo.Opt{
		kgo.SeedBrokers(os.Getenv("KAFKA_BOOTSTRAP")),
		kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}),
		kgo.SASL(scram.Auth{User: os.Getenv("KAFKA_USERNAME"), Pass: os.Getenv("KAFKA_PASSWORD")}.AsSha512Mechanism()),
	}

	producer, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("order-1"), Value: []byte("hello from Go")}).FirstErr(); err != nil {
		panic(err)
	}
	producer.Close()

	consumer, err := kgo.NewClient(append(opts,
		kgo.ConsumerGroup("example-go"),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// The availability zone ID your application runs in, such as use1-az1.
		kgo.Rack(os.Getenv("KAFKA_CLIENT_RACK")),
	)...)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()
	fetches := consumer.PollFetches(ctx)
	if err := fetches.Err(); err != nil {
		panic(err)
	}
	fmt.Println("go consumed:", string(fetches.Records()[0].Value))
}

```

### Node.js

With Confluent's
[JavaScript client](https://www.npmjs.com/package/@confluentinc/kafka-javascript)
(`npm install @confluentinc/kafka-javascript`), which is built on librdkafka
and takes the same settings as the Python client.

```js
const { Kafka } = require("@confluentinc/kafka-javascript").KafkaJS;

const config = {
  "bootstrap.servers": process.env.KAFKA_BOOTSTRAP,
  "security.protocol": "sasl_ssl",
  "sasl.mechanisms": "SCRAM-SHA-512",
  "sasl.username": process.env.KAFKA_USERNAME,
  "sasl.password": process.env.KAFKA_PASSWORD,
};
const topic = process.env.TOPIC;

async function main() {
  const producer = new Kafka(config).producer();
  await producer.connect();
  await producer.send({ topic, messages: [{ key: "order-1", value: "hello from Node.js" }] });
  await producer.disconnect();

  const consumer = new Kafka({
    ...config,
    "group.id": "example-node",
    "auto.offset.reset": "earliest",
    // The availability zone ID your application runs in, such as use1-az1.
    "client.rack": process.env.KAFKA_CLIENT_RACK,
  }).consumer();
  await consumer.connect();
  await consumer.subscribe({ topics: [topic] });
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("nothing consumed")), 60000);
    consumer.run({
      eachMessage: async ({ message }) => {
        console.log("node consumed:", message.value.toString());
        clearTimeout(timer);
        resolve();
      },
    });
  });
  await consumer.disconnect();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

### Command line

Kafka's own tools take `client.properties` with `--command-config`:

```bash
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --command-config client.properties --list
```

The console producer retries only 3 times by default (see above); for more
than a quick test, raise it with `--command-property retries=2147483647`.

## If you cannot connect

- **`SaslAuthenticationException` or "invalid credentials":** the user name
  or password is wrong, or the password was rotated. Download
  `client.properties` again.
- **Certificate or hostname errors:** connect by name, not by IP address, and
  do not override the client's trusted certificate authorities.
- **Timeouts:** outbound connections on port 9094 must be allowed from your
  network to the bootstrap address and to every broker name.
