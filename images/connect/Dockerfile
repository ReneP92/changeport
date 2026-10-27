# syntax=docker/dockerfile:1

# The Changeport Kafka Connect image, for the Postgres CDC source
# (docs/cdc.md): Strimzi's Kafka image, which Strimzi also runs Kafka Connect
# from, plus two plugins in /opt/kafka/plugins/:
# - Debezium's Postgres connector, which reads a database's changes from its
#   write-ahead log over logical replication, and
# - Apicurio Registry's converter, which writes them as Avro with their
#   schemas in the customer's registry.
# Build it with just connect-image.

# Strimzi 1.3.0 with Kafka 4.3.1, as images/kafka builds on, pinned by the
# digest of its multi-architecture index.
ARG STRIMZI_KAFKA_IMAGE=quay.io/strimzi/kafka:1.3.0-kafka-4.3.1@sha256:1f189b1f0f32684302d8b018b1d21b411e2469a9c465fbdc18fa3599cb5bbdad

# Debezium 3.7 is built against Kafka 4.3.1, the version Strimzi runs here.
# The plugin archives come from Maven Central, checked against the SHA-256 of
# the archives whose SHA-1 Maven Central publishes.
ARG DEBEZIUM_VERSION=3.7.0.Final
ARG APICURIO_VERSION=3.3.3

# The archives are the same on every architecture, so they are unpacked once
# on the build machine's own platform.
FROM --platform=$BUILDPLATFORM eclipse-temurin:21-jdk@sha256:23cc5734ff46e776f51ce66eb9ee08444fd47caabec0a95b7eb6e94be2d3696d AS plugins
ARG DEBEZIUM_VERSION
ARG APICURIO_VERSION
ADD --checksum=sha256:660673cbabd2f269da21409208ebd0f0d9977bd92e824fc9daa272f240e88e51 \
    https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz \
    /archives/debezium.tar.gz
ADD --checksum=sha256:1c28bb0fc4e737f56bba389a936535565607d89502458d5f28bbe4c02ac4bb3a \
    https://repo1.maven.org/maven2/io/apicurio/apicurio-registry-distro-connect-converter/${APICURIO_VERSION}/apicurio-registry-distro-connect-converter-${APICURIO_VERSION}.tar.gz \
    /archives/apicurio.tar.gz
# Each plugin gets a directory of its own, which Kafka Connect loads in
# isolation: the converter brings libraries that Debezium and Kafka bring in
# other versions.
RUN mkdir -p /plugins/debezium-connector-postgres /plugins/apicurio-converter \
    && tar -xzf /archives/debezium.tar.gz -C /plugins/debezium-connector-postgres --strip-components=1 \
    && tar -xzf /archives/apicurio.tar.gz -C /plugins/apicurio-converter \
    && test -f "/plugins/debezium-connector-postgres/debezium-connector-postgres-${DEBEZIUM_VERSION}.jar" \
    && test -f "/plugins/apicurio-converter/apicurio-registry-utils-converter-${APICURIO_VERSION}.jar"

# No RUN instructions below this line, so building for another architecture
# needs no emulation.
FROM ${STRIMZI_KAFKA_IMAGE}
ARG DEBEZIUM_VERSION
ARG APICURIO_VERSION
LABEL org.opencontainers.image.title="Changeport Kafka Connect" \
      org.opencontainers.image.source="https://github.com/ReneP92/changeport" \
      io.changeport.debezium.version="${DEBEZIUM_VERSION}" \
      io.changeport.apicurio.version="${APICURIO_VERSION}"
COPY --from=plugins /plugins/ /opt/kafka/plugins/
