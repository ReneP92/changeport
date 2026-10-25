# syntax=docker/dockerfile:1

# The Changeport Kafka broker image: Strimzi's Kafka image plus the Aiven
# remote storage manager for tiered storage on S3, installed into
# /opt/kafka/plugins/tiered-storage/ (the classPath of the template's
# tieredStorage). Build it with just kafka-image.

# Strimzi 1.3.0 with Kafka 4.3.1 (internal/provisioning.KafkaVersion),
# pinned by the digest of its multi-architecture index.
ARG STRIMZI_KAFKA_IMAGE=quay.io/strimzi/kafka:1.3.0-kafka-4.3.1@sha256:1f189b1f0f32684302d8b018b1d21b411e2469a9c465fbdc18fa3599cb5bbdad

# Aiven-Open/tiered-storage-for-apache-kafka is built from a commit on main:
# its latest release, 1.1.1, fails to load on Kafka 4.1 and newer (SLF4J 2,
# Aiven issue #776, fixed after the release by #778). Upstream CI, including
# the S3 end-to-end tests, passed on this commit.
ARG TIERED_STORAGE_COMMIT=7574b41af141726cdd7aa27cdb66572201a6e09d

# Java bytecode is the same on every architecture, so the plugin is built once
# on the build machine's own platform.
FROM --platform=$BUILDPLATFORM eclipse-temurin:21-jdk@sha256:23cc5734ff46e776f51ce66eb9ee08444fd47caabec0a95b7eb6e94be2d3696d AS tiered-storage
ARG TIERED_STORAGE_COMMIT
ADD https://github.com/Aiven-Open/tiered-storage-for-apache-kafka.git#${TIERED_STORAGE_COMMIT} /src
WORKDIR /src
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew --no-daemon distTar :storage:s3:distTar -x test -x integrationTest
# The core and S3 distributions unpack into one directory, as the Strimzi
# tiered storage documentation does. Each resolves its dependencies on its own,
# so a library can come twice in different versions (commons-codec and
# httpcore at this commit). The broker loads the directory with a wildcard,
# whose order is unspecified, so only the newest version of each is kept, and
# the build fails if any library is still there twice.
RUN mkdir /plugin \
    && tar -xzf build/distributions/tiered-storage-for-apache-kafka-*.tgz -C /plugin --strip-components=1 \
    && tar -xzf storage/s3/build/distributions/s3-*.tgz -C /plugin --strip-components=1
WORKDIR /plugin
RUN libraries() { ls -- *.jar | sed -E 's/-[0-9].*\.jar$//' | sort; } \
    && for library in $(libraries | uniq -d); do \
         for older in $(ls -- "$library"-[0-9]*.jar | sort -V | head -n -1); do \
           echo "removing $older, keeping the newest $library"; rm -- "$older"; \
         done; \
       done \
    && test -z "$(libraries | uniq -d)"

# No RUN instructions below this line, so building for another architecture
# needs no emulation.
FROM ${STRIMZI_KAFKA_IMAGE}
ARG TIERED_STORAGE_COMMIT
LABEL org.opencontainers.image.title="Changeport Kafka" \
      org.opencontainers.image.source="https://github.com/ReneP92/changeport" \
      io.changeport.tiered-storage.commit="${TIERED_STORAGE_COMMIT}"
COPY --from=tiered-storage /plugin/ /opt/kafka/plugins/tiered-storage/
