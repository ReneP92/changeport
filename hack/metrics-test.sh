#!/usr/bin/env bash
# Tests the metrics pipeline on kind: VictoriaMetrics scrapes every broker of
# every customer cluster, each series carries the labels of
# docs/metrics-labels.md, no Kafka series lacks a tenant, traffic shows up in
# the counters, and no broker exceeds the series budget the allowList keeps
# it under. Run it after some traffic, such as the smoke tests.
# Usage: hack/metrics-test.sh
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

# Series one broker may send after relabeling; an idle single-broker cluster
# sends about 240 (docs/metrics-labels.md).
series_budget=1000

ensure_kubectl

tenants=$(kubectl get kafka -A -o jsonpath='{range .items[*]}{.metadata.labels.changeport\.io/tenant}{"\n"}{end}' | sort -u)
[[ -n "$tenants" ]] || die "no customer Kafka clusters to check"

for tenant in $tenants; do
  brokers=$(kubectl -n "customer-$tenant" get pods -l "strimzi.io/cluster=$tenant,strimzi.io/broker-role=true" -o name | grep -c .)
  log "Waiting for the metrics of customer $tenant's $brokers brokers"
  for _ in $(seq 24); do
    scraped=$(metrics_query "count(kafka_server_replicamanager_partitioncount{tenant=\"$tenant\"})")
    [[ ${scraped:-0} == "$brokers" ]] && break
    sleep 5
  done
  [[ ${scraped:-0} == "$brokers" ]] || die "VictoriaMetrics has metrics of ${scraped:-0} of customer $tenant's $brokers brokers"
done

for label in tenant cluster node_id zone env; do
  missing=$(metrics_query "count({job=\"kafka\", __name__!~\"up|scrape_.*\", $label=\"\"})")
  [[ -z "$missing" ]] || die "$missing broker series have no $label label"
done
untenanted=$(metrics_query 'count({__name__=~"kafka_.*", tenant=""})')
[[ -z "$untenanted" ]] || die "$untenanted Kafka series have no tenant"
log "Every broker series carries tenant, cluster, node_id, zone and env"

bytes_in=$(metrics_query 'sum(kafka_server_brokertopicmetrics_bytesinpersec_total{tenant!="", topic=""})')
[[ -n "$bytes_in" && ${bytes_in%.*} -gt 0 ]] || die "no bytes-in traffic was recorded; run this after some traffic"

largest=$(metrics_query 'max(scrape_samples_post_metric_relabeling{job="kafka"})')
[[ ${largest%.*} -le $series_budget ]] || die "a broker sends $largest series, over the budget of $series_budget"
log "Metrics test passed: at most $largest series per broker, within the budget of $series_budget"
