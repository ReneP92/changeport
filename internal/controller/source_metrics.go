package controller

import (
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/ReneP92/changeport/api/v1alpha1"
	"github.com/ReneP92/changeport/internal/cdc"
	"github.com/ReneP92/changeport/internal/provisioning"
)

// The metrics of the Postgres CDC sources, by tenant and source, which the
// source controller exports and the alerts of
// deploy/base/victoria-metrics/alerts watch (docs/metrics-labels.md).
var (
	// retainedWAL is the WAL each source's slot holds back in the
	// customer's database: a source that stops reading makes it grow until
	// the database's disk is full.
	retainedWAL = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "changeport_source_retained_wal_bytes",
		Help: "The WAL a Postgres CDC source's replication slot keeps in the customer's database, in bytes.",
	}, []string{"tenant", "source"})
	safeWAL = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "changeport_source_slot_safe_wal_bytes",
		Help: "How much more WAL a source's slot may keep before the database invalidates it, in bytes; absent without max_slot_wal_keep_size.",
	}, []string{"tenant", "source"})
	slotLost = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "changeport_source_slot_lost",
		Help: "1 once the database has invalidated a source's slot (wal_status lost), else 0.",
	}, []string{"tenant", "source"})
	slotChecked = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "changeport_source_slot_checked_timestamp_seconds",
		Help: "When the source controller last read a source's slot in its database, in seconds since the epoch.",
	}, []string{"tenant", "source"})
	sourcePhase = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "changeport_source_phase",
		Help: "1 for a source's phase, 0 for the other phases.",
	}, []string{"tenant", "source", "phase"})
)

// phases are the phases changeport_source_phase reports.
var phases = []string{
	v1alpha1.PhasePending, v1alpha1.PhaseSnapshotting, v1alpha1.PhaseStreaming,
	v1alpha1.PhaseStopped, v1alpha1.PhaseFailed, v1alpha1.PhaseDeleting,
}

func init() {
	metrics.Registry.MustRegister(retainedWAL, safeWAL, slotLost, slotChecked, sourcePhase)
}

func tenantOf(source *v1alpha1.PostgresSource) string {
	return strings.TrimPrefix(source.Namespace, provisioning.NamespacePrefix)
}

// recordSlot records a reading of a source's slot.
func recordSlot(source *v1alpha1.PostgresSource, state cdc.SlotState, at time.Time) {
	tenant := tenantOf(source)
	slotChecked.WithLabelValues(tenant, source.Name).Set(float64(at.Unix()))
	if !state.Exists {
		retainedWAL.DeleteLabelValues(tenant, source.Name)
		safeWAL.DeleteLabelValues(tenant, source.Name)
		slotLost.DeleteLabelValues(tenant, source.Name)
		return
	}
	retainedWAL.WithLabelValues(tenant, source.Name).Set(float64(state.RetainedBytes))
	if state.SafeWALSize != nil {
		safeWAL.WithLabelValues(tenant, source.Name).Set(float64(*state.SafeWALSize))
	} else {
		safeWAL.DeleteLabelValues(tenant, source.Name)
	}
	lost := 0.0
	if state.WALStatus == walStatusLost {
		lost = 1
	}
	slotLost.WithLabelValues(tenant, source.Name).Set(lost)
}

// recordPhase records a source's phase.
func recordPhase(source *v1alpha1.PostgresSource, phase string) {
	tenant := tenantOf(source)
	for _, p := range phases {
		value := 0.0
		if p == phase {
			value = 1
		}
		sourcePhase.WithLabelValues(tenant, source.Name, p).Set(value)
	}
}

// forgetSourceMetrics removes a deleted source's series.
func forgetSourceMetrics(source *v1alpha1.PostgresSource) {
	tenant := tenantOf(source)
	for _, gauge := range []*prometheus.GaugeVec{retainedWAL, safeWAL, slotLost, slotChecked} {
		gauge.DeleteLabelValues(tenant, source.Name)
	}
	sourcePhase.DeletePartialMatch(prometheus.Labels{"tenant": tenant, "source": source.Name})
}
