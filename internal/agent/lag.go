package agent

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
)

// LagAdmin is the part of the Kafka Admin API the lag collector uses;
// *kadm.Client implements it. It needs Describe on groups and topics, and
// never reads a message.
type LagAdmin interface {
	ListGroupsByType(ctx context.Context, types []string, filterStates ...string) (kadm.ListedGroups, error)
	FetchManyOffsets(ctx context.Context, groups ...string) kadm.FetchOffsetsResponses
	ListEndOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	ListOffsetsAfterMilli(ctx context.Context, millisecond int64, topics ...string) (kadm.ListedOffsets, error)
}

var _ LagAdmin = (*kadm.Client)(nil)

// lagGroupTypes are the groups that commit offsets: classic consumer groups
// and KIP-848 consumer groups. Share groups keep no committed offsets.
var lagGroupTypes = []string{"classic", "consumer"}

// Anchors are the ages at which the collector looks up offsets in Kafka's
// time index, newest first. They stay inside the brokers' 24 hours of local
// retention, so lookups never read the tiered storage indexes. Lag older
// than the last anchor is reported as that anchor and flagged as capped.
var Anchors = []time.Duration{
	15 * time.Second, 30 * time.Second, 45 * time.Second, time.Minute, 90 * time.Second,
	2 * time.Minute, 3 * time.Minute, 5 * time.Minute, 450 * time.Second, 10 * time.Minute,
	15 * time.Minute, 20 * time.Minute, 30 * time.Minute, 45 * time.Minute, time.Hour,
	90 * time.Minute, 2 * time.Hour, 3 * time.Hour, 4 * time.Hour, 6 * time.Hour,
	9 * time.Hour, 12 * time.Hour, 18 * time.Hour, 23 * time.Hour,
}

// Age estimates the age of the oldest unconsumed message of a partition: the
// message at committed offset committed, when the partition ends at end.
// anchorOffsets[i] is the first offset produced at or after now-Anchors[i],
// or end when there is none. It interpolates linearly between the two
// anchors around the committed offset, assuming timestamps grow with offsets,
// so the estimate always lies between those two anchors: exact under steady
// traffic, off by at most the gap between them otherwise. It reports capped
// when the message is older than the oldest anchor.
func Age(committed, end int64, now time.Time, anchorOffsets []int64) (age time.Duration, capped bool) {
	if committed >= end {
		return 0, false
	}
	// The newer bound starts at (now, end).
	newerTime, newerOffset := now, end
	for i, offset := range anchorOffsets {
		anchorTime := now.Add(-Anchors[i])
		if offset <= committed {
			// The message was produced at or after this anchor and before the
			// newer bound: interpolate between the two.
			span := newerOffset - offset
			position := time.Duration(0)
			if span > 0 {
				position = time.Duration(float64(newerTime.Sub(anchorTime)) * float64(committed-offset) / float64(span))
			}
			return now.Sub(anchorTime.Add(position)), false
		}
		newerTime, newerOffset = anchorTime, offset
	}
	return Anchors[len(Anchors)-1], true
}

// lagKey identifies one series: a consumer group reading a topic.
type lagKey struct{ group, topic string }

type lagValue struct {
	seconds  float64
	messages int64
	capped   bool
}

// LagCollector measures each consumer group's lag per topic, in messages and
// in seconds, and serves the last measurement as Prometheus metrics. Groups
// and topics that disappear stop being reported at the next measurement.
type LagCollector struct {
	Admin LagAdmin
	// MaxSeries caps the group and topic pairs reported; the rest are
	// dropped and counted.
	MaxSeries int
	Now       func() time.Time

	mu       sync.Mutex
	snapshot map[lagKey]lagValue

	collections *prometheus.CounterVec
	dropped     prometheus.Counter
}

var (
	lagSecondsDesc = prometheus.NewDesc("changeport_consumergroup_lag_seconds",
		"Age of the oldest message the consumer group has not consumed yet, the highest over the topic's partitions.",
		[]string{"consumer_group", "topic"}, nil)
	lagMessagesDesc = prometheus.NewDesc("changeport_consumergroup_lag_messages",
		"Messages the consumer group has not consumed yet, summed over the topic's partitions.",
		[]string{"consumer_group", "topic"}, nil)
	lagCappedDesc = prometheus.NewDesc("changeport_consumergroup_lag_capped",
		"1 when the lag is older than the oldest time the agent looks up (23 hours), so lag_seconds is a lower bound.",
		[]string{"consumer_group", "topic"}, nil)
)

// NewLagCollector returns a collector registered with registry.
func NewLagCollector(admin LagAdmin, registry prometheus.Registerer) *LagCollector {
	c := &LagCollector{
		Admin:     admin,
		MaxSeries: 2000,
		Now:       time.Now,
		collections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "changeport_agent_lag_collections_total",
			Help: "Measurements of consumer group lag, by result (ok or error).",
		}, []string{"result"}),
		dropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "changeport_agent_lag_series_dropped_total",
			Help: "Consumer group and topic pairs left out because of the series cap.",
		}),
	}
	registry.MustRegister(c, c.collections, c.dropped)
	for _, result := range []string{"ok", "error"} {
		c.collections.WithLabelValues(result)
	}
	return c
}

// Describe implements prometheus.Collector.
func (c *LagCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- lagSecondsDesc
	ch <- lagMessagesDesc
	ch <- lagCappedDesc
}

// Collect implements prometheus.Collector with the last measurement.
func (c *LagCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, value := range c.snapshot {
		capped := 0.0
		if value.capped {
			capped = 1
		}
		ch <- prometheus.MustNewConstMetric(lagSecondsDesc, prometheus.GaugeValue, value.seconds, key.group, key.topic)
		ch <- prometheus.MustNewConstMetric(lagMessagesDesc, prometheus.GaugeValue, float64(value.messages), key.group, key.topic)
		ch <- prometheus.MustNewConstMetric(lagCappedDesc, prometheus.GaugeValue, capped, key.group, key.topic)
	}
}

// Measure measures every consumer group's lag and replaces the snapshot. A
// group and topic whose offsets could not all be looked up keep their last
// measurement rather than report a wrong one; the lookup errors are returned.
func (c *LagCollector) Measure(ctx context.Context) error {
	c.mu.Lock()
	previous := c.snapshot
	c.mu.Unlock()
	snapshot, err := c.measure(ctx, previous)
	if snapshot != nil {
		c.mu.Lock()
		c.snapshot = snapshot
		c.mu.Unlock()
	}
	if err != nil {
		c.collections.WithLabelValues("error").Inc()
		return err
	}
	c.collections.WithLabelValues("ok").Inc()
	return nil
}

func (c *LagCollector) measure(ctx context.Context, previous map[lagKey]lagValue) (map[lagKey]lagValue, error) {
	listed, err := c.Admin.ListGroupsByType(ctx, lagGroupTypes)
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}
	groups := listed.Groups()
	snapshot := map[lagKey]lagValue{}
	if len(groups) == 0 {
		return snapshot, nil
	}
	fetched := c.Admin.FetchManyOffsets(ctx, groups...)
	var errs []error
	committed := map[lagKey]map[int32]int64{}
	topicSet := map[string]bool{}
	for _, response := range fetched {
		if response.Err != nil {
			errs = append(errs, fmt.Errorf("fetch offsets of group %s: %w", response.Group, response.Err))
			continue
		}
		response.Fetched.Each(func(o kadm.OffsetResponse) {
			if o.Err != nil || o.At < 0 || internal(o.Topic) {
				return
			}
			key := lagKey{response.Group, o.Topic}
			if committed[key] == nil {
				committed[key] = map[int32]int64{}
			}
			committed[key][o.Partition] = o.At
			topicSet[o.Topic] = true
		})
	}
	if len(committed) == 0 {
		return snapshot, errors.Join(errs...)
	}
	topics := sortedKeys(topicSet)
	ends, err := c.Admin.ListEndOffsets(ctx, topics...)
	if err != nil {
		return nil, errors.Join(append(errs, fmt.Errorf("list end offsets: %w", err))...)
	}

	// Look up the anchors only for topics with a lagging partition.
	lagging := map[string]bool{}
	for key, partitions := range committed {
		for partition, offset := range partitions {
			if end, ok := ends.Lookup(key.topic, partition); ok && end.Err == nil && offset < end.Offset {
				lagging[key.topic] = true
			}
		}
	}
	now := c.Now()
	anchors := make([]kadm.ListedOffsets, len(Anchors))
	if len(lagging) > 0 {
		laggingTopics := sortedKeys(lagging)
		for i, anchor := range Anchors {
			anchors[i], err = c.Admin.ListOffsetsAfterMilli(ctx, now.Add(-anchor).UnixMilli(), laggingTopics...)
			if err != nil {
				return nil, errors.Join(append(errs, fmt.Errorf("list offsets %s ago: %w", anchor, err))...)
			}
		}
	}

	keys := make([]lagKey, 0, len(committed))
	for key := range committed {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b lagKey) int {
		return cmp.Or(cmp.Compare(a.group, b.group), cmp.Compare(a.topic, b.topic))
	})
	for _, key := range keys {
		if len(snapshot) >= c.MaxSeries {
			c.dropped.Inc()
			continue
		}
		value, ok, err := lagOf(key.topic, committed[key], ends, anchors, now)
		if err != nil {
			errs = append(errs, fmt.Errorf("group %s: %w", key.group, err))
			if last, found := previous[key]; found {
				snapshot[key] = last
			}
			continue
		}
		if ok {
			snapshot[key] = value
		}
	}
	return snapshot, errors.Join(errs...)
}

// lagOf combines a group's partitions of one topic: the highest age and the
// sum of messages. It reports nothing when no partition's end is known, and
// an error when an anchor of a lagging partition could not be looked up: a
// missing anchor would read as "no message since", and overstate the lag.
func lagOf(topic string, committed map[int32]int64, ends kadm.ListedOffsets, anchors []kadm.ListedOffsets, now time.Time) (lagValue, bool, error) {
	var value lagValue
	known := false
	for partition, offset := range committed {
		end, ok := ends.Lookup(topic, partition)
		if !ok || end.Err != nil {
			continue
		}
		known = true
		if offset >= end.Offset {
			continue
		}
		value.messages += end.Offset - offset
		anchorOffsets := make([]int64, len(anchors))
		for i, listed := range anchors {
			o, ok := listed.Lookup(topic, partition)
			switch {
			case !ok:
				return lagValue{}, false, fmt.Errorf("no offset of %s-%d %s ago", topic, partition, Anchors[i])
			case o.Err != nil:
				return lagValue{}, false, fmt.Errorf("offset of %s-%d %s ago: %w", topic, partition, Anchors[i], o.Err)
			case o.Offset < 0:
				// No message at or after the anchor: the partition's end.
				anchorOffsets[i] = end.Offset
			default:
				anchorOffsets[i] = o.Offset
			}
		}
		age, capped := Age(offset, end.Offset, now, anchorOffsets)
		value.seconds = max(value.seconds, age.Seconds())
		value.capped = value.capped || capped
	}
	return value, known, nil
}

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
