package distributor

import (
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/v3/pkg/distributor/shardstreams"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/util/constants"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	lokiring "github.com/grafana/loki/v3/pkg/util/ring"
)

// shardStreams shards the accepted streams, each with its policy's shard-streams config, and
// returns the sharder that holds them. Side effects: rate sharding a stream into more than one
// shard advances its shard rotation, a warning is logged when the push's policies disagree on
// whether sharding is enabled, and time sharding sorts each scope's entries by timestamp in place,
// even when it then does not time-shard.
func (d *Distributor) shardStreams(tenantID string, now time.Time, accepted []acceptedStream) streamSharder {
	sharder := streamSharder{
		rateStore:           d.rateStore,
		shardTracker:        d.shardTracker,
		logger:              d.logger,
		m:                   d.m,
		ingestLimitsEnabled: d.cfg.IngestLimitsEnabled,
		timeShardLen:        d.ingesterCfg.MaxChunkAge / 2,
		tenantID:            tenantID,
		now:                 now,
		streams:             make([]KeyedStream, 0, len(accepted)),
	}
	for _, a := range accepted {
		shardCfg, _ := d.validator.PolicyShardStreams(tenantID, a.policy)
		sharder.shard(a.stream, a.lbs, a.stats.expandedSize, a.policy, shardCfg)
	}
	if sharder.shardStreamsEnabledSeen && sharder.shardStreamsDisabledSeen {
		level.Warn(d.logger).Log(
			"msg", "tenant has shard_streams.enabled true for one policy and false for another in the same push; this is not expected and splits max_global_streams_per_user enforcement between the limits service and EnforceLimits",
			"tenant", tenantID,
			"enabled_policy", sharder.shardStreamsEnabledPolicy,
			"disabled_policy", sharder.shardStreamsDisabledPolicy,
		)
	}
	return sharder
}

// streamSharder shards the streams of one push. shardStreams shards them, and enforceStreamLimits
// shards the live-mode candidates once the limits service has decided their shard counts. streams
// are written once enforceStreamLimits has checked them.
//
// When a tenant asks the limits service for shard counts (see
// shardstreams.Config.LimitsServiceStreamShardingMode), its streams are also collected as
// candidates for one call to the service, after the rate limit. Shadow-mode streams are in streams
// too, sharded by the local rate store, and the service's answer is only compared. Live-mode
// streams are only time-sharded and are not in streams: the service decides their shard count. The
// mode is per tenant, so a push has candidates of one mode at most.
type streamSharder struct {
	// Shared by every push. Rate sharding a stream into more than one shard advances its shard
	// rotation in shardTracker.
	rateStore    RateStore
	shardTracker *ShardTracker
	logger       log.Logger
	m            *metrics

	ingestLimitsEnabled bool
	// timeShardLen is the width of a time shard: half the ingesters' max chunk age.
	timeShardLen time.Duration
	tenantID     string
	now          time.Time

	streams                          []KeyedStream
	shadowCandidates, liveCandidates []limitsServiceShardCandidate

	// The policies seen with sharding enabled and disabled, the first of each, for the warning in
	// shardStreams. A tenant's policies are expected to agree: in live mode, a stream with
	// sharding disabled is limited by EnforceLimits instead of CheckLimitsAndShard, splitting
	// max_global_streams_per_user between two trackers. The warning is logged whenever the ingest
	// limits are enabled, in every mode.
	shardStreamsEnabledSeen, shardStreamsDisabledSeen     bool
	shardStreamsEnabledPolicy, shardStreamsDisabledPolicy string
}

// shard time-shards stream if time sharding applies to it, then shards it by rate.
func (s *streamSharder) shard(stream logproto.InternalStreamAdapter, lbls labels.Labels, pushSize int, policy string, shardStreamsCfg shardstreams.Config) {
	// Backfill streams implement time sharding on the client side (via the
	// constants.BackfillShardLabel), so Loki's own time sharding is disabled for them to avoid
	// exploding stream cardinality. Rate-based sharding still applies.
	if !shardStreamsCfg.TimeShardingEnabled || lbls.Has(constants.BackfillLabel) {
		s.shardByRate(stream, lbls, pushSize, policy, shardStreamsCfg)
		return
	}

	ignoreRecentFrom := s.now.Add(-shardStreamsCfg.TimeShardingIgnoreRecent)
	timeShards, sharded := timeShardNested(stream, lbls, s.timeShardLen, ignoreRecentFrom)
	if !sharded {
		s.shardByRate(stream, lbls, pushSize, policy, shardStreamsCfg)
		return
	}

	for _, ts := range timeShards {
		s.shardByRate(ts.stream, ts.lbls, ts.expandedSize, policy, shardStreamsCfg)
	}
}

// shardByRate shards stream with the local rate store's count, or collects it as a candidate for
// the limits service, or both in shadow mode. See streamSharder.
func (s *streamSharder) shardByRate(stream logproto.InternalStreamAdapter, lbls labels.Labels, pushSize int, policy string, shardStreamsCfg shardstreams.Config) {
	tenantID := s.tenantID

	if s.ingestLimitsEnabled {
		if shardStreamsCfg.Enabled {
			if !s.shardStreamsEnabledSeen {
				s.shardStreamsEnabledSeen = true
				s.shardStreamsEnabledPolicy = policy
			}
		} else if !s.shardStreamsDisabledSeen {
			s.shardStreamsDisabledSeen = true
			s.shardStreamsDisabledPolicy = policy
		}
	}

	if !shardStreamsCfg.Enabled {
		s.streams = append(s.streams, KeyedStream{
			HashKey:        lokiring.TokenFor(tenantID, stream.Labels),
			HashKeyNoShard: stream.Hash,
			Stream:         stream,
			Policy:         policy,
		})
		return
	}

	// The limits service only decides shard counts while the ingest limits
	// are enabled. An unset mode means disabled.
	mode := shardStreamsCfg.LimitsServiceStreamShardingMode
	if !s.ingestLimitsEnabled || mode == shardstreams.LimitsServiceStreamShardingModeDisabled || mode == "" {
		sharded, _ := s.shardStream(stream, lbls, pushSize, policy, shardStreamsCfg)
		s.streams = append(s.streams, sharded...)
		return
	}

	// rateStoreShards is shardCountFor's recommendation rather than the
	// number of streams sharding produces, which shardNested limits to the
	// number of entries and which would look like a difference of opinion
	// for a small push on a hot stream. pushSize is passed on as it is, so
	// that the limits service sees the same size the rate store used.
	logger := log.With(util_log.WithUserID(tenantID, s.logger), "stream", stream.Labels)
	rateStoreRate, _ := s.rateStore.RateFor(tenantID, stream.Hash)
	candidate := limitsServiceShardCandidate{
		stream:          stream,
		labels:          lbls,
		policy:          policy,
		shardStreamsCfg: shardStreamsCfg,
		rateStoreShards: s.shardCountFor(logger, stream, pushSize, shardStreamsCfg),
		rateStoreRate:   rateStoreRate,
		totalSize:       uint64(pushSize),
	}

	// No default: shardstreams.Config.Validate only allows the modes handled
	// here and above.
	switch mode {
	case shardstreams.LimitsServiceStreamShardingModeLive:
		s.liveCandidates = append(s.liveCandidates, candidate)
	case shardstreams.LimitsServiceStreamShardingModeShadow:
		s.streams = append(s.streams, s.shardStreamToCount(stream, lbls, policy, shardStreamsCfg, candidate.rateStoreShards)...)
		s.shadowCandidates = append(s.shadowCandidates, candidate)
	}
}

// shardStream shards (divides) the given stream into N smaller streams, where
// N is the sharding size for the given stream. shardSteam returns the smaller
// streams and their associated keys for hashing to ingesters, along with the
// shard count shardCountFor recommended. That count is not necessarily the
// number of streams returned, as shardNested limits the shards to the number
// of entries.
//
// The number of shards is limited by the number of entries.
func (s *streamSharder) shardStream(stream logproto.InternalStreamAdapter, lbls labels.Labels, pushSize int, policy string, shardStreamsCfg shardstreams.Config) ([]KeyedStream, int) {
	logger := log.With(util_log.WithUserID(s.tenantID, s.logger), "stream", stream.Labels)
	shardCount := s.shardCountFor(logger, stream, pushSize, shardStreamsCfg)
	return s.shardStreamToCount(stream, lbls, policy, shardStreamsCfg, shardCount), shardCount
}

// shardStreamToCount shards stream into shardCount smaller streams, whoever
// decided that count, and returns them with their hash keys. The number of
// streams returned is not necessarily shardCount, as shardNested limits the
// shards to the number of entries.
func (s *streamSharder) shardStreamToCount(stream logproto.InternalStreamAdapter, lbls labels.Labels, policy string, shardStreamsCfg shardstreams.Config, shardCount int) []KeyedStream {
	if shardCount <= 1 {
		return []KeyedStream{{HashKey: lokiring.TokenFor(s.tenantID, stream.Labels), HashKeyNoShard: stream.Hash, Stream: stream, Policy: policy}}
	}

	s.m.streamShardCount.Inc()
	if shardStreamsCfg.LoggingEnabled {
		level.Info(log.With(util_log.WithUserID(s.tenantID, s.logger), "stream", stream.Labels)).Log("msg", "sharding request", "shard_count", shardCount)
	}

	return s.divideEntriesBetweenShards(lbls, shardCount, shardStreamsCfg, stream, policy)
}

func (s *streamSharder) divideEntriesBetweenShards(lbls labels.Labels, totalShards int, shardStreamsCfg shardstreams.Config, stream logproto.InternalStreamAdapter, policy string) []KeyedStream {
	tenantID := s.tenantID
	if totalShards <= 0 {
		level.Error(s.logger).Log("msg", "attempt to create shard with zeroed total shards", "org_id", tenantID, "stream", stream.Labels, "entries_len", stream.EntryCount())
		return nil
	}

	// Only nonempty shards advance the shard rotation.
	startShard := s.shardTracker.LastShardNum(tenantID, stream.Hash)
	shards := shardNested(stream, lbls, totalShards, startShard)

	derivedStreams := make([]KeyedStream, 0, len(shards))
	for i := range shards {
		derivedStreams = append(derivedStreams, KeyedStream{
			HashKey:        lokiring.TokenFor(tenantID, shards[i].Labels),
			HashKeyNoShard: stream.Hash,
			Stream:         shards[i],
			Policy:         policy,
		})

		if shardStreamsCfg.LoggingEnabled {
			level.Info(s.logger).Log("msg", "stream derived from sharding", "src-stream", stream.Labels, "derived-stream", shards[i].Labels)
		}
	}
	s.shardTracker.SetLastShardNum(tenantID, stream.Hash, startShard+len(shards))

	return derivedStreams
}

// shardCountFor returns the right number of shards to be used by the given stream.
//
// It first checks if the number of shards is present in the shard store. If it isn't it will calculate it
// based on the rate stored in the rate store and will store the new evaluated number of shards.
//
// desiredRate is expected to be given in bytes.
func (s *streamSharder) shardCountFor(logger log.Logger, stream logproto.InternalStreamAdapter, pushSize int, streamShardcfg shardstreams.Config) int {
	if streamShardcfg.DesiredRate.Val() <= 0 {
		if streamShardcfg.LoggingEnabled {
			level.Error(logger).Log("msg", "invalid desired rate", "desired_rate", streamShardcfg.DesiredRate.String())
		}
		return 1
	}

	rate, pushRate := s.rateStore.RateFor(s.tenantID, stream.Hash)
	if pushRate == 0 {
		// first push, don't shard until the rate is understood
		return 1
	}

	if pushRate > 1 {
		// High throughput. Let stream sharding do its job and
		// don't attempt to amortize the push size over the
		// real rate
		pushRate = 1
	}

	n, capped := calculateShards(rate, int(float64(pushSize)*pushRate), streamShardcfg.DesiredRate.Val(), streamShardcfg.MaxShardCount)
	if capped {
		s.m.rateStoreMaxShardsLimited.WithLabelValues(s.tenantID).Inc()
	}
	return n
}
