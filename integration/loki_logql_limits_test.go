//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/integration/client"
	"github.com/grafana/loki/v3/integration/cluster"

	"github.com/grafana/loki/v3/pkg/util/querylimits"
)

// TestLogQLLimits exercises the observable behaviour of the query/validation
// limits over the full HTTP path. Each group boots its own single-binary
// cluster because the groups need different limit flag values; within a group
// every subtest uses a fresh tenant so ingested data never crosses over.
//
// The flag names and defaults were verified against pkg/validation/limits.go:
//   - -store.max-query-length          (max_query_length,            default 721h)
//   - -querier.max-query-series        (max_query_series,            default 500)
//   - -validation.max-entries-limit    (max_entries_limit_per_query, default 5000)
//   - -validation.allow-structured-metadata (allow_structured_metadata, default true)
//   - -querier.query-timeout           (query_timeout,               default 1m)
func TestLogQLLimits(t *testing.T) {
	t.Run("query-and-entry-limits", testQueryAndEntryLimits)
	t.Run("restrictive-limits", testRestrictiveLimits)
}

// testQueryAndEntryLimits covers limits that can share a single cluster:
// max_query_series, max_entries_limit_per_query, the per-request query-limits
// header override, and the (positive) structured-metadata path. per-request
// limits are enabled so the header override takes effect; when no header is
// sent the base limits apply unchanged.
func testQueryAndEntryLimits(t *testing.T) {
	clu := cluster.New(nil, cluster.SchemaWithTSDB, func(c *cluster.Cluster) {
		c.SetSchemaVer("v13")
	})
	defer func() {
		assert.NoError(t, clu.Cleanup())
	}()

	// Integration clusters run in-process and validation.Limits defaults leak
	// across clusters via a package global (validation.SetDefaultLimitsForYAMLUnmarshalling,
	// invoked on every Loki init). Set every validation.Limits field this file
	// exercises explicitly on each cluster so the effective limits never depend
	// on which cluster booted first.
	tAll := clu.AddComponent(
		"all",
		"-target=all",
		"-store.max-query-length=721h",
		"-querier.max-query-series=2",
		"-validation.max-entries-limit=3",
		"-validation.allow-structured-metadata=true",
		"-querier.per-request-limits-enabled=true",
	)
	require.NoError(t, clu.Run())

	baseURL := tAll.HTTPURL()

	// max_query_series: a metric query producing more unique series than the
	// limit (2) must be rejected with HTTP 400 and the specific message.
	t.Run("max-query-series", func(t *testing.T) {
		cli := client.New(randStringRunes(), "", baseURL)
		cli.Now = time.Now()

		// Three distinct streams -> three series after sum by (instance).
		for _, inst := range []string{"a", "b", "c"} {
			require.NoError(t, cli.PushLogLine("line", cli.Now.Add(-time.Minute), nil, map[string]string{"instance": inst}))
		}

		_, err := cli.RunRangeQuery(context.Background(), `sum by (instance) (count_over_time({job="varlog"}[1h]))`)
		require.Error(t, err)
		require.ErrorContains(t, err, "status code 400")
		require.ErrorContains(t, err, "maximum number of series (2) reached for a single query")
	})

	// max_entries_limit_per_query: requesting more entries than the limit is a
	// 400; requesting exactly the limit truncates the returned entries to it.
	t.Run("max-entries-limit", func(t *testing.T) {
		cli := client.New(randStringRunes(), "", baseURL)
		cli.Now = time.Now()

		for i := 0; i < 6; i++ {
			require.NoError(t, cli.PushLogLine("line", cli.Now.Add(-time.Duration(i+1)*time.Second), nil, nil))
		}

		// Default limit param is 100 (> 3) -> rejected.
		_, err := cli.RunRangeQuery(context.Background(), `{job="varlog"}`)
		require.Error(t, err)
		require.ErrorContains(t, err, "status code 400")
		require.ErrorContains(t, err, "max entries limit per query exceeded, limit > max_entries_limit_per_query (100 > 3)")

		// limit=3 is allowed and caps the 6 ingested lines to 3 returned entries.
		resp, err := cli.RunRangeQueryWithLimit(context.Background(), `{job="varlog"}`, 3)
		require.NoError(t, err)
		require.Equal(t, "streams", resp.Data.ResultType)
		assert.Equal(t, 3, countEntries(resp))
	})

	// Per-request query-limits header: tightening maxEntriesLimitPerQuery below
	// the requested limit changes behaviour from success to a 400. This covers
	// a different limit than per_request_limits_test.go (which covers
	// maxQueryLength).
	t.Run("per-request-max-entries-override", func(t *testing.T) {
		tenant := randStringRunes()
		cli := client.New(tenant, "", baseURL)
		cli.Now = time.Now()

		for i := 0; i < 6; i++ {
			require.NoError(t, cli.PushLogLine("line", cli.Now.Add(-time.Duration(i+1)*time.Second), nil, nil))
		}

		// Without the header, the base limit (3) allows limit=3.
		resp, err := cli.RunRangeQueryWithLimit(context.Background(), `{job="varlog"}`, 3)
		require.NoError(t, err)
		assert.Equal(t, 3, countEntries(resp))

		// With the header tightening the cap to 2, the same limit=3 is rejected.
		override := client.InjectHeadersOption(map[string][]string{
			querylimits.HTTPHeaderQueryLimitsKey: {`{"maxEntriesLimitPerQuery": 2}`},
		})
		cliOverride := client.New(tenant, "", baseURL, override)
		cliOverride.Now = cli.Now

		_, err = cliOverride.RunRangeQueryWithLimit(context.Background(), `{job="varlog"}`, 3)
		require.Error(t, err)
		require.ErrorContains(t, err, "status code 400")
		require.ErrorContains(t, err, "max entries limit per query exceeded, limit > max_entries_limit_per_query (3 > 2)")
	})

	// allow_structured_metadata (positive): with the default (enabled), a push
	// carrying structured metadata succeeds and the metadata is queryable.
	t.Run("structured-metadata-allowed", func(t *testing.T) {
		cli := client.New(randStringRunes(), "", baseURL)
		cli.Now = time.Now()

		require.NoError(t, cli.PushLogLine("withmeta", cli.Now.Add(-time.Minute), map[string]string{"trace_id": "abc123"}, nil))

		// limit stays within this cluster's max_entries_limit_per_query (3).
		resp, err := cli.RunRangeQueryWithLimit(context.Background(), `{job="varlog"} | trace_id="abc123"`, 3)
		require.NoError(t, err)
		require.Equal(t, "streams", resp.Data.ResultType)
		assert.Equal(t, 1, countEntries(resp))
	})

	// query_timeout is intentionally skipped: triggering it end-to-end requires
	// a query slow enough to exceed the timeout, which cannot be made
	// deterministic against this tiny in-memory dataset without introducing
	// flakiness. The flag/limit itself (-querier.query-timeout) is covered by
	// unit tests in pkg/validation and pkg/util/querylimits.
	t.Run("query-timeout", func(t *testing.T) {
		t.Skip("cannot deterministically force a query timeout at the integration level without a flaky slow query")
	})
}

// testRestrictiveLimits covers limits that reject requests outright and so are
// isolated on their own cluster: a tiny max_query_length (which would otherwise
// reject the 7d range queries used elsewhere) and disabled structured metadata.
func testRestrictiveLimits(t *testing.T) {
	clu := cluster.New(nil, cluster.SchemaWithTSDB, func(c *cluster.Cluster) {
		c.SetSchemaVer("v13")
	})
	defer func() {
		assert.NoError(t, clu.Cleanup())
	}()

	// See the note in testQueryAndEntryLimits: set every validation.Limits field
	// this file exercises explicitly so leaked defaults from the other cluster
	// cannot affect these assertions. Only max_query_length and
	// allow_structured_metadata are meant to be restrictive here.
	tAll := clu.AddComponent(
		"all",
		"-target=all",
		"-store.max-query-length=1h",
		"-querier.max-query-series=500",
		"-validation.max-entries-limit=5000",
		"-validation.allow-structured-metadata=false",
	)
	require.NoError(t, clu.Run())

	baseURL := tAll.HTTPURL()

	// max_query_length: the default range query spans 7d, far exceeding the 1h
	// limit, so it is rejected with HTTP 400 and the query-length message.
	t.Run("max-query-length", func(t *testing.T) {
		cli := client.New(randStringRunes(), "", baseURL)
		cli.Now = time.Now()

		require.NoError(t, cli.PushLogLine("line", cli.Now.Add(-30*time.Minute), nil, nil))

		_, err := cli.RunRangeQuery(context.Background(), `{job="varlog"}`)
		require.Error(t, err)
		require.ErrorContains(t, err, "status code 400")
		require.ErrorContains(t, err, "the query time range exceeds the limit (query length")
	})

	// allow_structured_metadata=false: a push carrying structured metadata is
	// rejected, while an otherwise-identical push without metadata succeeds.
	t.Run("structured-metadata-disallowed", func(t *testing.T) {
		cli := client.New(randStringRunes(), "", baseURL)
		cli.Now = time.Now()

		// Plain push still works.
		require.NoError(t, cli.PushLogLine("plain", cli.Now.Add(-time.Minute), nil, nil))

		// Push with structured metadata is rejected.
		err := cli.PushLogLine("withmeta", cli.Now.Add(-time.Minute), map[string]string{"trace_id": "abc123"}, nil)
		require.Error(t, err)
		require.ErrorContains(t, err, "status code 400")
		require.ErrorContains(t, err, "structured metadata")
		require.ErrorContains(t, err, "disallowed")
	})
}

// countEntries returns the total number of log entries across all streams in a
// streams response.
func countEntries(resp *client.Response) int {
	var n int
	for _, stream := range resp.Data.Stream {
		n += len(stream.Values)
	}
	return n
}
