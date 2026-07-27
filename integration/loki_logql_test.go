//go:build integration

package integration

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/integration/client"
	"github.com/grafana/loki/v3/integration/cluster"
)

// TestLogQL exercises the major LogQL feature categories end-to-end against a
// running single-binary Loki: line filters, label matchers, parsers (logfmt and
// json), label filter expressions, line/label formatting, metric queries,
// vector aggregations, unwrap range aggregations and binary operations.
//
// The dataset below is intentionally small and fully enumerated so every
// expected value can be computed by hand:
//
//	service=api  (logfmt): 3 lines, levels info/info/error, statuses 200/201/500
//	service=db   (logfmt): 2 lines, levels info/warn,       statuses 200/404
//	service=json (json):   2 lines, levels error/info,      statuses 503/200
//
// All 7 lines share the stream label job="varlog" (added by PushLogLine).
func TestLogQL(t *testing.T) {
	clu := cluster.New(nil, cluster.SchemaWithTSDB, func(c *cluster.Cluster) {
		c.SetSchemaVer("v13")
	})
	defer func() {
		assert.NoError(t, clu.Cleanup())
	}()

	tAll := clu.AddComponent(
		"all",
		"-target=all",
	)
	require.NoError(t, clu.Run())

	tenantID := randStringRunes()
	cli := client.New(tenantID, "", tAll.HTTPURL())
	now := time.Now()
	cli.Now = now

	// Fully enumerated dataset. Referenced by name in assertions below.
	const (
		apiInfoGet  = `level=info method=GET status=200 latency=10 size=100 msg="ok"`
		apiInfoPost = `level=info method=POST status=201 latency=20 size=200 msg="created"`
		apiError    = `level=error method=GET status=500 latency=30 size=300 msg="boom"`
		dbInfo      = `level=info method=GET status=200 latency=40 size=400 msg="ok"`
		dbWarn      = `level=warn method=GET status=404 latency=50 size=500 msg="missing"`
		jsonError   = `{"level":"error","method":"DELETE","status":503,"latency":60,"msg":"unavailable"}`
		jsonInfo    = `{"level":"info","method":"GET","status":200,"latency":5,"msg":"ok"}`
	)

	dataset := []struct {
		service string
		line    string
	}{
		{"api", apiInfoGet},
		{"api", apiInfoPost},
		{"api", apiError},
		{"db", dbInfo},
		{"db", dbWarn},
		{"json", jsonError},
		{"json", jsonInfo},
	}

	t.Run("ingest", func(t *testing.T) {
		for i, d := range dataset {
			// Stagger timestamps within the last few seconds so they all fall
			// inside both the [1h] metric ranges and the 7d log query window,
			// while remaining strictly in the past relative to the query time.
			ts := now.Add(-time.Duration(len(dataset)-i) * time.Second)
			require.NoError(t, cli.PushLogLine(d.line, ts, nil, map[string]string{"service": d.service}))
		}
	})

	// ---- Log selector + line filters (operate on the raw line) ----

	t.Run("line-filter-contains", func(t *testing.T) {
		lines := queryLines(t, cli, `{service="api"} |= "boom"`)
		assert.ElementsMatch(t, []string{apiError}, lines)
	})

	t.Run("line-filter-not-contains", func(t *testing.T) {
		// A1 and D1 contain msg="ok"; everything else stays.
		lines := queryLines(t, cli, `{service="api"} != "ok"`)
		assert.ElementsMatch(t, []string{apiInfoPost, apiError}, lines)
	})

	t.Run("line-filter-regex-match", func(t *testing.T) {
		// Only the logfmt error line has the literal "level=error"; the JSON
		// line uses "level":"error" and does not match.
		lines := queryLines(t, cli, `{job="varlog"} |~ "level=error"`)
		assert.ElementsMatch(t, []string{apiError}, lines)
	})

	t.Run("line-filter-regex-not-match", func(t *testing.T) {
		lines := queryLines(t, cli, `{service=~"api|db"} !~ "level=(info|warn)"`)
		assert.ElementsMatch(t, []string{apiError}, lines)
	})

	// ---- Label matchers ----

	t.Run("label-matcher-eq", func(t *testing.T) {
		assert.Len(t, queryLines(t, cli, `{service="api"}`), 3)
	})

	t.Run("label-matcher-regex", func(t *testing.T) {
		assert.Len(t, queryLines(t, cli, `{service=~"api|db"}`), 5)
	})

	t.Run("label-matcher-neq", func(t *testing.T) {
		assert.Len(t, queryLines(t, cli, `{job="varlog", service!="api"}`), 4)
	})

	t.Run("label-matcher-multiple", func(t *testing.T) {
		assert.Len(t, queryLines(t, cli, `{job="varlog", service="db"}`), 2)
	})

	// ---- logfmt parser + label filter expressions ----

	t.Run("logfmt-numeric-label-filter-ge", func(t *testing.T) {
		lines := queryLines(t, cli, `{service=~"api|db"} | logfmt | status >= 500`)
		assert.ElementsMatch(t, []string{apiError}, lines)
	})

	t.Run("logfmt-numeric-label-filter-lt", func(t *testing.T) {
		lines := queryLines(t, cli, `{service=~"api|db"} | logfmt | status < 300`)
		assert.ElementsMatch(t, []string{apiInfoGet, apiInfoPost, dbInfo}, lines)
	})

	t.Run("logfmt-string-label-filter", func(t *testing.T) {
		lines := queryLines(t, cli, `{service=~"api|db"} | logfmt | level="info"`)
		assert.ElementsMatch(t, []string{apiInfoGet, apiInfoPost, dbInfo}, lines)
	})

	// ---- json parser + label filter ----

	t.Run("json-string-label-filter", func(t *testing.T) {
		lines := queryLines(t, cli, `{service="json"} | json | level="error"`)
		assert.ElementsMatch(t, []string{jsonError}, lines)
	})

	t.Run("json-numeric-field-as-string", func(t *testing.T) {
		// json renders numbers as their string form, so status="200" matches.
		lines := queryLines(t, cli, `{service="json"} | json | status="200"`)
		assert.ElementsMatch(t, []string{jsonInfo}, lines)
	})

	// ---- pattern / regexp parsers ----

	t.Run("pattern-parser", func(t *testing.T) {
		// The pattern parser extracts lvl and method positionally; line_format
		// then reprints just the captured fields.
		lines := queryLines(t, cli, `{service="api"} | pattern "level=<lvl> method=<method> <_>" | line_format "{{.lvl}}/{{.method}}"`)
		assert.ElementsMatch(t, []string{"info/GET", "info/POST", "error/GET"}, lines)
	})

	t.Run("regexp-parser", func(t *testing.T) {
		// Named capture over the logfmt lines; only status=200 lines survive the
		// label filter (JSON lines are excluded by the selector).
		lines := queryLines(t, cli, `{service=~"api|db"} | regexp "status=(?P<st>[0-9]+)" | st="200"`)
		assert.ElementsMatch(t, []string{apiInfoGet, dbInfo}, lines)
	})

	// ---- line_format / label_format ----

	t.Run("line-format", func(t *testing.T) {
		lines := queryLines(t, cli, `{service="api"} | logfmt | line_format "{{.method}}:{{.status}}"`)
		assert.ElementsMatch(t, []string{"GET:200", "POST:201", "GET:500"}, lines)
	})

	t.Run("label-format", func(t *testing.T) {
		// label_format copies level into a new "lvl" label; collect it per entry.
		resp, err := cli.RunRangeQuery(context.Background(), `{service="api"} | logfmt | label_format lvl=level`)
		require.NoError(t, err)
		require.Equal(t, "streams", resp.Data.ResultType)

		var lvls []string
		for _, stream := range resp.Data.Stream {
			for range stream.Values {
				lvls = append(lvls, stream.Stream["lvl"])
			}
		}
		assert.ElementsMatch(t, []string{"info", "info", "error"}, lvls)
	})

	// ---- Metric queries + vector aggregations (instant, [1h] range) ----

	t.Run("count-over-time", func(t *testing.T) {
		resp := queryMetric(t, cli, `count_over_time({service="api"}[1h])`)
		assert.Equal(t, 3.0, vectorSum(resp))
	})

	t.Run("rate", func(t *testing.T) {
		// rate = entries / range-in-seconds. 3 api entries over a 1h ([1h]=3600s)
		// range gives 3/3600.
		resp := queryMetric(t, cli, `rate({service="api"}[1h])`)
		require.Len(t, resp.Data.Vector, 1)
		assert.InDelta(t, 3.0/3600.0, mustParseFloat(t, resp.Data.Vector[0].Value), 1e-9)
	})

	t.Run("bytes-over-time", func(t *testing.T) {
		// bytes_over_time sums the byte length of the matching lines. The api
		// lines are ASCII, so their UTF-8 byte length equals len().
		resp := queryMetric(t, cli, `bytes_over_time({service="api"}[1h])`)
		wantBytes := float64(len(apiInfoGet) + len(apiInfoPost) + len(apiError))
		assert.Equal(t, wantBytes, vectorSum(resp))
	})

	t.Run("sum-count-over-time", func(t *testing.T) {
		resp := queryMetric(t, cli, `sum(count_over_time({job="varlog"}[1h]))`)
		assert.Equal(t, 7.0, vectorSum(resp))
	})

	t.Run("sum-by-service", func(t *testing.T) {
		resp := queryMetric(t, cli, `sum by (service) (count_over_time({job="varlog"}[1h]))`)
		assert.Equal(t, map[string]float64{"api": 3, "db": 2, "json": 2}, vectorByLabel(resp, "service"))
	})

	t.Run("topk", func(t *testing.T) {
		resp := queryMetric(t, cli, `topk(1, sum by (service) (count_over_time({job="varlog"}[1h])))`)
		require.Len(t, resp.Data.Vector, 1)
		assert.Equal(t, "api", resp.Data.Vector[0].Metric["service"])
		assert.Equal(t, "3", resp.Data.Vector[0].Value)
	})

	t.Run("bottomk", func(t *testing.T) {
		// Counts are api=3, db=2, json=2. bottomk(2) selects the two smallest;
		// the 3/2 boundary is unambiguous so api is always excluded.
		resp := queryMetric(t, cli, `bottomk(2, sum by (service) (count_over_time({job="varlog"}[1h])))`)
		assert.Equal(t, map[string]float64{"db": 2, "json": 2}, vectorByLabel(resp, "service"))
	})

	t.Run("max-min-avg", func(t *testing.T) {
		inner := `sum by (service) (count_over_time({job="varlog"}[1h]))`

		maxResp := queryMetric(t, cli, `max(`+inner+`)`)
		assert.Equal(t, 3.0, vectorSum(maxResp))

		minResp := queryMetric(t, cli, `min(`+inner+`)`)
		assert.Equal(t, 2.0, vectorSum(minResp))

		avgResp := queryMetric(t, cli, `avg(`+inner+`)`)
		require.Len(t, avgResp.Data.Vector, 1)
		assert.InDelta(t, 7.0/3.0, mustParseFloat(t, avgResp.Data.Vector[0].Value), 0.0001)
	})

	t.Run("metric-query-over-filtered-pipeline", func(t *testing.T) {
		// count_over_time counts entries surviving the pipeline. logfmt-parsed
		// status < 300 keeps the three 2xx logfmt lines; JSON lines fail logfmt
		// parsing and lack a numeric status, so they are dropped.
		resp := queryMetric(t, cli, `sum(count_over_time({job="varlog"} | logfmt | status < 300 [1h]))`)
		assert.Equal(t, 3.0, vectorSum(resp))
	})

	// ---- unwrap range aggregations ----

	t.Run("unwrap-sum-over-time", func(t *testing.T) {
		resp := queryMetric(t, cli, `sum(sum_over_time({service="api"} | logfmt | unwrap size [1h]))`)
		assert.Equal(t, 600.0, vectorSum(resp)) // 100 + 200 + 300
	})

	t.Run("unwrap-max-over-time", func(t *testing.T) {
		resp := queryMetric(t, cli, `max(max_over_time({service="db"} | logfmt | unwrap latency [1h]))`)
		assert.Equal(t, 50.0, vectorSum(resp))
	})

	t.Run("unwrap-sum-latency", func(t *testing.T) {
		resp := queryMetric(t, cli, `sum(sum_over_time({service=~"api|db"} | logfmt | unwrap latency [1h]))`)
		assert.Equal(t, 150.0, vectorSum(resp)) // 10 + 20 + 30 + 40 + 50
	})

	// ---- binary operations ----

	t.Run("binop-add", func(t *testing.T) {
		resp := queryMetric(t, cli, `sum(count_over_time({service="api"}[1h])) + sum(count_over_time({service="db"}[1h]))`)
		assert.Equal(t, 5.0, vectorSum(resp)) // 3 + 2
	})

	t.Run("binop-scalar-mul", func(t *testing.T) {
		resp := queryMetric(t, cli, `sum(count_over_time({service="json"}[1h])) * 2`)
		assert.Equal(t, 4.0, vectorSum(resp)) // 2 * 2
	})

	t.Run("binop-comparison-bool", func(t *testing.T) {
		// `> bool` yields 1/0 instead of filtering the series out.
		resp := queryMetric(t, cli, `sum(count_over_time({service="api"}[1h])) > bool 2`)
		assert.Equal(t, 1.0, vectorSum(resp)) // 3 > 2 -> 1
		resp = queryMetric(t, cli, `sum(count_over_time({service="api"}[1h])) > bool 5`)
		assert.Equal(t, 0.0, vectorSum(resp)) // 3 > 5 -> 0
	})

	t.Run("binop-comparison-filter", func(t *testing.T) {
		// Without `bool` the comparison filters series: only api (3) exceeds 2;
		// db and json (2) are dropped (2 > 2 is false).
		resp := queryMetric(t, cli, `sum by (service) (count_over_time({job="varlog"}[1h])) > 2`)
		assert.Equal(t, map[string]float64{"api": 3}, vectorByLabel(resp, "service"))
	})

	// ---- offset modifier ----

	t.Run("offset", func(t *testing.T) {
		// All data sits within the last few seconds. A [10m] range captures it,
		// but the same range offset 30m looks at [~40m, ~30m] ago and is empty.
		resp := queryMetric(t, cli, `count_over_time({service="api"}[10m])`)
		assert.Equal(t, 3.0, vectorSum(resp))

		resp, err := cli.RunQuery(context.Background(), `count_over_time({service="api"}[10m] offset 30m)`)
		require.NoError(t, err)
		require.Equal(t, "vector", resp.Data.ResultType)
		assert.Empty(t, resp.Data.Vector)
	})

	// ---- metadata endpoints (Series / Stats / LabelNames / LabelValues) ----
	// These run before the structured-metadata subtest so they observe only the
	// clean job="varlog" dataset.

	t.Run("label-names", func(t *testing.T) {
		names, err := cli.LabelNames(context.Background())
		require.NoError(t, err)
		assert.Subset(t, names, []string{"job", "service"})
	})

	t.Run("label-values", func(t *testing.T) {
		vals, err := cli.LabelValues(context.Background(), "service")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"api", "db", "json"}, vals)
	})

	t.Run("series", func(t *testing.T) {
		series, err := cli.Series(context.Background(), `{job="varlog"}`)
		require.NoError(t, err)
		require.Len(t, series, 3)
		var svcs []string
		for _, s := range series {
			svcs = append(svcs, s["service"])
		}
		assert.ElementsMatch(t, []string{"api", "db", "json"}, svcs)
	})

	t.Run("stats", func(t *testing.T) {
		// Index stats are served from the shipped TSDB index, so the exact
		// counts depend on flush/shipping timing; assert only that the endpoint
		// responds without error.
		_, err := cli.Stats(context.Background(), `{job="varlog"}`)
		require.NoError(t, err)
	})

	// ---- structured metadata filtering ----

	t.Run("structured-metadata-filter", func(t *testing.T) {
		// Push into a separate stream (job="smd") so the job="varlog" assertions
		// above are unaffected, then filter directly on the structured metadata
		// label (no parser stage required).
		base := now.Add(-30 * time.Minute)
		require.NoError(t, cli.PushLogLine("smd-hit", base, map[string]string{"color": "red"}, map[string]string{"job": "smd"}))
		require.NoError(t, cli.PushLogLine("smd-miss", base.Add(time.Second), map[string]string{"color": "blue"}, map[string]string{"job": "smd"}))

		lines := queryLines(t, cli, `{job="smd"} | color="red"`)
		assert.ElementsMatch(t, []string{"smd-hit"}, lines)
	})
}

// queryLines runs a range log query and returns every returned log line.
func queryLines(t *testing.T, cli *client.Client, query string) []string {
	t.Helper()
	resp, err := cli.RunRangeQuery(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, "streams", resp.Data.ResultType)

	var lines []string
	for _, stream := range resp.Data.Stream {
		for _, val := range stream.Values {
			lines = append(lines, val[1])
		}
	}
	return lines
}

// queryMetric runs an instant metric query and asserts a vector result.
func queryMetric(t *testing.T, cli *client.Client, query string) *client.Response {
	t.Helper()
	resp, err := cli.RunQuery(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, "vector", resp.Data.ResultType)
	return resp
}

// vectorSum sums the values of every series in a vector result.
func vectorSum(resp *client.Response) float64 {
	var total float64
	for _, v := range resp.Data.Vector {
		f, _ := strconv.ParseFloat(v.Value, 64)
		total += f
	}
	return total
}

// vectorByLabel sums vector values grouped by the given label.
func vectorByLabel(resp *client.Response, label string) map[string]float64 {
	out := map[string]float64{}
	for _, v := range resp.Data.Vector {
		f, _ := strconv.ParseFloat(v.Value, 64)
		out[v.Metric[label]] += f
	}
	return out
}

func mustParseFloat(t *testing.T, s string) float64 {
	t.Helper()
	f, err := strconv.ParseFloat(s, 64)
	require.NoError(t, err)
	return f
}
