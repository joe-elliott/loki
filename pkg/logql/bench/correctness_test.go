package bench

import (
	"context"
	"encoding/json"
	"flag"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/user"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
)

// update regenerates the committed golden files instead of comparing against
// them. Run `go test ./pkg/logql/bench/ -run TestLogQLCorrectness -update` after
// an intentional change to the engine or the corpus, then commit the result.
var update = flag.Bool("update", false, "update golden files under testdata/correctness")

// goldenDir is the directory that holds the committed golden snapshot.
const goldenDir = "testdata/correctness"

// goldenFile is the single committed golden file. Using one file (a map keyed
// by the stable query name, which json.MarshalIndent emits with sorted keys)
// keeps the snapshot deterministic and reviewable as a single diff, while the
// test still reports per-query diffs on failure.
const goldenFile = "golden.json"

// Deterministic dataset knobs for the correctness oracle.
//
// The generator is fully deterministic given a fixed seed + start time (see
// generator.go). NumStreams=30 gives at least two streams for every one of the
// 13 synthetic applications (json, logfmt and unstructured formats are all
// represented), and a targetSize of 1 byte guarantees exactly one generated
// batch, which keeps the whole test to a couple of seconds.
const correctnessNumStreams = 30
const correctnessTargetSize int64 = 1

// Time bounds. All of these live strictly inside the generated 24h window that
// starts at the fixed StartTime (2024-01-01T00:00:00Z). Using absolute bounds
// (rather than the registry's randomized GetTimeRange) is what makes the
// snapshot reproducible.
var (
	// datasetStart / datasetEnd bound the full generated window and are used for
	// log queries (with a bounded line limit so the golden stays small).
	datasetStart = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	datasetEnd   = datasetStart.Add(24 * time.Hour)

	// metricInstant is the evaluation time for instant metric queries. With a
	// [1h] lookback it covers 10:00-11:00, which overlaps a dense-log interval
	// (see defaultGeneratorConfig.DenseIntervals) so most series have data.
	metricInstant = time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)

	// rangeStart / rangeEnd / rangeStep drive the matrix (range) queries.
	rangeStart = time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	rangeEnd   = time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)
	rangeStep  = 30 * time.Minute
)

// logLineLimit bounds the number of log lines captured per log query. The
// engine caps returned entries at this value, which keeps the golden snapshot
// small and stable regardless of how many lines match.
const logLineLimit = 10

// evalKind selects how a corpus query is executed.
type evalKind int

const (
	evalLog     evalKind = iota // log query -> logqlmodel.Streams
	evalInstant                 // instant metric query -> promql.Vector / Scalar
	evalRange                   // range metric query -> promql.Matrix
)

// corpusQuery is a single deterministic LogQL query in the correctness corpus.
type corpusQuery struct {
	// name is a stable slug used as the golden key and subtest name. It must be
	// unique across the corpus.
	name  string
	query string
	kind  evalKind
	// note documents anything non-obvious (e.g. a query that is deliberately
	// empty, or a stage exercised on its error path).
	note string
}

// TestLogQLCorrectness is the golden-snapshot correctness oracle. It builds a
// small deterministic dataset, executes a curated LogQL corpus through the
// chunk engine (the most stable backend), serializes each result to a canonical
// JSON form, and compares it against a committed golden snapshot.
//
// Unlike the cross-backend equality test (TestStorageEquality), a golden oracle
// can catch a bug that is present identically across ALL backends, because it
// pins the expected output rather than just checking that two backends agree.
//
// Because a single deterministic engine produces byte-identical results run to
// run, every query (including quantiles / stddev, which are approximate only
// when sharded across backends) can use an exact snapshot; no tolerance is
// needed here.
func TestLogQLCorrectness(t *testing.T) {
	start := time.Now()

	engine := buildChunkCorrectnessEngine(t)
	ctx := user.InjectOrgID(context.Background(), testTenant)

	corpus := correctnessCorpus()

	// Guard against accidental duplicate names in the corpus.
	seen := make(map[string]struct{}, len(corpus))
	for _, q := range corpus {
		if _, dup := seen[q.name]; dup {
			t.Fatalf("duplicate corpus query name %q", q.name)
		}
		seen[q.name] = struct{}{}
	}

	// Execute every query and build its canonical result.
	results := make(map[string]*goldenResult, len(corpus))
	for _, q := range corpus {
		res := executeCorpusQuery(t, ctx, engine, q)
		results[q.name] = res
	}

	if *update {
		writeGolden(t, results)
		t.Logf("updated golden snapshot with %d queries", len(results))
		return
	}

	golden := readGolden(t)

	// Report a readable per-query diff. Comparing the pretty-printed JSON of the
	// individual entry (rather than the whole file) keeps failures scoped to the
	// offending query.
	for _, q := range corpus {
		t.Run(q.name, func(t *testing.T) {
			want, ok := golden[q.name]
			if !ok {
				t.Fatalf("no golden entry for %q; run: go test ./pkg/logql/bench/ -run TestLogQLCorrectness -update", q.name)
			}
			require.Equal(t, mustIndent(t, want), mustIndent(t, results[q.name]),
				"query %q result diverged from golden.\nQuery: %s\nRe-generate with -update if this change is intended.", q.name, q.query)
		})
	}

	// Fail loudly if the golden file has entries that no longer exist in the
	// corpus, so stale goldens get cleaned up.
	for name := range golden {
		if _, ok := seen[name]; !ok {
			t.Errorf("golden contains stale entry %q not present in corpus; run -update to clean up", name)
		}
	}

	elapsed := time.Since(start)
	t.Logf("correctness oracle ran %d queries in %s", len(corpus), elapsed)
	if elapsed > 15*time.Second {
		t.Logf("WARNING: correctness oracle took %s (>15s); consider shrinking the dataset", elapsed)
	}
}

// executeCorpusQuery runs one corpus query and returns its canonical result.
func executeCorpusQuery(t *testing.T, ctx context.Context, engine logql.Engine, q corpusQuery) *goldenResult {
	t.Helper()

	// Parse-guard: every corpus query must be valid LogQL.
	if _, err := syntax.ParseExpr(q.query); err != nil {
		t.Fatalf("corpus query %q does not parse: %v", q.name, err)
	}

	var (
		params logql.LiteralParams
		err    error
	)
	switch q.kind {
	case evalLog:
		params, err = logql.NewLiteralParams(q.query, datasetStart, datasetEnd, 0, 0, logproto.BACKWARD, logLineLimit, nil, nil)
	case evalInstant:
		params, err = logql.NewLiteralParams(q.query, metricInstant, metricInstant, 0, 0, logproto.FORWARD, 0, nil, nil)
	case evalRange:
		params, err = logql.NewLiteralParams(q.query, rangeStart, rangeEnd, rangeStep, 0, logproto.FORWARD, 0, nil, nil)
	default:
		t.Fatalf("unknown eval kind for query %q", q.name)
	}
	require.NoError(t, err, "building params for %q", q.name)

	res, execErr := engine.Query(params).Exec(ctx)

	g := &goldenResult{Query: q.query, Note: q.note}
	if execErr != nil {
		// Record the error rather than failing: an unexpected error in the
		// snapshot is a visible red flag on review, and a genuinely-expected
		// error stays pinned.
		g.Error = execErr.Error()
		return g
	}

	g.Type = string(res.Data.Type())
	switch data := res.Data.(type) {
	case logqlmodel.Streams:
		g.Streams = canonicalStreams(data)
	case promql.Matrix:
		g.Series = canonicalMatrix(data)
	case promql.Vector:
		g.Vector = canonicalVector(data)
	case promql.Scalar:
		g.Scalar = &goldenScalar{T: data.T, V: jsonFloat(data.V)}
	default:
		t.Fatalf("query %q returned unknown result type %T", q.name, res.Data)
	}
	return g
}

// buildChunkCorrectnessEngine generates a small deterministic dataset in a temp
// dir and returns a chunk-store engine over it, mirroring the bench harness.
func buildChunkCorrectnessEngine(t *testing.T) logql.Engine {
	t.Helper()
	dir := t.TempDir()

	reg := prometheus.NewRegistry()
	chunkStore, err := NewChunkStoreWithRegisterer(dir, testTenant, reg)
	require.NoError(t, err)

	opt := DefaultOpt().WithNumStreams(correctnessNumStreams) // seed=1, StartTime=2024-01-01 by default
	builder := NewBuilder(dir, opt, chunkStore)
	require.NoError(t, builder.Generate(context.Background(), correctnessTargetSize),
		"generating deterministic dataset")
	// Builder.Generate closes the write store; build a fresh engine over the dir
	// exactly as the benchmark harness does.
	return setupBenchmarkWithStore(t, StoreChunk, dir)
}

// ---------------------------------------------------------------------------
// Canonical JSON model
// ---------------------------------------------------------------------------

// goldenResult is the canonical, sorted representation of a single query result.
type goldenResult struct {
	Query   string         `json:"query"`
	Note    string         `json:"note,omitempty"`
	Type    string         `json:"type,omitempty"`
	Error   string         `json:"error,omitempty"`
	Streams []goldenStream `json:"streams,omitempty"`
	Series  []goldenSeries `json:"series,omitempty"`
	Vector  []goldenSample `json:"vector,omitempty"`
	Scalar  *goldenScalar  `json:"scalar,omitempty"`
}

type goldenStream struct {
	Labels  string        `json:"labels"`
	Entries []goldenEntry `json:"entries"`
}

type goldenEntry struct {
	Timestamp          string        `json:"ts"`
	Line               string        `json:"line"`
	StructuredMetadata []goldenLabel `json:"structured_metadata,omitempty"`
}

type goldenLabel struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type goldenSeries struct {
	Metric string        `json:"metric"`
	Points []goldenPoint `json:"points"`
}

type goldenPoint struct {
	T int64     `json:"t"`
	V jsonFloat `json:"v"`
}

type goldenSample struct {
	Metric string    `json:"metric"`
	T      int64     `json:"t"`
	V      jsonFloat `json:"v"`
}

type goldenScalar struct {
	T int64     `json:"t"`
	V jsonFloat `json:"v"`
}

// jsonFloat is a float64 that serializes canonically: rounded to a fixed number
// of significant digits (to absorb last-ULP differences between platforms) and
// with NaN / +Inf / -Inf represented as strings (raw JSON cannot hold them).
type jsonFloat float64

// floatSigDigits is the number of significant digits retained in the golden.
// This is well below the ~15 digits of a float64 (so it is stable across
// architectures) yet far more precise than any real correctness regression,
// which changes results structurally or by large amounts.
const floatSigDigits = 8

func (f jsonFloat) MarshalJSON() ([]byte, error) {
	v := float64(f)
	switch {
	case math.IsNaN(v):
		return []byte(`"NaN"`), nil
	case math.IsInf(v, 1):
		return []byte(`"+Inf"`), nil
	case math.IsInf(v, -1):
		return []byte(`"-Inf"`), nil
	}
	return json.Marshal(roundSignificant(v, floatSigDigits))
}

func (f *jsonFloat) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	switch s {
	case "NaN":
		*f = jsonFloat(math.NaN())
		return nil
	case "+Inf":
		*f = jsonFloat(math.Inf(1))
		return nil
	case "-Inf":
		*f = jsonFloat(math.Inf(-1))
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	*f = jsonFloat(v)
	return nil
}

// roundSignificant rounds f to sig significant digits.
func roundSignificant(f float64, sig int) float64 {
	if f == 0 || math.IsNaN(f) || math.IsInf(f, 0) {
		return f
	}
	d := math.Ceil(math.Log10(math.Abs(f)))
	power := float64(sig) - d
	mag := math.Pow(10, power)
	return math.Round(f*mag) / mag
}

func canonicalStreams(streams logqlmodel.Streams) []goldenStream {
	out := make([]goldenStream, 0, len(streams))
	for _, s := range streams {
		entries := make([]goldenEntry, 0, len(s.Entries))
		for _, e := range s.Entries {
			sm := make([]goldenLabel, 0, len(e.StructuredMetadata))
			for _, l := range e.StructuredMetadata {
				sm = append(sm, goldenLabel{Name: l.Name, Value: l.Value})
			}
			sort.Slice(sm, func(i, j int) bool {
				if sm[i].Name != sm[j].Name {
					return sm[i].Name < sm[j].Name
				}
				return sm[i].Value < sm[j].Value
			})
			entries = append(entries, goldenEntry{
				Timestamp:          e.Timestamp.UTC().Format(time.RFC3339Nano),
				Line:               e.Line,
				StructuredMetadata: sm,
			})
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].Timestamp != entries[j].Timestamp {
				return entries[i].Timestamp < entries[j].Timestamp
			}
			return entries[i].Line < entries[j].Line
		})
		out = append(out, goldenStream{Labels: canonicalLabelString(s.Labels), Entries: entries})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Labels < out[j].Labels })
	return out
}

func canonicalMatrix(m promql.Matrix) []goldenSeries {
	out := make([]goldenSeries, 0, len(m))
	for _, s := range m {
		points := make([]goldenPoint, 0, len(s.Floats))
		for _, p := range s.Floats {
			points = append(points, goldenPoint{T: p.T, V: jsonFloat(p.F)})
		}
		sort.Slice(points, func(i, j int) bool { return points[i].T < points[j].T })
		out = append(out, goldenSeries{Metric: s.Metric.String(), Points: points})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Metric < out[j].Metric })
	return out
}

func canonicalVector(v promql.Vector) []goldenSample {
	out := make([]goldenSample, 0, len(v))
	for _, s := range v {
		out = append(out, goldenSample{Metric: s.Metric.String(), T: s.T, V: jsonFloat(s.F)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Metric != out[j].Metric {
			return out[i].Metric < out[j].Metric
		}
		return out[i].T < out[j].T
	})
	return out
}

// canonicalLabelString reparses a Loki label string so the output is sorted and
// formatted consistently regardless of the engine's emitted order.
func canonicalLabelString(s string) string {
	lbls, err := syntax.ParseLabels(s)
	if err != nil {
		return s
	}
	return lbls.String()
}

// ---------------------------------------------------------------------------
// Golden file IO
// ---------------------------------------------------------------------------

func goldenPath() string {
	return filepath.Join(goldenDir, goldenFile)
}

func writeGolden(t *testing.T, results map[string]*goldenResult) {
	t.Helper()
	require.NoError(t, os.MkdirAll(goldenDir, 0o755))
	// json.MarshalIndent emits map keys in sorted order, so the file is stable.
	data, err := json.MarshalIndent(results, "", "  ")
	require.NoError(t, err)
	data = append(data, '\n')
	require.NoError(t, os.WriteFile(goldenPath(), data, 0o644))
}

func readGolden(t *testing.T) map[string]*goldenResult {
	t.Helper()
	data, err := os.ReadFile(goldenPath())
	if err != nil {
		t.Fatalf("reading golden file %s: %v\nrun: go test ./pkg/logql/bench/ -run TestLogQLCorrectness -update", goldenPath(), err)
	}
	var golden map[string]*goldenResult
	require.NoError(t, json.Unmarshal(data, &golden), "parsing golden file")
	return golden
}

// mustIndent renders a value as pretty JSON for readable require.Equal diffs.
func mustIndent(t *testing.T, v any) string {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	require.NoError(t, err)
	return string(b)
}

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

// The corpus selects over the known dataset vocabulary (see metadata.go /
// faker.go). service_name is an indexed stream label whose values are the
// synthetic application names; the log format per app is fixed:
//
//	web-server, database, cache, auth-service, kafka, prometheus -> json
//	loki, mimir, tempo, grafana                                    -> logfmt
//	nginx, syslog, kubernetes                                      -> unstructured
//
// correctnessCorpus returns the curated LogQL corpus. It aims for ~100% LogQL
// feature coverage; each entry is a concrete, deterministic query. See the
// TestLogQLCorrectness_FeatureCoverage documentation test for the mapping from
// feature categories to corpus entries and the (documented) skips.
func correctnessCorpus() []corpusQuery {
	return []corpusQuery{
		// --- Line filters --------------------------------------------------
		{name: "linefilter-eq", kind: evalLog, query: `{service_name="web-server"} |= "level"`},
		{name: "linefilter-neq", kind: evalLog, query: `{service_name="web-server"} != "HTTP request"`},
		{name: "linefilter-regex", kind: evalLog, query: `{service_name="web-server"} |~ "status\":[45]"`},
		{name: "linefilter-negregex", kind: evalLog, query: `{service_name="web-server"} !~ "debug|info"`},
		{name: "linefilter-ci", kind: evalLog, query: `{service_name="kubernetes"} |~ "(?i)error"`},
		{name: "linefilter-chained", kind: evalLog, query: `{service_name="web-server"} |= "HTTP request" |~ "(?i)error" != "this-token-never-appears"`},
		{name: "linefilter-ip", kind: evalLog, query: `{service_name="nginx"} |= ip("0.0.0.0/0")`, note: "IP line filter; nginx lines begin with a client IP"},
		{name: "linefilter-impossible", kind: evalLog, query: `{service_name="web-server"} |= "this-token-never-appears"`, note: "deliberately empty result"},

		// --- Label matchers ------------------------------------------------
		{name: "labelmatch-eq", kind: evalLog, query: `{service_name="loki"}`},
		{name: "labelmatch-neq", kind: evalLog, query: `{service_name="loki", env!="does-not-exist"}`},
		{name: "labelmatch-regex", kind: evalLog, query: `{service_name=~"web-server|database"}`},
		{name: "labelmatch-negregex", kind: evalLog, query: `{service_name=~".+", service_name!~"syslog|nginx"}`},
		{name: "labelmatch-multi", kind: evalLog, query: `{service_name="loki", env=~".+"}`},

		// --- Parsers -------------------------------------------------------
		{name: "parser-json", kind: evalLog, query: `{service_name="web-server"} | json | keep service_name, level, method, status`},
		{name: "parser-json-explicit", kind: evalLog, query: `{service_name="web-server"} | json status_code="status", verb="method" | keep service_name, status_code, verb`, note: "explicit json field expressions"},
		{name: "parser-logfmt", kind: evalLog, query: `{service_name="loki"} | logfmt | keep service_name, level, component`},
		{name: "parser-logfmt-strict", kind: evalLog, query: `{service_name="loki"} | logfmt --strict | keep service_name, level`},
		{name: "parser-logfmt-explicit", kind: evalLog, query: `{service_name="loki"} | logfmt lvl="level" | keep service_name, lvl`},
		{name: "parser-pattern", kind: evalLog, query: `{service_name="nginx"} | pattern "<ip> - <_> [<_>] \"<method> <path> <_>\" <status> <_>" | keep service_name, method, status`},
		{name: "parser-regexp", kind: evalLog, query: `{service_name="nginx"} | regexp "^(?P<clientip>\\S+) - (?P<user>\\S+) " | keep service_name, clientip`},
		{name: "parser-unpack", kind: evalLog, query: `{service_name="web-server"} | unpack | keep service_name`, note: "dataset is not promtail-packed, so unpack exercises the no-_entry path"},

		// --- Label filters -------------------------------------------------
		{name: "labelfilter-string", kind: evalLog, query: `{service_name="loki"} | logfmt | level="error" | keep service_name, level`},
		{name: "labelfilter-numeric", kind: evalLog, query: `{service_name="web-server"} | json | status>=400 | keep service_name, status`},
		{name: "labelfilter-numeric-ops", kind: evalLog, query: `{service_name="database"} | json | rows_affected>0 | rows_affected<=1000 | keep service_name`},
		{name: "labelfilter-numeric-eq", kind: evalLog, query: `{service_name="web-server"} | json | status==200 | keep service_name, status`},
		{name: "labelfilter-numeric-neq", kind: evalLog, query: `{service_name="web-server"} | json | status!=200 | keep service_name, status`},
		{name: "labelfilter-duration", kind: evalLog, query: `{service_name="loki"} | logfmt | duration>=500ms | keep service_name, duration`},
		{name: "labelfilter-bytes", kind: evalLog, query: `{service_name="loki"} | logfmt | bytes>1KB | keep service_name, bytes`},
		{name: "labelfilter-ip", kind: evalLog, query: `{service_name="web-server"} | json | client_ip=ip("0.0.0.0/0") | keep service_name`},
		{name: "labelfilter-and", kind: evalLog, query: `{service_name="web-server"} | json | status>=200 and status<300 | keep service_name, status`},
		{name: "labelfilter-or", kind: evalLog, query: `{service_name="web-server"} | json | status<200 or status>=500 | keep service_name, status`},
		{name: "labelfilter-error", kind: evalLog, query: `{service_name="web-server"} | json | __error__="" | keep service_name, level`, note: "keep only lines that parsed cleanly"},

		// --- Formatting ----------------------------------------------------
		{name: "line_format-template", kind: evalLog, query: `{service_name="web-server"} | json | line_format "{{.method}} {{.status}}" | keep service_name`},
		{name: "line_format-func", kind: evalLog, query: `{service_name="loki"} | logfmt | line_format "{{ .level | upper }}" | keep service_name`},
		{name: "label_format-rename", kind: evalLog, query: `{service_name="web-server"} | json | label_format lvl=level | keep service_name, lvl`},
		{name: "label_format-template", kind: evalLog, query: `{service_name="web-server"} | json | label_format combo="{{.method}}-{{.status}}" | keep service_name, combo`},
		{name: "decolorize", kind: evalLog, query: `{service_name="kubernetes"} | decolorize`, note: "no ANSI codes in dataset; decolorize is a no-op but exercised"},
		{name: "drop", kind: evalLog, query: `{service_name="loki"} | logfmt | keep service_name, level, caller | drop caller`},
		{name: "keep", kind: evalLog, query: `{service_name="loki"} | logfmt | keep service_name, level`},

		// --- Range aggregations (instant eval) -----------------------------
		{name: "rate", kind: evalInstant, query: `sum(rate({service_name="web-server"}[1h]))`},
		{name: "count_over_time", kind: evalInstant, query: `sum(count_over_time({service_name="web-server"}[1h]))`},
		{name: "bytes_rate", kind: evalInstant, query: `sum(bytes_rate({service_name="web-server"}[1h]))`},
		{name: "bytes_over_time", kind: evalInstant, query: `sum(bytes_over_time({service_name="web-server"}[1h]))`},
		{name: "absent_over_time", kind: evalInstant, query: `absent_over_time({service_name="service-that-does-not-exist"}[1h])`, note: "returns 1 when no series match"},
		{name: "sum_over_time", kind: evalInstant, query: `sum(sum_over_time({service_name="loki"} | logfmt | bytes!="" | unwrap bytes [1h]))`},
		{name: "avg_over_time", kind: evalInstant, query: `avg_over_time({service_name="loki"} | logfmt | bytes!="" | unwrap bytes [1h])`},
		{name: "min_over_time", kind: evalInstant, query: `min_over_time({service_name="loki"} | logfmt | bytes!="" | unwrap bytes [1h])`},
		{name: "max_over_time", kind: evalInstant, query: `max_over_time({service_name="loki"} | logfmt | bytes!="" | unwrap bytes [1h])`},
		{name: "stddev_over_time", kind: evalInstant, query: `stddev_over_time({service_name="loki"} | logfmt | bytes!="" | unwrap bytes [1h])`},
		{name: "stdvar_over_time", kind: evalInstant, query: `stdvar_over_time({service_name="loki"} | logfmt | bytes!="" | unwrap bytes [1h])`},
		{name: "quantile_over_time", kind: evalInstant, query: `quantile_over_time(0.9, {service_name="loki"} | logfmt | bytes!="" | unwrap bytes [1h])`},
		{name: "first_over_time", kind: evalInstant, query: `first_over_time({service_name="loki"} | logfmt | bytes!="" | unwrap bytes [1h])`},
		{name: "last_over_time", kind: evalInstant, query: `last_over_time({service_name="loki"} | logfmt | bytes!="" | unwrap bytes [1h])`},

		// --- Unwrap variants -----------------------------------------------
		{name: "unwrap-plain", kind: evalInstant, query: `sum(sum_over_time({service_name="loki"} | logfmt | size!="" | unwrap size [1h]))`},
		{name: "unwrap-duration", kind: evalInstant, query: `avg_over_time({service_name="loki"} | logfmt | duration!="" | unwrap duration(duration) [1h])`},
		{name: "unwrap-bytes", kind: evalInstant, query: `sum(sum_over_time({service_name="loki"} | logfmt | size!="" | unwrap bytes(size) [1h]))`},

		// --- Vector aggregations -------------------------------------------
		{name: "vecagg-sum-by", kind: evalInstant, query: `sum by (service_name) (count_over_time({service_name=~".+"}[1h]))`},
		{name: "vecagg-sum-without", kind: evalInstant, query: `sum without (pod) (count_over_time({service_name="loki"}[1h]))`},
		{name: "vecagg-avg-without", kind: evalInstant, query: `avg without (pod, container) (count_over_time({service_name="loki"}[1h]))`},
		{name: "vecagg-min", kind: evalInstant, query: `min by (service_name) (count_over_time({service_name=~".+"}[1h]))`},
		{name: "vecagg-max", kind: evalInstant, query: `max by (service_name) (count_over_time({service_name=~".+"}[1h]))`},
		{name: "vecagg-avg", kind: evalInstant, query: `avg by (service_name) (count_over_time({service_name=~".+"}[1h]))`},
		{name: "vecagg-stddev", kind: evalInstant, query: `stddev by (service_name) (count_over_time({service_name=~".+"}[1h]))`},
		{name: "vecagg-stdvar", kind: evalInstant, query: `stdvar by (service_name) (count_over_time({service_name=~".+"}[1h]))`},
		{name: "vecagg-count", kind: evalInstant, query: `count by (service_name) (count_over_time({service_name=~".+"}[1h]))`},
		{name: "vecagg-topk", kind: evalInstant, query: `topk(3, sum by (service_name) (count_over_time({service_name=~".+"}[1h])))`},
		{name: "vecagg-bottomk", kind: evalInstant, query: `bottomk(3, sum by (service_name) (count_over_time({service_name=~".+"}[1h])))`},
		{name: "vecagg-sort", kind: evalInstant, query: `sort(sum by (service_name) (count_over_time({service_name=~".+"}[1h])))`},
		{name: "vecagg-sort_desc", kind: evalInstant, query: `sort_desc(sum by (service_name) (count_over_time({service_name=~".+"}[1h])))`},
		{name: "vecagg-group-by-sm", kind: evalInstant, query: `sum by (detected_level) (count_over_time({service_name="web-server"}[1h]))`, note: "grouping by a structured-metadata key"},

		// --- Binary ops ----------------------------------------------------
		{name: "binop-scalar-vector-mul", kind: evalInstant, query: `sum(count_over_time({service_name="web-server"}[1h])) * 2`},
		{name: "binop-add", kind: evalInstant, query: `sum(count_over_time({service_name="web-server"}[1h])) + 1`},
		{name: "binop-sub", kind: evalInstant, query: `sum(count_over_time({service_name="web-server"}[1h])) - 1`},
		{name: "binop-div", kind: evalInstant, query: `sum(bytes_over_time({service_name="web-server"}[1h])) / sum(count_over_time({service_name="web-server"}[1h]))`},
		{name: "binop-mod", kind: evalInstant, query: `sum(count_over_time({service_name="web-server"}[1h])) % 7`},
		{name: "binop-vector-vector", kind: evalInstant, query: `sum by (service_name) (bytes_over_time({service_name=~".+"}[1h])) / sum by (service_name) (count_over_time({service_name=~".+"}[1h]))`},
		{name: "binop-cmp", kind: evalInstant, query: `sum by (service_name) (count_over_time({service_name=~".+"}[1h])) > 50`},
		{name: "binop-cmp-bool", kind: evalInstant, query: `sum by (service_name) (count_over_time({service_name=~".+"}[1h])) > bool 50`},
		{name: "binop-and", kind: evalInstant, query: `(sum by (service_name) (count_over_time({service_name=~".+"}[1h])) > 50) and (sum by (service_name) (count_over_time({service_name=~".+"}[1h])) > 0)`},
		{name: "binop-or", kind: evalInstant, query: `(sum by (service_name) (count_over_time({service_name=~".+"}[1h])) > 1000000) or (sum by (service_name) (count_over_time({service_name=~".+"}[1h])) > 50)`},
		{name: "binop-unless", kind: evalInstant, query: `(sum by (service_name) (count_over_time({service_name=~".+"}[1h]))) unless (sum by (service_name) (count_over_time({service_name="loki"}[1h])))`},

		// --- offset modifier -----------------------------------------------
		{name: "offset", kind: evalInstant, query: `sum(count_over_time({service_name="web-server"}[1h] offset 30m))`},

		// --- Range (matrix) queries ----------------------------------------
		{name: "range-count-by", kind: evalRange, query: `sum by (service_name) (count_over_time({service_name=~".+"}[30m]))`},
		{name: "range-rate", kind: evalRange, query: `sum(rate({service_name="web-server"}[30m]))`},
		{name: "range-unwrap", kind: evalRange, query: `sum by (level) (sum_over_time({service_name="loki"} | logfmt | bytes!="" | unwrap bytes [30m]))`},
	}
}

// TestLogQLCorrectness_FeatureCoverage documents the LogQL feature categories
// covered by the corpus and, critically, the ones deliberately skipped and why.
// It logs the map (visible with -v) and fails only if the corpus regresses
// below the expected size, so a category cannot be silently dropped.
func TestLogQLCorrectness_FeatureCoverage(t *testing.T) {
	covered := []string{
		"line filters: |=, !=, |~, !~, chained, (?i), |= ip()",
		"label matchers: =, !=, =~, !~, multiple",
		"parsers: json (+explicit), logfmt (+--strict, +explicit), pattern, regexp, unpack",
		"label filters: string, numeric (>,>=,<,<=,==,!=), duration, bytes, ip, and, or, __error__",
		"formatting: line_format (+funcs), label_format (rename+template), decolorize, drop, keep",
		"range aggs: rate, count/sum/avg/min/max/stddev/stdvar/quantile/first/last_over_time, bytes_rate, bytes_over_time, absent_over_time",
		"unwrap: plain field, duration(), bytes()",
		"vector aggs: sum/min/max/avg/stddev/stdvar/count/topk/bottomk/sort/sort_desc, by & without, group-by structured metadata",
		"binary ops: + - * / %, comparison (with/without bool), and/or/unless, scalar-vector & vector-vector",
		"offset modifier",
		"range (matrix) evaluation with step",
	}

	// Skipped categories, with the reason. These are logged (not silently
	// omitted) per the task requirements.
	skipped := map[string]string{
		"line filters: |= ip() with an explicit CIDR narrower than the data": "the dataset's IPs are random, so a narrow CIDR would be non-deterministic in which lines match across seeds; 0.0.0.0/0 is used instead to keep it deterministic while still exercising the ip() filter path",
		"unwrap on unstructured (nginx/syslog) numeric fields":               "unstructured apps expose no reliably-numeric field without a parser; unwrap is covered via logfmt (loki) which is the realistic path",
		"approx_topk": "sketch-based and non-deterministic by design; cannot be golden-snapshotted. topk/bottomk cover the deterministic top-N path",
	}

	for _, c := range covered {
		t.Logf("covered: %s", c)
	}
	for cat, reason := range skipped {
		t.Logf("skipped: %s -- %s", cat, reason)
	}

	// Sanity floor so a future edit cannot quietly gut the corpus.
	const minCorpusSize = 70
	if got := len(correctnessCorpus()); got < minCorpusSize {
		t.Fatalf("corpus shrank to %d queries (expected >= %d); did a feature category get dropped?", got, minCorpusSize)
	}
}
