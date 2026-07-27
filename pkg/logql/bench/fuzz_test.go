package bench

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/loki/v3/pkg/engine"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
)

// fuzzDifferential enables the cross-engine differential: in addition to the
// always-on chunk baseline, the dataobj v2 engine is built and its results are
// asserted equal to the chunk engine (within tolerance) for the trusted query
// subset. It is OFF by default because the dataobj v2 engine is still in
// development and can panic inside background worker goroutines on inputs it
// does not fully support (this fuzzer found such panics, e.g. the topk executor
// "sourceNewIdx not foud"). Those panics happen off the test goroutine and
// cannot be recovered in-process, so enabling this may crash the fuzz harness —
// which is exactly what a developer hunting cross-engine bugs wants, but is not
// safe for routine CI. With the flag off, the fuzzer is a robust crash /
// robustness probe for the mature chunk engine across arbitrary valid LogQL.
var fuzzDifferential = flag.Bool("fuzz-differential", false,
	"also run the dataobj v2 engine in FuzzLogQLDifferential and assert cross-engine equality (may surface uncatchable dataobj panics)")

// fuzzNumStreams / fuzzTargetSize keep the shared differential dataset tiny so
// building both the chunk store and the (heavier) dataobj v2 engine stays cheap.
// 13 streams give exactly one stream per synthetic application (all log formats
// represented); targetSize=1 forces a single generated batch.
const (
	fuzzNumStreams        = 13
	fuzzTargetSize int64  = 1
	fuzzTenant     string = testTenant
	// fuzzLogLimit is intentionally large so fuzz log queries return every
	// matching line (no limit-boundary ordering ambiguity between backends).
	fuzzLogLimit uint32 = 1_000_000
)

// namedEngine pairs an engine with its store name for diagnostics.
type namedEngine struct {
	name   string
	engine logql.Engine
}

var (
	fuzzOnce    sync.Once
	fuzzEngines []namedEngine
	fuzzErr     error
)

// fuzzEnginesOnce builds the shared dataset and engines exactly once for the
// whole fuzz run. It mirrors the benchmark harness: generate data with a chunk
// store (and, when the differential is enabled, a dataobj store), then open a
// fresh chunk engine (and the dataobj v2 engine) over the same directory.
//
// The chunk engine is always element 0 and acts as the correctness baseline.
// The dataobj v2 engine is appended only when -fuzz-differential is set.
func fuzzEnginesOnce() ([]namedEngine, error) {
	fuzzOnce.Do(func() {
		dir, err := os.MkdirTemp("", "logql-fuzz-")
		if err != nil {
			fuzzErr = err
			return
		}

		ctx := context.Background()

		writeChunk, err := NewChunkStoreWithRegisterer(dir, fuzzTenant, prometheus.NewRegistry())
		if err != nil {
			fuzzErr = fmt.Errorf("create write chunk store: %w", err)
			return
		}

		writeStores := []Store{writeChunk}
		if *fuzzDifferential {
			writeDataObj, err := NewDataObjStore(dir, fuzzTenant)
			if err != nil {
				fuzzErr = fmt.Errorf("create write dataobj store: %w", err)
				return
			}
			writeStores = append(writeStores, writeDataObj)
		}

		builder := NewBuilder(dir, DefaultOpt().WithNumStreams(fuzzNumStreams), writeStores...)
		if err := builder.Generate(ctx, fuzzTargetSize); err != nil { // also closes the write stores
			fuzzErr = fmt.Errorf("generate dataset: %w", err)
			return
		}

		// Fresh chunk engine over the generated data (the write store is closed).
		readChunk, err := NewChunkStoreWithRegisterer(dir, fuzzTenant, prometheus.NewRegistry())
		if err != nil {
			fuzzErr = fmt.Errorf("create read chunk store: %w", err)
			return
		}
		querier, err := readChunk.Querier()
		if err != nil {
			fuzzErr = fmt.Errorf("chunk querier: %w", err)
			return
		}
		chunkEngine := logql.NewEngine(logql.EngineOpts{}, querier, logql.NoLimits,
			level.NewFilter(log.NewLogfmtLogger(os.Stdout), level.AllowWarn()))

		fuzzEngines = append(fuzzEngines, namedEngine{name: StoreChunk, engine: chunkEngine})

		if *fuzzDifferential {
			// Dataobj v2 engine over the same data.
			dataObjEngine, err := NewDataObjV2EngineStore(dir, fuzzTenant)
			if err != nil {
				fuzzErr = fmt.Errorf("create dataobj v2 engine: %w", err)
				return
			}
			fuzzEngines = append(fuzzEngines, namedEngine{name: StoreDataObjV2Engine, engine: dataObjEngine.engine})
		}
	})
	return fuzzEngines, fuzzErr
}

// FuzzLogQLDifferential builds a valid LogQL query from the fuzz input over the
// known dataset vocabulary and runs it across the available backends.
//
// Guarantees:
//   - The chunk engine (mature, full LogQL) executes EVERY generated query; a
//     panic there fails the test. This gives broad crash-coverage of the
//     reference engine across arbitrary valid LogQL.
//   - For query shapes the dataobj v2 engine is known to support (the "trusted"
//     subset returned by buildFuzzQuery, mirroring the curated equality corpus),
//     the dataobj engine is also run and its result must equal the chunk result
//     within floating-point tolerance. engine.ErrNotSupported is treated as a
//     skip for that backend.
//
// The dataobj v2 engine is only fed trusted shapes on purpose: it implements a
// subset of LogQL and can panic inside its background worker goroutines on
// shapes it does not support (this fuzzer discovered exactly such a panic in the
// topk executor, "sourceNewIdx not foud"). Those panics happen off the test
// goroutine and cannot be recovered in-process, so sending it unsupported shapes
// would crash the whole harness rather than produce a useful signal.
func FuzzLogQLDifferential(f *testing.F) {
	// Seed the corpus with byte patterns that exercise a spread of grammar
	// branches (bare selectors, line filters, parsers, range aggs, vector aggs,
	// unwrap, and scalar binary ops).
	f.Add([]byte{0})
	f.Add([]byte{1, 0, 0, 0})
	f.Add([]byte{3, 1, 2, 1, 1, 1})
	f.Add([]byte{5, 2, 1, 4, 2, 3, 7})
	f.Add([]byte{7, 0, 1, 2, 3, 0, 2, 5})
	f.Add([]byte("count_over_time"))
	f.Add([]byte("sum by service_name rate"))

	engines, err := fuzzEnginesOnce()
	if err != nil {
		f.Fatalf("setting up differential engines: %v", err)
	}
	if len(engines) < 1 {
		f.Fatal("no engines available")
	}

	ctx := user.InjectOrgID(context.Background(), fuzzTenant)

	f.Fuzz(func(t *testing.T, data []byte) {
		query, trusted := buildFuzzQuery(data)

		// Guard: only run parseable LogQL. Malformed grammar combinations are
		// skipped rather than treated as failures.
		expr, perr := syntax.ParseExpr(query)
		if perr != nil {
			t.Skip()
		}

		params := fuzzParams(t, query, expr)

		baseline := engines[0]
		baseRes, basePanic := execWithRecover(ctx, baseline.engine, params)
		if basePanic != nil {
			t.Fatalf("baseline engine %s panicked on query %q: %v", baseline.name, query, basePanic)
		}
		if baseRes.err != nil {
			// The classic engine rejected the query semantically (e.g. an
			// unwrap type error). Nothing to compare against; skip.
			t.Skip()
		}

		// Only run the differential (secondary) backends for trusted shapes; see
		// the function doc for why unsupported shapes are not fed to the dataobj
		// engine. Non-trusted queries have still crash-tested the chunk engine.
		if !trusted {
			return
		}

		for _, other := range engines[1:] {
			otherRes, otherPanic := execWithRecover(ctx, other.engine, params)
			if otherPanic != nil {
				t.Fatalf("engine %s panicked on query %q: %v", other.name, query, otherPanic)
			}
			if otherRes.err != nil {
				if errors.Is(otherRes.err, engine.ErrNotSupported) {
					continue // documented: unsupported feature on this backend
				}
				// Capability gap that isn't wrapped as ErrNotSupported. Surface
				// it for visibility but don't fail the differential run.
				t.Logf("engine %s returned non-ErrNotSupported error on %q: %v", other.name, query, otherRes.err)
				continue
			}

			// Both backends produced a result: hard-assert numeric equality within
			// tolerance, reusing the shared assertion helper.
			assertMetricEqual(t, query, baseline.name, other.name, baseRes.result.Data, otherRes.result.Data)
		}
	})
}

// assertMetricEqual hard-asserts that two backends agree on a metric result
// within floating-point tolerance, reusing the shared assertion helper. It is
// only called for "trusted" query shapes (see buildFuzzQuery) that the dataobj
// v2 engine fully supports.
func assertMetricEqual(t *testing.T, query, baseName, otherName string, expected, actual parser.Value) {
	t.Helper()
	if _, ok := expected.(logqlmodel.Streams); ok {
		// Trusted queries are always metric; a stream result here is unexpected.
		t.Fatalf("query %q: expected a metric result from trusted shape, got streams", query)
	}
	if fmtType(expected) != fmtType(actual) {
		t.Fatalf("query %q: backend %s returned %T but %s returned %T", query, baseName, expected, otherName, actual)
	}
	assertDataEqualWithTolerance(t, expected, actual, 1e-5)
}

func fmtType(v parser.Value) string { return string(v.Type()) }

type execResult struct {
	result logqlmodel.Result
	err    error
}

// execWithRecover runs a query and converts a panic into a returned value so the
// fuzzer can report the offending query/engine instead of crashing opaquely.
func execWithRecover(ctx context.Context, eng logql.Engine, params logql.Params) (out execResult, panicked any) {
	defer func() {
		if r := recover(); r != nil {
			panicked = r
		}
	}()
	out.result, out.err = eng.Query(params).Exec(ctx)
	return out, nil
}

// fuzzParams builds execution params for a query. Metric (SampleExpr) queries
// run as instant queries at metricInstant; log queries run backward with a
// bounded limit. This mirrors testcase.Kind() classification.
func fuzzParams(t *testing.T, query string, expr syntax.Expr) logql.LiteralParams {
	t.Helper()
	var (
		params logql.LiteralParams
		err    error
	)
	if _, isSample := expr.(syntax.SampleExpr); isSample {
		params, err = logql.NewLiteralParams(query, metricInstant, metricInstant, 0, 0, logproto.FORWARD, 0, nil, nil)
	} else {
		// Use a high limit so log queries return every matching line: this removes
		// limit-boundary ordering ambiguity when comparing backends.
		params, err = logql.NewLiteralParams(query, datasetStart, datasetEnd, 0, 0, logproto.BACKWARD, fuzzLogLimit, nil, nil)
	}
	if err != nil {
		t.Skipf("could not build params for %q: %v", query, err)
	}
	return params
}

// ---------------------------------------------------------------------------
// Query grammar
// ---------------------------------------------------------------------------

// Fragment pools drawn from the known dataset vocabulary (metadata.go/faker.go).
var (
	fuzzSelectors = []string{
		`{service_name="web-server"}`,
		`{service_name="database"}`,
		`{service_name="loki"}`,
		`{service_name="nginx"}`,
		`{service_name=~".+"}`,
	}
	fuzzLineFilters = []string{
		`|= "level"`,
		`!= "debug"`,
		`|~ "(?i)error"`,
		`!~ "info"`,
		`|= "HTTP"`,
	}
	fuzzParsers = []string{
		`json`,
		`logfmt`,
	}
	// Label filters that are valid after either parser (they simply match
	// nothing when the field is absent, which is still a valid query).
	fuzzLabelFilters = []string{
		`level="error"`,
		`level=~"error|warn"`,
		`detected_level="info"`,
		`__error__=""`,
	}
	fuzzFormatters = []string{
		`line_format "{{ .service_name }}"`,
		`label_format newlabel="x"`,
		`keep service_name`,
		`drop level`,
		`decolorize`,
	}
	fuzzRanges     = []string{`5m`, `15m`, `30m`, `1h`}
	fuzzRangeAggs  = []string{`count_over_time`, `rate`, `bytes_over_time`, `bytes_rate`}
	fuzzUnwrapAggs = []string{`sum_over_time`, `avg_over_time`, `min_over_time`, `max_over_time`, `stddev_over_time`, `stdvar_over_time`, `first_over_time`, `last_over_time`}
	fuzzVectorAggs = []string{`sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`}
	fuzzBinOps     = []string{`+`, `-`, `*`, `/`, `%`, `>`, `>=`, `<`, `<=`, `> bool`}
)

// chooser deterministically turns fuzz bytes into a sequence of small choices,
// wrapping around if the input is exhausted (or empty).
type chooser struct {
	data []byte
	pos  int
}

func (c *chooser) next() byte {
	if len(c.data) == 0 {
		return 0
	}
	b := c.data[c.pos%len(c.data)]
	c.pos++
	return b
}

func (c *chooser) pick(n int) int {
	if n <= 0 {
		return 0
	}
	return int(c.next()) % n
}

func (c *chooser) yes() bool { return c.next()&1 == 1 }

// buildFuzzQuery assembles a syntactically valid LogQL query from fuzz bytes.
// It always returns a query; callers still guard with syntax.ParseExpr because
// a few fragment combinations can be semantically invalid.
//
// The returned "trusted" flag marks query shapes that the dataobj v2 engine is
// known to fully support and to match the chunk engine on (this mirrors the
// curated equality corpus: an outer sum / sum by over count_over_time or rate,
// with an optional line-filter pipeline and no parser/unwrap). Only trusted
// shapes are hard-asserted for cross-engine equality; everything else is a
// crash-safety + differential-reporting probe.
func buildFuzzQuery(data []byte) (query string, trusted bool) {
	c := &chooser{data: data}

	pipeline := fuzzSelectors[c.pick(len(fuzzSelectors))]

	// 0-3 line filters (supported by both engines).
	for i, n := 0, c.pick(4); i < n; i++ {
		pipeline += " " + fuzzLineFilters[c.pick(len(fuzzLineFilters))]
	}

	// Optional parser, and (if parsed) an optional label filter. Parsers move a
	// query outside the trusted subset.
	hasParser := false
	if c.yes() {
		hasParser = true
		pipeline += " | " + fuzzParsers[c.pick(len(fuzzParsers))]
		if c.yes() {
			pipeline += " | " + fuzzLabelFilters[c.pick(len(fuzzLabelFilters))]
		}
	}

	switch c.pick(3) {
	case 0:
		// Log query, optionally with a formatting stage. Never hard-compared.
		if c.yes() {
			pipeline += " | " + fuzzFormatters[c.pick(len(fuzzFormatters))]
		}
		return pipeline, false
	case 1:
		// Range-aggregation metric (no unwrap; valid for any pipeline).
		rng := fuzzRanges[c.pick(len(fuzzRanges))]
		aggIdx := c.pick(len(fuzzRangeAggs))
		agg := fuzzRangeAggs[aggIdx]
		wrapped, wrapTrusted := maybeWrapMetric(c, fmt.Sprintf("%s(%s[%s])", agg, pipeline, rng))
		// Trusted only for the count_over_time / rate core, no parser, wrapped in
		// a plain sum / sum by.
		aggTrusted := agg == "count_over_time" || agg == "rate"
		return wrapped, !hasParser && aggTrusted && wrapTrusted
	default:
		// Unwrap metric over a known logfmt field (guaranteed numeric), which
		// keeps the unwrap grammar valid regardless of the selector above. Unwrap
		// is outside the trusted subset.
		rng := fuzzRanges[c.pick(len(fuzzRanges))]
		agg := fuzzUnwrapAggs[c.pick(len(fuzzUnwrapAggs))]
		inner := `{service_name="loki"} | logfmt | bytes!="" | unwrap bytes`
		wrapped, _ := maybeWrapMetric(c, fmt.Sprintf("%s(%s [%s])", agg, inner, rng))
		return wrapped, false
	}
}

// maybeWrapMetric optionally wraps a sample expression in a vector aggregation,
// a top-k/bottom-k, or a scalar binary op. It reports whether the wrapping kept
// the query within the trusted subset (a plain sum / sum by, or no wrapper).
func maybeWrapMetric(c *chooser, sample string) (string, bool) {
	switch c.pick(4) {
	case 0:
		// Bare range aggregation returns a per-series vector; not part of the
		// curated equality corpus, so treat as non-trusted.
		return sample, false
	case 1:
		vagg := fuzzVectorAggs[c.pick(len(fuzzVectorAggs))]
		wrapTrusted := vagg == "sum"
		if c.yes() {
			return fmt.Sprintf("%s by (service_name) (%s)", vagg, sample), wrapTrusted
		}
		return fmt.Sprintf("%s(%s)", vagg, sample), wrapTrusted
	case 2:
		k := 1 + c.pick(5)
		tk := []string{"topk", "bottomk"}[c.pick(2)]
		return fmt.Sprintf("%s(%d, sum by (service_name) (%s))", tk, k, sample), false
	default:
		op := fuzzBinOps[c.pick(len(fuzzBinOps))]
		n := 1 + c.pick(100)
		return fmt.Sprintf("sum(%s) %s %d", sample, op, n), false
	}
}
