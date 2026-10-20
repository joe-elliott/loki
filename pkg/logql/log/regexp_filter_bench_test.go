package log

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

// regexpBenchCorpus is ~1000 realistic lines: logfmt, JSON and plain, 80-400 bytes, ~10% error/warn.
func regexpBenchCorpus() [][]byte {
	r := rand.New(rand.NewSource(1))
	levels := []string{"info", "info", "info", "info", "info", "info", "debug", "debug", "warn", "error"}
	msgs := []string{"request completed", "flushing chunk", "connection refused", "read timeout", "cache miss", "query executed", "retrying request", "context canceled"}
	paths := []string{"/api/v1/push", "/loki/api/v1/query_range", "/ready", "/metrics", "/api/prom/tail"}
	statuses := []string{"200", "200", "200", "204", "400", "404", "500", "503"}
	pad := strings.Repeat("abcdefghij", 30)
	var out [][]byte
	for i := 0; i < 1000; i++ {
		lvl, msg, path, st := levels[r.Intn(len(levels))], msgs[r.Intn(len(msgs))], paths[r.Intn(len(paths))], statuses[r.Intn(len(statuses))]
		extra := pad[:r.Intn(len(pad))]
		var l string
		switch i % 3 {
		case 0:
			l = fmt.Sprintf(`level=%s ts=2026-10-20T14:57:59.398312973Z caller=handler.go:%d traceID=%016x method=GET path=%s status=%s duration=%dms msg="%s" extra=%s`, lvl, r.Intn(900), r.Uint64(), path, st, r.Intn(5000), msg, extra)
		case 1:
			l = fmt.Sprintf(`{"@t":"2026-10-20T14:57:59Z","@l":"%s","path":"%s","status":%s,"msg":"%s","extra":"%s"}`, strings.ToUpper(lvl[:1])+lvl[1:], path, st, msg, extra)
		default:
			l = fmt.Sprintf(`2026-10-20 14:57:59 %s [worker-%d] %s %s -> %s %s`, strings.ToUpper(lvl), r.Intn(32), msg, path, st, extra)
		}
		// One line in ten is a multi-line entry, like a stack trace.
		if i%10 == 9 {
			l += "\n\tat handler.go:42\n\tat server.go:" + fmt.Sprint(r.Intn(900))
		}
		out = append(out, []byte(l))
	}
	return out
}

var regexpBenchLineShapes = []struct{ shape, re string }{
	{"literal", `error`},
	{"literal-rare", `query_range`},
	{"case-insensitive", `(?i)error`},
	{"dotstar-wrapped", `.*error.*`},
	{"literal-dotstar", `error.*`},
	{"alt-2", `error|warn`},
	{"alt-4", `error|warn|fatal|panic`},
	{"alt-prefix-factored", `refused|retrying|request`},
	{"alt-case-insensitive", `(?i)error|warn`},
	{"alt-mixed", `.*timeout.*|refused|canceled`},
	{"literal-then-alt", `level=(warn|error)`},
	{"literal-then-alt-3", `status=(200|404|503)`},
	{"ci-literal-then-alt", `(?i)level=(warn|error)`},
	{"nested-captures", `(timeout|(refused|canceled))`},
	{"dotplus", `.+`},
	{"anchored-prefix", `^level=error`},
	{"literal-gap-literal", `error.*timeout`},
	{"ci-literal-gap-literal", `(?i)error.*timeout`},
	{"digits", `status=5\d\d`},
	{"json-alt", `"@l":"(Warning|Error|Fatal)"`},
	{"http-alt", `(HTTP/.*\"|HEAD|GET) (2..|5..)`},
	{"class-plus", `[a-z]+_range`},
}

var regexpBenchLabelShapes = []struct{ shape, re string }{
	{"literal", `error`},
	{"alt", `api|api-v2`},
	{"prefix", `foo.*`},
	{"contains", `.*foo.*`},
	{"case-insensitive", `(?i)error`},
	{"digits", `5..`},
	{"dotplus", `.+`},
}

var regexpBenchSink int

// BenchmarkRegexpLineFilter measures regex line filters of common shapes, per line with Filter and
// over the whole corpus with FilterBatch.
func BenchmarkRegexpLineFilter(b *testing.B) {
	lines := regexpBenchCorpus()
	var (
		data    []byte
		offsets = []int32{0}
	)
	for _, l := range lines {
		data = append(data, l...)
		offsets = append(offsets, int32(len(data)))
	}
	batch := LinesBatch{Data: data, Offsets: offsets}
	dst := make([]int32, 0, len(lines))

	for _, c := range regexpBenchLineShapes {
		f, err := NewFilter(c.re, LineMatchRegexp)
		if err != nil {
			b.Fatal(err)
		}
		b.Run("line/"+c.shape, func(b *testing.B) {
			n := 0
			for i := 0; i < b.N; i++ {
				for _, l := range lines {
					if f.Filter(l) {
						n++
					}
				}
			}
			regexpBenchSink = n
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(lines)), "ns/line")
		})
		b.Run("batch/"+c.shape, func(b *testing.B) {
			n := 0
			for i := 0; i < b.N; i++ {
				n += len(FilterBatch(f, batch, dst[:0]))
			}
			regexpBenchSink = n
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(lines)), "ns/line")
		})
	}
}

// BenchmarkRegexpLabelFilter measures regex label filters of common shapes.
func BenchmarkRegexpLabelFilter(b *testing.B) {
	values := []string{"error", "api", "api-v2", "api-gateway", "foo", "foobar", "xfoox", "barbaz", "ERROR", "500", "503", "200", "", "something-longer-than-the-others"}
	for _, c := range regexpBenchLabelShapes {
		f, err := NewLabelFilter(c.re, labels.MatchRegexp)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(c.shape, func(b *testing.B) {
			n := 0
			for i := 0; i < b.N; i++ {
				for _, v := range values {
					if f.Filter([]byte(v)) {
						n++
					}
				}
			}
			regexpBenchSink = n
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(values)), "ns/value")
		})
	}
}
