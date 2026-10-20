package log

import (
	"fmt"
	"testing"

	"github.com/grafana/regexp"

	"github.com/stretchr/testify/require"
)

func Test_RegexpFilter(t *testing.T) {
	fixtures := []string{
		"foo", "foobar", "bar", "foobuzz", "buzz", "f", "  ", "fba", "foofoofoo", "b", "foob", "bfoo", "FoO",
		"foo, 世界", allunicode(), "fooÏbar", "", "\n", "\n\n", "foo\nbar", "bar\nfoo", "fo\no", "ſoo", "\u212Aey",
	}
	for _, test := range []struct {
		re string
		// expected is the filter that the regexp becomes. A nil expected means a regexpFilter.
		expected Filterer
	}{
		{"foo", newContainsFilter([]byte("foo"), false)},
		{"(foo|ba)", newOrFilter(newContainsFilter([]byte("foo"), false), newContainsFilter([]byte("ba"), false))},
		{".*foo.*", newContainsFilter([]byte("foo"), false)},
		{"(?:.*foo.*|bar)", newOrFilter(newContainsFilter([]byte("foo"), false), newContainsFilter([]byte("bar"), false))},
		{"f|foo|foobar", newOrFilter(newOrFilter(newContainsFilter([]byte("f"), false), newContainsFilter([]byte("foo"), false)), newContainsFilter([]byte("foobar"), false))},
		{"status=(2.*|3.*)", newPrefixedContainsFilter("status=", []string{"status=2", "status=3"})},
		{"level=(warn|error)", newPrefixedContainsFilter("level=", []string{"level=warn", "level=error"})},
		{"foo(ab|cd)(ab|cd)", newPrefixedContainsFilter("foo", []string{"fooabab", "foocdab", "fooabcd", "foocdcd"})},
		{"[a-z]+foo", nil},
		{".*fo.*o", nil},
		{"^foo", nil},
		{"foo$", nil},
		{"f.o", nil},
		{"foo|f(?i)oo", nil},
		{".*", TrueFilter},
		{".*|.*", TrueFilter},
		{".*||||", TrueFilter},
		{"", TrueFilter},
		{"(foo|.*)", TrueFilter},
		{"foo|", TrueFilter},
		{"x*", TrueFilter},
		{".+", ExistsFilter},
		{".*.+.*", ExistsFilter},
		{"(?i)foo", newContainsFilter([]byte("foo"), true)},
		{"(?i).*foo.*", newContainsFilter([]byte("foo"), true)},
		{"(?i)界", newContainsFilter([]byte("界"), false)},
		{"(?i)ïB", newContainsFilter([]byte("ÏB"), true)},
		{"(?i)foo|fatal|exception", newOrFilter(newOrFilter(newContainsFilter([]byte("foo"), true), newContainsFilter([]byte("fatal"), true)), newContainsFilter([]byte("exception"), true))},
		{"(?i)f|fatal|e.*", newOrFilter(newOrFilter(newContainsFilter([]byte("f"), true), newContainsFilter([]byte("fatal"), true)), newContainsFilter([]byte("e"), true))},
		{"(?i)level=(warn|error)", newOrFilter(newContainsFilter([]byte("level=warn"), true), newContainsFilter([]byte("level=error"), true))},
		{"(?i:level=)(warn|error)", nil},
		{"(?i)foo.*bar", nil},
	} {
		t.Run(test.re, func(t *testing.T) {
			want := regexp.MustCompile(test.re)

			f, err := parseRegexpFilter(test.re, true, false)
			require.NoError(t, err)
			if test.expected != nil {
				require.Equal(t, test.expected, f)
			} else {
				reFilter, ok := f.(regexpFilter)
				require.True(t, ok, "got %T", f)
				require.Equal(t, test.re, reFilter.String())
			}

			notF, err := parseRegexpFilter(test.re, false, false)
			require.NoError(t, err)
			for _, line := range fixtures {
				l := []byte(line)
				require.Equal(t, want.Match(l), f.Filter(l), "regexp %q line %q", test.re, line)
				require.Equal(t, !want.Match(l), notF.Filter(l), "negated regexp %q line %q", test.re, line)
			}
		})
	}
}

// Test_RegexpFilterRegressions holds the regexps that the simplifier which the filters replaced got
// wrong. See https://github.com/grafana/loki/issues/23892.
func Test_RegexpFilterRegressions(t *testing.T) {
	for _, test := range []struct {
		re      string
		isLabel bool
		line    string
		want    bool
	}{
		// An alternate that matches the empty string makes the search match every line.
		{`(error|.*)`, false, "info: all good", true},
		{`error|`, false, "info: all good", true},
		{`(GET|POST|.*)`, false, "info: all good", true},
		// Alternates that share a prefix.
		{`status=(2.*|3.*)`, false, "status=200", true},
		{`(error.*|errno.*)`, false, "error: disk full", true},
		// Two alternations in sequence.
		{`foo(ab|cd)(ab|cd)`, false, "fooab", false},
		{`foo(ab|cd)(ab|cd)`, false, "fooabcd", true},
		// Case-insensitivity that covers only part of the regexp.
		{`(?i:level=)(warn|error)`, false, "level=ERROR", false},
		{`(?i:level=)(warn|error)`, false, "LEVEL=error", true},
		{`(?i:level=)(w.*|e.*)`, false, "level=warn", true},
		// Unicode simple case folding.
		{`(?i)kelvin`, false, "\u212Aelvin", true},
		{`(?i)soo`, false, "ſoo", true},
		{`(?i)xi`, false, "xİ", false},
		// A . does not match a newline.
		{`.+`, false, "\n", false},
		{`foo.bar`, false, "foo\nbar", false},
		{`foo.*bar`, false, "foo\nbar", false},
		{`foo.*|bar`, false, "x\nfoo", true},
		// Label filters match the whole value.
		{`api|api-v2`, true, "api-gateway", false},
		{`api|api-v2`, true, "api-v2", true},
		{`err|error`, true, "xerr", false},
		{`foo.*`, true, "xfoo", false},
		{`.*foo`, true, "foox", false},
		// .* stays a no-op on a label, so it matches a value with a newline too.
		{`.*`, true, "foo\nbar", true},
		{`.*`, true, "", true},
		{`(?s).*`, true, "foo\nbar", true},
		{`(?i)foo`, true, "xFOO", false},
		{`(?i)foo`, true, "FOO", true},
	} {
		name := fmt.Sprintf("%s/%q", test.re, test.line)
		if test.isLabel {
			name = "label/" + name
		}
		t.Run(name, func(t *testing.T) {
			f, err := parseRegexpFilter(test.re, true, test.isLabel)
			require.NoError(t, err)
			require.Equal(t, test.want, f.Filter([]byte(test.line)))

			notF, err := parseRegexpFilter(test.re, false, test.isLabel)
			require.NoError(t, err)
			require.Equal(t, !test.want, notF.Filter([]byte(test.line)))
		})
	}
}

func allunicode() string {
	var b []byte
	for i := 0x00; i < 0x10FFFF; i++ {
		b = append(b, byte(i))
	}
	return string(b)
}

func Test_TrueFilter(t *testing.T) {
	empty := []byte("")
	for _, test := range []struct {
		name       string
		f          Filterer
		expectTrue bool
	}{
		{"empty match", newContainsFilter(empty, false), true},
		{"not empty match", NewNotFilter(newContainsFilter(empty, true)), false},
		{"match", newContainsFilter([]byte("foo"), false), false},
		{"empty match and", NewAndFilter(newContainsFilter(empty, false), newContainsFilter(empty, false)), true},
		{"empty match or", newOrFilter(newContainsFilter(empty, false), newContainsFilter(empty, false)), true},
		{"nil right and", NewAndFilter(newContainsFilter(empty, false), nil), true},
		{"nil left or", newOrFilter(nil, newContainsFilter(empty, false)), true},
		{"nil right and not empty", NewAndFilter(newContainsFilter([]byte("foo"), false), nil), false},
		{"nil left or not empty", newOrFilter(nil, newContainsFilter([]byte("foo"), false)), false},
		{"nil both and", NewAndFilter(nil, nil), false}, // returns nil
		{"nil both or", newOrFilter(nil, nil), false},   // returns nil
		{"empty match and chained", NewAndFilter(newContainsFilter(empty, false), NewAndFilter(newContainsFilter(empty, false), NewAndFilter(newContainsFilter(empty, false), newContainsFilter(empty, false)))), true},
		{"empty match or chained", newOrFilter(newContainsFilter(empty, false), newOrFilter(newContainsFilter(empty, true), newOrFilter(newContainsFilter(empty, false), newContainsFilter(empty, false)))), true},
		{"empty match and", NewNotFilter(NewAndFilter(newContainsFilter(empty, false), newContainsFilter(empty, false))), false},
		{"empty match or", NewNotFilter(newOrFilter(newContainsFilter(empty, false), newContainsFilter(empty, false))), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.expectTrue {
				require.Equal(t, TrueFilter, test.f)
			} else {
				require.NotEqual(t, TrueFilter, test.f)
			}
		})
	}
}

func Benchmark_LineFilter(b *testing.B) {
	b.ReportAllocs()
	logline := `level=bar ts=2020-02-22T14:57:59.398312973Z caller=logging.go:44 traceID=2107b6b551458908 msg="GET /buzz (200) 4.599635ms`
	for _, test := range []struct {
		re string
	}{
		{"foo.*"},
		{"(?i)foo"},
		{".*foo.*"},
		{".*foo"},
		{"foo|bar"},
		{"foo|bar|buzz"},
		{"foo|(bar|buzz)"},
		{"foo|bar.*|buzz"},
		{".*foo.*|bar|uzz"},
		{"((f.*)|foobar.*)|.*buzz"},
		{"(?P<foo>.*foo.*|bar)"},
		{"(\\s|\")+(?i)bar"},
		{"(node:24) buzz*"},
		{"(HTTP/.*\\\"|HEAD|GET) (2..|5..)"},
		{"\"@l\":\"(Warning|Error|Fatal)\""},
		{"(?:)foo|fatal|exception"},
	} {
		benchmarkRegex(b, test.re, logline, true)
		benchmarkRegex(b, test.re, logline, false)
	}
}

// see https://dave.cheney.net/2013/06/30/how-to-write-benchmarks-in-go
// A note on compiler optimizations
var res bool

func benchmarkRegex(b *testing.B, re, line string, match bool) {
	var m bool
	l := []byte(line)
	reg, err := regexp.Compile(re)
	if err != nil {
		b.Fatal(err)
	}
	d := FiltererFunc(func(line []byte) bool { return reg.Match(line) == match })
	s, err := parseRegexpFilter(re, match, false)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.Run(fmt.Sprintf("default_%v_%s", match, re), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m = d.Filter(l)
		}
	})
	b.Run(fmt.Sprintf("simplified_%v_%s", match, re), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m = s.Filter(l)
		}
	})
	res = m
}

func Test_rune(t *testing.T) {
	require.True(t, newContainsFilter([]byte("foo"), true).Filter([]byte("foo")))
}

var cases = []struct {
	name     string
	line     string
	substr   string
	expected bool
}{
	{
		name:     "short_line_no_match",
		line:     "this is a short log line",
		substr:   "missing",
		expected: false,
	},
	{
		name:     "short_line_no_match_special_chars",
		line:     "this contains a \\ character",
		substr:   "|",
		expected: false,
	},
	{
		name:     "short_line_no_match_special_chars_match",
		line:     "this contains a | character",
		substr:   "|",
		expected: true,
	},
	{
		name:     "short_line_with_match",
		line:     "this is a shorT log line",
		substr:   "short",
		expected: true,
	},
	{
		name:     "long_line_no_match",
		line:     "2023-06-14T12:34:56.789Z INFO  [service_name] This is a much longer log line with timestamps, levels and other information that typically appears in production logs. RequestID=123456 UserID=789 Action=GetUser Duration=123ms Status=200",
		substr:   "nonexistent",
		expected: false,
	},
	{
		name:     "long_line_match_start",
		line:     "2023-06-14T12:34:56.789Z INFO  [service_name] This is a much longer log line with timestamps, levels and other information that typically appears in production logs. RequestID=123456 UserID=789 Action=GetUser Duration=123ms Status=200",
		substr:   "2023",
		expected: true,
	},
	{
		name:     "long_line_match_middle",
		line:     "2023-06-14T12:34:56.789Z INFO  [service_name] This is a much longer log line with timestamps, leVelS and other information that typically appears in production logs. RequestID=123456 UserID=789 Action=GetUser Duration=123ms Status=200",
		substr:   "levels",
		expected: true,
	},
	{
		name:     "long_line_match_end",
		line:     "2023-06-14T12:34:56.789Z INFO  [service_name] This is a much longer log line with timestamps, levels and other information that typically appears in production logs. RequestID=123456 UserID=789 Action=GetUser Duration=123ms Status=200",
		substr:   "status",
		expected: true,
	},
	{
		name:     "short_unicode_line_no_match",
		line:     "🌟 Unicode line with emojis 🎉 and special chars ñ é ß",
		substr:   "missing",
		expected: false,
	},
	{
		name:     "short_unicode_line_with_match",
		line:     "🌟 Unicode line with eMojiS 🎉 and special chars ñ é ß",
		substr:   "emojis",
		expected: true,
	},
	{
		name:     "long_unicode_line_no_match",
		line:     "2023-06-14T12:34:56.789Z 🚀 [микросервис] Длинное сообщение с Unicode символами 统一码 が大好き! エラー分析: システムは正常に動作しています。RequestID=123456 状態=良好 Résultat=Succès ß=γ 🎯 τέλος",
		substr:   "nonexistent",
		expected: false,
	},
	{
		name:     "long_unicode_line_match_start",
		line:     "2023-06-14T12:34:56.789Z 🚀[МИКРОСервис] Длинное сообщение с Unicode символами 统一码 が大好き! エラー分析: システムは正常に動作しています。RequestID=123456 状態=良好 Résultat=Succès ß=γ 🎯 τέλος",
		substr:   "микросервис",
		expected: true,
	},
	{
		name:     "long_unicode_line_match_middle",
		line:     "2023-06-14T12:34:56.789Z 🚀 [микросервис] Длинное сообщение с Unicode символами 统一码 が大好き! エラー分析: システムは正常に動作しています。RequestID=123456 状態=良好 Résultat=Succès ß=γ 🎯 τέλος",
		substr:   "unicode",
		expected: true,
	},
	{
		name:     "long_unicode_line_match_end",
		line:     "2023-06-14T12:34:56.789Z 🚀 [микросервис] Длинное сообщение с Unicode символами 统一码 が大好き! エラー分析: システムは正常に動作しています。RequestID=123456 状態=良好 Résultat=Succès ß=γ 🎯 τέλος",
		substr:   "τέλος",
		expected: true,
	},
	{
		name:     "utf8_case_insensitive_match_middle",
		line:     "ΣΑΣ ΓΕΙΑ ΚΟΣΜΕ", // "WORLD HELLO WORLD" in Greek uppercase
		substr:   "γεια",           // "hello" in Greek lowercase
		expected: true,
	},
	{
		name:     "utf8_case_insensitive_no_match",
		line:     "ΣΑΣ ΚΟΣΜΕ", // "WORLD WORLD" in Greek uppercase
		substr:   "γεια",      // "hello" in Greek lowercase
		expected: false,
	},
	{
		name:     "empty_substr",
		line:     "any line",
		substr:   "",
		expected: true,
	},
	{
		name:     "empty_line",
		line:     "",
		substr:   "something",
		expected: false,
	},
	{
		name:     "both_empty",
		line:     "",
		substr:   "",
		expected: true,
	},
	{
		name:     "substr_longer_than_line",
		line:     "short",
		substr:   "longer than line",
		expected: false,
	},
	{
		name:     "invalid_utf8_in_line",
		line:     string([]byte{0xFF, 0xFE, 0xFD}),
		substr:   "test",
		expected: false,
	},
	{
		name:     "partial_utf8_match",
		line:     "Hello 世界", // "Hello World" with CJK characters
		substr:   "世",        // Just "World"
		expected: true,
	},
}

func Test_containsFold(t *testing.T) {
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			line := []byte(c.line)
			substr := []byte(c.substr)
			m := containsFold(line, substr)
			require.Equal(t, c.expected, m, "line: %s substr: %s", c.line, c.substr)
		})
	}
}

func BenchmarkContainsFold(b *testing.B) {
	var m bool
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			line := []byte(c.line)
			substr := []byte(c.substr)
			for i := 0; i < b.N; i++ {
				m = containsFold(line, substr)
			}
			if m != c.expected {
				b.Fatalf("expected %v but got %v", c.expected, m)
			}
		})
	}
	res = m // Avoid compiler optimization
}
