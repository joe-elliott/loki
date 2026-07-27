// This file holds the modern native Go fuzz tests (testing.F based). The legacy
// dvyukov-style corpus fuzzer in fuzz.go / fuzz_test.go is guarded by
// //go:build gofuzz and declares its own FuzzParseExpr(data []byte) int. To
// avoid a redeclaration collision under -tags=gofuzz (where the legacy files are
// compiled), the native tests are excluded from that build. Native fuzzing runs
// under the normal build with no tags.
//go:build !gofuzz

package syntax

import (
	"math"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/logql/log"
)

// fuzzEdgeCaseSeeds are nasty inputs that exercise the lexer/parser boundaries.
// They complement the valid queries pulled from ParseTestCases below.
var fuzzEdgeCaseSeeds = []string{
	"",
	" ",
	"\n\t ",
	"{",
	"}",
	"{}",
	"()",
	`{foo="bar"`,
	`foo="bar"}`,
	`{foo=~"("}`,                   // invalid regex
	`{foo="bar"} |= "unterminated`, // unterminated string
	`{` + strings.Repeat("a", 1000) + `="b"}`,                            // long label name
	strings.Repeat("(", 200),                                             // deeply unbalanced parens
	`{foo="bar"}` + strings.Repeat(" |= \"x\"", 500),                     // huge repetition of filters
	"{foo=\"\xff\xfe\"}",                                                 // invalid utf-8
	`{foo="ünïcödé 日本語 🎉"}`,                                              // unicode
	`sum(rate({app="foo"}[5m])) by (` + strings.Repeat("l,", 100) + "l)", // many groupings
	`{foo="bar"} | json | line_format "{{.foo}}"`,
	`1 + 2 * 3 - 4 / 5`,
	`{foo="bar"}[5m]`,
	`quantile_over_time(0.99, {foo="bar"} | unwrap bytes [5m]) by (namespace)`,
}

// addRoundTripSeeds seeds a fuzz function with every valid query from
// ParseTestCases (those with a nil expected error) plus the edge-case seeds.
// Reusing the existing table guarantees the seed corpus tracks the parser's
// real feature surface (selectors, filters, parsers, metric queries,
// aggregations, binary ops, unwrap, line/label_format, ...) without
// duplicating hundreds of query strings by hand.
func addRoundTripSeeds(f *testing.F) {
	f.Helper()
	for _, tc := range ParseTestCases {
		if tc.err == nil {
			f.Add(tc.in)
		}
	}
	for _, s := range fuzzEdgeCaseSeeds {
		f.Add(s)
	}
}

// FuzzParseExpr is the crash-safety net: no input, however malformed, may make
// ParseExpr panic. A returned error is a perfectly acceptable outcome; only a
// panic (caught by the native fuzzing engine as a failure) is a bug. We do not
// recover here on purpose so that any panic surfaces loudly.
func FuzzParseExpr(f *testing.F) {
	addRoundTripSeeds(f)

	f.Fuzz(func(_ *testing.T, s string) {
		// The only assertion is "does not panic". Errors are expected and fine.
		_, _ = ParseExpr(s)
	})
}

// FuzzParseExprRoundTrip is the important invariant fuzzer. For any input that
// parses successfully it asserts:
//
//  1. String() of a parsed expression always re-parses without error. A
//     failure here means String() emitted something the parser cannot read
//     back - a real bug.
//  2. String() is a fixpoint: re-parsing and re-stringifying yields exactly the
//     same text. This is the reliable structural-stability invariant; it
//     catches formatting drift and lossy round-trips.
//
// We deliberately assert the String() fixpoint rather than a deep structural
// equality of e1 vs e2: several AST nodes carry unexported state (cached
// pipelines, lazily-built stages, parse-time errors) that is not meaningfully
// comparable across a parse boundary, whereas the canonical String() form is.
func FuzzParseExprRoundTrip(f *testing.F) {
	addRoundTripSeeds(f)

	f.Fuzz(func(t *testing.T, s string) {
		e1, err := ParseExpr(s)
		if err != nil {
			// Not a valid query; nothing to round-trip. Not interesting.
			return
		}

		// The parser has a handful of documented round-trip defects where
		// String() emits text that either does not re-parse or re-parses to a
		// different expression. We detect those structurally and skip ONLY
		// those specific cases (as narrowly as possible) so the fuzzer keeps
		// exploring the rest of the input space. Any *other* round-trip failure
		// is asserted below and fails loudly - it is not swallowed.
		if bug := knownRoundTripBug(e1); bug != "" {
			t.Skipf("KNOWN BUG (%s)\n input   : %q\n String(): %q", bug, s, e1.String())
		}

		s1 := e1.String()

		e2, err := ParseExpr(s1)
		require.NoErrorf(t, err, "String() produced an unparseable expression\n input   : %q\n String(): %q", s, s1)

		s2 := e2.String()
		require.Equalf(t, s1, s2, "String() is not a fixpoint\n input: %q\n s1   : %q\n s2   : %q", s, s1, s2)
	})
}

// knownRoundTripBug returns a short description when e is affected by a
// documented parser round-trip defect: a case where e.String() emits text that
// the parser cannot read back (or reads back as a different expression). It
// returns "" for expressions expected to round-trip cleanly, so any
// *undocumented* round-trip failure still fails the fuzzer rather than being
// silently swallowed.
//
// These are genuine defects in the parser/stringer, surfaced (not hidden) by
// this fuzzer and reported alongside it. They are skipped rather than asserted
// as failures only so a `-fuzz` run keeps making forward progress past them.
//
// KNOWN BUG 1 - literal folding to a value fmt.Sprint cannot round-trip:
//
//	The parser eagerly constant-folds binary operations whose legs are both
//	literals (reduceBinOp in ast.go). LiteralExpr.String() formats the result
//	with fmt.Sprint, which for a few float64 values emits text the lexer cannot
//	read back as that number:
//	  - NaN     (0/0, 1/0, -1/0, 0%0, ...) -> "NaN"  (lexes as IDENTIFIER)
//	  - +/-Inf  (overflow, e.g. 1e308*10)  -> "+Inf"/"-Inf"
//	  - -0.0    (e.g. -1%1, 0*-1)          -> "-0"   (does not re-lex)
//	e.g. ParseExpr("0%0").String() == "NaN" -> ParseExpr("NaN") fails.
//	All other finite values (including scientific notation and negatives) DO
//	round-trip. The bad literal can also be nested as a leg of an outer
//	BinOpExpr, e.g. `count_over_time({a="b"}[1m]) / (1/0)` -> `(... / NaN)`.
//	Queries whose text merely contains "NaN"/"Inf" as a quoted matcher value
//	(e.g. {foo="NaN"}) are NOT affected and are still checked.
//
// KNOWN BUG 2 - zero-value bytes label filter:
//
//	BytesLabelFilter.String() formats its value with humanize.Bytes and strips
//	spaces. A value of exactly 0 becomes "0B" (every other magnitude gets a
//	kB/MB/... suffix, e.g. "1.0kB"). When re-lexed, the leading "0B"/"0b" is
//	interpreted as the start of a Go binary integer literal with no digits:
//	  ParseExpr(`{a="0"} | a==0B`) -> "binary literal has no digits".
//	The filter may be nested inside a BinaryLabelFilter (a==0B and b>1KB).
//
// KNOWN BUG 3 - json/logfmt extraction with a non-identifier label:
//
//	The json/logfmt expression parsers can capture a label-extraction whose
//	Identifier is not a valid identifier, e.g. ParseExpr(`{a="0"} | json!`)
//	yields an extraction {Identifier:"!", Expression:"!"}. Their String()
//	writes the identifier verbatim followed by ="...":
//	  `| json !="!"`
//	On re-parse "!=" lexes as the not-equal operator, so the text reads back as
//	`json != "!"` (a bare label filter) - a different, lossy expression, and
//	the String() fixpoint fails. Valid identifiers (json foo="bar") round-trip.
//
// KNOWN BUG 4 - quoted non-identifier matcher label name:
//
//	Stream-selector matchers and string label filters carry a
//	prometheus labels.Matcher, and Matcher.String() quotes label names that are
//	not valid identifiers, e.g. `$` becomes `"$"`:
//	  ParseExpr(`{$="x"}`).String()   == `{"$"="x"}`
//	  ParseExpr(`{a="1"} | $=""`).String() == `{a="1"} | "$"=""`
//	The LogQL parser does not accept a quoted string in label-name position, so
//	both fail to re-parse. Note this is specific to the matcher/string-filter
//	path: numeric/bytes/duration/ip filters, groupings, label_format, drop,
//	keep and unwrap all emit the name unquoted and DO round-trip, so they are
//	not skipped.
func knownRoundTripBug(e Expr) string {
	if hasUnRoundTrippableLiteral(e) {
		return "literal folds to NaN/+Inf/-Inf/-0, which does not re-lex as that NUMBER"
	}
	if hasZeroValueBytesFilter(e) {
		return `zero-value bytes label filter stringifies to "0B", which re-lexes as a binary literal`
	}
	if hasNonIdentifierExtraction(e) {
		return "json/logfmt extraction identifier is not a valid identifier and does not re-lex"
	}
	if hasQuotedMatcherName(e) {
		return "matcher/string-filter label name is not an identifier and is quoted by labels.Matcher.String()"
	}
	return ""
}

// FuzzParseSampleExpr is a cheap crash-safety fuzzer for the metric-query entry
// point. Errors are expected; only panics are bugs.
func FuzzParseSampleExpr(f *testing.F) {
	addRoundTripSeeds(f)

	f.Fuzz(func(_ *testing.T, s string) {
		_, _ = ParseSampleExpr(s)
	})
}

// FuzzParseLogSelector is a cheap crash-safety fuzzer for the log-selector
// entry point. Errors are expected; only panics are bugs.
func FuzzParseLogSelector(f *testing.F) {
	addRoundTripSeeds(f)

	f.Fuzz(func(_ *testing.T, s string) {
		// Exercise both validated and non-validated paths.
		_, _ = ParseLogSelector(s, true)
		_, _ = ParseLogSelector(s, false)
	})
}

// hasUnRoundTrippableLiteral reports whether the AST contains a LiteralExpr
// whose value is NaN, +/-Inf or negative zero. Such literals are produced by
// constant-folding of literal binary operations and cannot be re-parsed from
// their fmt.Sprint-based String() form. See the KNOWN BUG note in
// knownRoundTripBug.
func hasUnRoundTrippableLiteral(e Expr) bool {
	var found bool
	e.Walk(func(n Expr) bool {
		if lit, ok := n.(*LiteralExpr); ok {
			v := lit.Val
			if math.IsNaN(v) || math.IsInf(v, 0) || (v == 0 && math.Signbit(v)) {
				found = true
			}
		}
		return !found
	})
	return found
}

// hasZeroValueBytesFilter reports whether the AST contains a bytes label filter
// whose value is 0, whose String() form is the unparseable token "0B".
// See the KNOWN BUG note in knownRoundTripBug.
func hasZeroValueBytesFilter(e Expr) bool {
	var found bool
	e.Walk(func(n Expr) bool {
		if lf, ok := n.(*LabelFilterExpr); ok {
			if labelFiltererHasZeroBytes(lf.LabelFilterer) {
				found = true
			}
		}
		return !found
	})
	return found
}

func labelFiltererHasZeroBytes(f log.LabelFilterer) bool {
	switch t := f.(type) {
	case *log.BytesLabelFilter:
		return t.Value == 0
	case *log.BinaryLabelFilter:
		return labelFiltererHasZeroBytes(t.Left) || labelFiltererHasZeroBytes(t.Right)
	}
	return false
}

// hasNonIdentifierExtraction reports whether the AST contains a json/logfmt
// expression-parser stage whose extraction identifier is not a valid identifier
// and therefore does not re-lex to itself. See the KNOWN BUG note in
// knownRoundTripBug.
func hasNonIdentifierExtraction(e Expr) bool {
	var found bool
	e.Walk(func(n Expr) bool {
		switch t := n.(type) {
		case *JSONExpressionParserExpr:
			for _, exp := range t.Expressions {
				if !isReparseableIdentifier(exp.Identifier) {
					found = true
				}
			}
		case *LogfmtExpressionParserExpr:
			for _, exp := range t.Expressions {
				if !isReparseableIdentifier(exp.Identifier) {
					found = true
				}
			}
		}
		return !found
	})
	return found
}

// hasQuotedMatcherName reports whether the AST contains a stream-selector
// matcher or a matcher-backed string label filter whose label name is not a
// valid identifier and is therefore quoted (and rendered unparseable) by
// labels.Matcher.String(). See the KNOWN BUG note in knownRoundTripBug.
func hasQuotedMatcherName(e Expr) bool {
	var found bool
	e.Walk(func(n Expr) bool {
		switch t := n.(type) {
		case *MatchersExpr:
			for _, m := range t.Mts {
				if matcherNameNeedsQuoting(m.Name) {
					found = true
				}
			}
		case *LabelFilterExpr:
			if labelFiltererHasQuotedName(t.LabelFilterer) {
				found = true
			}
		}
		return !found
	})
	return found
}

// labelFiltererHasQuotedName reports whether a label filter tree contains a
// matcher-backed filter (StringLabelFilter/LineFilterLabelFilter/NoopLabelFilter,
// all of which delegate to labels.Matcher.String()) whose label name is not a
// valid identifier. Numeric/bytes/duration/ip filters render names unquoted and
// are intentionally excluded.
func labelFiltererHasQuotedName(f log.LabelFilterer) bool {
	switch t := f.(type) {
	case *log.StringLabelFilter:
		return matcherNameNeedsQuoting(t.Name)
	case *log.LineFilterLabelFilter:
		return matcherNameNeedsQuoting(t.Name)
	case *log.NoopLabelFilter:
		return matcherNameNeedsQuoting(t.Name)
	case *log.BinaryLabelFilter:
		return labelFiltererHasQuotedName(t.Left) || labelFiltererHasQuotedName(t.Right)
	}
	return false
}

// matcherNameNeedsQuoting mirrors prometheus labels.(*Matcher).shouldQuoteName:
// a name is quoted (and thus not re-parseable as a LogQL label name) unless it
// consists solely of ASCII letters, digits and underscores and does not start
// with a digit. This intentionally differs from isReparseableIdentifier (which
// follows the LogQL lexer and accepts unicode letters), because matcher names
// are rendered by prometheus's stricter ASCII rule, e.g. the unicode-letter
// name "ɒ" lexes fine on input but is quoted as `"ɒ"` on output.
func matcherNameNeedsQuoting(name string) bool {
	for i, c := range name {
		if c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (i > 0 && c >= '0' && c <= '9') {
			continue
		}
		return true
	}
	return name == ""
}

// isReparseableIdentifier mirrors the lexer's default identifier rule (see
// (*Scanner).isIdentRune in query_scanner.go): a leading underscore or unicode
// letter, followed by underscores, unicode letters or digits.
func isReparseableIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_' || unicode.IsLetter(r):
			// always allowed
		case unicode.IsDigit(r) && i > 0:
			// allowed after the first rune
		default:
			return false
		}
	}
	return true
}
