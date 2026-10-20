package log

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/grafana/regexp"
	"github.com/grafana/regexp/syntax"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/v3/pkg/logql/log/pattern"
	"github.com/grafana/loki/v3/pkg/util"
)

// LineMatchType is an enum for line matching types.
type LineMatchType int

// Possible LineMatchTypes.
const (
	LineMatchEqual LineMatchType = iota
	LineMatchNotEqual
	LineMatchRegexp
	LineMatchNotRegexp
	LineMatchPattern
	LineMatchNotPattern
)

func (t LineMatchType) String() string {
	switch t {
	case LineMatchEqual:
		return "|="
	case LineMatchNotEqual:
		return "!="
	case LineMatchRegexp:
		return "|~"
	case LineMatchNotRegexp:
		return "!~"
	case LineMatchPattern:
		return "|>"
	case LineMatchNotPattern:
		return "!>"
	default:
		return ""
	}
}

// Checker is an interface that matches against the input line or regexp.
type Checker interface {
	Test(line []byte, caseInsensitive bool, equal bool) bool
	TestRegex(reg *regexp.Regexp) bool
}

// Matcher is a interface to match log lines against a Checker.
// This works in the opposite direction of Filterer. Whereas Filterer.Filter
// checks if an input log line satisfies the filter, Matcher.Matches checks if
// a filter satisfies an input log line (or regexp).
type Matcher interface {
	Matches(test Checker) bool
}

// Filterer is a interface to filter log lines.
type Filterer interface {
	Filter(line []byte) bool
	ToStage() Stage
}

type MatcherFilterer interface {
	Matcher
	Filterer
}

type wrapper struct {
	Filterer
	Matcher
}

func (w wrapper) IsMatcher() bool {
	return w.Matcher != nil
}

func (w wrapper) IsFilterer() bool {
	return w.Filterer != nil
}

func WrapFilterer(f Filterer) MatcherFilterer {
	return wrapper{Filterer: f}
}

func WrapMatcher(m Matcher) MatcherFilterer {
	return wrapper{Matcher: m}
}

// LineFilterFunc is a syntax sugar for creating line filter from a function
type FiltererFunc func(line []byte) bool

func (f FiltererFunc) Filter(line []byte) bool {
	return f(line)
}

type trueFilter struct{}

func (trueFilter) Filter(_ []byte) bool { return true }
func (trueFilter) ToStage() Stage       { return NoopStage }

// Matches implements Matcher
func (trueFilter) Matches(_ Checker) bool { return true }

// TrueFilter is a filter that returns and matches all log lines whatever their content.
var TrueFilter = trueFilter{}

func isTrueFilter(f MatcherFilterer) bool {
	if f == TrueFilter {
		return true
	}

	if _, ok := f.(trueFilter); ok {
		return true
	}

	if wrap, ok := f.(wrapper); ok {
		if wrap.IsFilterer() {
			if _, ok = wrap.Filterer.(trueFilter); ok {
				return true
			}
		}
		// Otherwise, it's a matcher
		if _, ok = wrap.Matcher.(trueFilter); ok {
			return true
		}
	}

	return false
}

type existsFilter struct{}

func (e existsFilter) Filter(line []byte) bool {
	// `.` does not match a newline, so a line of only newlines has nothing for it to match.
	for _, c := range line {
		if c != '\n' {
			return true
		}
	}
	return false
}

func (e existsFilter) ToStage() Stage {
	return NewStageFunc(nil, StageHints{CanModifyLabels: false, CanModifyLine: false}, func(_ int64, line []byte, _ *LabelsBuilder) ([]byte, bool) {
		return line, e.Filter(line)
	})
}

// Matches implements Matcher
func (e existsFilter) Matches(_ Checker) bool { return true }

// ExistsFilter is a filter that returns and matches when a line has any character other than a
// newline, like the regexp `.+`.
var ExistsFilter = existsFilter{}

type notFilter struct {
	MatcherFilterer
}

func (n notFilter) Filter(line []byte) bool {
	return !n.MatcherFilterer.Filter(line)
}

func (n notFilter) ToStage() Stage {
	return NewStageFunc(nil, StageHints{CanModifyLabels: false, CanModifyLine: false}, func(_ int64, line []byte, _ *LabelsBuilder) ([]byte, bool) {
		return line, n.Filter(line)
	})
}

func (n notFilter) Matches(test Checker) bool {
	return !n.MatcherFilterer.Matches(test)
}

// NewNotFilter creates a new filter which matches only if the base filter doesn't match.
// If the base filter is a `or` it will recursively simplify with `and` operations.
func NewNotFilter(base MatcherFilterer) MatcherFilterer {
	// not(a|b) = not(a) and not(b) , and operation can't benefit from this optimization because both legs always needs to be executed.
	if or, ok := base.(orFilter); ok {
		return NewAndFilter(NewNotFilter(or.left), NewNotFilter(or.right))
	}
	return notFilter{MatcherFilterer: base}
}

type andFilter struct {
	left  MatcherFilterer
	right MatcherFilterer
}

// NewAndFilter creates a new filter which matches only if left and right matches.
func NewAndFilter(left MatcherFilterer, right MatcherFilterer) MatcherFilterer {
	// Make sure we take care of panics in case a nil or noop filter is passed.
	if right == nil || isTrueFilter(right) {
		return left
	}

	if left == nil || isTrueFilter(left) {
		return right
	}

	return andFilter{
		left:  left,
		right: right,
	}
}

func (a andFilter) Filter(line []byte) bool {
	return a.left.Filter(line) && a.right.Filter(line)
}

func (a andFilter) ToStage() Stage {
	return NewStageFunc(nil, StageHints{CanModifyLabels: false, CanModifyLine: false}, func(_ int64, line []byte, _ *LabelsBuilder) ([]byte, bool) {
		return line, a.Filter(line)
	})
}

func (a andFilter) Matches(test Checker) bool {
	return a.left.Matches(test) && a.right.Matches(test)
}

type andFilters struct {
	filters []Filterer
}

// NewAndFilters creates a new filter which matches only if all filters match
func NewAndFilters(filters []Filterer) Filterer {
	var containsFilterAcc *containsAllFilter
	regexpFilters := make([]Filterer, 0)
	n := 0
	for _, filter := range filters {
		// Make sure we take care of panics in case a nil or noop filter is passed.
		if filter != nil && !isTrueFilter(WrapFilterer(filter)) {
			switch c := filter.(type) {
			case *containsFilter:
				// Start accumulating contains filters.
				if containsFilterAcc == nil {
					containsFilterAcc = &containsAllFilter{}
				}

				// Join all contain filters.
				containsFilterAcc.Add(*c)
			case regexpFilter:
				regexpFilters = append(regexpFilters, c)

			default:
				// Finish accumulating contains filters.
				if containsFilterAcc != nil {
					filters[n] = containsFilterAcc
					n++
					containsFilterAcc = nil
				}

				// Keep filter
				filters[n] = filter
				n++
			}
		}
	}
	filters = filters[:n]

	if containsFilterAcc != nil {
		filters = append(filters, containsFilterAcc)
	}

	// Push regex filters to end
	if len(regexpFilters) > 0 {
		filters = append(filters, regexpFilters...)
	}

	if len(filters) == 0 {
		return TrueFilter
	} else if len(filters) == 1 {
		return filters[0]
	}

	return andFilters{
		filters: filters,
	}
}

func (a andFilters) Filter(line []byte) bool {
	for _, filter := range a.filters {
		if !filter.Filter(line) {
			return false
		}
	}
	return true
}

func (a andFilters) ToStage() Stage {
	return NewStageFunc(nil, StageHints{CanModifyLabels: false, CanModifyLine: false}, func(_ int64, line []byte, _ *LabelsBuilder) ([]byte, bool) {
		return line, a.Filter(line)
	})
}

type orFilter struct {
	left  MatcherFilterer
	right MatcherFilterer
}

// newOrFilter creates a new filter which matches only if left or right matches.
func newOrFilter(left MatcherFilterer, right MatcherFilterer) MatcherFilterer {
	if left == nil || isTrueFilter(left) {
		return right
	}

	if right == nil || isTrueFilter(right) {
		return left
	}

	return orFilter{
		left:  left,
		right: right,
	}
}

// ChainOrMatcherFilterer is a syntax sugar to chain multiple `or` filters. (1 or many)
func ChainOrMatcherFilterer(curr, newFilterer MatcherFilterer) MatcherFilterer {
	if curr == nil {
		return newFilterer
	}
	return newOrFilter(curr, newFilterer)
}

// ChainOrFilter is a syntax sugar to chain multiple `or` filters. (1 or many)
func ChainOrFilter(curr, newFilterer Filterer) Filterer {
	return ChainOrMatcherFilterer(WrapFilterer(curr), WrapFilterer(newFilterer))
}

func (a orFilter) Filter(line []byte) bool {
	return a.left.Filter(line) || a.right.Filter(line)
}

func (a orFilter) ToStage() Stage {
	return NewStageFunc(nil, StageHints{CanModifyLabels: false, CanModifyLine: false}, func(_ int64, line []byte, _ *LabelsBuilder) ([]byte, bool) {
		return line, a.Filter(line)
	})
}

// Matches implements Matcher
func (a orFilter) Matches(test Checker) bool {
	return a.left.Matches(test) || a.right.Matches(test)
}

type regexpFilter struct {
	// match matches the whole line.
	match func(string) bool
	// goRegexp returns the Go regexp that Matches hands the Checker. It has the meaning of the
	// filter, and it is compiled on first use.
	goRegexp func() *regexp.Regexp

	orig string
}

// newRegexpFilter creates a filter that matches the whole line against pattern with
// FastRegexMatcher. goPattern is a Go regexp with the same meaning, for Matches. orig is the
// regexp of the query.
func newRegexpFilter(pattern, goPattern, orig string) (regexpFilter, error) {
	m, err := labels.NewFastRegexMatcher(pattern)
	if err != nil {
		return regexpFilter{}, err
	}
	return fastRegexpFilter(m, goPattern, orig), nil
}

func fastRegexpFilter(m *labels.FastRegexMatcher, goPattern, orig string) regexpFilter {
	return regexpFilter{
		match:    m.MatchString,
		goRegexp: sync.OnceValue(func() *regexp.Regexp { return regexp.MustCompile(goPattern) }),
		orig:     orig,
	}
}

// newGoRegexpFilter creates a filter that matches the line with the Go regexp goPattern, for a
// regexp that FastRegexMatcher would match wrongly or slowly. orig is the regexp of the query.
func newGoRegexpFilter(goPattern, orig string) (regexpFilter, error) {
	re, err := regexp.Compile(goPattern)
	if err != nil {
		return regexpFilter{}, err
	}
	return regexpFilter{
		match:    re.MatchString,
		goRegexp: func() *regexp.Regexp { return re },
		orig:     orig,
	}, nil
}

func (r regexpFilter) Filter(line []byte) bool {
	return r.match(unsafeGetString(line))
}

func (r regexpFilter) ToStage() Stage {
	return NewStageFunc(nil, StageHints{CanModifyLabels: false, CanModifyLine: false}, func(_ int64, line []byte, _ *LabelsBuilder) ([]byte, bool) {
		return line, r.Filter(line)
	})
}

func (r regexpFilter) Matches(test Checker) bool {
	return test.TestRegex(r.goRegexp())
}

func (r regexpFilter) String() string {
	return r.orig
}

type equalFilter struct {
	match []byte
}

func (l equalFilter) Filter(line []byte) bool {
	return bytes.Equal(line, l.match)
}

func (l equalFilter) ToStage() Stage {
	return NewStageFunc(nil, StageHints{CanModifyLabels: false, CanModifyLine: false}, func(_ int64, line []byte, _ *LabelsBuilder) ([]byte, bool) {
		return line, l.Filter(line)
	})
}

func (l equalFilter) Matches(test Checker) bool {
	return test.Test(l.match, false, true)
}

func (l equalFilter) String() string {
	return string(l.match)
}

func newEqualFilter(match []byte) MatcherFilterer {
	return equalFilter{match}
}

type containsFilter struct {
	match           []byte
	caseInsensitive bool
}

func (l *containsFilter) Filter(line []byte) bool {
	return contains(line, l.match, l.caseInsensitive)
}

func contains(line, substr []byte, caseInsensitive bool) bool {
	if !caseInsensitive {
		return bytes.Contains(line, substr)
	}
	return containsFold(line, substr)
}

// containsFold verifies if substr is a substring of line under Unicode simple case folding, the
// folding that (?i) applies in a regexp. So K matches the Kelvin sign and s matches ſ, but i does
// not match İ. The ASCII letters of substr MUST be lowercase before calling this function, and
// substr must be valid UTF-8 without utf8.RuneError.
func containsFold(line, substr []byte) bool {
	if len(substr) == 0 {
		return true
	}

	// Fast path for an ASCII letter that folds only to its other case, which is every one but k and
	// s: c|0x20 is the lowercase letter for both cases.
	if c := substr[0]; 'a' <= c && c <= 'z' && c != 'k' && c != 's' {
		for i := range line {
			if line[i]|0x20 == c && hasPrefixFold(line[i:], substr) {
				return true
			}
		}
		return false
	}

	// Every rune that folds to the first rune of substr can start a match. Collect their first
	// bytes. A first byte is never a continuation byte, so the scan below only stops where a rune
	// starts.
	var (
		starts  [8]byte
		nStarts int
		buf     [utf8.UTFMax]byte
	)
	first, _ := utf8.DecodeRune(substr)
	for r := first; nStarts < len(starts); {
		utf8.EncodeRune(buf[:], r)
		if !slices.Contains(starts[:nStarts], buf[0]) {
			starts[nStarts] = buf[0]
			nStarts++
		}
		if r = unicode.SimpleFold(r); r == first {
			break
		}
	}

	// next[k] is the position of the next starts[k] at or after pos, or -1 before the first search
	// for it. A search with bytes.IndexByte per start byte is faster than testing each byte.
	var next [len(starts)]int
	for k := range nStarts {
		next[k] = -1
	}
	for pos := 0; pos < len(line); {
		i := len(line)
		for k := range nStarts {
			if next[k] < pos {
				if j := bytes.IndexByte(line[pos:], starts[k]); j < 0 {
					next[k] = len(line)
				} else {
					next[k] = pos + j
				}
			}
			i = min(i, next[k])
		}
		if i == len(line) {
			return false
		}
		if hasPrefixFold(line[i:], substr) {
			return true
		}
		pos = i + 1
	}
	return false
}

// hasPrefixFold verifies if s starts with prefix under Unicode simple case folding. The ASCII
// letters of prefix MUST be lowercase.
func hasPrefixFold(s, prefix []byte) bool {
	i, j := 0, 0
	for j < len(prefix) {
		if i >= len(s) {
			return false
		}

		// Fast path for ASCII. An ASCII letter can also fold to a rune outside ASCII, like k to the
		// Kelvin sign, but then the byte of s is not ASCII and the slow path compares them.
		c, p := s[i], prefix[j]
		if c < utf8.RuneSelf && p < utf8.RuneSelf {
			if 'A' <= c && c <= 'Z' {
				c += 'a' - 'A'
			}
			if c != p {
				return false
			}
			i++
			j++
			continue
		}

		// A byte of s that is not valid UTF-8 decodes to utf8.RuneError, which prefix does not hold.
		sr, sSize := utf8.DecodeRune(s[i:])
		pr, pSize := utf8.DecodeRune(prefix[j:])
		if sr != pr && !foldEqual(sr, pr) {
			return false
		}
		i += sSize
		j += pSize
	}
	return true
}

// foldEqual tells whether two distinct runes are equal under Unicode simple case folding.
func foldEqual(a, b rune) bool {
	for r := unicode.SimpleFold(a); r != a; r = unicode.SimpleFold(r) {
		if r == b {
			return true
		}
	}
	return false
}

func (l containsFilter) ToStage() Stage {
	return NewStageFunc(nil, StageHints{CanModifyLabels: false, CanModifyLine: false}, func(_ int64, line []byte, _ *LabelsBuilder) ([]byte, bool) {
		return line, l.Filter(line)
	})
}

// Matches implements Matcher
func (l containsFilter) Matches(test Checker) bool {
	return test.Test(l.match, l.caseInsensitive, false)
}

func (l containsFilter) String() string {
	return string(l.match)
}

// newContainsFilter creates a contains filter that checks if a log line contains a match.
func newContainsFilter(match []byte, caseInsensitive bool) MatcherFilterer {
	if len(match) == 0 {
		return TrueFilter
	}
	if caseInsensitive {
		// containsFold expects the ASCII letters in lowercase. Only those are lowered: a letter
		// outside ASCII can lower to a rune that it does not fold to, like İ to i.
		match = bytes.Clone(match)
		for i, c := range match {
			if 'A' <= c && c <= 'Z' {
				match[i] = c + 'a' - 'A'
			}
		}
	}
	return &containsFilter{
		match:           match,
		caseInsensitive: caseInsensitive,
	}
}

// prefixedContainsFilter matches a line that holds one of literals, which all start with prefix. It
// searches the line once for prefix and compares the literals where it finds it, so it is faster
// than a containsFilter per literal.
type prefixedContainsFilter struct {
	prefix   []byte
	literals [][]byte
}

func newPrefixedContainsFilter(prefix string, literals []string) MatcherFilterer {
	f := prefixedContainsFilter{prefix: []byte(prefix)}
	for _, l := range literals {
		f.literals = append(f.literals, []byte(l))
	}
	return f
}

func (f prefixedContainsFilter) Filter(line []byte) bool {
	for pos := 0; ; pos++ {
		i := bytes.Index(line[pos:], f.prefix)
		if i < 0 {
			return false
		}
		pos += i
		for _, l := range f.literals {
			if bytes.HasPrefix(line[pos:], l) {
				return true
			}
		}
	}
}

func (f prefixedContainsFilter) ToStage() Stage {
	return NewStageFunc(nil, StageHints{CanModifyLabels: false, CanModifyLine: false}, func(_ int64, line []byte, _ *LabelsBuilder) ([]byte, bool) {
		return line, f.Filter(line)
	})
}

// Matches implements Matcher
func (f prefixedContainsFilter) Matches(test Checker) bool {
	for _, l := range f.literals {
		if test.Test(l, false, false) {
			return true
		}
	}
	return false
}

// commonPrefix returns the longest prefix that all of strs share.
func commonPrefix(strs []string) string {
	if len(strs) == 0 {
		return ""
	}
	prefix := strs[0]
	for _, s := range strs[1:] {
		n := 0
		for n < len(prefix) && n < len(s) && prefix[n] == s[n] {
			n++
		}
		prefix = prefix[:n]
	}
	return prefix
}

type containsAllFilter struct {
	matches []containsFilter
}

func (f *containsAllFilter) Add(filter containsFilter) {
	f.matches = append(f.matches, filter)
}

func (f *containsAllFilter) Empty() bool {
	return len(f.matches) == 0
}

func (f containsAllFilter) Filter(line []byte) bool {
	for _, m := range f.matches {
		if !contains(line, m.match, m.caseInsensitive) {
			return false
		}
	}
	return true
}

func (f containsAllFilter) ToStage() Stage {
	return NewStageFunc(nil, StageHints{CanModifyLabels: false, CanModifyLine: false}, func(_ int64, line []byte, _ *LabelsBuilder) ([]byte, bool) {
		return line, f.Filter(line)
	})
}

func (f containsAllFilter) Matches(test Checker) bool {
	for _, m := range f.matches {
		if !test.Test(m.match, m.caseInsensitive, false) {
			return false
		}
	}
	return true
}

// NewFilter creates a new line filter from a match string and type.
func NewFilter(match string, mt LineMatchType) (Filterer, error) {
	switch mt {
	case LineMatchRegexp:
		return parseRegexpFilter(match, true, false)
	case LineMatchNotRegexp:
		return parseRegexpFilter(match, false, false)
	case LineMatchEqual:
		return newContainsFilter([]byte(match), false), nil
	case LineMatchNotEqual:
		return NewNotFilter(newContainsFilter([]byte(match), false)), nil
	case LineMatchPattern:
		return newPatternFilterer([]byte(match), true)
	case LineMatchNotPattern:
		return newPatternFilterer([]byte(match), false)
	default:
		return nil, fmt.Errorf("unknown matcher: %v", match)
	}
}

// NewLabelFilter creates a new filter that has label regex semantics
func NewLabelFilter(match string, mt labels.MatchType) (Filterer, error) {
	switch mt {
	case labels.MatchRegexp:
		return parseRegexpFilter(match, true, true)
	case labels.MatchNotRegexp:
		return parseRegexpFilter(match, false, true)
	case labels.MatchEqual:
		return newEqualFilter([]byte(match)), nil
	case labels.MatchNotEqual:
		return NewNotFilter(newEqualFilter([]byte(match))), nil
	default:
		return nil, fmt.Errorf("unknown matcher: %v", match)
	}
}

// parseRegexpFilter parses a regexp and creates the filter that evaluates it. A line filter
// searches the line for a match. A label filter matches the whole value, so it is anchored to the
// beginning and the end of the value.
//
// The filter matches with Prometheus' FastRegexMatcher, which replaces the regexp engine with
// string comparisons for common regexps. A line filter that searches for a few literals, like
// error|warn or (?i)error, uses containsFilter instead: it has a faster batch form, and
// FastRegexMatcher runs the regexp engine for case-insensitive literals, which is an order of
// magnitude slower.
func parseRegexpFilter(re string, match bool, isLabel bool) (MatcherFilterer, error) {
	reg, err := syntax.Parse(re, syntax.Perl)
	if err != nil {
		return nil, err
	}

	var filter MatcherFilterer
	if isLabel {
		filter, err = newLabelRegexpFilter(re, reg)
	} else {
		filter, err = newLineRegexpFilter(re, reg)
	}
	if err != nil {
		return nil, err
	}

	if match {
		return filter, nil
	}
	return NewNotFilter(filter), nil
}

// maxLiterals is the most literals that a line filter searches with containsFilter. A regexp that
// expands to more is left to FastRegexMatcher.
const maxLiterals = 16

// newLineRegexpFilter creates the filter that searches a line for a match of reg, the parse of orig.
func newLineRegexpFilter(orig string, reg *syntax.Regexp) (MatcherFilterer, error) {
	// A search lets anything come before and after the match, so a .* at either end of reg is
	// redundant.
	core := trimAnyStar(trimAnyStar(reg, true), false)

	// A regexp that matches the empty string matches at the start of every line.
	if matchesEmpty(core) {
		return TrueFilter, nil
	}

	// `.` and `.+` match a line that has any character other than a newline.
	if core.Op == syntax.OpAnyCharNotNL || (core.Op == syntax.OpPlus && core.Sub[0].Op == syntax.OpAnyCharNotNL) {
		return ExistsFilter, nil
	}

	// A search for literals is a containsFilter, which has a faster batch form than
	// FastRegexMatcher, and which is faster for case-insensitive literals. Literals that share a
	// prefix, like level=(warn|error), are searched with one scan for the prefix instead.
	if literals, caseInsensitive, ok := literalAlternatives(core); ok {
		if prefix := commonPrefix(literals); !caseInsensitive && len(literals) > 1 && len(prefix) >= 2 {
			return newPrefixedContainsFilter(prefix, literals), nil
		}
		var filter MatcherFilterer
		for _, l := range literals {
			filter = ChainOrMatcherFilterer(filter, newContainsFilter([]byte(l), caseInsensitive))
		}
		return filter, nil
	}

	// FastRegexMatcher matches the whole line, so the search matches core with anything on each
	// side, except on a side that core anchors with ^ or $. Anything includes a newline, because a
	// search can skip one. FastRegexMatcher lets . match a newline, but core prints its own . as
	// (?-s:.), which keeps the meaning of a line filter, where . does not match one.
	var pattern strings.Builder
	if !startsWith(core, syntax.OpBeginText) {
		pattern.WriteString("(?s:.)*")
	}
	pattern.WriteString("(?:")
	pattern.WriteString(core.String())
	pattern.WriteString(")")
	if !endsWith(core, syntax.OpEndText) {
		pattern.WriteString("(?s:.)*")
	}
	m, err := labels.NewFastRegexMatcher(pattern.String())
	if err != nil {
		return nil, err
	}

	// FastRegexMatcher runs the regexp engine on the whole line when it finds nothing to optimize,
	// and the (?s:.)* of pattern slow the engine down. A search with the engine is faster.
	if !m.IsOptimized() {
		util.AllNonGreedy(core)
		return newGoRegexpFilter(core.String(), orig)
	}
	return fastRegexpFilter(m, orig, orig), nil
}

// newLabelRegexpFilter creates the filter that matches a whole label value against reg, the parse
// of orig.
func newLabelRegexpFilter(orig string, reg *syntax.Regexp) (MatcherFilterer, error) {
	// .* matches every value, even though . does not match a newline. So the common | label=~".*"
	// stays a no-op, as it was before label filters matched with FastRegexMatcher.
	if r := unwrapCapture(reg); r.Op == syntax.OpStar && (r.Sub[0].Op == syntax.OpAnyChar || r.Sub[0].Op == syntax.OpAnyCharNotNL) {
		return TrueFilter, nil
	}

	// FastRegexMatcher compares a value with 16 or more case-insensitive literals after NFKD
	// normalization, which (?i) does not apply. So it would match ＦＯＯ or ① with an alternation
	// that holds (?i)foo or 1. The regexp engine matches those instead.
	if hasCaseInsensitiveLiteral(reg) && countAlternatives(reg) >= 16 {
		return newGoRegexpFilter("^(?:"+orig+")$", orig)
	}

	// FastRegexMatcher lets . match a newline, and a label filter does not. reg prints its . as
	// (?-s:.), which keeps that meaning.
	pattern := orig
	if hasOp(reg, syntax.OpAnyCharNotNL) {
		pattern = reg.String()
	}
	return newRegexpFilter(pattern, "^(?:"+orig+")$", orig)
}

// trimAnyStar removes the .* that reg starts with when leading is true, or that it ends with
// otherwise. It looks through concatenations, alternations and captures, so it removes the .* of
// both foo.*|bar.* and (foo.*). It returns an empty match when nothing is left.
func trimAnyStar(reg *syntax.Regexp, leading bool) *syntax.Regexp {
	switch reg.Op {
	case syntax.OpCapture:
		return trimAnyStar(reg.Sub[0], leading)

	case syntax.OpStar:
		if op := reg.Sub[0].Op; op == syntax.OpAnyChar || op == syntax.OpAnyCharNotNL {
			return &syntax.Regexp{Op: syntax.OpEmptyMatch}
		}

	case syntax.OpConcat:
		subs := slices.Clone(reg.Sub)
		for len(subs) > 0 {
			i := len(subs) - 1
			if leading {
				i = 0
			}
			sub := trimAnyStar(subs[i], leading)
			if sub.Op != syntax.OpEmptyMatch {
				subs[i] = sub
				break
			}
			subs = slices.Delete(subs, i, i+1)
		}
		switch len(subs) {
		case 0:
			return &syntax.Regexp{Op: syntax.OpEmptyMatch}
		case 1:
			return subs[0]
		}
		return &syntax.Regexp{Op: syntax.OpConcat, Flags: reg.Flags, Sub: subs}

	case syntax.OpAlternate:
		subs := make([]*syntax.Regexp, len(reg.Sub))
		for i, sub := range reg.Sub {
			subs[i] = trimAnyStar(sub, leading)
		}
		return &syntax.Regexp{Op: syntax.OpAlternate, Flags: reg.Flags, Sub: subs}
	}
	return reg
}

// matchesEmpty reports whether reg matches the empty string at any position. It reports false for
// a regexp that needs an assertion to match it, like ^ or \b.
func matchesEmpty(reg *syntax.Regexp) bool {
	switch reg.Op {
	case syntax.OpEmptyMatch, syntax.OpStar, syntax.OpQuest:
		return true
	case syntax.OpPlus, syntax.OpCapture:
		return matchesEmpty(reg.Sub[0])
	case syntax.OpRepeat:
		return reg.Min == 0 || matchesEmpty(reg.Sub[0])
	case syntax.OpConcat:
		for _, sub := range reg.Sub {
			if !matchesEmpty(sub) {
				return false
			}
		}
		return true
	case syntax.OpAlternate:
		return slices.ContainsFunc(reg.Sub, matchesEmpty)
	}
	return false
}

// literalAlternatives returns the strings that reg matches when it matches only a few strings,
// like error|warn or (?i)level=(warn|error), and whether to compare them case-insensitively. It
// returns false when reg has both a case-sensitive and a case-insensitive literal with letters,
// because a containsFilter can't compare part of its match with case and the rest without.
func literalAlternatives(reg *syntax.Regexp) (literals []string, caseInsensitive bool, ok bool) {
	var caseSensitive bool
	var expand func(reg *syntax.Regexp, prefixes []string) ([]string, bool)
	expand = func(reg *syntax.Regexp, prefixes []string) ([]string, bool) {
		switch reg.Op {
		case syntax.OpEmptyMatch:
			return prefixes, true

		case syntax.OpCapture:
			return expand(reg.Sub[0], prefixes)

		case syntax.OpLiteral:
			// A regexp matches utf8.RuneError with a byte that is not valid UTF-8, and a
			// containsFilter does not.
			if slices.Contains(reg.Rune, utf8.RuneError) {
				return nil, false
			}
			if slices.ContainsFunc(reg.Rune, func(r rune) bool { return unicode.SimpleFold(r) != r }) {
				if reg.Flags&syntax.FoldCase != 0 {
					caseInsensitive = true
				} else {
					caseSensitive = true
				}
			}
			if caseInsensitive && caseSensitive {
				return nil, false
			}

			out := make([]string, len(prefixes))
			for i, p := range prefixes {
				out[i] = p + string(reg.Rune)
			}
			return out, true

		case syntax.OpConcat:
			var ok bool
			for _, sub := range reg.Sub {
				if prefixes, ok = expand(sub, prefixes); !ok {
					return nil, false
				}
			}
			return prefixes, true

		case syntax.OpAlternate:
			var out []string
			for _, sub := range reg.Sub {
				strs, ok := expand(sub, prefixes)
				if !ok || len(out)+len(strs) > maxLiterals {
					return nil, false
				}
				out = append(out, strs...)
			}
			return out, true
		}
		return nil, false
	}

	literals, ok = expand(reg, []string{""})
	return literals, caseInsensitive, ok
}

func unwrapCapture(reg *syntax.Regexp) *syntax.Regexp {
	for reg.Op == syntax.OpCapture {
		reg = reg.Sub[0]
	}
	return reg
}

// startsWith reports whether the first operation of reg is op.
func startsWith(reg *syntax.Regexp, op syntax.Op) bool {
	reg = unwrapCapture(reg)
	if reg.Op == syntax.OpConcat {
		return startsWith(reg.Sub[0], op)
	}
	return reg.Op == op
}

// endsWith reports whether the last operation of reg is op.
func endsWith(reg *syntax.Regexp, op syntax.Op) bool {
	reg = unwrapCapture(reg)
	if reg.Op == syntax.OpConcat {
		return endsWith(reg.Sub[len(reg.Sub)-1], op)
	}
	return reg.Op == op
}

// hasCaseInsensitiveLiteral reports whether reg holds a case-insensitive literal with a letter.
func hasCaseInsensitiveLiteral(reg *syntax.Regexp) bool {
	if reg.Op == syntax.OpLiteral && reg.Flags&syntax.FoldCase != 0 &&
		slices.ContainsFunc(reg.Rune, func(r rune) bool { return unicode.SimpleFold(r) != r }) {
		return true
	}
	return slices.ContainsFunc(reg.Sub, hasCaseInsensitiveLiteral)
}

// countAlternatives returns how many ways an alternation in reg can match, like FastRegexMatcher
// counts them: the alternatives of an alternation and the runes of a class add up, and those of a
// concatenation multiply. It stops counting at 1<<16.
func countAlternatives(reg *syntax.Regexp) int {
	switch reg.Op {
	case syntax.OpAlternate:
		n := 0
		for _, sub := range reg.Sub {
			n = min(n+countAlternatives(sub), 1<<16)
		}
		return n
	case syntax.OpConcat:
		n := 1
		for _, sub := range reg.Sub {
			n = min(n*countAlternatives(sub), 1<<16)
		}
		return n
	case syntax.OpCharClass:
		n := 0
		for i := 0; i+1 < len(reg.Rune); i += 2 {
			n = min(n+int(reg.Rune[i+1]-reg.Rune[i])+1, 1<<16)
		}
		return n
	case syntax.OpCapture:
		return countAlternatives(reg.Sub[0])
	}
	return 1
}

// hasOp reports whether reg or any of its sub-expressions is op.
func hasOp(reg *syntax.Regexp, op syntax.Op) bool {
	return reg.Op == op || slices.ContainsFunc(reg.Sub, func(sub *syntax.Regexp) bool { return hasOp(sub, op) })
}

type patternFilter struct {
	matcher *pattern.Matcher
	pattern []byte
}

func newPatternFilterer(p []byte, match bool) (MatcherFilterer, error) {
	m, err := pattern.ParseLineFilter(p)
	if err != nil {
		return nil, err
	}
	filter := &patternFilter{
		matcher: m,
		pattern: p,
	}
	if !match {
		return NewNotFilter(filter), nil
	}
	return filter, nil
}

func (f *patternFilter) Filter(line []byte) bool { return f.matcher.Test(line) }

func (f *patternFilter) Matches(test Checker) bool {
	return test.Test(f.pattern, false, false)
}

func (f *patternFilter) ToStage() Stage {
	return NewStageFunc(nil, StageHints{CanModifyLabels: false, CanModifyLine: false}, func(_ int64, line []byte, _ *LabelsBuilder) ([]byte, bool) {
		return line, f.Filter(line)
	})
}
