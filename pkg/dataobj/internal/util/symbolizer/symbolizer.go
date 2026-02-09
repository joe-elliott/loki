// Package symbolizer provides a string interning mechanism to reduce memory usage
// by reusing identical strings.
//
// The Symbolizer maintains a cache of strings and returns the same instance
// when the same string is requested multiple times. This reduces memory usage
// when dealing with repeated strings, such as label names or values. It is not
// thread safe.
//
// When the cache exceeds the maximum size, a small percentage of entries are
// randomly discarded to keep memory usage under control.
package symbolizer

import (
	"strings"
)

// New creates a new Symbolizer with the given initial capacity and maximum size.
func New(initialCapacity int, maxSize int) *Symbolizer {
	return &Symbolizer{
		symbols:   make(map[string]string, initialCapacity),
		maxSize:   maxSize,
		fifoQueue: make([]string, 0, maxSize/10), // Pre-allocate for eviction tracking
	}
}

type Symbolizer struct {
	symbols   map[string]string
	maxSize   int
	fifoQueue []string // Tracks insertion order for FIFO eviction
}

// Get returns a string from the symbolizer. If the string is not in the cache,
// a clone is inserted into the cache and returned.
//
// Get may delete some values from the cache prior to inserting a new value if
// the maximum size is exceeded. Eviction uses a FIFO strategy for better
// predictability and cache efficiency.
func (s *Symbolizer) Get(name string) string {
	if value, ok := s.symbols[name]; ok {
		return value
	}

	// Control maximum memory use by discarding oldest 1% of symbols using FIFO when map gets too big.
	// This provides better cache coherency than random eviction.
	if len(s.symbols) > s.maxSize {
		evictCount := max(1, s.maxSize/100)
		// If queue is smaller than evict count, fall back to clearing entire queue
		if len(s.fifoQueue) < evictCount {
			for _, k := range s.fifoQueue {
				delete(s.symbols, k)
			}
			s.fifoQueue = s.fifoQueue[:0]
		} else {
			// Evict oldest entries
			for i := 0; i < evictCount; i++ {
				delete(s.symbols, s.fifoQueue[i])
			}
			// Shift queue to remove evicted entries
			copy(s.fifoQueue, s.fifoQueue[evictCount:])
			s.fifoQueue = s.fifoQueue[:len(s.fifoQueue)-evictCount]
		}
	}

	newString := strings.Clone(name)
	s.symbols[newString] = newString
	s.fifoQueue = append(s.fifoQueue, newString)
	return newString
}

// Reset clears the cache and resets the Symbolizer to its initial state,
// maintaining the existing maxSize.
func (s *Symbolizer) Reset() {
	clear(s.symbols)
	s.fifoQueue = s.fifoQueue[:0]
}
