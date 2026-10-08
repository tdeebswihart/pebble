// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

var rangeDelIndexLayouts = []string{"disjoint", "concentric", "shared-start", "staircase", "random", "full-cover", "cycle", "unique-0", "unique-0.01", "unique-0.1", "unique-1"}

// Mirror bench/rangedel.go's single-writer queue rule with PCG(6207, 1).
// Cycle uses its 500 even deletion slots; point sets and scheduling are excluded.
func makeRangeDelIndexFixture(layout string, n int) rangeDelFixture {
	if layout != "cycle" && !strings.HasPrefix(layout, "unique-") {
		return makeRangeDelFixture(layout, n, DefaultComparer)
	}
	f := rangeDelFixture{comparer: &cockroachkvs.Comparer, spans: make([]keyspan.Span, 0, n)}
	rng := rand.New(rand.NewPCG(6207, 1))
	acks := make([]uint64, 1024)
	frac := float64(0)
	if layout != "cycle" {
		var err error
		frac, err = strconv.ParseFloat(layout[7:], 64)
		if err != nil {
			panic(err)
		}
	}
	key := func(q int, ack uint64) []byte {
		return cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "d/q%06d/%012d", q, ack), 0, 0)
	}
	for j := range n {
		var start, end []byte
		if layout == "cycle" {
			start = cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "d/%d/a", (2*j)%1000), 0, 0)
			end = cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "d/%d/z", (2*j)%1000), 0, 0)
		} else {
			q := rng.IntN(len(acks))
			ack := acks[q]
			if ack > 0 && rng.Float64() < frac {
				start, end = key(q, 0), key(q, ack)
			} else {
				next := ack + 1 + rng.Uint64N(1000)
				acks[q] = next
				start, end = key(q, ack), key(q, next)
			}
		}
		f.spans = append(f.spans, keyspan.Span{Start: start, End: end, Keys: []keyspan.Key{{Trailer: base.MakeTrailer(base.SeqNum(j+1), base.InternalKeyKindRangeDelete)}}})
	}
	return f
}

func buildRangeDelIndex(
	tb testing.TB, a *arenaskl.Arena, cmp base.Compare, ops []rangeDelIndexOp, batch int,
) *rangeDelIntervalIndex {
	tb.Helper()
	i := newRangeDelIntervalIndex(a, cmp)
	for j := 0; j < len(ops); j += batch {
		end := min(j+batch, len(ops))
		if err := i.publish(base.SeqNum(j+1), base.SeqNum(end+1), ops[j:end]); err != nil {
			tb.Fatal(err)
		}
	}
	return i
}

func BenchmarkRangeDelIntervalIndex(b *testing.B) {
	for _, layout := range rangeDelIndexLayouts {
		for _, n := range []int{100, 1000, 10_000, 100_000} {
			for _, batch := range rangeDelIndexBatchSizes(n) {
				b.Run(fmt.Sprintf("%s/n=%d/batch=%d", layout, n, batch), func(b *testing.B) {
					f := makeRangeDelIndexFixture(layout, n)
					a, ops := rangeDelIndexInput(b, f)
					b.ReportAllocs()
					b.ResetTimer()
					var i *rangeDelIntervalIndex
					for range b.N {
						i = buildRangeDelIndex(b, a, f.comparer.Compare, ops, batch)
					}
					b.StopTimer()
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/tombstone")
					if invariants.Enabled {
						b.ReportMetric(float64(i.counters.allocations)/float64(n), "nodes/tombstone")
						b.ReportMetric(float64(i.counters.pushCopies)/float64(n), "push-copies/tombstone")
						b.ReportMetric(float64(i.counters.reused)/float64(n), "reused/tombstone")
						b.ReportMetric(float64(i.counters.pushReused)/float64(n), "push-reused/tombstone")
						b.ReportMetric(float64(i.counters.visits)/float64(n), "visits/tombstone")
						b.ReportMetric(float64(i.counters.registryAllocations)/float64(n), "registry-nodes/tombstone")
					}
					runtime.KeepAlive(a)
				})
			}
		}
	}
}

func rangeDelIndexBatchSizes(n int) []int {
	if n == 100_000 {
		return []int{1, 16, 64, 256, 1000}
	}
	return []int{1, 16, 1000}
}
