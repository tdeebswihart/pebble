// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/stretchr/testify/require"
)

func TestRangeDelIntervalIter(t *testing.T) {
	for _, layout := range rangeDelFixtureLayouts {
		t.Run(layout, func(t *testing.T) {
			f := makeRangeDelFixture(layout, 40, DefaultComparer)
			a, ops := rangeDelIndexInput(t, f)
			index := buildRangeDelIndex(t, a, f.comparer.Compare, ops, 8)
			for _, threshold := range []base.SeqNum{1, 9, 17, 41, base.SeqNumMax} {
				root, err := index.selectRoot(threshold)
				require.NoError(t, err)
				it := &memTableRangeDelIter{index: index, root: root, exhausted: -1}
				prefix := f
				prefix.spans = f.spans[:min(int(threshold-1), len(f.spans))]
				var projected []keyspan.Span
				for _, span := range referenceRangeDelPartition(prefix) {
					if len(span.Keys) != 0 {
						span.Keys = span.Keys[:1]
						projected = append(projected, span)
					}
				}
				ref := keyspan.NewIter(f.comparer.Compare, projected)
				check := func(got, want *keyspan.Span) {
					t.Helper()
					if want == nil {
						require.Nil(t, got)
						return
					}
					require.NotNil(t, got)
					require.Zero(t, f.comparer.Compare(got.Start, want.Start))
					require.Zero(t, f.comparer.Compare(got.End, want.End))
					require.Equal(t, want.Keys, got.Keys)
					require.Equal(t, keyspan.ByTrailerDesc, got.KeysOrder)
				}
				compareCall := func(a, b func() (*keyspan.Span, error)) {
					got, err := a()
					require.NoError(t, err)
					want, err := b()
					require.NoError(t, err)
					check(got, want)
				}
				before := rangeDelCountersSnapshot(index)
				compareCall(it.First, ref.First)
				for range len(projected) + 1 {
					compareCall(it.Next, ref.Next)
				}
				for range len(projected) + 2 {
					compareCall(it.Prev, ref.Prev)
				}
				rng := rand.New(rand.NewPCG(6207, uint64(threshold)))
				for range 500 {
					span := f.spans[rng.IntN(len(f.spans))]
					key := span.Start
					if rng.IntN(2) == 0 {
						key = span.End
					}
					switch rng.IntN(6) {
					case 0:
						compareCall(func() (*keyspan.Span, error) { return it.SeekGE(key) },
							func() (*keyspan.Span, error) { return ref.SeekGE(key) })
					case 1:
						compareCall(func() (*keyspan.Span, error) { return it.SeekLT(key) },
							func() (*keyspan.Span, error) { return ref.SeekLT(key) })
					case 2:
						compareCall(it.First, ref.First)
					case 3:
						compareCall(it.Last, ref.Last)
					case 4:
						compareCall(it.Next, ref.Next)
					case 5:
						compareCall(it.Prev, ref.Prev)
					}
				}
				require.Equal(t, before, rangeDelCountersSnapshot(index))
				it.Close()
				require.Nil(t, it.root)
				require.Nil(t, it.index)
				require.Nil(t, it.path)
			}
		})
	}
}
