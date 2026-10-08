// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/stretchr/testify/require"
)

func rangeDelIndexInput(tb testing.TB, f rangeDelFixture) (*arenaskl.Arena, []rangeDelIndexOp) {
	tb.Helper()
	var size uint64 = 1024
	for _, s := range f.spans {
		size += memTableEntrySize(len(s.Start), len(s.End))
	}
	a := arenaskl.NewArena(make([]byte, size))
	skl := arenaskl.NewSkiplist(a, f.comparer.Compare)
	ops := make([]rangeDelIndexOp, 0, len(f.spans))
	for _, s := range f.spans {
		k := base.InternalKey{UserKey: s.Start, Trailer: s.Keys[0].Trailer}
		ko, vo, err := skl.AddWithOffsets(k, s.End)
		require.NoError(tb, err)
		ops = append(ops, rangeDelIndexOp{start: arenaKey{ko, uint32(len(s.Start))}, end: arenaKey{vo, uint32(len(s.End))}, trailer: k.Trailer})
	}
	return a, ops
}

// rangeDelExpandedFragment is the test oracle's flattened view of one fragment
// after pending updates are applied: its effective trailer and its tombstone
// count (depth).
type rangeDelExpandedFragment struct {
	start, end arenaKey
	trailer    base.InternalKeyTrailer
	depth      uint64
}

// rangeDelExpand is a test oracle only. Readers carry ancestors' pending
// updates without pushing them down.
func rangeDelExpand(
	n *rangeDelIntervalNode,
	inherited base.InternalKeyTrailer,
	added uint64,
	out *[]rangeDelExpandedFragment,
) {
	if n == nil {
		return
	}
	assignment := inherited
	if assignment == 0 && n.pendingTrailer != 0 {
		assignment = n.pendingTrailer
	}
	childAdded := added + n.pendingTombstones
	rangeDelExpand(n.left, assignment, childAdded, out)
	trailer := n.trailer
	if inherited != 0 {
		trailer = inherited
	}
	*out = append(*out, rangeDelExpandedFragment{n.start, n.end, trailer, n.tombstoneCount + added})
	rangeDelExpand(n.right, assignment, childAdded, out)
}

func assertRangeDelTreeInvariants(
	tb testing.TB,
	i *rangeDelIntervalIndex,
	n *rangeDelIntervalNode,
	ancestor base.InternalKeyTrailer,
) {
	tb.Helper()
	if n == nil {
		return
	}
	if ancestor != 0 {
		require.GreaterOrEqual(tb, ancestor, n.trailer)
		if n.pendingTrailer != 0 {
			require.GreaterOrEqual(tb, ancestor, n.pendingTrailer)
		}
	}
	if n.pendingTrailer != 0 {
		require.Positive(tb, n.pendingTombstones)
		require.Equal(tb, n.trailer, n.pendingTrailer)
		if ancestor == 0 {
			ancestor = n.pendingTrailer
		}
	} else {
		require.Zero(tb, n.pendingTombstones)
		require.Zero(tb, n.pendingTrailer)
	}
	require.Equal(tb, 1+max(rangeDelHeight(n.left), rangeDelHeight(n.right)), n.height)
	delta := int(rangeDelHeight(n.left)) - int(rangeDelHeight(n.right))
	require.True(tb, delta >= -1 && delta <= 1, "AVL balance %d", delta)
	assertRangeDelTreeInvariants(tb, i, n.left, ancestor)
	assertRangeDelTreeInvariants(tb, i, n.right, ancestor)
	var intervals []rangeDelExpandedFragment
	rangeDelExpand(n, 0, 0, &intervals)
	require.EqualValues(tb, len(intervals), n.fragmentCount)
	var covered uint64
	var first, last arenaKey
	for j, s := range intervals {
		require.Less(tb, i.cmp(i.arena.Bytes(s.start.off, s.start.length), i.arena.Bytes(s.end.off, s.end.length)), 0)
		if j > 0 {
			require.Zero(tb, i.cmp(i.arena.Bytes(intervals[j-1].end.off, intervals[j-1].end.length), i.arena.Bytes(s.start.off, s.start.length)))
		}
		if s.depth > 0 {
			if covered == 0 {
				first = s.start
			}
			last = s.end
			covered++
			require.NotZero(tb, s.trailer)
		} else {
			require.Zero(tb, s.trailer)
		}
	}
	require.Equal(tb, covered, n.coveredCount)
	require.Equal(tb, intervals[0].start, n.firstStart)
	require.Equal(tb, intervals[len(intervals)-1].end, n.lastEnd)
	require.Equal(tb, first, n.firstCoveredStart)
	require.Equal(tb, last, n.lastCoveredEnd)
}

func rangeDelCheckReference(
	tb testing.TB,
	i *rangeDelIntervalIndex,
	root *rangeDelIntervalNode,
	f rangeDelFixture,
	endpoints map[arenaKey]bool,
) {
	tb.Helper()
	assertRangeDelTreeInvariants(tb, i, root, 0)
	var got []rangeDelExpandedFragment
	rangeDelExpand(root, 0, 0, &got)
	want := referenceRangeDelPartition(f)
	require.Len(tb, got, len(want))
	for j, s := range got {
		require.True(tb, endpoints[s.start])
		require.True(tb, endpoints[s.end])
		require.Zero(tb, f.comparer.Compare(want[j].Start, i.arena.Bytes(s.start.off, s.start.length)))
		require.Zero(tb, f.comparer.Compare(want[j].End, i.arena.Bytes(s.end.off, s.end.length)))
		require.EqualValues(tb, len(want[j].Keys), s.depth)
		var trailer base.InternalKeyTrailer
		if len(want[j].Keys) > 0 {
			trailer = want[j].Keys[0].Trailer
		}
		require.Equal(tb, trailer, s.trailer)
	}
}

// rangeDelCountersSnapshot returns a copy of i's counters, or zero counters when
// the index does not track them (the invariants build tag is off).
func rangeDelCountersSnapshot(i *rangeDelIntervalIndex) rangeDelTreeCounters {
	if i.counters == nil {
		return rangeDelTreeCounters{}
	}
	return *i.counters
}

func rangeDelCounterDelta(a, b rangeDelTreeCounters) rangeDelTreeCounters {
	return rangeDelTreeCounters{
		comparisons:         b.comparisons - a.comparisons,
		visits:              b.visits - a.visits,
		allocations:         b.allocations - a.allocations,
		pushes:              b.pushes - a.pushes,
		rotations:           b.rotations - a.rotations,
		registryVisits:      b.registryVisits - a.registryVisits,
		registryAllocations: b.registryAllocations - a.registryAllocations,
		registryRotations:   b.registryRotations - a.registryRotations,
		publications:        b.publications - a.publications,
		pushCopies:          b.pushCopies - a.pushCopies,
		reused:              b.reused - a.reused,
		pushReused:          b.pushReused - a.pushReused,
	}
}

// rangeDelAssertWork checks that the work counted for one fused update is
// bounded by a constant multiple of the tree height. The remaining multipliers
// are measured ceilings chosen to catch complexity regressions, not derived
// bounds.
func rangeDelAssertWork(tb testing.TB, c rangeDelTreeCounters, h, vh uint8) {
	tb.Helper()
	if !invariants.Enabled {
		return
	}
	height := uint64(h) + 3
	require.LessOrEqual(tb, c.visits, 16*height)
	require.LessOrEqual(tb, c.allocations, 52*height)
	require.LessOrEqual(tb, c.comparisons, 32*height)
	require.LessOrEqual(tb, c.pushes, c.visits)
	require.LessOrEqual(tb, c.pushCopies, 2*c.pushes)
	require.LessOrEqual(tb, c.pushReused, c.reused)
	require.LessOrEqual(tb, c.rotations, 2*c.visits)
	require.LessOrEqual(tb, c.rotations, uint64(4))
	require.LessOrEqual(tb, c.registryVisits, uint64(vh)+1)
	require.LessOrEqual(tb, c.registryAllocations, uint64(vh)+6)
	require.LessOrEqual(tb, c.registryRotations, uint64(2))
}

func TestRangeDelIntervalTreeReference(t *testing.T) {
	for _, layout := range rangeDelFixtureLayouts {
		t.Run(layout, func(t *testing.T) {
			f := makeRangeDelFixture(layout, 40, DefaultComparer)
			a, ops := rangeDelIndexInput(t, f)
			i := newRangeDelIntervalIndex(a, f.comparer.Compare)
			endpoints := make(map[arenaKey]bool)
			var roots []*rangeDelIntervalNode
			for j, op := range ops {
				endpoints[op.start], endpoints[op.end] = true, true
				before := rangeDelCountersSnapshot(i)
				require.NoError(t, i.publish(base.SeqNum(j+1), base.SeqNum(j+2), ops[j:j+1]))
				s := i.state.Load()
				rangeDelAssertWork(t, rangeDelCounterDelta(before, rangeDelCountersSnapshot(i)), rangeDelHeight(s.latest.root), rangeDelVersionHeight(s.versions))
				roots = append(roots, s.latest.root)
				for k, root := range roots {
					prefix := f
					prefix.spans = f.spans[:k+1]
					rangeDelCheckReference(t, i, root, prefix, endpoints)
					selected, err := i.selectRoot(base.SeqNum(k + 2))
					require.NoError(t, err)
					require.Same(t, root, selected)
				}
			}
			t.Logf("counters: %+v", i.counters)
		})
	}
}

func TestRangeDelIntervalTreeRandom(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))
	for iter := range 20 {
		f := rangeDelFixture{comparer: DefaultComparer}
		for j := range 35 {
			a, b := rng.IntN(80), rng.IntN(80)
			f.spans = append(f.spans, keyspan.Span{Start: []byte{byte(a)}, End: []byte{byte(b)}, Keys: []keyspan.Key{{Trailer: base.MakeTrailer(base.SeqNum(j+1), base.InternalKeyKindRangeDelete)}}})
		}
		a, ops := rangeDelIndexInput(t, f)
		i := newRangeDelIntervalIndex(a, f.comparer.Compare)
		endpoints := make(map[arenaKey]bool)
		var roots []*rangeDelIntervalNode
		for j, op := range ops {
			endpoints[op.start], endpoints[op.end] = true, true
			require.NoError(t, i.publish(base.SeqNum(j+1), base.SeqNum(j+2), ops[j:j+1]))
			roots = append(roots, i.state.Load().latest.root)
			for k, root := range roots {
				prefix := f
				prefix.spans = f.spans[:k+1]
				rangeDelCheckReference(t, i, root, prefix, endpoints)
			}
		}
		t.Logf("iter %d: %+v", iter, i.counters)
	}
}

func TestRangeDelIntervalTreeExistingBoundaries(t *testing.T) {
	f := makeRangeDelFixture("full-cover", 128, DefaultComparer)
	a, ops := rangeDelIndexInput(t, f)
	i := newRangeDelIntervalIndex(a, f.comparer.Compare)
	require.NoError(t, i.publish(1, 129, ops))
	root := i.state.Load().latest.root
	var want []rangeDelExpandedFragment
	rangeDelExpand(root, 0, 0, &want)
	for _, op := range ops {
		for _, bound := range []arenaKey{op.start, op.end} {
			next := i.splitAt(root, bound)
			assertRangeDelTreeInvariants(t, i, next, 0)
			var got []rangeDelExpandedFragment
			rangeDelExpand(next, 0, 0, &got)
			require.Equal(t, want, got)
		}
	}
	if invariants.Enabled {
		require.Positive(t, i.counters.rotations)
		require.Positive(t, i.counters.pushes)
	}
}

func TestRangeDelIntervalTreeBoundaryInsertion(t *testing.T) {
	comparer := *DefaultComparer
	comparer.Compare = func(a, b []byte) int {
		return bytes.Compare(bytes.ToLower(a), bytes.ToLower(b))
	}
	comparer.Equal = func(a, b []byte) bool { return comparer.Compare(a, b) == 0 }
	for _, batch := range []int{1, 4, 16} {
		t.Run(fmt.Sprintf("batch=%d", batch), func(t *testing.T) {
			f := rangeDelFixture{comparer: &comparer}
			// The spans include bounds that are equal under the case-folding
			// comparer but not as bytes (A/a, Z/z), nested and overlapping ranges,
			// and ranges that extend the domain on either edge, so the test checks
			// that the index orders by the comparer rather than by bytes.
			for j, bounds := range [][2]string{
				{"A", "Z"}, {"a", "z"}, {"D", "w"}, {"b", "Y"},
				{"a", "z"}, {"f", "u"}, {"0", "c"}, {"x", "~"},
				{"!", "1"}, {"y", "~~"}, {"2", "3"}, {"g", "t"},
				{"a", "z"}, {"b", "D"}, {"e", "F"}, {"d", "w"},
				{" ", " !"}, {"~~~", "~~~~"},
			} {
				f.spans = append(f.spans, keyspan.Span{
					Start: []byte(bounds[0]), End: []byte(bounds[1]),
					Keys: []keyspan.Key{{
						Trailer: base.MakeTrailer(base.SeqNum(j+1), base.InternalKeyKindRangeDelete),
					}},
				})
			}
			a, ops := rangeDelIndexInput(t, f)
			i := newRangeDelIntervalIndex(a, comparer.Compare)
			endpoints := make(map[arenaKey]bool)
			var roots []*rangeDelIntervalNode
			for j := 0; j < len(ops); j += batch {
				end := min(j+batch, len(ops))
				for _, op := range ops[j:end] {
					endpoints[op.start], endpoints[op.end] = true, true
				}
				err := i.publish(base.SeqNum(j+1), base.SeqNum(end+1), ops[j:end])
				require.NoError(t, err)
				roots = append(roots, i.state.Load().latest.root)
				for k, root := range roots {
					prefix := f
					prefix.spans = f.spans[:min((k+1)*batch, len(ops))]
					rangeDelCheckReference(t, i, root, prefix, endpoints)
				}
				if batch == 1 && end == 2 {
					root := i.state.Load().latest.root
					require.EqualValues(t, 1, root.fragmentCount)
					require.Equal(t, ops[0].start, root.start)
					require.Equal(t, ops[0].end, root.end)
					require.NotZero(t, root.pendingTrailer)
				}
			}
			if invariants.Enabled {
				require.Positive(t, i.counters.pushes)
			}
		})
	}
}

func TestRangeDelIntervalTreeTaggedRotations(t *testing.T) {
	for _, order := range [][3]int{{2, 1, 0}, {2, 0, 1}, {0, 2, 1}, {0, 1, 2}} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			f := rangeDelFixture{comparer: DefaultComparer}
			for j := range 3 {
				f.spans = append(f.spans, keyspan.Span{
					Start: []byte{byte('a' + j)}, End: []byte{byte('b' + j)},
					Keys: []keyspan.Key{{
						Trailer: base.MakeTrailer(base.SeqNum(j+1), base.InternalKeyKindRangeDelete),
					}},
				})
			}
			a, ops := rangeDelIndexInput(t, f)
			i := newRangeDelIntervalIndex(a, f.comparer.Compare)
			var root *rangeDelIntervalNode
			for _, j := range order[:2] {
				leaf := i.interval(ops[j].start, ops[j].end, 0, 0)
				root = i.insertExtreme(root, leaf, j < order[0])
			}
			root = i.applyToSubtree(root, ops[2].trailer, 2)
			var frozen []rangeDelExpandedFragment
			rangeDelExpand(root, 0, 0, &frozen)
			before := rangeDelCountersSnapshot(i)
			j := order[2]
			var got *rangeDelIntervalNode
			if j < min(order[0], order[1]) || j > max(order[0], order[1]) {
				leaf := i.interval(ops[j].start, ops[j].end, 0, 0)
				got = i.insertExtreme(root, leaf, j < order[0])
			} else {
				// A successor insertion below the root forces a double rotation.
				n := i.pushDown(root)
				if j < order[0] {
					n.left = i.insertExtreme(n.left, i.interval(ops[j].start, ops[j].end, 0, 0), false)
				} else {
					n.right = i.insertExtreme(n.right, i.interval(ops[j].start, ops[j].end, 0, 0), true)
				}
				got = i.balance(n)
			}
			assertRangeDelTreeInvariants(t, i, got, 0)
			var intervals, retained []rangeDelExpandedFragment
			rangeDelExpand(got, 0, 0, &intervals)
			rangeDelExpand(root, 0, 0, &retained)
			require.Equal(t, frozen, retained)
			for k, s := range intervals {
				if k == j {
					require.Zero(t, s.depth)
					require.Zero(t, s.trailer)
				} else {
					require.EqualValues(t, 2, s.depth)
					require.Equal(t, ops[2].trailer, s.trailer)
				}
			}
			rotations := uint64(1)
			if j == 1 {
				rotations = 2
			}
			if invariants.Enabled {
				require.Equal(t, rotations, i.counters.rotations-before.rotations)
			}
		})
	}
}

func TestRangeDelIntervalTreeTagStructure(t *testing.T) {
	f := makeRangeDelFixture("disjoint", 32, DefaultComparer)
	for j, bounds := range [][2]int{{0, 31}, {5, 25}} {
		f.spans = append(f.spans, keyspan.Span{
			Start: f.spans[bounds[0]].Start, End: f.spans[bounds[1]].End,
			Keys: []keyspan.Key{{
				Trailer: base.MakeTrailer(base.SeqNum(33+j), base.InternalKeyKindRangeDelete),
			}},
		})
	}
	a, ops := rangeDelIndexInput(t, f)
	i := newRangeDelIntervalIndex(a, f.comparer.Compare)
	require.NoError(t, i.publish(1, 33, ops[:32]))
	endpoints := make(map[arenaKey]bool)
	for _, op := range ops {
		endpoints[op.start], endpoints[op.end] = true, true
	}
	var checkShape func(*rangeDelIntervalNode, *rangeDelIntervalNode)
	checkShape = func(old, next *rangeDelIntervalNode) {
		if old == nil {
			require.Nil(t, next)
			return
		}
		require.NotNil(t, next)
		require.Equal(t, old.start, next.start)
		require.Equal(t, old.end, next.end)
		require.Equal(t, old.height, next.height)
		checkShape(old.left, next.left)
		checkShape(old.right, next.right)
	}
	for j := 32; j < len(ops); j++ {
		old := i.state.Load().latest.root
		before := rangeDelCountersSnapshot(i)
		require.NoError(t, i.publish(base.SeqNum(j+1), base.SeqNum(j+2), ops[j:j+1]))
		root := i.state.Load().latest.root
		checkShape(old, root)
		if invariants.Enabled {
			require.Equal(t, before.rotations, i.counters.rotations)
		}
		prefix := f
		prefix.spans = f.spans[:j]
		rangeDelCheckReference(t, i, old, prefix, endpoints)
		prefix.spans = f.spans[:j+1]
		rangeDelCheckReference(t, i, root, prefix, endpoints)
		if j == 32 {
			require.NotZero(t, root.pendingTrailer)
		}
	}
}

func TestRangeDelIntervalTreeComplexity(t *testing.T) {
	for _, layout := range []string{"disjoint", "concentric", "shared-start", "staircase", "random", "full-cover"} {
		for _, n := range []int{100, 1000, 10_000, 100_000} {
			f := makeRangeDelFixture(layout, n, DefaultComparer)
			a, ops := rangeDelIndexInput(t, f)
			i := newRangeDelIntervalIndex(a, f.comparer.Compare)
			for j := range ops {
				before := rangeDelCountersSnapshot(i)
				s := i.state.Load()
				var h, vh uint8
				if s != nil {
					h = rangeDelHeight(s.latest.root)
					vh = rangeDelVersionHeight(s.versions)
				}
				require.NoError(t, i.publish(base.SeqNum(j+1), base.SeqNum(j+2), ops[j:j+1]))
				s = i.state.Load()
				h = max(h, rangeDelHeight(s.latest.root))
				rangeDelAssertWork(t, rangeDelCounterDelta(before, rangeDelCountersSnapshot(i)), h, vh)
			}
			s := i.state.Load()
			require.LessOrEqual(t, s.latest.root.fragmentCount, uint64(2*n-1))
			// AVL minimum nodes grows by Fibonacci recurrence, so height is below
			// twice ceil(log2(N+1)). Sum the per-update bound at the final height.
			log := uint64(0)
			for v := uint64(2 * n); v > 0; v >>= 1 {
				log++
			}
			require.LessOrEqual(t, uint64(s.latest.root.height), 2*log)
			if invariants.Enabled {
				// Measured ceilings that catch complexity regressions, not derived
				// bounds.
				require.LessOrEqual(t, i.counters.visits, uint64(n)*16*(2*log+3))
				require.LessOrEqual(t, i.counters.allocations, uint64(n)*52*(2*log+3))
			}
			t.Logf("%s n=%d height=%d counters=%+v", layout, n, s.latest.root.height, i.counters)
		}
	}
}

func rangeDelCheckVersions(t *testing.T, n *rangeDelVersionNode) {
	if n == nil {
		return
	}
	require.Equal(t, 1+max(rangeDelVersionHeight(n.left), rangeDelVersionHeight(n.right)), n.height)
	delta := int(rangeDelVersionHeight(n.left)) - int(rangeDelVersionHeight(n.right))
	require.True(t, delta >= -1 && delta <= 1)
	if n.left != nil {
		require.Less(t, n.left.end, n.end)
	}
	if n.right != nil {
		require.Greater(t, n.right.end, n.end)
	}
	rangeDelCheckVersions(t, n.left)
	rangeDelCheckVersions(t, n.right)
}

func TestRangeDelIntervalRegistry(t *testing.T) {
	f := makeRangeDelFixture("disjoint", 20, DefaultComparer)
	// Include point-only sequence gaps and multi-record, all-invalid batches.
	for j := range f.spans {
		f.spans[j].Keys[0].Trailer = base.MakeTrailer(base.SeqNum(10*j), base.InternalKeyKindRangeDelete)
	}
	f.spans[8].End = bytes.Clone(f.spans[8].Start)
	a, ops := rangeDelIndexInput(t, f)
	i := newRangeDelIntervalIndex(a, f.comparer.Compare)
	root, err := i.selectRoot(0)
	require.NoError(t, err)
	require.Nil(t, root)
	var oldStates []*rangeDelIndexState
	for j := range ops {
		start := base.SeqNum(10 * j)
		previous, err := i.selectRoot(start)
		require.NoError(t, err)
		require.NoError(t, i.publish(start, start+3, ops[j:j+1]))
		s := i.state.Load()
		oldStates = append(oldStates, s)
		atStart, err := i.selectRoot(start)
		require.NoError(t, err)
		require.Same(t, previous, atStart)
		_, err = i.selectRoot(start + 1)
		require.Error(t, err)
		atEnd, err := i.selectRoot(start + 3)
		require.NoError(t, err)
		require.Same(t, s.latest.root, atEnd)
		for _, old := range oldStates {
			rangeDelCheckVersions(t, old.versions)
		}
	}
	old := i.state.Load()
	require.Error(t, i.publish(0, 2, ops[:1]))
	require.Same(t, old, i.state.Load())
	require.Error(t, i.publish(200, 201, nil))
	require.Same(t, old, i.state.Load())
	require.Error(t, i.publish(200, 200, ops[:1]))
	require.Same(t, old, i.state.Load())
	require.Error(t, i.publish(200, 201, ops[:1]))
	require.Same(t, old, i.state.Load())
	batchOp := ops[0]
	batchOp.trailer = base.MakeTrailer(base.SeqNumBatchBit, base.InternalKeyKindRangeDelete)
	require.Error(t, i.publish(base.SeqNumBatchBit, base.SeqNumBatchBit+1, []rangeDelIndexOp{batchOp}))
	require.Same(t, old, i.state.Load())
	root, err = i.selectRoot(base.SeqNumMax)
	require.NoError(t, err)
	require.Same(t, old.latest.root, root)
}

func TestRangeDelIntervalAccounting(t *testing.T) {
	require.EqualValues(t, 120, unsafe.Sizeof(rangeDelIntervalNode{}))
	f := makeRangeDelFixture("concentric", 20, DefaultComparer)
	a, ops := rangeDelIndexInput(t, f)
	i := newRangeDelIntervalIndex(a, f.comparer.Compare)
	require.NoError(t, i.publish(1, 21, ops))
}

func TestRangeDelBatchReuse(t *testing.T) {
	f := makeRangeDelFixture("concentric", 16, DefaultComparer)
	a, ops := rangeDelIndexInput(t, f)
	i := newRangeDelIntervalIndex(a, f.comparer.Compare)
	require.NoError(t, i.publish(1, 2, ops[:1]))
	require.EqualValues(t, 1, i.epoch)
	old := i.state.Load()
	before := rangeDelCountersSnapshot(i)
	require.Error(t, i.publish(0, 1, ops[:1]))
	require.EqualValues(t, 1, i.epoch)
	require.Equal(t, before, rangeDelCountersSnapshot(i))
	require.NoError(t, i.publish(2, 17, ops[1:]))
	// A batch advances the epoch once, however many ops it contains.
	require.EqualValues(t, 2, i.epoch)
	require.EqualValues(t, 2, i.batchEpoch)
	if invariants.Enabled {
		require.Positive(t, i.counters.reused)
	}
	require.Less(t, old.latest.root.epoch, i.batchEpoch)
	prefix := f
	prefix.spans = f.spans[:1]
	endpoints := map[arenaKey]bool{ops[0].start: true, ops[0].end: true}
	rangeDelCheckReference(t, i, old.latest.root, prefix, endpoints)
}

func TestRangeDelEpochOverflow(t *testing.T) {
	f := makeRangeDelFixture("disjoint", 3, DefaultComparer)
	a, ops := rangeDelIndexInput(t, f)
	i := newRangeDelIntervalIndex(a, f.comparer.Compare)
	i.epoch = ^uint32(0) - 1
	require.NoError(t, i.publish(1, 2, ops[:1]))
	require.False(t, i.epochExhausted)
	require.EqualValues(t, ^uint32(0), i.epoch)
	var reused uint64
	if invariants.Enabled {
		reused = i.counters.reused
	}
	require.NoError(t, i.publish(2, 3, ops[1:2]))
	require.True(t, i.epochExhausted)
	require.Zero(t, i.epoch)
	require.NoError(t, i.publish(3, 4, ops[2:]))
	require.Zero(t, i.epoch)
	if invariants.Enabled {
		// Wrap permanently disables in-place reuse.
		require.Equal(t, reused, i.counters.reused)
		require.Positive(t, i.counters.allocations)
	}
	endpoints := make(map[arenaKey]bool)
	for _, op := range ops {
		endpoints[op.start], endpoints[op.end] = true, true
	}
	rangeDelCheckReference(t, i, i.state.Load().latest.root, f, endpoints)
}

func TestRangeDelBatchOwnership(t *testing.T) {
	i := newRangeDelIntervalIndex(nil, nil)
	i.epoch, i.batchEpoch, i.batchActive = 1, 1, true
	leaf := i.interval(arenaKey{}, arenaKey{}, 0, 0)
	require.Same(t, leaf, i.copy(leaf))
	if invariants.Enabled {
		require.EqualValues(t, 1, i.counters.allocations)
		require.EqualValues(t, 1, i.counters.reused)
	}
	i.advanceEpoch()
	require.Same(t, leaf, i.copy(leaf))
	if invariants.Enabled {
		require.EqualValues(t, 2, i.counters.reused)
	}
	i.batchActive = false
	require.NotSame(t, leaf, i.copy(leaf))
	i.batchActive, i.batchEpoch = true, i.epoch
	require.NotSame(t, leaf, i.copy(leaf))
	i.epoch = ^uint32(0)
	i.advanceEpoch()
	require.NotSame(t, leaf, i.copy(leaf))
}

func TestRangeDelBatchOwnershipReference(t *testing.T) {
	layouts := append(append([]string(nil), rangeDelIndexLayouts...), "invalid")
	for _, layout := range layouts {
		for _, batch := range []int{1, 4, 16} {
			t.Run(fmt.Sprintf("%s/batch=%d", layout, batch), func(t *testing.T) {
				f := makeRangeDelIndexFixture(layout, 48)
				a, ops := rangeDelIndexInput(t, f)
				i := newRangeDelIntervalIndex(a, f.comparer.Compare)
				endpoints := make(map[arenaKey]bool)
				frozen := make(map[*rangeDelIntervalNode]rangeDelIntervalNode)
				var roots []*rangeDelIntervalNode
				var freeze func(*rangeDelIntervalNode)
				freeze = func(n *rangeDelIntervalNode) {
					if n == nil {
						return
					}
					if _, ok := frozen[n]; ok {
						return
					}
					require.LessOrEqual(t, n.epoch, i.epoch)
					frozen[n] = *n
					freeze(n.left)
					freeze(n.right)
				}
				for j := 0; j < len(ops); j += batch {
					end := min(j+batch, len(ops))
					for _, op := range ops[j:end] {
						endpoints[op.start], endpoints[op.end] = true, true
					}
					require.NoError(t, i.publish(base.SeqNum(j+1), base.SeqNum(end+1), ops[j:end]))
					require.False(t, i.batchActive)
					for n, want := range frozen {
						require.Equal(t, want, *n, "published node mutated")
					}
					root := i.state.Load().latest.root
					freeze(root)
					roots = append(roots, root)
					for k, retained := range roots {
						prefix := f
						prefix.spans = f.spans[:min((k+1)*batch, len(ops))]
						rangeDelCheckReference(t, i, retained, prefix, endpoints)
					}
				}
			})
		}
	}
}

func TestRangeDelBatchOwnershipPanic(t *testing.T) {
	f := makeRangeDelFixture("shared-start", 8, DefaultComparer)
	a, ops := rangeDelIndexInput(t, f)
	i := newRangeDelIntervalIndex(a, f.comparer.Compare)
	require.NoError(t, i.publish(1, 5, ops[:4]))
	old := i.state.Load()
	i.cmp = func(a, b []byte) int { panic("comparison failed") }
	require.Panics(t, func() { _ = i.publish(5, 9, ops[4:]) })
	require.False(t, i.batchActive)
	require.Same(t, old, i.state.Load())
	i.cmp = f.comparer.Compare
	require.NoError(t, i.publish(5, 9, ops[4:]))
}

func TestRangeDelIntervalAllocatorLayout(t *testing.T) {
	const count = 10_000
	nodes := make([]*rangeDelIntervalNode, count)
	versions := make([]*rangeDelVersionNode, count)
	states := make([]*rangeDelIndexState, count)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for j := range nodes {
		nodes[j] = new(rangeDelIntervalNode)
	}
	runtime.ReadMemStats(&after)
	nodeBytes := (after.TotalAlloc - before.TotalAlloc) / count
	before = after
	for j := range versions {
		versions[j] = new(rangeDelVersionNode)
	}
	runtime.ReadMemStats(&after)
	versionBytes := (after.TotalAlloc - before.TotalAlloc) / count
	before = after
	for j := range states {
		states[j] = new(rangeDelIndexState)
	}
	runtime.ReadMemStats(&after)
	stateBytes := (after.TotalAlloc - before.TotalAlloc) / count
	// Header/size-class overhead is runtime evidence, not payload.
	t.Logf("payload/runtime interval=%d/%d registry=%d/%d state=%d/%d", unsafe.Sizeof(rangeDelIntervalNode{}), nodeBytes, unsafe.Sizeof(rangeDelVersionNode{}), versionBytes, unsafe.Sizeof(rangeDelIndexState{}), stateBytes)
	runtime.KeepAlive(nodes)
	runtime.KeepAlive(versions)
	runtime.KeepAlive(states)
}

func TestRangeDelIntervalConcurrentPublication(t *testing.T) {
	f := makeRangeDelFixture("shared-start", 500, DefaultComparer)
	a, ops := rangeDelIndexInput(t, f)
	i := newRangeDelIntervalIndex(a, f.comparer.Compare)
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			for range 500 {
				s := i.state.Load()
				if s == nil {
					continue
				}
				// The threshold follows the loaded state; it never uses a newer
				// threshold with a captured older registry.
				root, err := i.selectRoot(s.latest.end)
				require.NoError(t, err)
				require.NotNil(t, root)
				require.EqualValues(t, s.latest.end-1, root.fragmentCount)
				var intervals []rangeDelExpandedFragment
				rangeDelExpand(s.latest.root, 0, 0, &intervals)
				require.Len(t, intervals, int(s.latest.end-1))
				for j, interval := range intervals {
					require.EqualValues(t, len(intervals)-j, interval.depth)
				}
			}
		})
	}
	for j := 0; j < len(ops); j += 4 {
		end := min(j+4, len(ops))
		require.NoError(t, i.publish(base.SeqNum(j+1), base.SeqNum(end+1), ops[j:end]))
	}
	readers.Wait()
}
