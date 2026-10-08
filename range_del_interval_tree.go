// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// arenaKey locates a user key stored in the memtable arena. The arena must stay
// alive while it is read.
type arenaKey struct{ off, length uint32 }

// rangeDelIntervalNode owns one fragment of a non-overlapping partition of the
// indexed key space. Fragment edges fall at every tombstone start and end key,
// so every key in a fragment is covered by exactly the same set of tombstones.
// Published nodes are immutable. Only the active batch may mutate its private
// nodes before publishing the final root.
type rangeDelIntervalNode struct {
	// Children contain fragments ordered by start key.
	left, right *rangeDelIntervalNode
	// start and end bound this node's half-open fragment [start, end).
	start, end arenaKey
	// trailer is the newest RANGEDEL trailer covering this fragment, or zero
	// when the fragment is uncovered.
	trailer base.InternalKeyTrailer
	// tombstoneCount counts tombstones covering this fragment.
	tombstoneCount uint64
	// A pending update already affects this fragment and its aggregates but
	// has not been pushed down to its children. pendingTrailer is its newest
	// trailer; pendingTombstones is the count to add to each descendant
	// fragment.
	// A nonzero pendingTrailer marks an update because indexed trailers are
	// always nonzero.
	pendingTrailer    base.InternalKeyTrailer
	pendingTombstones uint64
	// fragmentCount is the number of fragments in this subtree, including
	// uncovered ones.
	fragmentCount uint64
	// coveredCount counts covered fragments in this subtree. A fragment is
	// covered when at least one tombstone covers it.
	coveredCount uint64
	// firstStart and lastEnd bound every fragment in this subtree.
	firstStart, lastEnd arenaKey
	// firstCoveredStart and lastCoveredEnd bound covered fragments in this
	// subtree. Both are zero when coveredCount is zero.
	firstCoveredStart, lastCoveredEnd arenaKey
	// height is the AVL height of this subtree.
	height uint8
	// epoch identifies the creating batch so the active batch can recognize its
	// private nodes. Zero disables in-place reuse.
	epoch uint32
}

// rangeDelVersionNode maps an indexed batch's sequence range [start, end) to
// its published interval root. The persistent AVL registry is keyed by end so
// readers can select the newest batch ending at or below their snapshot.
type rangeDelVersionNode struct {
	left, right *rangeDelVersionNode
	root        *rangeDelIntervalNode
	start, end  base.SeqNum
	height      uint8
}

// rangeDelIndexState is an immutable pair of the registry root and its newest
// entry. An atomic pointer publishes both together; readers at or above
// latest.end can use latest.root without searching the registry.
type rangeDelIndexState struct {
	versions, latest *rangeDelVersionNode
}

// rangeDelTreeCounters records structural work for tests and benchmarks.
// Updates to these counters occur only when invariants.Enabled.
type rangeDelTreeCounters struct {
	comparisons, visits, allocations, pushes, rotations    uint64
	registryVisits, registryAllocations, registryRotations uint64
	publications                                           uint64
	pushCopies                                             uint64
	reused, pushReused                                     uint64
}

// rangeDelIndexOp holds one tombstone's arena bounds and RANGEDEL trailer.
type rangeDelIndexOp struct {
	start, end arenaKey
	trailer    base.InternalKeyTrailer
}

// rangeDelIntervalIndex stores memtable range deletions in a persistent AVL
// tree. The memtable's rangeDelTail chain serializes writers: each batch waits
// for its predecessor before publishing. Readers access only published state
// and the arena.
type rangeDelIntervalIndex struct {
	arena    *arenaskl.Arena
	cmp      base.Compare
	state    atomic.Pointer[rangeDelIndexState]
	terminal atomic.Pointer[error]
	// counters is non-nil only when invariants.Enabled.
	counters *rangeDelTreeCounters
	// epoch advances once per batch, at batch start, so a node's epoch
	// identifies its creating batch; batchEpoch separates the active batch's
	// private nodes from published nodes. While batchActive is set, copy
	// mutates private nodes in place instead of cloning them, which is safe
	// because readers cannot reach them until publication. batchActive is
	// cleared before publication or on panic, and rejected batches do not
	// advance epoch. Epoch wrap permanently disables in-place reuse.
	epoch, batchEpoch uint32
	epochExhausted    bool
	batchActive       bool
}

func (i *rangeDelIntervalIndex) fail(err error) {
	i.terminal.CompareAndSwap(nil, &err)
}

func (i *rangeDelIntervalIndex) err() error {
	if e := i.terminal.Load(); e != nil {
		return *e
	}
	return nil
}

func newRangeDelIntervalIndex(arena *arenaskl.Arena, cmp base.Compare) *rangeDelIntervalIndex {
	i := &rangeDelIntervalIndex{arena: arena, cmp: cmp}
	if invariants.Enabled {
		i.counters = &rangeDelTreeCounters{}
	}
	return i
}

func (i *rangeDelIntervalIndex) compare(a, b arenaKey) int {
	if invariants.Enabled {
		i.counters.comparisons++
	}
	return i.cmp(i.arena.Bytes(a.off, a.length), i.arena.Bytes(b.off, b.length))
}

func rangeDelHeight(n *rangeDelIntervalNode) uint8 {
	if n == nil {
		return 0
	}
	return n.height
}

// isPrivate reports whether the active batch owns n and may mutate it in place.
func (i *rangeDelIntervalIndex) isPrivate(n *rangeDelIntervalNode) bool {
	return i.batchActive && i.epoch != 0 && n.epoch >= i.batchEpoch
}

// copy returns a mutable node, reusing n only when the active batch owns it.
// Tree operations consume private nodes, so callers must not retain an
// intermediate root within a batch.
func (i *rangeDelIntervalIndex) copy(n *rangeDelIntervalNode) *rangeDelIntervalNode {
	if i.isPrivate(n) {
		if invariants.Enabled {
			i.counters.reused++
		}
		return n
	}
	if invariants.Enabled {
		i.counters.allocations++
	}
	c := *n
	c.epoch = i.epoch
	return &c
}

// advanceEpoch starts a new ownership epoch until the counter wraps. After
// wrap, epoch zero prevents old nodes from appearing private.
func (i *rangeDelIntervalIndex) advanceEpoch() {
	if i.epochExhausted {
		return
	}
	i.epoch++
	if i.epoch == 0 {
		i.epochExhausted = true
	}
}

// interval creates a private leaf for the half-open fragment [a, b).
func (i *rangeDelIntervalIndex) interval(
	a, b arenaKey, trailer base.InternalKeyTrailer, tombstoneCount uint64,
) *rangeDelIntervalNode {
	// A temporary passed through copy could escape and add an allocation.
	if invariants.Enabled {
		i.counters.allocations++
	}
	n := &rangeDelIntervalNode{start: a, end: b, trailer: trailer, tombstoneCount: tombstoneCount, epoch: i.epoch}
	rangeDelPull(n)
	return n
}

// rangeDelPull restores height and subtree aggregates when a private node's
// fragment or children change.
func rangeDelPull(n *rangeDelIntervalNode) {
	n.height = 1 + max(rangeDelHeight(n.left), rangeDelHeight(n.right))
	n.fragmentCount = 1
	n.firstStart, n.lastEnd = n.start, n.end
	n.coveredCount = 0
	n.firstCoveredStart, n.lastCoveredEnd = arenaKey{}, arenaKey{}
	if n.tombstoneCount > 0 {
		n.coveredCount = 1
		n.firstCoveredStart, n.lastCoveredEnd = n.start, n.end
	}
	if l := n.left; l != nil {
		n.fragmentCount += l.fragmentCount
		n.coveredCount += l.coveredCount
		n.firstStart = l.firstStart
		if l.coveredCount > 0 {
			n.firstCoveredStart = l.firstCoveredStart
			if n.tombstoneCount == 0 {
				n.lastCoveredEnd = l.lastCoveredEnd
			}
		}
	}
	if r := n.right; r != nil {
		if n.coveredCount == 0 && r.coveredCount > 0 {
			n.firstCoveredStart = r.firstCoveredStart
		}
		n.fragmentCount += r.fragmentCount
		n.coveredCount += r.coveredCount
		n.lastEnd = r.lastEnd
		if r.coveredCount > 0 {
			n.lastCoveredEnd = r.lastCoveredEnd
		}
	}
}

// applyToSubtree adds count tombstones and assigns trailer across n's entire
// subtree. A pending update lets full subtree coverage take constant time.
func (i *rangeDelIntervalIndex) applyToSubtree(
	n *rangeDelIntervalNode, trailer base.InternalKeyTrailer, count uint64,
) *rangeDelIntervalNode {
	if n == nil {
		return nil
	}
	c := i.copy(n)
	c.trailer, c.pendingTrailer = trailer, trailer
	c.tombstoneCount += count
	c.pendingTombstones += count
	c.coveredCount = c.fragmentCount
	c.firstCoveredStart, c.lastCoveredEnd = c.firstStart, c.lastEnd
	return c
}

// pushDown returns a private node whose pending update has been pushed down to
// its children. Call it before changing child links so an old update cannot
// reach newly attached fragments. The iterator gives an ancestor's pending
// trailer precedence over a descendant's own trailer, so partial updates push
// pending updates down the path before changing a descendant.
func (i *rangeDelIntervalIndex) pushDown(n *rangeDelIntervalNode) *rangeDelIntervalNode {
	if invariants.Enabled {
		i.counters.visits++
	}
	c := i.copy(n)
	if c.pendingTrailer != 0 {
		var allocations, reused uint64
		if invariants.Enabled {
			i.counters.pushes++
			allocations = i.counters.allocations
			reused = i.counters.reused
		}
		c.left = i.applyToSubtree(c.left, c.pendingTrailer, c.pendingTombstones)
		c.right = i.applyToSubtree(c.right, c.pendingTrailer, c.pendingTombstones)
		if invariants.Enabled {
			i.counters.pushCopies += i.counters.allocations - allocations
			i.counters.pushReused += i.counters.reused - reused
		}
		c.pendingTrailer = 0
		c.pendingTombstones = 0
	}
	return c
}

// rotateLeft rebalances at private node n. It pushes down the rotated child's
// pending update before changing its links so pending updates keep their
// original coverage.
func (i *rangeDelIntervalIndex) rotateLeft(n *rangeDelIntervalNode) *rangeDelIntervalNode {
	if invariants.Enabled {
		i.counters.rotations++
	}
	r := i.pushDown(n.right)
	n.right = r.left
	rangeDelPull(n)
	r.left = n
	rangeDelPull(r)
	return r
}

// rotateRight is the mirror of rotateLeft.
func (i *rangeDelIntervalIndex) rotateRight(n *rangeDelIntervalNode) *rangeDelIntervalNode {
	if invariants.Enabled {
		i.counters.rotations++
	}
	l := i.pushDown(n.left)
	n.left = l.right
	rangeDelPull(n)
	l.right = n
	rangeDelPull(l)
	return l
}

// balance restores the AVL height bound at private node n.
func (i *rangeDelIntervalIndex) balance(n *rangeDelIntervalNode) *rangeDelIntervalNode {
	rangeDelPull(n)
	if int(rangeDelHeight(n.left))-int(rangeDelHeight(n.right)) > 1 {
		if rangeDelHeight(n.left.right) > rangeDelHeight(n.left.left) {
			n.left = i.rotateLeft(i.pushDown(n.left))
		}
		return i.rotateRight(n)
	}
	if int(rangeDelHeight(n.right))-int(rangeDelHeight(n.left)) > 1 {
		if rangeDelHeight(n.right.left) > rangeDelHeight(n.right.right) {
			n.right = i.rotateRight(i.pushDown(n.right))
		}
		return i.rotateLeft(n)
	}
	return n
}

// insertExtreme attaches a known edge fragment without searching by key.
// first selects the left edge; otherwise the leaf belongs at the right edge.
func (i *rangeDelIntervalIndex) insertExtreme(
	t, leaf *rangeDelIntervalNode, first bool,
) *rangeDelIntervalNode {
	if t == nil {
		return leaf
	}
	n := i.pushDown(t)
	if first {
		n.left = i.insertExtreme(n.left, leaf, first)
	} else {
		n.right = i.insertExtreme(n.right, leaf, first)
	}
	return i.balance(n)
}

// splitAt splits the fragment strictly containing x into [start, x) and
// [x, end). It is a no-op when x is already a fragment boundary. Pending
// updates on the search path are pushed down first so both pieces keep the
// fragment's full coverage.
func (i *rangeDelIntervalIndex) splitAt(t *rangeDelIntervalNode, x arenaKey) *rangeDelIntervalNode {
	if t == nil {
		return nil
	}
	start := i.compare(x, t.start)
	if start == 0 {
		return t
	}
	if start < 0 {
		n := i.pushDown(t)
		n.left = i.splitAt(n.left, x)
		return i.balance(n)
	}
	end := i.compare(x, t.end)
	if end == 0 {
		return t
	}
	n := i.pushDown(t)
	if end > 0 {
		n.right = i.splitAt(n.right, x)
	} else {
		remainder := i.interval(x, n.end, n.trailer, n.tombstoneCount)
		n.end = x
		n.right = i.insertExtreme(n.right, remainder, true)
	}
	return i.balance(n)
}

// applyToRange assigns op's trailer and adds one tombstone to every fragment in
// its range. Both endpoints must be fragment boundaries, and the range must
// intersect t. Because gaps are stored as uncovered fragments, comparing op
// with a node's own fragment determines which children it intersects.
func (i *rangeDelIntervalIndex) applyToRange(
	t *rangeDelIntervalNode, op rangeDelIndexOp,
) *rangeDelIntervalNode {
	if t == nil {
		return nil
	}
	if i.compare(op.start, t.firstStart) <= 0 && i.compare(op.end, t.lastEnd) >= 0 {
		if invariants.Enabled {
			i.counters.visits++
		}
		return i.applyToSubtree(t, op.trailer, 1)
	}
	n := i.pushDown(t)
	start, end := i.compare(op.start, n.start), i.compare(op.end, n.end)
	if start < 0 {
		n.left = i.applyToRange(n.left, op)
	}
	if end > 0 {
		n.right = i.applyToRange(n.right, op)
	}
	if start <= 0 && end >= 0 {
		n.trailer = op.trailer
		n.tombstoneCount++
	}
	rangeDelPull(n)
	return n
}

// insert applies one RANGEDEL and returns the new root. It extends the
// partition across uncovered edges so applyToRange receives fragment boundaries.
func (i *rangeDelIntervalIndex) insert(
	t *rangeDelIntervalNode, op rangeDelIndexOp,
) *rangeDelIntervalNode {
	if i.compare(op.start, op.end) >= 0 {
		return t
	}
	startBoundary, endBoundary := true, true
	if t == nil {
		t = i.interval(op.start, op.end, 0, 0)
	} else {
		if c := i.compare(op.start, t.firstStart); c < 0 {
			t = i.insertExtreme(t, i.interval(op.start, t.firstStart, 0, 0), true)
		} else {
			startBoundary = c == 0
		}
		if c := i.compare(op.end, t.lastEnd); c > 0 {
			t = i.insertExtreme(t, i.interval(t.lastEnd, op.end, 0, 0), false)
		} else {
			endBoundary = c == 0
		}
	}
	if !startBoundary {
		t = i.splitAt(t, op.start)
	}
	if !endBoundary {
		t = i.splitAt(t, op.end)
	}
	return i.applyToRange(t, op)
}

func rangeDelVersionHeight(n *rangeDelVersionNode) uint8 {
	if n == nil {
		return 0
	}
	return n.height
}

// versionCopy returns a private registry node so published versions remain
// immutable. Registry nodes have no pending updates.
func (i *rangeDelIntervalIndex) versionCopy(n *rangeDelVersionNode) *rangeDelVersionNode {
	if invariants.Enabled {
		i.counters.registryAllocations++
	}
	c := *n
	return &c
}

func rangeDelVersionPull(n *rangeDelVersionNode) {
	n.height = 1 + max(rangeDelVersionHeight(n.left), rangeDelVersionHeight(n.right))
}

// versionLeft rebalances a private registry node, copying its published child
// before changing its links.
func (i *rangeDelIntervalIndex) versionLeft(n *rangeDelVersionNode) *rangeDelVersionNode {
	if invariants.Enabled {
		i.counters.registryRotations++
	}
	r := i.versionCopy(n.right)
	n.right = r.left
	rangeDelVersionPull(n)
	r.left = n
	rangeDelVersionPull(r)
	return r
}

// versionRight rebalances a private registry node, copying its published child
// before changing its links.
func (i *rangeDelIntervalIndex) versionRight(n *rangeDelVersionNode) *rangeDelVersionNode {
	if invariants.Enabled {
		i.counters.registryRotations++
	}
	l := i.versionCopy(n.left)
	n.left = l.right
	rangeDelVersionPull(n)
	l.right = n
	rangeDelVersionPull(l)
	return l
}

// versionInsert adds an entry keyed by batch end while preserving older
// registry roots for snapshot readers.
func (i *rangeDelIntervalIndex) versionInsert(t, entry *rangeDelVersionNode) *rangeDelVersionNode {
	if t == nil {
		return entry
	}
	if invariants.Enabled {
		i.counters.registryVisits++
	}
	n := i.versionCopy(t)
	if entry.end < n.end {
		n.left = i.versionInsert(n.left, entry)
	} else {
		n.right = i.versionInsert(n.right, entry)
	}
	rangeDelVersionPull(n)
	if int(rangeDelVersionHeight(n.left))-int(rangeDelVersionHeight(n.right)) > 1 {
		if rangeDelVersionHeight(n.left.right) > rangeDelVersionHeight(n.left.left) {
			n.left = i.versionLeft(i.versionCopy(n.left))
		}
		return i.versionRight(n)
	}
	if int(rangeDelVersionHeight(n.right))-int(rangeDelVersionHeight(n.left)) > 1 {
		if rangeDelVersionHeight(n.right.left) > rangeDelVersionHeight(n.right.right) {
			n.right = i.versionRight(i.versionCopy(n.right))
		}
		return i.versionLeft(n)
	}
	return n
}

// publish adds one batch's RANGEDELs and atomically publishes its root and
// registry entry for [start, end). Callers must serialize batches in sequence
// order. Ops must have increasing trailers so each fragment retains its newest
// covering trailer.
func (i *rangeDelIntervalIndex) publish(start, end base.SeqNum, ops []rangeDelIndexOp) error {
	if start >= end || end > base.SeqNumMax {
		return errors.New("invalid rangedel batch bounds")
	}
	old := i.state.Load()
	if old != nil && start < old.latest.end {
		return errors.New("unordered rangedel batch")
	}
	previous := base.InternalKeyTrailer(0)
	for _, op := range ops {
		if op.trailer.Kind() != base.InternalKeyKindRangeDelete || op.trailer.SeqNum()&base.SeqNumBatchBit != 0 || op.trailer.SeqNum() < start || op.trailer.SeqNum() >= end || op.trailer <= previous {
			return errors.New("invalid rangedel trailer order")
		}
		previous = op.trailer
	}
	if len(ops) == 0 {
		return errors.New("point-only batch has no rangedel version")
	}
	var root *rangeDelIntervalNode
	var versions *rangeDelVersionNode
	if old != nil {
		root = old.latest.root
		versions = old.versions
	}
	i.advanceEpoch()
	i.batchEpoch = i.epoch
	i.batchActive = true
	defer func() { i.batchActive = false }()
	// Intermediate roots are private and may be reused by later inserts.
	for _, op := range ops {
		root = i.insert(root, op)
	}
	entry := i.versionCopy(&rangeDelVersionNode{start: start, end: end, root: root, height: 1})
	versions = i.versionInsert(versions, entry)
	if invariants.Enabled {
		i.counters.publications++
	}
	i.batchActive = false
	i.state.Store(&rangeDelIndexState{versions: versions, latest: entry})
	return nil
}

// selectRoot returns the newest published root whose batch ends at or below
// threshold. A threshold inside a registered batch is unsupported because no
// root represents a partial batch.
func (i *rangeDelIntervalIndex) selectRoot(threshold base.SeqNum) (*rangeDelIntervalNode, error) {
	s := i.state.Load()
	if s == nil {
		return nil, nil
	}
	if s.latest.end <= threshold {
		return s.latest.root, nil
	}
	var predecessor *rangeDelVersionNode
	for n := s.versions; n != nil; {
		if n.end <= threshold {
			predecessor = n
			n = n.right
		} else {
			if n.start < threshold {
				return nil, errors.New("threshold inside rangedel batch")
			}
			n = n.left
		}
	}
	if predecessor == nil {
		return nil, nil
	}
	return predecessor.root, nil
}
