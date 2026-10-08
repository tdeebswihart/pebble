// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/treesteps"
)

// rangeDelIterFrame is one entry in the iterator's path. Published nodes are
// shared between versions and have no parent pointers, so the iterator keeps an
// explicit path of frames. Each frame carries the trailer inherited from
// ancestors' pending updates, because readers cannot push updates down into
// published nodes.
type rangeDelIterFrame struct {
	node      *rangeDelIntervalNode
	inherited base.InternalKeyTrailer
}

func (f rangeDelIterFrame) trailer() base.InternalKeyTrailer {
	if f.inherited != 0 {
		return f.inherited
	}
	return f.node.trailer
}

// pendingTrailer returns the trailer this frame's children inherit: the
// inherited trailer if set, else the node's own pending trailer.
func (f rangeDelIterFrame) pendingTrailer() base.InternalKeyTrailer {
	if f.inherited != 0 {
		return f.inherited
	}
	return f.node.pendingTrailer
}

// memTableRangeDelIter walks one published root and returns keyspan.Spans, one
// per covered fragment, each with a single key carrying the newest trailer.
// That is enough to shadow point keys. The outer Iterator's readState pins the
// arena until Iterator.Close. Other callers must keep the memtable alive
// throughout iteration.
type memTableRangeDelIter struct {
	index     *rangeDelIntervalIndex
	root      *rangeDelIntervalNode
	path      []rangeDelIterFrame
	span      keyspan.Span
	key       [1]keyspan.Key
	exhausted int8 // -1 before the first fragment, +1 after the last
}

var _ keyspan.FragmentIterator = (*memTableRangeDelIter)(nil)

// newPointRangeDelIter selects the published batch root visible at snapshot
// for point-key shadowing. A snapshot inside a batch has no matching root.
func (m *memTable) newPointRangeDelIter(snapshot base.SeqNum) keyspan.FragmentIterator {
	if !m.incrementalRangeDels {
		return m.newRangeDelIter(nil)
	}
	index := m.rangeDelIndex.Load()
	if index == nil {
		return nil
	}
	root, err := index.selectRoot(snapshot)
	if err != nil {
		panic(errors.AssertionFailedf("pebble: unsupported rangedel snapshot %d: %v", snapshot, err))
	}
	if root == nil || root.coveredCount == 0 {
		return nil
	}
	return &memTableRangeDelIter{index: index, root: root, exhausted: -1}
}

func newPointRangeDelIter(
	f flushable, opts *IterOptions, snapshot base.SeqNum,
) keyspan.FragmentIterator {
	if m, ok := f.(*memTable); ok && m.incrementalRangeDels {
		return m.newPointRangeDelIter(snapshot)
	}
	return f.newRangeDelIter(opts)
}

func (i *memTableRangeDelIter) bytes(b arenaKey) []byte {
	return i.index.arena.Bytes(b.off, b.length)
}

// seek searches the subtree rooted at n and leaves path at the first covered
// fragment in scan order that satisfies key. When bounded and not reverse, that
// is the first covered fragment ending after key (SeekGE). When bounded and
// reverse, it is the last covered fragment starting before key (SeekLT). When
// unbounded, it is the first or last covered fragment. A covered fragment has
// at least one tombstone covering it. inherited is the pending trailer from n's
// ancestors that has not been pushed down; it makes descendants covered even if
// their cached coveredCount is zero.
func (i *memTableRangeDelIter) seek(
	n *rangeDelIntervalNode, inherited base.InternalKeyTrailer, key []byte, bounded, reverse bool,
) bool {
	if n == nil || (inherited == 0 && n.coveredCount == 0) {
		return false
	}
	if bounded {
		if !reverse && i.index.cmp(i.bytes(n.lastEnd), key) <= 0 {
			return false
		}
		if reverse && i.index.cmp(i.bytes(n.firstStart), key) >= 0 {
			return false
		}
	}
	f := rangeDelIterFrame{node: n, inherited: inherited}
	i.path = append(i.path, f)
	first, last := n.left, n.right
	if reverse {
		first, last = last, first
	}
	if i.seek(first, f.pendingTrailer(), key, bounded, reverse) {
		return true
	}
	if f.trailer() != 0 && (!bounded ||
		(!reverse && i.index.cmp(i.bytes(n.end), key) > 0) ||
		(reverse && i.index.cmp(i.bytes(n.start), key) < 0)) {
		return true
	}
	if i.seek(last, f.pendingTrailer(), key, bounded, reverse) {
		return true
	}
	i.path = i.path[:len(i.path)-1]
	return false
}

func (i *memTableRangeDelIter) exhaust(reverse bool) (*keyspan.Span, error) {
	i.exhausted = 1
	if reverse {
		i.exhausted = -1
	}
	i.span = keyspan.Span{}
	return nil, nil
}

// current returns the frame of the current fragment.
func (i *memTableRangeDelIter) current() rangeDelIterFrame {
	return i.path[len(i.path)-1]
}

// currentSpan returns a span backed by the iterator's reusable key and span
// storage. Callers must finish using it before repositioning the iterator.
func (i *memTableRangeDelIter) currentSpan() (*keyspan.Span, error) {
	i.exhausted = 0
	f := i.current()
	i.key[0] = keyspan.Key{Trailer: f.trailer()}
	i.span = keyspan.Span{
		Start: i.bytes(f.node.start), End: i.bytes(f.node.end),
		Keys: i.key[:], KeysOrder: keyspan.ByTrailerDesc,
	}
	return &i.span, nil
}

func (i *memTableRangeDelIter) position(key []byte, bounded, reverse bool) (*keyspan.Span, error) {
	clear(i.path)
	i.path = i.path[:0]
	if i.seek(i.root, 0, key, bounded, reverse) {
		return i.currentSpan()
	}
	return i.exhaust(reverse)
}

func (i *memTableRangeDelIter) SeekGE(key []byte) (*keyspan.Span, error) {
	return i.position(key, true, false)
}

func (i *memTableRangeDelIter) SeekLT(key []byte) (*keyspan.Span, error) {
	return i.position(key, true, true)
}

func (i *memTableRangeDelIter) First() (*keyspan.Span, error) {
	return i.position(nil, false, false)
}

func (i *memTableRangeDelIter) Last() (*keyspan.Span, error) {
	return i.position(nil, false, true)
}

// step advances the iterator to the adjacent covered fragment in the scan
// direction.
func (i *memTableRangeDelIter) step(reverse bool) (*keyspan.Span, error) {
	if i.exhausted != 0 {
		if !reverse && i.exhausted < 0 {
			return i.First()
		}
		if reverse && i.exhausted > 0 {
			return i.Last()
		}
		return nil, nil
	}
	f := i.current()
	child := f.node.right
	if reverse {
		child = f.node.left
	}
	if i.seek(child, f.pendingTrailer(), nil, false, reverse) {
		return i.currentSpan()
	}
	for len(i.path) > 1 {
		child = i.current().node
		i.path[len(i.path)-1] = rangeDelIterFrame{}
		i.path = i.path[:len(i.path)-1]
		f = i.current()
		if (!reverse && child == f.node.left) || (reverse && child == f.node.right) {
			if f.trailer() != 0 {
				return i.currentSpan()
			}
			next := f.node.right
			if reverse {
				next = f.node.left
			}
			if i.seek(next, f.pendingTrailer(), nil, false, reverse) {
				return i.currentSpan()
			}
		}
	}
	clear(i.path)
	i.path = i.path[:0]
	return i.exhaust(reverse)
}

func (i *memTableRangeDelIter) Next() (*keyspan.Span, error) { return i.step(false) }
func (i *memTableRangeDelIter) Prev() (*keyspan.Span, error) { return i.step(true) }
func (i *memTableRangeDelIter) Close()                       { *i = memTableRangeDelIter{} }
func (i *memTableRangeDelIter) SetContext(context.Context)   {}
func (i *memTableRangeDelIter) WrapChildren(keyspan.WrapFn)  {}
func (i *memTableRangeDelIter) TreeStepsNode() treesteps.NodeInfo {
	return treesteps.NodeInfof(i, "%T(%p)", i, i)
}

// rangeDelBounds returns the bounds of covered fragments in the latest
// published root. The returned keys refer to the memtable arena.
func (m *memTable) rangeDelBounds() base.UserKeyBounds {
	if index := m.rangeDelIndex.Load(); index != nil {
		if state := index.state.Load(); state != nil {
			root := state.latest.root
			if root != nil && root.coveredCount != 0 {
				return base.UserKeyBounds{
					Start: index.arena.Bytes(root.firstCoveredStart.off, root.firstCoveredStart.length),
					End:   base.UserKeyExclusive(index.arena.Bytes(root.lastCoveredEnd.off, root.lastCoveredEnd.length)),
				}
			}
		}
	}
	return base.UserKeyBounds{}
}
