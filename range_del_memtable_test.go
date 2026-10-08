// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func newRangeDelTestMemTable(t *testing.T) *memTable {
	t.Helper()
	m := newMemTable(memTableOptions{
		Options:                      &Options{IncrementalMemTableRangeDels: func() bool { return true }},
		releaseAccountingReservation: func() {},
	})
	t.Cleanup(m.free)
	return m
}

func newRangeDelTestBatch(t *testing.T, start, end string) *Batch {
	t.Helper()
	b := newBatch(nil)
	require.NoError(t, b.DeleteRange([]byte(start), []byte(end), nil))
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	return b
}

func TestRangeDelMemTablePublication(t *testing.T) {
	m := newRangeDelTestMemTable(t)
	first := newRangeDelTestBatch(t, "a", "c")
	second := newRangeDelTestBatch(t, "b", "d")
	require.NoError(t, m.prepare(first))
	require.NoError(t, m.prepare(second))
	require.Nil(t, m.newPointRangeDelIter(1))
	task := second.rangeDelTask
	done := make(chan error, 1)
	go func() { done <- m.apply(second, 2) }()
	require.Eventually(t, func() bool {
		it := m.rangeDelSkl.NewIter(base.DefaultSplit, nil, nil)
		defer it.Close()
		return it.First() != nil
	}, 5*time.Second, time.Millisecond)
	select {
	case <-task.done:
		t.Fatal("later batch published before its predecessor")
	default:
	}
	require.Nil(t, m.rangeDelIndex.Load().state.Load())
	point := newBatch(nil)
	defer point.Close()
	require.NoError(t, point.Set([]byte("z"), []byte("v"), nil))
	require.NoError(t, m.prepare(point))
	require.Nil(t, point.rangeDelTask)
	require.NoError(t, m.apply(point, 3))
	m.writerUnref()
	require.NoError(t, m.apply(first, 1))
	m.writerUnref()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("successor did not complete")
	}
	m.writerUnref()
	require.Nil(t, first.rangeDelTask)
	require.Nil(t, second.rangeDelTask)
	require.Nil(t, task.predecessor)
	old := m.newPointRangeDelIter(2)
	defer old.Close()
	s, err := old.First()
	require.NoError(t, err)
	require.Equal(t, "a", string(s.Start))
	require.Equal(t, "c", string(s.End))
	require.Equal(t, base.SeqNum(1), s.Keys[0].SeqNum())
	newer := m.newPointRangeDelIter(3)
	defer newer.Close()
	s, err = newer.Last()
	require.NoError(t, err)
	require.Equal(t, "d", string(s.End))
	require.Equal(t, base.SeqNum(2), s.Keys[0].SeqNum())
	require.Panics(t, func() {
		mixed := newRangeDelTestBatch(t, "e", "f")
		require.NoError(t, mixed.Set([]byte("q"), nil, nil))
		require.NoError(t, m.prepare(mixed))
		require.NoError(t, m.apply(mixed, 4))
		m.writerUnref()
		m.newPointRangeDelIter(5)
	})
}

func TestRangeDelMemTableFailures(t *testing.T) {
	for _, failure := range []string{"sequence", "decode", "insertion", "count", "publication", "panic"} {
		t.Run(failure, func(t *testing.T) {
			m := newRangeDelTestMemTable(t)
			initial := newRangeDelTestBatch(t, "a", "b")
			require.NoError(t, m.prepare(initial))
			require.NoError(t, m.apply(initial, 10))
			m.writerUnref()
			index := m.rangeDelIndex.Load()
			state := index.state.Load()
			bad := newRangeDelTestBatch(t, "c", "d")
			successor := newRangeDelTestBatch(t, "e", "f")
			if failure == "count" {
				bad.countRangeDels++
			}
			require.NoError(t, m.prepare(bad))
			if invariants.Enabled {
				require.Error(t, m.prepare(bad))
			}
			require.NoError(t, m.prepare(successor))
			badTask, nextTask := bad.rangeDelTask, successor.rangeDelTask
			seq := base.SeqNum(11)
			switch failure {
			case "sequence":
				m.logSeqNum = 11
				seq = 1
			case "decode":
				bad.data = bad.data[:len(bad.data)-1]
			case "insertion":
				require.NoError(t, m.rangeDelSkl.Add(base.MakeInternalKey([]byte("c"), seq, base.InternalKeyKindRangeDelete), []byte("d")))
			case "publication":
				seq = 9
			case "panic":
				index.cmp = func(_, _ []byte) int { panic("index construction failed") }
			}
			if failure == "panic" {
				require.Panics(t, func() { _ = m.apply(bad, seq) })
			} else {
				require.Error(t, m.apply(bad, seq))
			}
			m.writerUnref()
			done := make(chan error, 1)
			go func() { done <- m.apply(successor, 12) }()
			select {
			case err := <-done:
				require.Error(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("failed predecessor left a waiting successor")
			}
			m.writerUnref()
			require.Same(t, state, index.state.Load())
			require.Error(t, index.err())
			for _, task := range []*rangeDelIndexTask{badTask, nextTask} {
				select {
				case <-task.done:
				default:
					t.Fatal("task completion channel remained open")
				}
				require.Nil(t, task.predecessor)
			}
			require.Nil(t, bad.rangeDelTask)
			require.Nil(t, successor.rangeDelTask)
		})
	}
}

func TestRangeDelMemTableAdmissionAndHistory(t *testing.T) {
	for _, layout := range rangeDelFixtureLayouts {
		t.Run(layout, func(t *testing.T) {
			f := makeRangeDelFixture(layout, 40, DefaultComparer)
			m := newMemTable(memTableOptions{
				Options:                      &Options{Comparer: f.comparer, IncrementalMemTableRangeDels: func() bool { return true }},
				releaseAccountingReservation: func() {},
			})
			defer m.free()
			baseline := rangeDelFixtureMemTable(f.comparer)
			defer baseline.free()
			batches := rangeDelFixtureBatches(t, f, 8)
			seq := base.SeqNum(1)
			for _, batch := range batches {
				require.NoError(t, m.prepare(batch))
				require.NoError(t, m.apply(batch, seq))
				m.writerUnref()
				require.NoError(t, baseline.prepare(batch))
				require.NoError(t, baseline.apply(batch, seq))
				baseline.writerUnref()
				seq += base.SeqNum(batch.Count())
				// Skiplist heights are randomized, so actual occupancy may differ.
				require.Equal(t, baseline.totalBytes(), m.totalBytes())
			}
			frags := m.tombstones.frags.Load()
			require.Nil(t, frags.spans)
			it := m.newPointRangeDelIter(seq)
			if it != nil {
				_, err := it.First()
				require.NoError(t, err)
				it.Close()
			}
			bounds := m.rangeDelBounds()
			if layout != "invalid" {
				require.True(t, bounds.Valid(m.cmp))
			}
			m.computePossibleOverlaps(func(b bounded) shouldContinue {
				return continueIteration
			}, KeyRange{Start: []byte("a"), End: []byte("z")})
			require.Nil(t, frags.spans)
			history := m.newRangeDelIter(nil)
			ref := baseline.newRangeDelIter(nil)
			if ref == nil {
				require.Nil(t, history)
				return
			}
			require.NotNil(t, history)
			defer history.Close()
			defer ref.Close()
			s, err := history.First()
			require.NoError(t, err)
			want, err := ref.First()
			require.NoError(t, err)
			for want != nil {
				require.Equal(t, want, s)
				s, err = history.Next()
				require.NoError(t, err)
				want, err = ref.Next()
				require.NoError(t, err)
			}
			require.Nil(t, s)
		})
	}
	m := newRangeDelTestMemTable(t)
	point := newBatch(nil)
	defer point.Close()
	require.NoError(t, point.Set([]byte("a"), []byte("b"), nil))
	require.NoError(t, m.prepare(point))
	require.NoError(t, m.apply(point, 1))
	m.writerUnref()
	require.Nil(t, m.rangeDelIndex.Load())
	require.Nil(t, m.rangeDelTail)
	require.Nil(t, m.newPointRangeDelIter(2))
	tooLarge := newRangeDelTestBatch(t, "a", "z")
	tooLarge.memTableSize = m.totalBytes()
	require.ErrorIs(t, m.prepare(tooLarge), arenaskl.ErrArenaFull)
	require.Nil(t, tooLarge.rangeDelTask)
	require.Nil(t, m.rangeDelIndex.Load())
}

func TestRangeDelMemTableDB(t *testing.T) {
	for _, stack := range []IteratorStack{IteratorStackV1, IteratorStackV2} {
		t.Run(stack.String(), func(t *testing.T) {
			fs := vfs.NewMem()
			opts := &Options{FS: fs, IteratorStack: stack, IncrementalMemTableRangeDels: func() bool { return true }, MemTableSize: 64 << 10}
			db, err := Open("db", opts)
			require.NoError(t, err)
			for _, k := range []string{"a", "b", "c", "d", "e", "f", "g"} {
				require.NoError(t, db.Set([]byte(k), []byte(k), NoSync))
			}
			require.NoError(t, db.Flush())
			snapshot := db.NewSnapshot()
			require.NoError(t, db.DeleteRange([]byte("b"), []byte("f"), NoSync))
			old, err := db.NewIter(nil)
			require.NoError(t, err)
			require.NoError(t, db.Set([]byte("c"), []byte("new-c"), NoSync))
			require.NoError(t, db.DeleteRange([]byte("a"), []byte("d"), NoSync))
			for _, k := range []string{"a", "b", "c", "d", "e"} {
				_, closer, err := db.Get([]byte(k))
				require.ErrorIs(t, err, ErrNotFound)
				require.Nil(t, closer)
			}
			check := func(it *Iterator, want string) {
				t.Helper()
				var got string
				for valid := it.First(); valid; valid = it.Next() {
					got += string(it.Key())
				}
				require.NoError(t, it.Error())
				require.Equal(t, want, got)
				got = ""
				for valid := it.Last(); valid; valid = it.Prev() {
					got = string(it.Key()) + got
				}
				require.Equal(t, want, got)
				require.NoError(t, it.Error())
			}
			check(old, "afg")
			clone, err := old.Clone(CloneOptions{})
			require.NoError(t, err)
			old.SetOptions(&IterOptions{LowerBound: []byte("a"), UpperBound: []byte("h")})
			check(old, "afg")
			check(clone, "afg")
			snapIter, err := snapshot.NewIter(nil)
			require.NoError(t, err)
			check(snapIter, "abcdefg")
			require.NoError(t, db.Flush())
			check(old, "afg")
			require.NoError(t, old.Close())
			require.NoError(t, clone.Close())
			require.NoError(t, snapIter.Close())
			snapIter, err = snapshot.NewIter(nil)
			require.NoError(t, err)
			check(snapIter, "abcdefg")
			require.NoError(t, snapIter.Close())
			require.NoError(t, snapshot.Close())
			// Leave WAL-only writes for replay, including mixed batches.
			b := db.NewBatch()
			require.NoError(t, b.DeleteRange([]byte("f"), []byte("g"), nil))
			require.NoError(t, b.Set([]byte("z"), []byte("z"), nil))
			require.NoError(t, b.Commit(Sync))
			require.NoError(t, b.Close())
			require.NoError(t, db.Close())
			db, err = Open("db", opts)
			require.NoError(t, err)
			it, err := db.NewIter(nil)
			require.NoError(t, err)
			check(it, "gz")
			require.NoError(t, it.Close())
			require.NoError(t, db.Close())
		})
	}
}

func TestRangeDelMemTableOptions(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		o := &Options{}
		require.NoError(t, o.Parse(fmt.Sprintf("[Options]\nincremental_mem_table_range_dels=%t\n", enabled), nil))
		require.Equal(t, enabled, o.IncrementalMemTableRangeDels())
		o.EnsureDefaults()
		var parsed Options
		require.NoError(t, parsed.Parse(o.String(), nil))
		parsed.EnsureDefaults()
		require.Equal(t, enabled, parsed.IncrementalMemTableRangeDels())
	}
	m := newRangeDelTestMemTable(t)
	if invariants.Enabled {
		require.Error(t, m.apply(newRangeDelTestBatch(t, "a", "b"), 1))
	}
	// A RANGEDEL-containing batch with only invalid ranges still publishes a version.
	b := newRangeDelTestBatch(t, "x", "x")
	require.NoError(t, m.prepare(b))
	require.NoError(t, m.apply(b, 1))
	m.writerUnref()
	require.Nil(t, m.newPointRangeDelIter(2))
	require.Equal(t, base.SeqNum(2), m.rangeDelIndex.Load().state.Load().latest.end)
}

// TestRangeDelMemTableOptionPerMemTable checks that an open DB reads
// IncrementalMemTableRangeDels for each new memtable, so a change takes effect
// at the next rotation without reopening.
func TestRangeDelMemTableOptionPerMemTable(t *testing.T) {
	var enabled atomic.Bool
	d, err := Open("", &Options{FS: vfs.NewMem(), IncrementalMemTableRangeDels: enabled.Load})
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()
	mutableIncremental := func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.mu.mem.mutable.incrementalRangeDels
	}
	rotate := func() {
		require.NoError(t, d.Set([]byte("a"), nil, NoSync))
		require.NoError(t, d.Flush())
	}

	require.False(t, mutableIncremental())
	enabled.Store(true)
	require.False(t, mutableIncremental(), "the current memtable keeps its choice")
	rotate()
	require.True(t, mutableIncremental())
	enabled.Store(false)
	rotate()
	require.False(t, mutableIncremental())
}

func TestRangeDelMemTableFlushBoundsAndRecycling(t *testing.T) {
	m := newRangeDelTestMemTable(t)
	b := newRangeDelTestBatch(t, "a", "z")
	require.NoError(t, m.prepare(b))
	require.NoError(t, m.apply(b, 1))
	m.writerUnref()
	frags := m.tombstones.frags.Load()
	opts := &Options{IncrementalMemTableRangeDels: func() bool { return true }}
	opts.EnsureDefaults()
	opts.FlushSplitBytes = 0
	flush, err := newFlush(opts, nil, manifest.NewL0Organizer(opts.Comparer, 0), 1,
		flushableList{&flushableEntry{flushable: m}}, time.Now(), false, neverSeparateValues)
	require.NoError(t, err)
	require.Equal(t, "a", string(flush.bounds.Start))
	require.Equal(t, "z", string(flush.bounds.End.Key))
	require.Nil(t, frags.spans)
	capacity := m.totalBytes()
	m.free()
	require.Nil(t, m.rangeDelIndex.Load())
	require.Nil(t, m.rangeDelTail)
	resetOpts := &Options{}
	resetOpts.EnsureDefaults()
	m.init(memTableOptions{Options: resetOpts, releaseAccountingReservation: func() {}})
	require.False(t, m.incrementalRangeDels)
	require.Nil(t, m.rangeDelIndex.Load())
	require.Nil(t, m.rangeDelTail)
	require.Equal(t, capacity, m.totalBytes())
	require.Equal(t, memTableEmptySize, m.reserved)
	require.Nil(t, m.newRangeDelIter(nil))
}

func TestRangeDelMemTableConcurrentReaders(t *testing.T) {
	m := newRangeDelTestMemTable(t)
	f := makeRangeDelFixture("random", 300, DefaultComparer)
	batches := rangeDelFixtureBatches(t, f, 3)
	for _, b := range batches {
		require.NoError(t, m.prepare(b))
	}
	var writers sync.WaitGroup
	errs := make(chan error, len(batches))
	// Start successors first so raw skiplist insertion can run ahead of indexing.
	for j := len(batches) - 1; j >= 0; j-- {
		writers.Add(1)
		go func(j int) {
			defer writers.Done()
			errs <- m.apply(batches[j], base.SeqNum(1+3*j))
			m.writerUnref()
		}(j)
	}
	var readers sync.WaitGroup
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for range 100 {
				it := m.newPointRangeDelIter(base.SeqNumMax)
				if it == nil {
					continue
				}
				_, _ = it.SeekGE([]byte("d/00000100"))
				_, _ = it.Next()
				_, _ = it.Prev()
				_, _ = it.Last()
				it.Close()
			}
		}()
	}
	writers.Wait()
	readers.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	if invariants.Enabled {
		require.EqualValues(t, len(batches), m.rangeDelIndex.Load().counters.publications)
	}
}
