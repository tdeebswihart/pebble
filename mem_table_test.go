// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	stdcmp "cmp"
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/itertest"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// get gets the value for the given key. It returns ErrNotFound if the DB does
// not contain the key.
func (m *memTable) get(key []byte) (value []byte, err error) {
	it := m.skl.NewIter(nil, nil)
	kv := it.SeekGE(key, base.SeekGEFlagsNone)
	if kv == nil {
		return nil, ErrNotFound
	}
	if !m.equal(key, kv.K.UserKey) {
		return nil, ErrNotFound
	}
	switch kv.Kind() {
	case InternalKeyKindDelete, InternalKeyKindSingleDelete, InternalKeyKindDeleteSized:
		return nil, ErrNotFound
	default:
		return kv.InPlaceValue(), nil
	}
}

// Set sets the value for the given key. It overwrites any previous value for
// that key; a DB is not a multi-map. NB: this might have unexpected
// interaction with prepare/apply. Caveat emptor!
func (m *memTable) set(key InternalKey, value []byte) error {
	if key.Kind() == InternalKeyKindRangeDelete {
		if !m.incrementalRangeDels {
			if err := m.rangeDelSkl.Add(key, value); err != nil {
				return err
			}
			m.tombstoneCache.invalidate(1)
			return nil
		}
		start, end, err := m.rangeDelSkl.AddAndGetSlices(key, value)
		if err != nil {
			return err
		}
		m.tombstones.add([]rangeDelTombstone{{start: start, end: end, trailer: key.Trailer}})
		return nil
	}
	if rangekey.IsRangeKey(key.Kind()) {
		if err := m.rangeKeySkl.Add(key, value); err != nil {
			return err
		}
		m.rangeKeys.invalidate(1)
		return nil
	}
	return m.skl.Add(key, value)
}

// rangeDelSpans returns the memtable's fragmented range deletions in a single
// slice, or nil if it has none.
func (m *memTable) rangeDelSpans() []keyspan.Span {
	if !m.incrementalRangeDels {
		return m.tombstoneCache.get()
	}
	return m.tombstones.version.Load().spans()
}

// incrementalRangeDelOptions returns Options that set
// Experimental.IncrementalRangeDelFragments to incremental.
func incrementalRangeDelOptions(incremental bool) *Options {
	opts := &Options{}
	opts.Experimental.IncrementalRangeDelFragments = func() bool { return incremental }
	return opts
}

// count returns the number of entries in a DB.
func (m *memTable) count() (n int) {
	x := m.newIter(nil)
	for kv := x.First(); kv != nil; kv = x.Next() {
		n++
	}
	if x.Close() != nil {
		return -1
	}
	return n
}

func ikey(s string) InternalKey {
	return base.MakeInternalKey([]byte(s), 0, InternalKeyKindSet)
}

func TestMemTableBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Check the empty DB.
	m := newMemTable(memTableOptions{})
	if got, want := m.count(), 0; got != want {
		t.Fatalf("0.count: got %v, want %v", got, want)
	}
	v, err := m.get([]byte("cherry"))
	if string(v) != "" || err != ErrNotFound {
		t.Fatalf("1.get: got (%q, %v), want (%q, %v)", v, err, "", ErrNotFound)
	}
	// Add some key/value pairs.
	m.set(ikey("cherry"), []byte("red"))
	m.set(ikey("peach"), []byte("yellow"))
	m.set(ikey("grape"), []byte("red"))
	m.set(ikey("grape"), []byte("green"))
	m.set(ikey("plum"), []byte("purple"))
	if got, want := m.count(), 4; got != want {
		t.Fatalf("2.count: got %v, want %v", got, want)
	}
	// Get keys that are and aren't in the DB.
	v, err = m.get([]byte("plum"))
	if string(v) != "purple" || err != nil {
		t.Fatalf("6.get: got (%q, %v), want (%q, %v)", v, err, "purple", error(nil))
	}
	v, err = m.get([]byte("lychee"))
	if string(v) != "" || err != ErrNotFound {
		t.Fatalf("7.get: got (%q, %v), want (%q, %v)", v, err, "", ErrNotFound)
	}
	// Check an iterator.
	s, x := "", m.newIter(nil)
	for kv := x.SeekGE([]byte("mango"), base.SeekGEFlagsNone); kv != nil; kv = x.Next() {
		v, _, err := kv.Value(nil)
		require.NoError(t, err)
		s += fmt.Sprintf("%s/%s.", kv.K.UserKey, v)
	}
	if want := "peach/yellow.plum/purple."; s != want {
		t.Fatalf("8.iter: got %q, want %q", s, want)
	}
	if err = x.Close(); err != nil {
		t.Fatalf("9.close: %v", err)
	}
	// Check some more sets and deletes.
	if err := m.set(ikey("apricot"), []byte("orange")); err != nil {
		t.Fatalf("12.set: %v", err)
	}
	if got, want := m.count(), 5; got != want {
		t.Fatalf("13.count: got %v, want %v", got, want)
	}
}

func TestMemTableCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	m := newMemTable(memTableOptions{})
	for i := 0; i < 200; i++ {
		if j := m.count(); j != i {
			t.Fatalf("count: got %d, want %d", j, i)
		}
		m.set(InternalKey{UserKey: []byte{byte(i)}}, nil)
	}
}

func TestMemTableEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	m := newMemTable(memTableOptions{})
	if !m.empty() {
		t.Errorf("got !empty, want empty")
	}
	// Add one key/value pair with an empty key and empty value.
	m.set(InternalKey{}, nil)
	if m.empty() {
		t.Errorf("got empty, want !empty")
	}
}

func TestMemTable1000Entries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Initialize the DB.
	const N = 1000
	m0 := newMemTable(memTableOptions{})
	for i := 0; i < N; i++ {
		k := ikey(strconv.Itoa(i))
		v := []byte(strings.Repeat("x", i))
		m0.set(k, v)
	}
	// Check the DB count.
	if got, want := m0.count(), 1000; got != want {
		t.Fatalf("count: got %v, want %v", got, want)
	}
	// Check random-access lookup.
	r := rand.New(rand.NewPCG(0, 0))
	for i := 0; i < 3*N; i++ {
		j := r.IntN(N)
		k := []byte(strconv.Itoa(j))
		v, err := m0.get(k)
		require.NoError(t, err)
		if len(v) != cap(v) {
			t.Fatalf("get: j=%d, got len(v)=%d, cap(v)=%d", j, len(v), cap(v))
		}
		var c uint8
		if len(v) != 0 {
			c = v[0]
		} else {
			c = 'x'
		}
		if len(v) != j || c != 'x' {
			t.Fatalf("get: j=%d, got len(v)=%d,c=%c, want %d,%c", j, len(v), c, j, 'x')
		}
	}
	// Check that iterating through the middle of the DB looks OK.
	// Keys are in lexicographic order, not numerical order.
	// Multiples of 3 are not present.
	wants := []string{
		"499",
		"5",
		"50",
		"500",
		"501",
		"502",
		"503",
		"504",
		"505",
		"506",
		"507",
	}
	x := m0.newIter(nil)
	kv := x.SeekGE([]byte(wants[0]), base.SeekGEFlagsNone)
	for _, want := range wants {
		if kv == nil {
			t.Fatalf("iter: next failed, want=%q", want)
		}
		if got := string(kv.K.UserKey); got != want {
			t.Fatalf("iter: got %q, want %q", got, want)
		}
		if k := kv.K.UserKey; len(k) != cap(k) {
			t.Fatalf("iter: len(k)=%d, cap(k)=%d", len(k), cap(k))
		}
		v, _, err := kv.Value(nil)
		require.NoError(t, err)
		if len(v) != cap(v) {
			t.Fatalf("iter: len(v)=%d, cap(v)=%d", len(v), cap(v))
		}
		x.Next()
	}
	if err := x.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestMemTableIter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var mem *memTable
	for _, testdata := range []string{
		"testdata/internal_iter_next", "testdata/internal_iter_bounds"} {
		datadriven.RunTest(t, testdata, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "define":
				mem = newMemTable(memTableOptions{})
				for key := range crstrings.LinesSeq(d.Input) {
					j := strings.Index(key, ":")
					if err := mem.set(base.ParseInternalKey(key[:j]), []byte(key[j+1:])); err != nil {
						return err.Error()
					}
				}
				return ""

			case "iter":
				var options IterOptions
				for _, arg := range d.CmdArgs {
					switch arg.Key {
					case "lower":
						if len(arg.Vals) != 1 {
							return fmt.Sprintf(
								"%s expects at most 1 value for lower", d.Cmd)
						}
						options.LowerBound = []byte(arg.Vals[0])
					case "upper":
						if len(arg.Vals) != 1 {
							return fmt.Sprintf(
								"%s expects at most 1 value for upper", d.Cmd)
						}
						options.UpperBound = []byte(arg.Vals[0])
					default:
						return fmt.Sprintf("unknown arg: %s", arg.Key)
					}
				}
				iter := mem.newIter(&options)
				defer iter.Close()
				return itertest.RunInternalIterCmd(t, d, iter)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	}
}

func TestMemTableDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, incremental := range []bool{false, true} {
		t.Run(fmt.Sprintf("incremental=%t", incremental), func(t *testing.T) {
			testMemTableDeleteRange(t, incremental)
		})
	}
}

func testMemTableDeleteRange(t *testing.T, incremental bool) {
	var mem *memTable
	var seqNum base.SeqNum

	datadriven.RunTest(t, "testdata/delete_range", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "clear":
			mem = nil
			seqNum = 0
			return ""

		case "define":
			b := newBatch(nil)
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if mem == nil {
				mem = newMemTable(memTableOptions{Options: incrementalRangeDelOptions(incremental)})
			}
			if err := mem.apply(b, seqNum); err != nil {
				return err.Error()
			}
			seqNum += base.SeqNum(b.Count())
			return ""

		case "scan":
			var buf bytes.Buffer
			if td.HasArg("range-del") {
				iter := mem.newRangeDelIter(nil)
				defer iter.Close()
				scanKeyspanIterator(&buf, iter)
			} else {
				iter := mem.newIter(nil)
				defer iter.Close()
				scanInternalIter(&buf, iter)
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestMemTableConcurrentDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, incremental := range []bool{false, true} {
		t.Run(fmt.Sprintf("incremental=%t", incremental), func(t *testing.T) {
			testMemTableConcurrentDeleteRange(t, incremental)
		})
	}
}

func testMemTableConcurrentDeleteRange(t *testing.T, incremental bool) {
	// Concurrently write and read range tombstones. Workers add range
	// tombstones, and then immediately retrieve them verifying that the
	// tombstones they've added are all present.

	opts := incrementalRangeDelOptions(incremental)
	opts.MemTableSize = 64 << 20
	m := newMemTable(memTableOptions{Options: opts})

	const workers = 10
	eg, _ := errgroup.WithContext(context.Background())
	var seqNum base.AtomicSeqNum
	seqNum.Store(1)
	for i := 0; i < workers; i++ {
		i := i
		eg.Go(func() error {
			start := ([]byte)(fmt.Sprintf("%03d", i))
			end := ([]byte)(fmt.Sprintf("%03d", i+1))
			for j := 0; j < 100; j++ {
				b := newBatch(nil)
				b.DeleteRange(start, end, nil)
				n := seqNum.Add(1) - 1
				require.NoError(t, m.apply(b, n))
				b.Close()

				var count int
				it := m.newRangeDelIter(nil)
				s, err := it.SeekGE(start)
				for ; s != nil; s, err = it.Next() {
					if m.cmp(s.Start, end) >= 0 {
						break
					}
					count += len(s.Keys)
				}
				if err != nil {
					return err
				}
				if j+1 != count {
					return errors.Errorf("%d: expected %d tombstones, but found %d", i, j+1, count)
				}
			}
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		t.Error(err)
	}
}

// histogramSamples returns the number of samples recorded by h and their sum.
func histogramSamples(t testing.TB, h prometheus.Histogram) (count uint64, sum float64) {
	t.Helper()
	require.NotNil(t, h)
	var m prometheusgo.Metric
	require.NoError(t, h.Write(&m))
	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}

// rangeDelCacheSamples summarizes the sample counts and sums of the
// histograms in MemTableRangeDelCacheMetrics.
type rangeDelCacheSamples struct {
	invalidations uint64
	// rebuilds is the sample count of RebuildDuration. The count of every
	// histogram other than ReaderWait must equal it.
	rebuilds       uint64
	readerWaits    uint64
	tombstonesSum  float64
	fragmentsSum   float64
	concurrencySum float64
	// splices is the sample count of SpliceDuration, which must equal that of
	// SpliceVersionFragments.
	splices             uint64
	versionFragmentsSum float64
	// touched and touchedSum are the sample count and sum of
	// SpliceFragmentsTouched.
	touched    uint64
	touchedSum float64
}

func readRangeDelCacheSamples(t testing.TB, m MemTableRangeDelCacheMetrics) rangeDelCacheSamples {
	t.Helper()
	s := rangeDelCacheSamples{invalidations: m.Invalidations}
	s.rebuilds, _ = histogramSamples(t, m.RebuildDuration)
	s.readerWaits, _ = histogramSamples(t, m.ReaderWait)
	var n uint64
	n, s.tombstonesSum = histogramSamples(t, m.RebuildTombstones)
	require.Equal(t, s.rebuilds, n)
	n, s.fragmentsSum = histogramSamples(t, m.RebuildFragments)
	require.Equal(t, s.rebuilds, n)
	n, s.concurrencySum = histogramSamples(t, m.ConcurrentRebuilds)
	require.Equal(t, s.rebuilds, n)
	s.splices, _ = histogramSamples(t, m.SpliceDuration)
	n, s.versionFragmentsSum = histogramSamples(t, m.SpliceVersionFragments)
	require.Equal(t, s.splices, n)
	s.touched, s.touchedSum = histogramSamples(t, m.SpliceFragmentsTouched)
	return s
}

// TestMemTableRangeDelCacheStats checks the stats that a memtable records when
// it rebuilds its range deletion fragments on a read.
func TestMemTableRangeDelCacheStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stats := newKeySpanCacheStats()
	m := newMemTable(memTableOptions{Options: incrementalRangeDelOptions(false), rangeDelCacheStats: stats})

	seqNum := base.SeqNum(1)
	apply := func(fn func(b *Batch)) {
		t.Helper()
		b := newBatch(nil)
		defer b.Close()
		fn(b)
		require.NoError(t, m.apply(b, seqNum))
		seqNum += base.SeqNum(b.Count())
	}
	// read creates a range deletion iterator and returns the number of
	// fragments it contains.
	read := func() (fragments int) {
		t.Helper()
		it := m.newRangeDelIter(nil)
		require.NotNil(t, it)
		defer it.Close()
		for s, err := it.First(); s != nil; s, err = it.Next() {
			require.NoError(t, err)
			fragments++
		}
		return fragments
	}
	samples := func() rangeDelCacheSamples {
		t.Helper()
		return readRangeDelCacheSamples(t, stats.metrics())
	}

	// Nothing has been recorded before any range deletion is applied, and
	// there is no cache to build.
	require.Equal(t, rangeDelCacheSamples{}, samples())
	require.Nil(t, m.newRangeDelIter(nil))
	require.Equal(t, rangeDelCacheSamples{}, samples())

	// Applying a range deletion invalidates the cache but does not build it.
	apply(func(b *Batch) { require.NoError(t, b.DeleteRange([]byte("a"), []byte("c"), nil)) })
	require.Equal(t, rangeDelCacheSamples{invalidations: 1}, samples())

	// The first read builds the cache.
	require.Equal(t, 1, read())
	require.Equal(t, rangeDelCacheSamples{
		invalidations: 1, rebuilds: 1, tombstonesSum: 1, fragmentsSum: 1, concurrencySum: 1,
	}, samples())

	// Reads of the built cache record nothing.
	for i := 0; i < 3; i++ {
		require.Equal(t, 1, read())
	}
	require.Equal(t, rangeDelCacheSamples{
		invalidations: 1, rebuilds: 1, tombstonesSum: 1, fragmentsSum: 1, concurrencySum: 1,
	}, samples())

	// Batches without range deletions leave the cache and the stats alone.
	apply(func(b *Batch) { require.NoError(t, b.Set([]byte("a"), []byte("v"), nil)) })
	apply(func(b *Batch) {
		require.NoError(t, b.RangeKeySet([]byte("a"), []byte("z"), nil, []byte("v"), nil))
	})
	require.NotNil(t, m.newRangeKeyIter(nil))
	require.Equal(t, 1, read())
	require.Equal(t, rangeDelCacheSamples{
		invalidations: 1, rebuilds: 1, tombstonesSum: 1, fragmentsSum: 1, concurrencySum: 1,
	}, samples())

	// A batch with two range deletions invalidates once, and the rebuild sees
	// all three tombstones: [a,c), [b,d), [c,e) fragment into [a,b), [b,c),
	// [c,d), [d,e).
	apply(func(b *Batch) {
		require.NoError(t, b.DeleteRange([]byte("b"), []byte("d"), nil))
		require.NoError(t, b.DeleteRange([]byte("c"), []byte("e"), nil))
	})
	require.Equal(t, uint64(2), samples().invalidations)
	require.Equal(t, 4, read())
	require.Equal(t, rangeDelCacheSamples{
		invalidations: 2, rebuilds: 2, tombstonesSum: 1 + 3, fragmentsSum: 1 + 4, concurrencySum: 2,
	}, samples())

	// Several invalidations between reads are absorbed by a single rebuild.
	apply(func(b *Batch) { require.NoError(t, b.DeleteRange([]byte("e"), []byte("f"), nil)) })
	apply(func(b *Batch) { require.NoError(t, b.DeleteRange([]byte("f"), []byte("g"), nil)) })
	require.Equal(t, uint64(4), samples().invalidations)
	require.Equal(t, 6, read())
	require.Equal(t, 6, read())
	require.Equal(t, rangeDelCacheSamples{
		invalidations: 4, rebuilds: 3, tombstonesSum: 1 + 3 + 5, fragmentsSum: 1 + 4 + 6,
		concurrencySum: 3,
	}, samples())

	// Nothing above waited on another goroutine's rebuild, or spliced.
	require.Equal(t, uint64(0), samples().readerWaits)
	require.Equal(t, uint64(0), samples().splices)
	require.Equal(t, uint64(0), samples().touched)
}

// TestMemTableRangeDelSpliceStats checks the stats that a memtable records
// when it splices range deletions into its fragments.
func TestMemTableRangeDelSpliceStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stats := newKeySpanCacheStats()
	m := newMemTable(memTableOptions{Options: incrementalRangeDelOptions(true), rangeDelCacheStats: stats})

	seqNum := base.SeqNum(1)
	apply := func(fn func(b *Batch)) {
		t.Helper()
		b := newBatch(nil)
		defer b.Close()
		fn(b)
		require.NoError(t, m.apply(b, seqNum))
		seqNum += base.SeqNum(b.Count())
	}
	// read creates a range deletion iterator and returns the number of
	// fragments it contains.
	read := func() (fragments int) {
		t.Helper()
		it := m.newRangeDelIter(nil)
		require.NotNil(t, it)
		defer it.Close()
		for s, err := it.First(); s != nil; s, err = it.Next() {
			require.NoError(t, err)
			fragments++
		}
		return fragments
	}
	samples := func() rangeDelCacheSamples {
		t.Helper()
		return readRangeDelCacheSamples(t, stats.metrics())
	}

	// Nothing has been recorded before any range deletion is applied, and
	// there is no cache to build.
	require.Equal(t, rangeDelCacheSamples{}, samples())
	require.Nil(t, m.newRangeDelIter(nil))
	require.Equal(t, rangeDelCacheSamples{}, samples())

	// Applying a range deletion splices it into the fragments, rewriting one
	// fragment and leaving a version of one fragment.
	apply(func(b *Batch) { require.NoError(t, b.DeleteRange([]byte("a"), []byte("c"), nil)) })
	afterFirst := rangeDelCacheSamples{
		invalidations: 1, splices: 1, versionFragmentsSum: 1, touched: 1, touchedSum: 1,
	}
	require.Equal(t, afterFirst, samples())

	// The first read finds the fragments built and records nothing.
	require.Equal(t, 1, read())
	require.Equal(t, afterFirst, samples())

	// Reads of the built cache record nothing.
	for i := 0; i < 3; i++ {
		require.Equal(t, 1, read())
	}
	require.Equal(t, afterFirst, samples())

	// Batches without range deletions leave the cache and the stats alone.
	apply(func(b *Batch) { require.NoError(t, b.Set([]byte("a"), []byte("v"), nil)) })
	apply(func(b *Batch) {
		require.NoError(t, b.RangeKeySet([]byte("a"), []byte("z"), nil, []byte("v"), nil))
	})
	require.NotNil(t, m.newRangeKeyIter(nil))
	require.Equal(t, 1, read())
	require.Equal(t, afterFirst, samples())

	// A batch with two range deletions counts as one invalidation and one
	// splice. [b,d) rewrites [a,c) into [a,b), [b,c) and adds [c,d). [c,e) then
	// rewrites [c,d) and adds [d,e), leaving four fragments.
	apply(func(b *Batch) {
		require.NoError(t, b.DeleteRange([]byte("b"), []byte("d"), nil))
		require.NoError(t, b.DeleteRange([]byte("c"), []byte("e"), nil))
	})
	require.Equal(t, uint64(2), samples().invalidations)
	require.Equal(t, 4, read())
	require.Equal(t, rangeDelCacheSamples{
		invalidations: 2, splices: 2, versionFragmentsSum: 1 + 4, touched: 3, touchedSum: 1 + 3 + 2,
	}, samples())

	// Each batch is spliced as it's applied, so several between reads are
	// recorded separately.
	apply(func(b *Batch) { require.NoError(t, b.DeleteRange([]byte("e"), []byte("f"), nil)) })
	apply(func(b *Batch) { require.NoError(t, b.DeleteRange([]byte("f"), []byte("g"), nil)) })
	require.Equal(t, uint64(4), samples().invalidations)
	require.Equal(t, 6, read())
	require.Equal(t, 6, read())
	require.Equal(t, rangeDelCacheSamples{
		invalidations: 4, splices: 4, versionFragmentsSum: 1 + 4 + 5 + 6, touched: 5,
		touchedSum: 1 + 3 + 2 + 1 + 1,
	}, samples())

	// Nothing above rebuilt the fragments or waited on a rebuild.
	require.Equal(t, uint64(0), samples().rebuilds)
	require.Equal(t, uint64(0), samples().readerWaits)
}

// TestMemTableRangeDelCacheStatsNil checks that a memtable without stats
// rebuilds or splices its range deletion fragments.
func TestMemTableRangeDelCacheStatsNil(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, incremental := range []bool{false, true} {
		m := newMemTable(memTableOptions{Options: incrementalRangeDelOptions(incremental)})
		b := newBatch(nil)
		require.NoError(t, b.DeleteRange([]byte("a"), []byte("c"), nil))
		require.NoError(t, m.apply(b, 1))
		b.Close()
		it := m.newRangeDelIter(nil)
		require.NotNil(t, it)
		s, err := it.First()
		require.NoError(t, err)
		require.NotNil(t, s)
		it.Close()
	}
	require.Equal(t, MemTableRangeDelCacheMetrics{}, (*keySpanCacheStats)(nil).metrics())
}

// TestMemTableRangeDelCacheConcurrentRebuilds blocks rebuilds of two
// keySpanFrags in the middle of their build to observe rebuilds in flight, and
// races readers against a rebuild.
func TestMemTableRangeDelCacheConcurrentRebuilds(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stats := newKeySpanCacheStats()
	m := newMemTable(memTableOptions{rangeDelCacheStats: stats})
	b := newBatch(nil)
	defer b.Close()
	require.NoError(t, b.DeleteRange([]byte("a"), []byte("c"), nil))
	require.NoError(t, m.apply(b, 1))

	started := make(chan struct{})
	release := make(chan struct{})
	blockingConstructSpan := func(
		ik base.InternalKey, v []byte, keysDst []keyspan.Key,
	) (keyspan.Span, error) {
		started <- struct{}{}
		<-release
		return rangeDelConstructSpan(ik, v, keysDst)
	}
	get := func(f *keySpanFrags) []keyspan.Span {
		return f.get(&m.rangeDelSkl, m.cmp, m.formatKey, blockingConstructSpan, false /* bypassDisjoint */, stats)
	}

	// Start a rebuild of first and wait for it to block inside the build. Then
	// start a rebuild of a second keySpanFrags: it observes the first one in
	// flight. Readers of first race with its rebuild.
	first, second := &keySpanFrags{count: 1}, &keySpanFrags{count: 1}
	const readers = 8
	var wg sync.WaitGroup
	spans := make([][]keyspan.Span, readers+2)
	wg.Add(1)
	go func() { defer wg.Done(); spans[0] = get(first) }()
	<-started
	wg.Add(1)
	go func() { defer wg.Done(); spans[1] = get(second) }()
	<-started
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); spans[2+i] = get(first) }()
	}
	close(release)
	wg.Wait()

	for i := range spans {
		require.Len(t, spans[i], 1)
	}
	s := readRangeDelCacheSamples(t, stats.metrics())
	require.Equal(t, uint64(2), s.rebuilds)
	// The first rebuild saw itself in flight; the second saw both.
	require.Equal(t, float64(1+2), s.concurrencySum)
	require.Equal(t, float64(2), s.tombstonesSum)
	require.Equal(t, float64(2), s.fragmentsSum)
	// Each reader either waited for the rebuild or found it already built.
	require.LessOrEqual(t, s.readerWaits, uint64(readers))
	require.Equal(t, int64(0), stats.rebuildsInFlight.Load())
	require.True(t, first.built.Load())
	require.True(t, second.built.Load())

	// Reads of a built keySpanFrags record nothing further.
	require.Len(t, get(first), 1)
	require.Equal(t, s, readRangeDelCacheSamples(t, stats.metrics()))
}

// referenceRangeDelFragments is a copy of how keySpanFrags.get rebuilt range
// deletions before disjoint tombstones bypassed the Fragmenter: every
// tombstone in skl goes through a single keyspan.Fragmenter.
func referenceRangeDelFragments(
	skl *arenaskl.Skiplist, cmp Compare, formatKey base.FormatKey,
) []keyspan.Span {
	var spans []keyspan.Span
	frag := &keyspan.Fragmenter{
		Cmp:    cmp,
		Format: formatKey,
		Emit: func(fragmented keyspan.Span) {
			spans = append(spans, fragmented)
		},
	}
	it := skl.NewIter(nil, nil)
	var keysDst []keyspan.Key
	for kv := it.First(); kv != nil; kv = it.Next() {
		s := rangedel.Decode(kv.K, kv.InPlaceValue(), keysDst)
		frag.Add(s)
		keysDst = s.Keys[len(s.Keys):]
	}
	frag.Finish()
	return spans
}

// rangeDelTestBounds are the bounds of a test range deletion, as positions
// that a rangeDelTestKeys turns into keys.
type rangeDelTestBounds struct{ start, end int }

// rangeDelTestKeys is a key space for test range deletions: positions in [0,
// maxPos], which key turns into keys in the same order, and bounds that
// randBounds keeps within maxWidth positions of each other.
type rangeDelTestKeys struct {
	maxPos, maxWidth int
	key              func(pos int) []byte
}

// smallRangeDelTestKeys has single-letter keys, so that shared and touching
// bounds are common. Its memtables hold at most 14 fragments.
var smallRangeDelTestKeys = rangeDelTestKeys{
	maxPos:   14,
	maxWidth: 14,
	key:      func(pos int) []byte { return []byte{byte('a' + pos)} },
}

// largeRangeDelTestKeys has enough positions, and narrow enough range
// deletions, for a memtable's fragments to fill many chunks of the default
// size.
var largeRangeDelTestKeys = rangeDelTestKeys{
	maxPos:   9999,
	maxWidth: 20,
	key:      func(pos int) []byte { return fmt.Appendf(nil, "%04d", pos) },
}

// randBounds returns random range deletion bounds in [0, maxPos]. They may be
// inverted or empty, or relate to prev: the same start, nested within it,
// touching it, disjoint from it, or straddling its end.
func (ks rangeDelTestKeys) randBounds(rng *rand.Rand, prev rangeDelTestBounds) rangeDelTestBounds {
	maxPos, maxWidth := ks.maxPos, ks.maxWidth
	// between returns a random position in [lo, hi], or lo if hi < lo.
	between := func(lo, hi int) int {
		if hi <= lo {
			return lo
		}
		return lo + rng.IntN(hi-lo+1)
	}
	// after returns a random end for a range deletion that starts at start.
	after := func(start int) int { return between(start+1, min(maxPos, start+maxWidth)) }
	var start, end int
	switch rng.IntN(8) {
	case 0: // Arbitrary, possibly inverted or empty.
		start = between(0, maxPos)
		end = between(max(0, start-maxWidth), min(maxPos, start+maxWidth))
	case 1: // Inverted.
		start = between(1, maxPos)
		end = between(max(0, start-maxWidth), start-1)
	case 2: // Empty.
		start = between(0, maxPos)
		end = start
	case 3: // Same start as prev, possibly a different end.
		start = prev.start
		end = after(start)
	case 4: // Nested within prev.
		start = between(prev.start, prev.end)
		end = between(start, prev.end)
	case 5: // Touching: starts where prev ends.
		start = prev.end
		end = after(start)
	case 6: // Disjoint from prev.
		start = between(prev.end+1, maxPos)
		end = after(start)
	case 7: // Straddling prev's end.
		start = between(prev.start, prev.end-1)
		end = between(prev.end, min(maxPos, prev.end+maxWidth))
	}
	return rangeDelTestBounds{start: start, end: end}
}

// TestMemTableRangeDelFragmentsMatchReference checks that the memtable's
// fragmented range deletions, rebuilt or spliced, match
// referenceRangeDelFragments over random tombstone sets.
func TestMemTableRangeDelFragmentsMatchReference(t *testing.T) {
	defer leaktest.AfterTest(t)()
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))
	ks := smallRangeDelTestKeys
	key := ks.key

	sentinel := keyspan.Key{Trailer: base.MakeTrailer(base.SeqNumMax, base.InternalKeyKindRangeDelete)}
	for iter := 0; iter < 2000; iter++ {
		// Alternate between memtables that rebuild their fragments and ones that
		// splice range deletions into them.
		incremental := iter%2 == 1
		m := newMemTable(memTableOptions{
			Options:                      incrementalRangeDelOptions(incremental),
			size:                         256 << 10,
			releaseAccountingReservation: func() {},
		})
		n := rng.IntN(48)
		tombstones := make([]rangeDelTestBounds, n)
		for i := range tombstones {
			var prev rangeDelTestBounds
			if i > 0 {
				prev = tombstones[rng.IntN(i)]
			}
			tombstones[i] = ks.randBounds(rng, prev)
		}
		// Assign sequence numbers in a random order so that they are unrelated
		// to the key order.
		seqNums := rng.Perm(n)
		var desc strings.Builder
		for i, ts := range tombstones {
			seqNum := base.SeqNum(seqNums[i] + 1)
			fmt.Fprintf(&desc, "%s-%s#%d ", key(ts.start), key(ts.end), seqNum)
			ik := base.MakeInternalKey(key(ts.start), seqNum, InternalKeyKindRangeDelete)
			require.NoError(t, m.set(ik, key(ts.end)))
		}

		want := referenceRangeDelFragments(&m.rangeDelSkl, m.cmp, m.formatKey)
		check := func(name string, got []keyspan.Span) {
			t.Helper()
			msg := fmt.Sprintf(
				"seed %d, iteration %d, incremental %t, %s\ntombstones: %s\nwant: %s\ngot:  %s",
				seed, iter, incremental, name, desc.String(), want, got)
			require.Equal(t, want == nil, got == nil, msg)
			require.Equal(t, len(want), len(got), msg)
			for i := range want {
				require.Zero(t, m.cmp(want[i].Start, got[i].Start), msg)
				require.Zero(t, m.cmp(want[i].End, got[i].End), msg)
				require.Equal(t, want[i].KeysOrder, got[i].KeysOrder, msg)
				require.Equal(t, want[i].Keys, got[i].Keys, msg)
			}
		}

		got := m.rangeDelSpans()
		check("memtable", got)

		// Appending to one span's keys must not change any other span.
		for i := range got {
			extended := append(got[i].Keys, sentinel)
			require.Equal(t, sentinel, extended[len(extended)-1])
		}
		check("memtable after appends", got)

		// The count is only a capacity hint: the skiplist may hold more
		// tombstones than it says.
		hint := rng.IntN(2*n + 1)
		f := &keySpanFrags{count: uint32(hint)}
		check(fmt.Sprintf("count hint %d", hint),
			f.get(&m.rangeDelSkl, m.cmp, m.formatKey, rangeDelConstructSpan, true /* bypassDisjoint */, nil /* stats */))

		m.free()
	}
}

// requireRangeDelSpansEqual fails the test unless got holds the same fragments
// as want: bounds that compare equal, and identical KeysOrder and Keys.
// The message is only formatted on failure.
func requireRangeDelSpansEqual(
	t testing.TB, cmp Compare, want, got []keyspan.Span, format string, args ...interface{},
) {
	t.Helper()
	msgAndArgs := append([]interface{}{format + "\nwant: %s\ngot:  %s"}, args...)
	msgAndArgs = append(msgAndArgs, want, got)
	require.Equal(t, want == nil, got == nil, msgAndArgs...)
	require.Equal(t, len(want), len(got), msgAndArgs...)
	for i := range want {
		require.Zero(t, cmp(want[i].Start, got[i].Start), msgAndArgs...)
		require.Zero(t, cmp(want[i].End, got[i].End), msgAndArgs...)
		require.Equal(t, want[i].KeysOrder, got[i].KeysOrder, msgAndArgs...)
		require.Equal(t, want[i].Keys, got[i].Keys, msgAndArgs...)
	}
}

// fragmentRangeDelTombstones returns the fragments of the given range
// deletions, fed to a keyspan.Fragmenter in the order the memtable's skiplist
// holds them.
func fragmentRangeDelTombstones(
	cmp Compare, formatKey base.FormatKey, tombstones []rangeDelTombstone,
) []keyspan.Span {
	tombstones = slices.Clone(tombstones)
	slices.SortFunc(tombstones, func(a, b rangeDelTombstone) int {
		if c := cmp(a.start, b.start); c != 0 {
			return c
		}
		return stdcmp.Compare(b.trailer, a.trailer)
	})
	var spans []keyspan.Span
	frag := keyspan.Fragmenter{
		Cmp:    cmp,
		Format: formatKey,
		Emit:   func(s keyspan.Span) { spans = append(spans, s) },
	}
	for _, ts := range tombstones {
		frag.Add(keyspan.Span{Start: ts.start, End: ts.end, Keys: []keyspan.Key{{Trailer: ts.trailer}}})
	}
	frag.Finish()
	return spans
}

// rangeDelTestChunkSizes are the chunk sizes the splice tests run with. The
// small ones split a memtable over smallRangeDelTestKeys into several chunks.
var rangeDelTestChunkSizes = []int{1, 2, 3, rangeDelChunkSize}

// newRangeDelTestMemTable returns a memtable of the given size that splices
// range deletions into fragments with chunks of chunkSize.
func newRangeDelTestMemTable(size, chunkSize int) *memTable {
	m := newMemTable(memTableOptions{
		Options:                      incrementalRangeDelOptions(true),
		size:                         size,
		releaseAccountingReservation: func() {},
	})
	m.tombstones.chunkSize = chunkSize
	return m
}

// requireRangeDelVersionValid fails the test unless the memtable's current
// version is structured as checkRangeDelVersion requires.
func requireRangeDelVersionValid(t testing.TB, m *memTable) {
	t.Helper()
	require.NoError(t, checkRangeDelVersion(m.cmp, m.tombstones.version.Load(), m.tombstones.chunkSize))
}

// TestMemTableRangeDelSpliceMatchesReference applies random range deletions
// through batches, one to several per batch and in an order unrelated to their
// sequence numbers. After every batch, it checks that the memtable's
// fragments match referenceRangeDelFragments over its skiplist. It runs with
// several chunk sizes, and with a key space large enough to fill many chunks
// of the default size.
func TestMemTableRangeDelSpliceMatchesReference(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, chunkSize := range rangeDelTestChunkSizes {
		t.Run(fmt.Sprintf("small/chunk=%d", chunkSize), func(t *testing.T) {
			testRangeDelSpliceMatchesReference(t, rangeDelSpliceTestConfig{
				ks: smallRangeDelTestKeys, chunkSize: chunkSize,
				iters: 500, minBatches: 1, maxBatches: 24, maxRangeDels: 4,
			})
		})
	}
	t.Run(fmt.Sprintf("large/chunk=%d", rangeDelChunkSize), func(t *testing.T) {
		testRangeDelSpliceMatchesReference(t, rangeDelSpliceTestConfig{
			ks: largeRangeDelTestKeys, chunkSize: rangeDelChunkSize,
			iters: 2, minBatches: 300, maxBatches: 300, maxRangeDels: 16,
		})
	})
}

// rangeDelSpliceTestConfig configures testRangeDelSpliceMatchesReference: it
// fills iters memtables, each with minBatches to maxBatches batches holding 1
// to maxRangeDels range deletions over ks.
type rangeDelSpliceTestConfig struct {
	ks                                                     rangeDelTestKeys
	chunkSize, iters, minBatches, maxBatches, maxRangeDels int
}

func testRangeDelSpliceMatchesReference(t *testing.T, cfg rangeDelSpliceTestConfig) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))
	ks, chunkSize, maxRangeDels := cfg.ks, cfg.chunkSize, cfg.maxRangeDels
	key := ks.key

	// A batch holds up to maxRangeDels range deletions and possibly a point
	// key.
	maxChunks := 0
	for iter := 0; iter < cfg.iters; iter++ {
		m := newRangeDelTestMemTable(8<<20, chunkSize)
		numBatches := cfg.minBatches + rng.IntN(cfg.maxBatches-cfg.minBatches+1)
		// Batch k's sequence numbers start at order[k]*(maxRangeDels+1)+1.
		order := rng.Perm(numBatches)
		var added []rangeDelTestBounds
		var desc strings.Builder
		for k := 0; k < numBatches; k++ {
			b := newBatch(nil)
			seqNum := base.SeqNum(order[k]*(maxRangeDels+1) + 1)
			n := 1 + rng.IntN(maxRangeDels)
			pointAt := -1
			if rng.IntN(3) == 0 {
				pointAt = rng.IntN(n + 1)
			}
			fmt.Fprintf(&desc, "batch %d:", k)
			for i := 0; i <= n; i++ {
				if i == pointAt {
					require.NoError(t, b.Set(key(rng.IntN(ks.maxPos+1)), nil, nil))
				}
				if i == n {
					break
				}
				var prev rangeDelTestBounds
				if len(added) > 0 {
					prev = added[rng.IntN(len(added))]
				}
				ts := ks.randBounds(rng, prev)
				added = append(added, ts)
				fmt.Fprintf(&desc, " %s-%s#%d", key(ts.start), key(ts.end), seqNum+base.SeqNum(b.Count()))
				require.NoError(t, b.DeleteRange(key(ts.start), key(ts.end), nil))
			}
			desc.WriteString("\n")
			require.NoError(t, m.apply(b, seqNum))
			b.Close()

			want := referenceRangeDelFragments(&m.rangeDelSkl, m.cmp, m.formatKey)
			requireRangeDelSpansEqual(t, m.cmp, want, m.rangeDelSpans(),
				"seed %d, chunk size %d, iteration %d, after batch %d\n%s",
				seed, chunkSize, iter, k, desc.String())
			requireRangeDelVersionValid(t, m)
			if v := m.tombstones.version.Load(); v != nil {
				maxChunks = max(maxChunks, len(v.chunks))
			}
		}
		m.free()
	}
	t.Logf("at most %d chunks", maxChunks)
	if ks.maxPos > 2*chunkSize {
		require.Greater(t, maxChunks, 1, "the test never split a chunk")
	}
}

// TestMemTableRangeDelSpliceConcurrent applies range deletion batches from
// several goroutines while others read the fragments. Every version a reader
// loads must be exactly the fragments of the range deletions in it, and must
// hold every range deletion whose apply returned before the load, as a reader
// that can see the batch's sequence numbers would require. The final version
// must match referenceRangeDelFragments.
func TestMemTableRangeDelSpliceConcurrent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, chunkSize := range rangeDelTestChunkSizes {
		t.Run(fmt.Sprintf("chunk=%d", chunkSize), func(t *testing.T) {
			testRangeDelSpliceConcurrent(t, chunkSize)
		})
	}
}

func testRangeDelSpliceConcurrent(t *testing.T, chunkSize int) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	ks := smallRangeDelTestKeys
	key := ks.key

	m := newRangeDelTestMemTable(8<<20, chunkSize)
	defer m.free()
	const writers, readers, batchesPerWriter, maxRangeDels = 4, 4, 50, 3

	var seqNums atomic.Uint64
	var mu sync.Mutex
	// byTrailer holds every range deletion. A writer adds a batch's range
	// deletions before applying it.
	byTrailer := make(map[base.InternalKeyTrailer]rangeDelTombstone)
	// applied holds the trailers of the non-empty range deletions of every
	// batch whose apply has returned.
	var applied []base.InternalKeyTrailer
	// outOfOrder counts the batches whose apply returned after that of a batch
	// with higher sequence numbers.
	var outOfOrder int
	var maxApplied base.SeqNum

	var writersGroup, readersGroup errgroup.Group
	for w := 0; w < writers; w++ {
		rng := rand.New(rand.NewPCG(seed, uint64(w)))
		writersGroup.Go(func() error {
			for range batchesPerWriter {
				n := 1 + rng.IntN(maxRangeDels)
				first := base.SeqNum(seqNums.Add(uint64(n)) - uint64(n) + 1)
				b := newBatch(nil)
				tombstones := make([]rangeDelTombstone, n)
				var prev rangeDelTestBounds
				for i := range tombstones {
					bounds := ks.randBounds(rng, prev)
					prev = bounds
					tombstones[i] = rangeDelTombstone{
						start:   key(bounds.start),
						end:     key(bounds.end),
						trailer: base.MakeTrailer(first+base.SeqNum(i), InternalKeyKindRangeDelete),
					}
					if err := b.DeleteRange(tombstones[i].start, tombstones[i].end, nil); err != nil {
						return err
					}
				}
				mu.Lock()
				for _, ts := range tombstones {
					byTrailer[ts.trailer] = ts
				}
				mu.Unlock()
				// Let batches reach apply out of sequence number order.
				runtime.Gosched()
				if err := m.apply(b, first); err != nil {
					return err
				}
				b.Close()
				mu.Lock()
				if first < maxApplied {
					outOfOrder++
				}
				maxApplied = max(maxApplied, first)
				for _, ts := range tombstones {
					if m.cmp(ts.start, ts.end) < 0 {
						applied = append(applied, ts.trailer)
					}
				}
				mu.Unlock()
			}
			return nil
		})
	}

	var done atomic.Bool
	var checks, multiChunk atomic.Int64
	check := func() error {
		checks.Add(1)
		mu.Lock()
		mustHold := slices.Clone(applied)
		mu.Unlock()
		v := m.tombstones.version.Load()
		if err := checkRangeDelVersion(m.cmp, v, chunkSize); err != nil {
			return err
		}
		if v != nil && len(v.chunks) > 1 {
			multiChunk.Add(1)
		}
		spans := v.spans()
		held := make(map[base.InternalKeyTrailer]bool)
		for i := range spans {
			for _, k := range spans[i].Keys {
				held[k.Trailer] = true
			}
		}
		for _, trailer := range mustHold {
			if !held[trailer] {
				return errors.Errorf("seed %d: %s applied before the load is missing from %s",
					seed, trailer, spans)
			}
		}
		tombstones := make([]rangeDelTombstone, 0, len(held))
		mu.Lock()
		for trailer := range held {
			tombstones = append(tombstones, byTrailer[trailer])
		}
		mu.Unlock()
		want := fragmentRangeDelTombstones(m.cmp, m.formatKey, tombstones)
		if !rangeDelSpansEqual(m.cmp, want, spans) {
			return errors.Errorf("seed %d: loaded fragments aren't the fragments of their "+
				"range deletions\nwant: %s\ngot:  %s", seed, want, spans)
		}
		return nil
	}
	for r := 0; r < readers; r++ {
		readersGroup.Go(func() error {
			for !done.Load() {
				if err := check(); err != nil {
					return err
				}
				runtime.Gosched()
			}
			return check()
		})
	}
	require.NoError(t, writersGroup.Wait())
	done.Store(true)
	require.NoError(t, readersGroup.Wait())
	t.Logf("%d of %d batches applied out of sequence number order; readers checked %d versions, "+
		"%d with several chunks", outOfOrder, writers*batchesPerWriter, checks.Load(), multiChunk.Load())

	want := referenceRangeDelFragments(&m.rangeDelSkl, m.cmp, m.formatKey)
	requireRangeDelSpansEqual(t, m.cmp, want, m.rangeDelSpans(), "seed %d", seed)
	requireRangeDelVersionValid(t, m)
}

// TestMemTableRangeDelSpliceAliasing holds on to every version of a memtable's
// fragments and checks that splicing later range deletions leaves each one
// unchanged, and that no published slice has spare capacity that an append
// could write into.
func TestMemTableRangeDelSpliceAliasing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))
	ks := smallRangeDelTestKeys
	key := ks.key

	deepCopy := func(spans []keyspan.Span) []keyspan.Span {
		if spans == nil {
			return nil
		}
		c := make([]keyspan.Span, len(spans))
		for i, s := range spans {
			c[i] = keyspan.Span{
				Start:     slices.Clone(s.Start),
				End:       slices.Clone(s.End),
				Keys:      slices.Clone(s.Keys),
				KeysOrder: s.KeysOrder,
			}
		}
		return c
	}
	set := func(m *memTable, start, end []byte, seqNum base.SeqNum) {
		t.Helper()
		require.NoError(t, m.set(base.MakeInternalKey(start, seqNum, InternalKeyKindRangeDelete), end))
	}

	for _, chunkSize := range rangeDelTestChunkSizes {
		for iter := 0; iter < 100; iter++ {
			m := newRangeDelTestMemTable(256<<10, chunkSize)
			const n = 40
			seqNums := rng.Perm(n)
			// A version is a published version, its chunk index as it was
			// published, and a copy of its spans.
			type version struct {
				v      *rangeDelVersion
				chunks []*rangeDelChunk
				spans  []keyspan.Span
			}
			var versions []version
			var added []rangeDelTestBounds
			for i := 0; i < n; i++ {
				var prev rangeDelTestBounds
				if len(added) > 0 {
					prev = added[rng.IntN(len(added))]
				}
				bounds := ks.randBounds(rng, prev)
				added = append(added, bounds)
				set(m, key(bounds.start), key(bounds.end), base.SeqNum(seqNums[i]+1))

				v := m.tombstones.version.Load()
				if v == nil {
					continue
				}
				require.Equal(t, len(v.chunks), cap(v.chunks))
				for _, c := range v.chunks {
					require.Equal(t, len(c.spans), cap(c.spans))
					for j := range c.spans {
						require.Equal(t, len(c.spans[j].Keys), cap(c.spans[j].Keys))
					}
				}
				if len(versions) == 0 || versions[len(versions)-1].v != v {
					versions = append(versions, version{
						v: v, chunks: slices.Clone(v.chunks), spans: deepCopy(v.spans()),
					})
				}
				for k, old := range versions {
					msg := fmt.Sprintf("seed %d, chunk size %d, iteration %d: version %d changed by "+
						"range deletion %d", seed, chunkSize, iter, k, i)
					require.Equal(t, old.chunks, old.v.chunks, msg)
					require.Equal(t, old.spans, old.v.spans(), msg)
				}
			}
			m.free()
		}
	}

	// Fragments that a range deletion doesn't cover keep their Keys, including
	// the parts of a fragment that it splits.
	m := newRangeDelTestMemTable(256<<10, rangeDelChunkSize)
	defer m.free()
	set(m, []byte("a"), []byte("b"), 1)
	set(m, []byte("e"), []byte("z"), 2)
	v1 := m.rangeDelSpans()
	set(m, []byte("c"), []byte("d"), 3)
	v2 := m.rangeDelSpans()
	require.Len(t, v2, 3)
	require.Same(t, &v1[0].Keys[0], &v2[0].Keys[0])
	require.Same(t, &v1[1].Keys[0], &v2[2].Keys[0])
	set(m, []byte("m"), []byte("n"), 4)
	v3 := m.rangeDelSpans()
	require.Equal(t, "[a-b:{(#1,RANGEDEL)} c-d:{(#3,RANGEDEL)} e-m:{(#2,RANGEDEL)} "+
		"m-n:{(#4,RANGEDEL) (#2,RANGEDEL)} n-z:{(#2,RANGEDEL)}]", fmt.Sprint(v3))
	require.Same(t, &v2[2].Keys[0], &v3[2].Keys[0])
	require.Same(t, &v2[2].Keys[0], &v3[4].Keys[0])
}

// makeRangeDelTestVersion returns a version holding the given chunks of
// spans, where each span is written as "start-end#seqnum,...".
func makeRangeDelTestVersion(chunks ...[]string) *rangeDelVersion {
	v := &rangeDelVersion{}
	for _, c := range chunks {
		chunk := &rangeDelChunk{spans: make([]keyspan.Span, len(c))}
		for i, s := range c {
			bounds, seqNums, _ := strings.Cut(s, "#")
			start, end, _ := strings.Cut(bounds, "-")
			var keys []keyspan.Key
			for _, sn := range strings.Split(seqNums, ",") {
				keys = append(keys, keyspan.Key{
					Trailer: base.MakeTrailer(base.ParseSeqNum(sn), InternalKeyKindRangeDelete),
				})
			}
			chunk.spans[i] = keyspan.Span{Start: []byte(start), End: []byte(end), Keys: slices.Clip(keys)}
		}
		v.chunks = append(v.chunks, chunk)
		v.n += len(c)
	}
	v.chunks = slices.Clip(v.chunks)
	return v
}

// TestMemTableRangeDelSpliceChunks checks which chunks spliceRangeDelChunks
// splices a range deletion into, that it leaves the others alone, and that the
// result holds the fragments of every range deletion.
func TestMemTableRangeDelSpliceChunks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cmp := base.DefaultComparer.Compare
	const chunkSize = 2
	v := makeRangeDelTestVersion(
		[]string{"b-c#1", "d-e#2"},
		[]string{"f-g#3", "h-i#4"},
		[]string{"j-k#5", "l-m#6", "n-o#7"},
	)
	require.NoError(t, checkRangeDelVersion(cmp, v, chunkSize))
	tombstones := func(v *rangeDelVersion) []rangeDelTombstone {
		var ts []rangeDelTombstone
		for _, s := range v.spans() {
			ts = append(ts, rangeDelTombstone{start: s.Start, end: s.End, trailer: s.Keys[0].Trailer})
		}
		return ts
	}
	existing := tombstones(v)

	for _, tc := range []struct {
		start, end string
		lo, hi     int
	}{
		{"d", "e", 0, 1},         // Covers a fragment of the first chunk.
		{"c", "d", 0, 1},         // A gap inside the first chunk.
		{"e", "f", 1, 2},         // The gap between the first two chunks.
		{"e\x00", "f", 1, 2},     // Inside that gap.
		{"a", "b", 0, 1},         // Before every chunk.
		{"0", "a", 0, 1},         // Before every chunk, not touching it.
		{"o", "z", 2, 3},         // After every chunk.
		{"g", "g\x00", 1, 2},     // Inside a fragment.
		{"i", "j", 2, 3},         // The gap between the last two chunks.
		{"g", "k", 1, 3},         // Across two chunks.
		{"c", "n\x00", 0, 3},     // Across every chunk.
		{"a", "z", 0, 3},         // Around every chunk.
		{"e", "f\x00", 1, 2},     // Starts where a chunk ends.
		{"d\x00", "f", 0, 1},     // Ends where a chunk starts.
		{"i\x00", "j\x00", 2, 3}, // Starts after a chunk ends.
	} {
		t.Run(fmt.Sprintf("%q-%q", tc.start, tc.end), func(t *testing.T) {
			ts := rangeDelTombstone{
				start: []byte(tc.start), end: []byte(tc.end),
				trailer: base.MakeTrailer(100, InternalKeyKindRangeDelete),
			}
			lo, hi, repl, added, _ := spliceRangeDelChunks(cmp, chunkSize, v.chunks, ts)
			require.Equal(t, [2]int{tc.lo, tc.hi}, [2]int{lo, hi})
			next := &rangeDelVersion{
				chunks: slices.Concat(v.chunks[:lo], repl, v.chunks[hi:]),
				n:      v.n + added,
			}
			next.chunks = slices.Clip(next.chunks)
			require.NoError(t, checkRangeDelVersion(cmp, next, chunkSize))
			// Chunks outside [lo, hi) are shared.
			for i := 0; i < lo; i++ {
				require.Same(t, v.chunks[i], next.chunks[i])
			}
			for i := hi; i < len(v.chunks); i++ {
				require.Same(t, v.chunks[i], next.chunks[len(next.chunks)-len(v.chunks)+i])
			}
			want := fragmentRangeDelTombstones(cmp, base.DefaultFormatter, append(slices.Clone(existing), ts))
			requireRangeDelSpansEqual(t, cmp, want, next.spans(), "splicing %s-%s", tc.start, tc.end)
		})
	}

	// An empty version gets a single chunk.
	ts := rangeDelTombstone{
		start: []byte("a"), end: []byte("b"), trailer: base.MakeTrailer(1, InternalKeyKindRangeDelete),
	}
	lo, hi, repl, added, touched := spliceRangeDelChunks(cmp, chunkSize, nil, ts)
	require.Equal(t, [5]int{0, 0, 1, 1, 1}, [5]int{lo, hi, len(repl), added, touched})
	require.Equal(t, "[a-b:{(#1,RANGEDEL)}]", fmt.Sprint(repl[0].spans))
}

// TestMemTableRangeDelChunkSpans checks how chunkRangeDelSpans splits spans.
func TestMemTableRangeDelChunkSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, chunkSize := range []int{1, 2, 3, 128} {
		for n := 1; n <= 5*chunkSize+1; n++ {
			spans := make([]keyspan.Span, n, n+10)
			for i := range spans {
				spans[i] = keyspan.Span{Start: []byte{byte(i)}, End: []byte{byte(i + 1)}}
			}
			chunks := chunkRangeDelSpans(spans, chunkSize)
			if n <= 2*chunkSize {
				require.Len(t, chunks, 1)
				require.Same(t, &spans[0], &chunks[0].spans[0])
			} else {
				require.Len(t, chunks, n/chunkSize)
			}
			var got []keyspan.Span
			for _, c := range chunks {
				require.Equal(t, len(c.spans), cap(c.spans))
				if len(chunks) > 1 {
					require.GreaterOrEqual(t, len(c.spans), chunkSize)
					require.Less(t, len(c.spans), 2*chunkSize)
					// A piece doesn't share an array with the input.
					require.NotSame(t, &spans[len(got)], &c.spans[0])
				}
				got = append(got, c.spans...)
			}
			require.Equal(t, spans, got)
		}
	}
}

// TestMemTableRangeDelCheckVersion checks that checkRangeDelVersion rejects
// versions that break its structure rules.
func TestMemTableRangeDelCheckVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cmp := base.DefaultComparer.Compare
	valid := func() *rangeDelVersion {
		return makeRangeDelTestVersion(
			[]string{"a-b#1", "b-c#2,1"},
			[]string{"c-d#3", "e-f#4"},
			[]string{"f-g#5"},
		)
	}
	require.NoError(t, checkRangeDelVersion(cmp, nil, 2))
	require.NoError(t, checkRangeDelVersion(cmp, valid(), 2))

	for _, tc := range []struct {
		name   string
		mutate func(v *rangeDelVersion)
		size   int
	}{
		{"no chunks", func(v *rangeDelVersion) { v.chunks, v.n = v.chunks[:0], 0 }, 2},
		{"index capacity", func(v *rangeDelVersion) { v.chunks = append(v.chunks, v.chunks[0])[:3] }, 2},
		{"empty chunk", func(v *rangeDelVersion) { v.chunks[2].spans = v.chunks[2].spans[:0:0]; v.n-- }, 2},
		{"chunk too large", func(v *rangeDelVersion) {
			v.chunks[2] = makeRangeDelTestVersion([]string{"f-g#5", "g-h#6", "h-i#7"}).chunks[0]
			v.n += 2
		}, 1},
		{"chunk capacity", func(v *rangeDelVersion) {
			v.chunks[1].spans = append(v.chunks[1].spans, keyspan.Span{})[:2]
		}, 2},
		{"chunks overlap", func(v *rangeDelVersion) { v.chunks[1].spans[0].Start = []byte("b\x00") }, 2},
		{"chunks out of order", func(v *rangeDelVersion) { v.chunks[0], v.chunks[1] = v.chunks[1], v.chunks[0] }, 2},
		{"count", func(v *rangeDelVersion) { v.n++ }, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := valid()
			tc.mutate(v)
			require.Error(t, checkRangeDelVersion(cmp, v, tc.size))
		})
	}
}

// TestMemTableRangeDelChunkIter checks that the iterator over a version with
// several chunks behaves as a keyspan.Iter over the version's spans in a single
// slice, over random sequences of positioning calls, and that the spans it
// returns point into the chunks.
func TestMemTableRangeDelChunkIter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))

	testVersion := func(t *testing.T, m *memTable, ks rangeDelTestKeys, sequences, ops int) {
		v := m.tombstones.version.Load()
		flat := v.spans()
		// stored[k] is where flat[k] is stored in the version's chunks.
		var stored []*keyspan.Span
		for _, c := range v.chunks {
			for i := range c.spans {
				stored = append(stored, &c.spans[i])
			}
		}
		index := make(map[*keyspan.Span]int, len(flat))
		for k := range flat {
			index[&flat[k]] = k
		}
		seekKey := func() []byte {
			switch rng.IntN(10) {
			case 0:
				return nil
			case 1:
				return []byte("\xff")
			}
			k := ks.key(rng.IntN(ks.maxPos + 1))
			if rng.IntN(3) == 0 {
				k = append(k, 0)
			}
			return k
		}
		for seq := 0; seq < sequences; seq++ {
			want := keyspan.NewIter(m.cmp, flat)
			got := m.newRangeDelIter(nil)
			require.IsType(t, (*rangeDelChunkIter)(nil), got)
			var desc strings.Builder
			for op := 0; op < ops; op++ {
				var ws, gs *keyspan.Span
				var werr, gerr error
				switch rng.IntN(8) {
				case 0:
					k := seekKey()
					fmt.Fprintf(&desc, "SeekGE(%q) ", k)
					ws, werr = want.SeekGE(k)
					gs, gerr = got.SeekGE(k)
				case 1:
					k := seekKey()
					fmt.Fprintf(&desc, "SeekLT(%q) ", k)
					ws, werr = want.SeekLT(k)
					gs, gerr = got.SeekLT(k)
				case 2:
					desc.WriteString("First ")
					ws, werr = want.First()
					gs, gerr = got.First()
				case 3:
					desc.WriteString("Last ")
					ws, werr = want.Last()
					gs, gerr = got.Last()
				case 4, 5:
					desc.WriteString("Next ")
					ws, werr = want.Next()
					gs, gerr = got.Next()
				default:
					desc.WriteString("Prev ")
					ws, werr = want.Prev()
					gs, gerr = got.Prev()
				}
				require.NoError(t, werr)
				require.NoError(t, gerr)
				var wantSpan *keyspan.Span
				if ws != nil {
					wantSpan = stored[index[ws]]
				}
				if gs != wantSpan {
					t.Fatalf("seed %d: %s\ngot %s at %p, want %s at %p\nspans: %s",
						seed, desc.String(), gs, gs, wantSpan, wantSpan, flat)
				}
			}
			got.Close()
		}
	}

	set := func(m *memTable, ks rangeDelTestKeys, b rangeDelTestBounds, seqNum base.SeqNum) {
		ik := base.MakeInternalKey(ks.key(b.start), seqNum, InternalKeyKindRangeDelete)
		require.NoError(t, m.set(ik, ks.key(b.end)))
	}
	for _, chunkSize := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("small/chunk=%d", chunkSize), func(t *testing.T) {
			ks := smallRangeDelTestKeys
			tested := 0
			for iter := 0; iter < 200; iter++ {
				m := newRangeDelTestMemTable(256<<10, chunkSize)
				var prev rangeDelTestBounds
				for i, n := 0, 1+rng.IntN(40); i < n; i++ {
					prev = ks.randBounds(rng, prev)
					set(m, ks, prev, base.SeqNum(i+1))
				}
				if v := m.tombstones.version.Load(); v != nil && len(v.chunks) > 1 {
					testVersion(t, m, ks, 10, 20)
					tested++
				}
				m.free()
			}
			require.Greater(t, tested, 100)
		})
	}
	t.Run(fmt.Sprintf("large/chunk=%d", rangeDelChunkSize), func(t *testing.T) {
		ks := largeRangeDelTestKeys
		m := newRangeDelTestMemTable(8<<20, rangeDelChunkSize)
		defer m.free()
		// Apply the range deletions in batches of 100, so that invariants
		// builds don't compare the fragments with a rebuild after each one.
		var prev rangeDelTestBounds
		for i := 0; i < 30; i++ {
			b := newBatch(nil)
			for j := 0; j < 100; j++ {
				prev = ks.randBounds(rng, prev)
				require.NoError(t, b.DeleteRange(ks.key(prev.start), ks.key(prev.end), nil))
			}
			require.NoError(t, m.apply(b, base.SeqNum(100*i+1)))
			b.Close()
		}
		v := m.tombstones.version.Load()
		t.Logf("%d fragments in %d chunks", v.n, len(v.chunks))
		require.Greater(t, len(v.chunks), 4)
		testVersion(t, m, ks, 100, 50)
	})
}

// TestMemTableRangeDelSpliceStaircase applies range deletions that each
// delete from the start of a queue to its ack level, which advances, as a
// queue reload does. Each covers every earlier fragment of its queue, and the
// fragments must match referenceRangeDelFragments after every batch. The
// staircase has a single queue; the queues case interleaves several, and
// advances a queue's ack level without a reload most of the time.
func TestMemTableRangeDelSpliceStaircase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))
	for _, chunkSize := range []int{2, rangeDelChunkSize} {
		for _, tc := range []struct {
			name             string
			queues, deletes  int
			reloadOneInEvery int
		}{
			{"staircase", 1, 300, 1},
			{"queues", 4, 800, 4},
		} {
			t.Run(fmt.Sprintf("%s/chunk=%d", tc.name, chunkSize), func(t *testing.T) {
				m := newRangeDelTestMemTable(16<<20, chunkSize)
				defer m.free()
				key := func(q, pos int) []byte { return fmt.Appendf(nil, "q%02d/%05d", q, pos) }
				acks := make([]int, tc.queues)
				for i := 0; i < tc.deletes; i++ {
					q := rng.IntN(tc.queues)
					start, end := acks[q], acks[q]+1+rng.IntN(3)
					if rng.IntN(tc.reloadOneInEvery) == 0 {
						start = 0
					}
					acks[q] = end
					b := newBatch(nil)
					require.NoError(t, b.DeleteRange(key(q, start), key(q, end), nil))
					require.NoError(t, m.apply(b, base.SeqNum(i+1)))
					b.Close()
					want := referenceRangeDelFragments(&m.rangeDelSkl, m.cmp, m.formatKey)
					requireRangeDelSpansEqual(t, m.cmp, want, m.rangeDelSpans(),
						"seed %d, after range deletion %d", seed, i)
					requireRangeDelVersionValid(t, m)
				}
				v := m.tombstones.version.Load()
				t.Logf("%d fragments in %d chunks", v.n, len(v.chunks))
			})
		}
	}
}

// TestMemTableRangeDelCheckFragments checks that checkRangeDelFragments panics
// on fragments that differ from a rebuild of the skiplist, and skips the
// comparison when the skiplist doesn't hold exactly the given number of range
// deletions, or when the version holds more than maxCheckedRangeDelFragments
// fragments.
func TestMemTableRangeDelCheckFragments(t *testing.T) {
	defer leaktest.AfterTest(t)()
	m := newRangeDelTestMemTable(256<<10, rangeDelChunkSize)
	defer m.free()
	set := func(start, end string, seqNum base.SeqNum) {
		ik := base.MakeInternalKey([]byte(start), seqNum, InternalKeyKindRangeDelete)
		require.NoError(t, m.set(ik, []byte(end)))
	}
	set("a", "c", 1)
	set("b", "d", 2)
	check := func(spans []keyspan.Span, chunkSize, n int) func() {
		v := &rangeDelVersion{chunks: chunkRangeDelSpans(spans, chunkSize), n: len(spans)}
		return func() { checkRangeDelFragments(&m.rangeDelSkl, m.cmp, m.formatKey, v, n) }
	}

	spans := m.rangeDelSpans()
	require.Len(t, spans, 3)
	for _, chunkSize := range []int{1, rangeDelChunkSize} {
		require.NotPanics(t, check(spans, chunkSize, 2))
		missingKey := slices.Clone(spans)
		missingKey[1].Keys = missingKey[1].Keys[:1]
		require.Panics(t, check(missingKey, chunkSize, 2))
		require.Panics(t, check(spans[:2], chunkSize, 2))

		// The skiplist holds 2 range deletions, so a different count means that
		// some aren't spliced yet (or that too many were).
		require.NotPanics(t, check(missingKey, chunkSize, 1))
		require.NotPanics(t, check(missingKey, chunkSize, 3))
	}

	// A version with more than 4096 fragments isn't checked.
	large := &rangeDelVersion{chunks: chunkRangeDelSpans(spans[:1], 1), n: 4096}
	require.Panics(t, func() { checkRangeDelFragments(&m.rangeDelSkl, m.cmp, m.formatKey, large, 2) })
	large.n++
	require.NotPanics(t, func() { checkRangeDelFragments(&m.rangeDelSkl, m.cmp, m.formatKey, large, 2) })
}

// TestMemTableRangeDelOptionSwitch flips
// Options.Experimental.IncrementalRangeDelFragments between memtables and
// between batches. Each memtable must keep the mode it was created with, use
// only that mode's structure, and hold the fragments of its range deletions
// after every batch.
func TestMemTableRangeDelOptionSwitch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))
	ks := smallRangeDelTestKeys

	var incremental atomic.Bool
	opts := &Options{}
	opts.Experimental.IncrementalRangeDelFragments = incremental.Load
	type memWithMode struct {
		m           *memTable
		incremental bool
		seqNum      base.SeqNum
	}
	var mems []memWithMode
	defer func() {
		for _, mm := range mems {
			mm.m.free()
		}
	}()
	for i := 0; i < 6; i++ {
		incremental.Store(i%2 == 0)
		m := newMemTable(memTableOptions{
			Options: opts, size: 256 << 10, releaseAccountingReservation: func() {},
		})
		mems = append(mems, memWithMode{m: m, incremental: i%2 == 0, seqNum: 1})
		for j := 0; j < 20; j++ {
			incremental.Store(rng.IntN(2) == 0)
			mm := &mems[rng.IntN(len(mems))]
			b := newBatch(nil)
			var prev rangeDelTestBounds
			for k, n := 0, 1+rng.IntN(3); k < n; k++ {
				prev = ks.randBounds(rng, prev)
				require.NoError(t, b.DeleteRange(ks.key(prev.start), ks.key(prev.end), nil))
			}
			require.NoError(t, mm.m.apply(b, mm.seqNum))
			mm.seqNum += base.SeqNum(b.Count())
			b.Close()

			require.Equal(t, mm.incremental, mm.m.incrementalRangeDels)
			if mm.incremental {
				require.Nil(t, mm.m.tombstoneCache.frags.Load())
			} else {
				require.Nil(t, mm.m.tombstones.version.Load())
			}
			want := referenceRangeDelFragments(&mm.m.rangeDelSkl, mm.m.cmp, mm.m.formatKey)
			requireRangeDelSpansEqual(t, mm.m.cmp, want, mm.m.rangeDelSpans(),
				"seed %d, memtable created with incremental=%t", seed, mm.incremental)
		}
	}
}

// TestMemTableRangeDelOptionSwitchDB flips
// Options.Experimental.IncrementalRangeDelFragments while a DB is open. The
// mutable memtable keeps its mode until it's rotated, the memtable that
// replaces it takes the new mode, and reads see every range deletion
// throughout.
func TestMemTableRangeDelOptionSwitchDB(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var incremental atomic.Bool
	opts := &Options{FS: vfs.NewMem(), Logger: testutils.Logger{T: t}}
	opts.Experimental.IncrementalRangeDelFragments = incremental.Load
	d, err := Open("", opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	mutableMode := func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.mu.mem.mutable.incrementalRangeDels
	}
	key := func(i int, suffix string) []byte { return fmt.Appendf(nil, "%03d/%s", i, suffix) }
	// requireDeleted checks that range deletion i deleted key i/b but not i/c.
	requireDeleted := func(i int) {
		t.Helper()
		_, _, err := d.Get(key(i, "b"))
		require.ErrorIs(t, err, ErrNotFound)
		v, closer, err := d.Get(key(i, "c"))
		require.NoError(t, err)
		require.Equal(t, "v", string(v))
		require.NoError(t, closer.Close())
	}
	write := func(i int) {
		t.Helper()
		require.NoError(t, d.Set(key(i, "b"), []byte("v"), nil))
		require.NoError(t, d.Set(key(i, "c"), []byte("v"), nil))
		require.NoError(t, d.DeleteRange(key(i, "a"), key(i, "c"), nil))
	}

	require.False(t, mutableMode())
	for i := 0; i < 6; i++ {
		mode := mutableMode()
		write(2 * i)
		requireDeleted(2 * i)
		// Flipping the option leaves the mutable memtable's mode alone.
		incremental.Store(!mode)
		require.Equal(t, mode, mutableMode())
		write(2*i + 1)
		for j := 0; j <= 2*i+1; j++ {
			requireDeleted(j)
		}
		// The memtable that replaces it takes the new mode.
		require.NoError(t, d.Flush())
		require.Equal(t, !mode, mutableMode())
		for j := 0; j <= 2*i+1; j++ {
			requireDeleted(j)
		}
	}
}

func TestMemTableReserved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	m := newMemTable(memTableOptions{size: 5000})
	// Increase to 2 references.
	m.writerRef()
	// The initial reservation accounts for the already allocated bytes from the
	// arena.
	require.Equal(t, m.reserved, m.skl.Arena().Size())
	b := newBatch(nil)
	b.Set([]byte("blueberry"), []byte("pie"), nil)
	require.NotEqual(t, 0, int(b.memTableSize))
	prevReserved := m.reserved
	m.prepare(b)
	require.Equal(t, int(m.reserved), int(b.memTableSize)+int(prevReserved))
}

func TestMemTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var m *memTable
	var buf bytes.Buffer
	batches := map[string]*Batch{}

	summary := func() string {
		return fmt.Sprintf("%d of %d bytes available",
			m.availBytes(), m.totalBytes())
	}

	datadriven.RunTest(t, "testdata/mem_table", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "new":
			var o memTableOptions
			td.MaybeScanArgs(t, "size", &o.size)
			m = newMemTable(o)
			return ""
		case "prepare":
			var name string
			td.ScanArgs(t, "name", &name)
			b := newBatch(nil)
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			batches[name] = b
			if err := m.prepare(b); err != nil {
				return err.Error()
			}
			return summary()
		case "apply":
			var name string
			var seqNum uint64
			td.ScanArgs(t, "name", &name)
			td.ScanArgs(t, "seq", &seqNum)
			if err := m.apply(batches[name], base.SeqNum(seqNum)); err != nil {
				return err.Error()
			}
			delete(batches, name)
			return summary()
		case "computePossibleOverlaps":
			stopAfterFirst := td.HasArg("stop-after-first")

			var keyRanges []bounded
			for l := range crstrings.LinesSeq(td.Input) {
				s := strings.FieldsFunc(l, func(r rune) bool { return unicode.IsSpace(r) || r == '-' })
				keyRanges = append(keyRanges, KeyRange{Start: []byte(s[0]), End: []byte(s[1])})
			}

			m.computePossibleOverlaps(func(b bounded) shouldContinue {
				fmt.Fprintf(&buf, "%s\n", b)
				if stopAfterFirst {
					return stopIteration
				}
				return continueIteration
			}, keyRanges...)

			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

func buildMemTable(b *testing.B) (*memTable, [][]byte) {
	m := newMemTable(memTableOptions{})
	var keys [][]byte
	var ikey InternalKey
	for i := 0; ; i++ {
		key := []byte(fmt.Sprintf("%08d", i))
		keys = append(keys, key)
		ikey = base.MakeInternalKey(key, 0, InternalKeyKindSet)
		if m.set(ikey, nil) == arenaskl.ErrArenaFull {
			break
		}
	}
	return m, keys
}

func BenchmarkMemTableIterSeekGE(b *testing.B) {
	m, keys := buildMemTable(b)
	iter := m.newIter(nil)
	rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter.SeekGE(keys[rng.IntN(len(keys))], base.SeekGEFlagsNone)
	}
}

func BenchmarkMemTableIterSeqSeekGEWithBounds(b *testing.B) {
	m, keys := buildMemTable(b)
	rng := rand.New(rand.NewPCG(0, uint64(17136275210000)))
	// Set bounds to restrict iteration to the middle 50% of keys.
	iter := m.newIter(&IterOptions{
		LowerBound: keys[len(keys)/4],
		UpperBound: keys[3*len(keys)/4],
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter.SeekGE(keys[rng.IntN(len(keys))], base.SeekGEFlagsNone)
	}
}

// BenchmarkMemTableIterSeekGESuccessiveWithBounds benchmarks a particular case
// where an upper bound excludes the majority of the memtable keys and the user
// seeks the iterator with successively increasing keys. This pattern is
// expected to be common in CockroachDB: eg, intent resolution with an upper
// bound at the end of the lock table span, or a MVCC iterator with an upper
// bound restricting constraining iteration to a single CockroachDB Range.
func BenchmarkMemTableIterSeekGESuccessiveWithBounds(b *testing.B) {
	m, keys := buildMemTable(b)
	iter := m.newIter(&IterOptions{
		UpperBound: keys[1],
	})
	flags := base.SeekGEFlagsNone.EnableTrySeekUsingNext()

	seekKeys := make([][]byte, 256)
	for i := 1; i < len(seekKeys); i++ {
		seekKeys[i] = append(append([]byte(nil), keys[0]...), byte(i-1))
	}

	b.ResetTimer()
	iter.SeekGE(keys[0], base.SeekGEFlagsNone)
	for i := 0; i < b.N-1; i++ {
		iter.SeekGE(seekKeys[i%len(seekKeys)], flags)
	}
}

func BenchmarkMemTableIterNext(b *testing.B) {
	m, _ := buildMemTable(b)
	iter := m.newIter(nil)
	_ = iter.First()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv := iter.Next()
		if kv == nil {
			kv = iter.First()
		}
		_ = kv
	}
}

func BenchmarkMemTableIterNextWithBounds(b *testing.B) {
	m, keys := buildMemTable(b)
	// Set bounds to restrict iteration to the middle 50% of keys.
	opts := &IterOptions{
		LowerBound: keys[len(keys)/4],
		UpperBound: keys[3*len(keys)/4],
	}
	iter := m.newIter(opts)
	_ = iter.SeekGE(opts.LowerBound, base.SeekGEFlagsNone)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv := iter.Next()
		if kv == nil {
			kv = iter.SeekGE(opts.LowerBound, base.SeekGEFlagsNone)
		}
		_ = kv
	}
}

func BenchmarkMemTableIterPrev(b *testing.B) {
	m, _ := buildMemTable(b)
	iter := m.newIter(nil)
	_ = iter.Last()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv := iter.Prev()
		if kv == nil {
			kv = iter.Last()
		}
		_ = kv
	}
}

func BenchmarkMemTableIterPrevWithBounds(b *testing.B) {
	m, keys := buildMemTable(b)
	// Set bounds to restrict iteration to the middle 50% of keys.
	opts := &IterOptions{
		LowerBound: keys[len(keys)/4],
		UpperBound: keys[3*len(keys)/4],
	}
	iter := m.newIter(opts)
	_ = iter.SeekLT(opts.UpperBound, base.SeekLTFlagsNone)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv := iter.Prev()
		if kv == nil {
			kv = iter.SeekLT(opts.UpperBound, base.SeekLTFlagsNone)
		}
		_ = kv
	}
}

// rangeDelBenchKey returns the key at position i for the range deletion
// benchmarks.
func rangeDelBenchKey(i int) []byte { return fmt.Appendf(nil, "%08d", i) }

// newRangeDelBenchMemTable returns a memtable holding n range deletions in the
// given layout, with its fragments built by a single full rebuild rather than n
// splices.
func newRangeDelBenchMemTable(b *testing.B, n int, layout string) *memTable {
	m := newMemTable(memTableOptions{
		Options:                      incrementalRangeDelOptions(true),
		size:                         max(8<<20, n*128),
		releaseAccountingReservation: func() {},
	})
	key := rangeDelBenchKey
	for i := 0; i < n; i++ {
		// Tombstone i covers [2i, 2i+1). An overlapping tombstone instead covers
		// [2i, 2i+3), which overlaps tombstone i+1.
		end := 2*i + 1
		if layout == "chained" || (layout == "mostly-disjoint" && i%10 == 0) {
			end = 2*i + 3
		}
		ik := base.MakeInternalKey(key(2*i), base.SeqNum(i+1), InternalKeyKindRangeDelete)
		if err := m.rangeDelSkl.Add(ik, key(end)); err != nil {
			b.Fatal(err)
		}
	}
	f := &keySpanFrags{count: uint32(n)}
	spans := f.get(&m.rangeDelSkl, m.cmp, m.formatKey, rangeDelConstructSpan,
		true /* bypassDisjoint */, nil /* stats */)
	m.tombstones.version.Store(&rangeDelVersion{chunks: chunkRangeDelSpans(spans, m.tombstones.chunkSize), n: len(spans)})
	m.tombstones.mu.count = n
	return m
}

// BenchmarkMemTableRangeDelRebuild measures rebuilding a memtable's fragmented
// range deletions from scratch, as the first read after a range deletion was
// applied had to do before range deletions were spliced in as they're applied.
// The frags metric is the number of fragments the rebuild produces.
func BenchmarkMemTableRangeDelRebuild(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		for _, layout := range []string{"disjoint", "chained", "mostly-disjoint"} {
			b.Run(fmt.Sprintf("n=%d/layout=%s", n, layout), func(b *testing.B) {
				m := newRangeDelBenchMemTable(b, n, layout)
				defer m.free()
				var spans []keyspan.Span
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					f := &keySpanFrags{count: uint32(n)}
					spans = f.get(&m.rangeDelSkl, m.cmp, m.formatKey, rangeDelConstructSpan,
						true /* bypassDisjoint */, nil /* stats */)
					if len(spans) == 0 {
						b.Fatal("no range deletions")
					}
				}
				b.ReportMetric(float64(len(spans)), "frags")
			})
		}
	}
}

// BenchmarkMemTableRangeDelSplice measures splicing a batch holding one range
// deletion into a memtable's fragments, as apply does, for the layouts of
// BenchmarkMemTableRangeDelRebuild. Every op starts from the same version. The
// frags metric is that version's fragment count.
func BenchmarkMemTableRangeDelSplice(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		for _, layout := range []string{"disjoint", "chained", "mostly-disjoint"} {
			b.Run(fmt.Sprintf("n=%d/layout=%s", n, layout), func(b *testing.B) {
				m := newRangeDelBenchMemTable(b, n, layout)
				defer m.free()
				v0 := m.tombstones.version.Load()
				// Each batch deletes [2i, 2i+2) for a random i, which overlaps the
				// fragments of tombstone i, at a sequence number above every
				// tombstone's.
				rng := rand.New(rand.NewPCG(0, 0))
				batches := make([][]rangeDelTombstone, 1024)
				for k := range batches {
					i := rng.IntN(n)
					batches[k] = []rangeDelTombstone{{
						start:   rangeDelBenchKey(2 * i),
						end:     rangeDelBenchKey(2*i + 2),
						trailer: base.MakeTrailer(base.SeqNum(n+1+k), InternalKeyKindRangeDelete),
					}}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					m.tombstones.version.Store(v0)
					m.tombstones.add(batches[i%len(batches)])
				}
				b.StopTimer()
				b.ReportMetric(float64(v0.n), "frags")
			})
		}
	}
}
