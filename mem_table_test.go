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
				mem = newMemTable(memTableOptions{})
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
	// Concurrently write and read range tombstones. Workers add range
	// tombstones, and then immediately retrieve them verifying that the
	// tombstones they've added are all present.

	m := newMemTable(memTableOptions{Options: &Options{MemTableSize: 64 << 20}})

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

func TestMemTableRangeDelCacheStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stats := newKeySpanCacheStats()
	m := newMemTable(memTableOptions{rangeDelCacheStats: stats})

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
// splices range deletions into its fragments.
func TestMemTableRangeDelCacheStatsNil(t *testing.T) {
	defer leaktest.AfterTest(t)()
	m := newMemTable(memTableOptions{})
	b := newBatch(nil)
	defer b.Close()
	require.NoError(t, b.DeleteRange([]byte("a"), []byte("c"), nil))
	require.NoError(t, m.apply(b, 1))
	it := m.newRangeDelIter(nil)
	require.NotNil(t, it)
	s, err := it.First()
	require.NoError(t, err)
	require.NotNil(t, s)
	it.Close()
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
// that rangeDelTestKey turns into keys.
type rangeDelTestBounds struct{ start, end int }

// rangeDelTestMaxPos is the largest position randRangeDelBounds returns.
const rangeDelTestMaxPos = 14

// rangeDelTestKey returns the key at pos. Keys are single letters so that
// shared and touching bounds are common.
func rangeDelTestKey(pos int) []byte { return []byte{byte('a' + pos)} }

// randRangeDelBounds returns random range deletion bounds in [0,
// rangeDelTestMaxPos]. They may be inverted or empty, or relate to prev: the
// same start, nested within it, touching it, disjoint from it, or straddling
// its end.
func randRangeDelBounds(rng *rand.Rand, prev rangeDelTestBounds) rangeDelTestBounds {
	const maxPos = rangeDelTestMaxPos
	// between returns a random position in [lo, hi], or lo if hi < lo.
	between := func(lo, hi int) int {
		if hi <= lo {
			return lo
		}
		return lo + rng.IntN(hi-lo+1)
	}
	var start, end int
	switch rng.IntN(8) {
	case 0: // Arbitrary, possibly inverted or empty.
		start, end = between(0, maxPos), between(0, maxPos)
	case 1: // Inverted.
		start = between(1, maxPos)
		end = between(0, start-1)
	case 2: // Empty.
		start = between(0, maxPos)
		end = start
	case 3: // Same start as prev, possibly a different end.
		start = prev.start
		end = between(start+1, maxPos)
	case 4: // Nested within prev.
		start = between(prev.start, prev.end)
		end = between(start, prev.end)
	case 5: // Touching: starts where prev ends.
		start = prev.end
		end = between(start+1, maxPos)
	case 6: // Disjoint from prev.
		start = between(prev.end+1, maxPos)
		end = between(start+1, maxPos)
	case 7: // Straddling prev's end.
		start = between(prev.start, prev.end-1)
		end = between(prev.end, maxPos)
	}
	return rangeDelTestBounds{start: start, end: end}
}

// TestMemTableRangeDelFragmentsMatchReference checks that the memtable's
// fragmented range deletions match referenceRangeDelFragments over random
// tombstone sets.
func TestMemTableRangeDelFragmentsMatchReference(t *testing.T) {
	defer leaktest.AfterTest(t)()
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))
	key := rangeDelTestKey

	sentinel := keyspan.Key{Trailer: base.MakeTrailer(base.SeqNumMax, base.InternalKeyKindRangeDelete)}
	for iter := 0; iter < 2000; iter++ {
		m := newMemTable(memTableOptions{size: 256 << 10, releaseAccountingReservation: func() {}})
		n := rng.IntN(48)
		tombstones := make([]rangeDelTestBounds, n)
		for i := range tombstones {
			var prev rangeDelTestBounds
			if i > 0 {
				prev = tombstones[rng.IntN(i)]
			}
			tombstones[i] = randRangeDelBounds(rng, prev)
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
			msg := fmt.Sprintf("seed %d, iteration %d, %s\ntombstones: %s\nwant: %s\ngot:  %s",
				seed, iter, name, desc.String(), want, got)
			require.Equal(t, want == nil, got == nil, msg)
			require.Equal(t, len(want), len(got), msg)
			for i := range want {
				require.Zero(t, m.cmp(want[i].Start, got[i].Start), msg)
				require.Zero(t, m.cmp(want[i].End, got[i].End), msg)
				require.Equal(t, want[i].KeysOrder, got[i].KeysOrder, msg)
				require.Equal(t, want[i].Keys, got[i].Keys, msg)
			}
		}

		got := m.tombstones.get()
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

// TestMemTableRangeDelSpliceMatchesReference applies random range deletions
// through batches, one to several per batch and in an order unrelated to their
// sequence numbers. After every batch, it checks that the memtable's
// fragments match referenceRangeDelFragments over its skiplist.
func TestMemTableRangeDelSpliceMatchesReference(t *testing.T) {
	defer leaktest.AfterTest(t)()
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))
	key := rangeDelTestKey

	// A batch holds up to maxRangeDels range deletions and possibly a point
	// key.
	const maxRangeDels = 4
	for iter := 0; iter < 500; iter++ {
		m := newMemTable(memTableOptions{size: 256 << 10, releaseAccountingReservation: func() {}})
		numBatches := 1 + rng.IntN(24)
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
					require.NoError(t, b.Set(key(rng.IntN(rangeDelTestMaxPos+1)), nil, nil))
				}
				if i == n {
					break
				}
				var prev rangeDelTestBounds
				if len(added) > 0 {
					prev = added[rng.IntN(len(added))]
				}
				ts := randRangeDelBounds(rng, prev)
				added = append(added, ts)
				fmt.Fprintf(&desc, " %s-%s#%d", key(ts.start), key(ts.end), seqNum+base.SeqNum(b.Count()))
				require.NoError(t, b.DeleteRange(key(ts.start), key(ts.end), nil))
			}
			desc.WriteString("\n")
			require.NoError(t, m.apply(b, seqNum))
			b.Close()

			want := referenceRangeDelFragments(&m.rangeDelSkl, m.cmp, m.formatKey)
			requireRangeDelSpansEqual(t, m.cmp, want, m.tombstones.get(),
				"seed %d, iteration %d, after batch %d\n%s", seed, iter, k, desc.String())
		}
		m.free()
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
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	key := rangeDelTestKey

	m := newMemTable(memTableOptions{size: 8 << 20, releaseAccountingReservation: func() {}})
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
					bounds := randRangeDelBounds(rng, prev)
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
	var checks atomic.Int64
	check := func() error {
		checks.Add(1)
		mu.Lock()
		mustHold := slices.Clone(applied)
		mu.Unlock()
		spans := m.tombstones.get()
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
	t.Logf("%d of %d batches applied out of sequence number order; readers checked %d versions",
		outOfOrder, writers*batchesPerWriter, checks.Load())

	want := referenceRangeDelFragments(&m.rangeDelSkl, m.cmp, m.formatKey)
	requireRangeDelSpansEqual(t, m.cmp, want, m.tombstones.get(), "seed %d", seed)
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
	key := rangeDelTestKey

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

	for iter := 0; iter < 100; iter++ {
		m := newMemTable(memTableOptions{size: 256 << 10, releaseAccountingReservation: func() {}})
		const n = 40
		seqNums := rng.Perm(n)
		type version struct{ spans, copy []keyspan.Span }
		var versions []version
		var added []rangeDelTestBounds
		for i := 0; i < n; i++ {
			var prev rangeDelTestBounds
			if len(added) > 0 {
				prev = added[rng.IntN(len(added))]
			}
			bounds := randRangeDelBounds(rng, prev)
			added = append(added, bounds)
			set(m, key(bounds.start), key(bounds.end), base.SeqNum(seqNums[i]+1))

			spans := m.tombstones.get()
			require.Equal(t, len(spans), cap(spans))
			for j := range spans {
				require.Equal(t, len(spans[j].Keys), cap(spans[j].Keys))
			}
			versions = append(versions, version{spans: spans, copy: deepCopy(spans)})
			for v := range versions {
				require.Equal(t, versions[v].copy, versions[v].spans,
					"seed %d, iteration %d: version %d changed by range deletion %d", seed, iter, v, i)
			}
		}
		m.free()
	}

	// Fragments that a range deletion doesn't cover keep their Keys, including
	// the parts of a fragment that it splits.
	m := newMemTable(memTableOptions{size: 256 << 10, releaseAccountingReservation: func() {}})
	defer m.free()
	set(m, []byte("a"), []byte("b"), 1)
	set(m, []byte("e"), []byte("z"), 2)
	v1 := m.tombstones.get()
	set(m, []byte("c"), []byte("d"), 3)
	v2 := m.tombstones.get()
	require.Len(t, v2, 3)
	require.Same(t, &v1[0].Keys[0], &v2[0].Keys[0])
	require.Same(t, &v1[1].Keys[0], &v2[2].Keys[0])
	set(m, []byte("m"), []byte("n"), 4)
	v3 := m.tombstones.get()
	require.Equal(t, "[a-b:{(#1,RANGEDEL)} c-d:{(#3,RANGEDEL)} e-m:{(#2,RANGEDEL)} "+
		"m-n:{(#4,RANGEDEL) (#2,RANGEDEL)} n-z:{(#2,RANGEDEL)}]", fmt.Sprint(v3))
	require.Same(t, &v2[2].Keys[0], &v3[2].Keys[0])
	require.Same(t, &v2[2].Keys[0], &v3[4].Keys[0])
}

// TestMemTableRangeDelCheckFragments checks that checkRangeDelFragments panics
// on fragments that differ from a rebuild of the skiplist, and skips the
// comparison when the skiplist doesn't hold exactly the given number of range
// deletions.
func TestMemTableRangeDelCheckFragments(t *testing.T) {
	defer leaktest.AfterTest(t)()
	m := newMemTable(memTableOptions{size: 256 << 10, releaseAccountingReservation: func() {}})
	defer m.free()
	set := func(start, end string, seqNum base.SeqNum) {
		ik := base.MakeInternalKey([]byte(start), seqNum, InternalKeyKindRangeDelete)
		require.NoError(t, m.set(ik, []byte(end)))
	}
	set("a", "c", 1)
	set("b", "d", 2)
	check := func(spans []keyspan.Span, n int) func() {
		return func() { checkRangeDelFragments(&m.rangeDelSkl, m.cmp, m.formatKey, spans, n) }
	}

	spans := m.tombstones.get()
	require.Len(t, spans, 3)
	require.NotPanics(t, check(spans, 2))
	missingKey := slices.Clone(spans)
	missingKey[1].Keys = missingKey[1].Keys[:1]
	require.Panics(t, check(missingKey, 2))
	require.Panics(t, check(spans[:2], 2))

	// The skiplist holds 2 range deletions, so a different count means that
	// some aren't spliced yet (or that too many were).
	require.NotPanics(t, check(missingKey, 1))
	require.NotPanics(t, check(missingKey, 3))
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
	m.tombstones.version.Store(&rangeDelVersion{spans: spans})
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
				b.ReportMetric(float64(len(v0.spans)), "frags")
			})
		}
	}
}
