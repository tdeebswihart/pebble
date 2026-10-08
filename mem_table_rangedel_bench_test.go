// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

var rangeDelFixtureLayouts = []string{
	"disjoint", "touching", "concentric", "shared-start", "staircase",
	"repeated", "random", "full-cover", "invalid", "empty-start", "equivalent",
}

type rangeDelFixture struct {
	comparer *Comparer
	spans    []keyspan.Span
}

// The fixtures retain input endpoints and sequence order, not fragmented output.
// Large nested inputs can therefore be reused by the tree prototype.
func makeRangeDelFixture(layout string, n int, comparer *Comparer) rangeDelFixture {
	f := rangeDelFixture{comparer: comparer, spans: make([]keyspan.Span, 0, n)}
	if layout == "equivalent" {
		c := *DefaultComparer
		c.Name = "rangedel-fixture-case-fold"
		c.Compare = func(a, b []byte) int { return bytes.Compare(bytes.ToLower(a), bytes.ToLower(b)) }
		c.Equal = func(a, b []byte) bool { return bytes.EqualFold(a, b) }
		c.AbbreviatedKey = func([]byte) uint64 { return 0 }
		f.comparer = &c
	}
	key := func(i int) []byte {
		k := fmt.Appendf(nil, "d/%08d", i)
		if comparer == &cockroachkvs.Comparer && layout != "equivalent" {
			k = cockroachkvs.EncodeMVCCKey(nil, k, 0, 0)
		}
		return k
	}
	rng := rand.New(rand.NewPCG(6207, 1))
	for i := range n {
		var a, z int
		switch layout {
		case "disjoint", "full-cover":
			a, z = 3*i, 3*i+1
			if layout == "full-cover" && i == n-1 {
				a, z = 0, max(1, 3*i)
			}
		case "touching", "empty-start", "equivalent":
			a, z = i, i+1
		case "concentric":
			a, z = i, 2*n-i
		case "shared-start":
			a, z = 0, i+1
		case "staircase":
			a, z = i, n+i+1
		case "repeated":
			a, z = 0, n+1
		case "random":
			a = rng.IntN(max(1, 2*n))
			z = a + 1 + rng.IntN(max(1, n))
		case "invalid":
			a, z = i+1, i+1-i%2
		default:
			panic("unknown range deletion fixture: " + layout)
		}
		start, end := key(a), key(z)
		if layout == "empty-start" && i == 0 {
			start = []byte{}
		}
		if layout == "equivalent" {
			end[0] = 'D'
		}
		f.spans = append(f.spans, keyspan.Span{Start: start, End: end, Keys: []keyspan.Key{{
			Trailer: base.MakeTrailer(base.SeqNum(i+1), InternalKeyKindRangeDelete),
		}}})
	}
	return f
}

// referenceRangeDelPartition scans all input histories for each elementary
// interval. This small-input oracle deliberately shares no Fragmenter machinery.
func referenceRangeDelPartition(f rangeDelFixture) []keyspan.Span {
	compare := f.comparer.Compare
	var endpoints [][]byte
	for _, s := range f.spans {
		if compare(s.Start, s.End) < 0 {
			endpoints = append(endpoints, s.Start, s.End)
		}
	}
	slices.SortFunc(endpoints, compare)
	endpoints = slices.CompactFunc(endpoints, func(a, b []byte) bool { return compare(a, b) == 0 })
	var partition []keyspan.Span
	for i := 1; i < len(endpoints); i++ {
		s := keyspan.Span{Start: endpoints[i-1], End: endpoints[i]}
		for _, input := range f.spans {
			if compare(input.Start, input.End) < 0 && compare(input.Start, s.Start) <= 0 && compare(s.End, input.End) <= 0 {
				s.Keys = append(s.Keys, input.Keys...)
			}
		}
		slices.SortFunc(s.Keys, func(a, b keyspan.Key) int { return cmp.Compare(b.Trailer, a.Trailer) })
		partition = append(partition, s)
	}
	return partition
}

func rangeDelFixtureBatches(tb testing.TB, f rangeDelFixture, size int) []*Batch {
	tb.Helper()
	var batches []*Batch
	for i := 0; i < len(f.spans); i += size {
		b := newBatch(nil)
		for _, s := range f.spans[i:min(i+size, len(f.spans))] {
			require.NoError(tb, b.DeleteRange(s.Start, s.End, nil))
		}
		batches = append(batches, b)
		tb.Cleanup(func() { require.NoError(tb, b.Close()) })
	}
	return batches
}

func applyRangeDelFixture(tb testing.TB, m *memTable, batches []*Batch) {
	tb.Helper()
	seq := base.SeqNum(1)
	for _, batch := range batches {
		if err := m.prepare(batch); err != nil {
			tb.Fatal(err)
		}
		err := m.apply(batch, seq)
		m.writerUnref()
		if err != nil {
			tb.Fatal(err)
		}
		seq += base.SeqNum(batch.Count())
	}
}

func rangeDelFixtureMemTable(comparer *Comparer) *memTable {
	return newMemTable(memTableOptions{
		Options:                      &Options{Comparer: comparer, MemTableSize: 4 << 20},
		releaseAccountingReservation: func() {},
	})
}

func TestRangeDelFixtures(t *testing.T) {
	for _, layout := range rangeDelFixtureLayouts {
		t.Run(layout, func(t *testing.T) {
			f := makeRangeDelFixture(layout, 4, DefaultComparer)
			require.Equal(t, f.spans, makeRangeDelFixture(layout, 4, DefaultComparer).spans)
			want := referenceRangeDelPartition(f)
			var active, history int
			for _, s := range want {
				if len(s.Keys) > 0 {
					active++
					history += len(s.Keys)
				}
			}
			if expected, ok := map[string][2]int{
				"disjoint": {4, 4}, "touching": {4, 4}, "concentric": {7, 16},
				"shared-start": {4, 10}, "staircase": {7, 16}, "repeated": {1, 4},
				"full-cover": {6, 9}, "invalid": {0, 0}, "empty-start": {4, 4}, "equivalent": {4, 4},
			}[layout]; ok {
				require.Equal(t, expected, [2]int{active, history})
			}
			m := rangeDelFixtureMemTable(f.comparer)
			defer m.free()
			batches := rangeDelFixtureBatches(t, f, 1)
			for i, batch := range batches {
				require.NoError(t, m.prepare(batch))
				require.NoError(t, m.apply(batch, base.SeqNum(i+1)))
				m.writerUnref()
				prefix := f
				prefix.spans = f.spans[:i+1]
				iter := m.newRangeDelIter(nil)
				if iter == nil {
					require.Equal(t, "invalid", layout)
					continue
				}
				got, err := iter.First()
				require.NoError(t, err)
				for _, s := range referenceRangeDelPartition(prefix) {
					if len(s.Keys) == 0 {
						continue
					}
					require.NotNil(t, got)
					require.Zero(t, f.comparer.Compare(s.Start, got.Start))
					require.Zero(t, f.comparer.Compare(s.End, got.End))
					require.Equal(t, s.Keys, got.Keys)
					got, err = iter.Next()
					require.NoError(t, err)
				}
				require.Nil(t, got)
				iter.Close()
			}
		})
	}
}

func TestRangeDelFixturesLarge(t *testing.T) {
	for _, layout := range rangeDelFixtureLayouts {
		t.Run(layout, func(t *testing.T) {
			f := makeRangeDelFixture(layout, 100_000, DefaultComparer)
			require.Len(t, f.spans, 100_000)
			for i, s := range f.spans {
				require.NotNil(t, s.Start)
				require.NotNil(t, s.End)
				require.Len(t, s.Keys, 1)
				require.Equal(t, base.SeqNum(i+1), s.Keys[0].SeqNum())
			}
		})
	}
}

func reportRangeDelEpoch(b *testing.B, n, batchSize int) {
	b.ReportMetric(float64(n), "tombstones/epoch")
	b.ReportMetric(float64((n+batchSize-1)/batchSize), "batches/epoch")
	b.ReportMetric(float64(4<<20), "configured-arena-bytes")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/tombstone")
}

// Each epoch replays a bounded input. Memtable allocation and release are outside
// the timer, but their garbage can still affect GC during the timed operation.
func BenchmarkMemTableRangeDelBaseline(b *testing.B) {
	for _, comparer := range []*Comparer{DefaultComparer, &cockroachkvs.Comparer} {
		b.Run(comparer.Name, func(b *testing.B) {
			for _, layout := range rangeDelFixtureLayouts[:8] {
				b.Run(layout, func(b *testing.B) {
					for _, n := range []int{100, 1000, 10_000} {
						b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
							f := makeRangeDelFixture(layout, n, comparer)
							for _, size := range []int{1, 16, 1000} {
								b.Run(fmt.Sprintf("batch=%d", size), func(b *testing.B) {
									batches := rangeDelFixtureBatches(b, f, size)
									b.Run("apply", func(b *testing.B) {
										benchmarkRangeDelApply(b, f, batches, size)
									})
									if n > 1000 && layout != "disjoint" && layout != "touching" && layout != "repeated" {
										return
									}
									for _, warm := range []bool{false, true} {
										name := "history-cold"
										if warm {
											name = "history-warm"
										}
										b.Run(name, func(b *testing.B) {
											benchmarkRangeDelHistory(b, f, batches, size, warm)
										})
									}
								})
							}
						})
					}
				})
			}
		})
	}
}

func benchmarkRangeDelApply(b *testing.B, f rangeDelFixture, batches []*Batch, size int) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		m := rangeDelFixtureMemTable(f.comparer)
		b.StartTimer()
		applyRangeDelFixture(b, m, batches)
		b.StopTimer()
		m.free()
	}
	reportRangeDelEpoch(b, len(f.spans), size)
	reportRangeDelShape(b, f)
}

// History cases measure constructor cost, including upstream's eager cold
// materialization. Traversal for output accounting is outside the timed region.
func benchmarkRangeDelHistory(
	b *testing.B, f rangeDelFixture, batches []*Batch, size int, warm bool,
) {
	newPopulated := func() *memTable {
		m := rangeDelFixtureMemTable(f.comparer)
		applyRangeDelFixture(b, m, batches)
		return m
	}
	var cached *memTable
	if warm {
		cached = newPopulated()
		cached.newRangeDelIter(nil).Close()
		defer cached.free()
	}
	b.ReportAllocs()
	b.ResetTimer()
	var fragments, keys int
	for range b.N {
		b.StopTimer()
		m := cached
		if m == nil {
			m = newPopulated()
		}
		b.StartTimer()
		iter := m.newRangeDelIter(nil)
		b.StopTimer()
		if !warm || fragments == 0 {
			fragments, keys = 0, 0
			for s, err := iter.First(); s != nil || err != nil; s, err = iter.Next() {
				if err != nil {
					b.Fatal(err)
				}
				fragments++
				keys += len(s.Keys)
			}
		}
		iter.Close()
		if !warm {
			m.free()
		}
	}
	reportRangeDelEpoch(b, len(f.spans), size)
	b.ReportMetric(float64(fragments), "fragments/epoch")
	b.ReportMetric(float64(keys), "history-keys/epoch")
}

func reportRangeDelShape(b *testing.B, f rangeDelFixture) {
	type event struct {
		key   []byte
		delta int
	}
	events := make([]event, 0, 2*len(f.spans))
	for _, s := range f.spans {
		if f.comparer.Compare(s.Start, s.End) < 0 {
			events = append(events, event{s.Start, 1}, event{s.End, -1})
		}
	}
	slices.SortFunc(events, func(a, z event) int { return f.comparer.Compare(a.key, z.key) })
	var depth, fragments, history, intervals int
	for i := 0; i < len(events); {
		key := events[i].key
		for i < len(events) && f.comparer.Compare(key, events[i].key) == 0 {
			depth += events[i].delta
			i++
		}
		if i < len(events) {
			intervals++
			if depth > 0 {
				fragments++
				history += depth
			}
		}
	}
	b.ReportMetric(float64(intervals), "intervals/epoch")
	b.ReportMetric(float64(fragments), "fragments/epoch")
	b.ReportMetric(float64(history), "history-keys/epoch")
}

func BenchmarkRangeDelWriteReadBaseline(b *testing.B) {
	for _, comparer := range []*Comparer{DefaultComparer, &cockroachkvs.Comparer} {
		b.Run(comparer.Name, func(b *testing.B) {
			for _, layout := range []string{"disjoint", "concentric", "shared-start", "staircase"} {
				for _, n := range []int{100, 1000} {
					for _, size := range []int{1, 16, 1000} {
						for _, read := range []string{"get", "seek"} {
							b.Run(fmt.Sprintf("%s/n=%d/batch=%d/%s", layout, n, size, read), func(b *testing.B) {
								f := makeRangeDelFixture(layout, n, comparer)
								batches := rangeDelFixtureBatches(b, f, size)
								benchmarkRangeDelWriteRead(b, f, batches, size, read)
							})
						}
					}
				}
			}
		})
	}
}

func benchmarkRangeDelWriteRead(
	b *testing.B, f rangeDelFixture, batches []*Batch, size int, read string,
) {
	key := []byte("r/key")
	if f.comparer == &cockroachkvs.Comparer {
		key = cockroachkvs.EncodeMVCCKey(nil, key, 0, 0)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		// DB.Apply changes Batch state and the sequence header. Each epoch owns
		// copies of the input representations, prepared outside the timer.
		epochBatches := make([]*Batch, len(batches))
		for i, input := range batches {
			epochBatches[i] = newBatch(nil)
			require.NoError(b, epochBatches[i].SetRepr(append([]byte(nil), input.Repr()...)))
		}
		d, err := Open("", &Options{
			FS: vfs.NewMem(), DisableWAL: true, MemTableSize: 4 << 20,
			Comparer: f.comparer, Logger: base.NoopLoggerAndTracer{},
		})
		require.NoError(b, err)
		require.NoError(b, d.Set(key, []byte("v"), NoSync))
		b.StartTimer()
		for _, batch := range epochBatches {
			if err := d.Apply(batch, NoSync); err != nil {
				b.Fatal(err)
			}
			if err := rangeDelBenchRead(d, key, read); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		m := d.Metrics()
		require.EqualValues(b, 1, m.MemTable.Count)
		require.Zero(b, m.Flush.Count)
		b.ReportMetric(float64(m.MemTable.Size), "allocated-arena-bytes")
		require.NoError(b, d.Close())
		for _, batch := range epochBatches {
			require.NoError(b, batch.Close())
		}
	}
	reportRangeDelEpoch(b, len(f.spans), size)
	reportRangeDelShape(b, f)
}

func rangeDelBenchRead(d *DB, key []byte, read string) error {
	if read == "get" {
		value, closer, err := d.Get(key)
		if err != nil {
			return err
		}
		equal := bytes.Equal(value, []byte("v"))
		err = closer.Close()
		if !equal {
			return errors.New("unexpected read value")
		}
		return err
	}
	iter, err := d.NewIter(nil)
	if err != nil {
		return err
	}
	found := iter.SeekGE(key) && bytes.Equal(key, iter.Key())
	err = iter.Close()
	if !found {
		return errors.New("read key missing")
	}
	return err
}
