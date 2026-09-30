// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// benchOpLatencies records the latency of each op of a benchmark, for
// benchmarks whose tail latency matters as much as their mean.
type benchOpLatencies struct {
	ns    []int64
	start crtime.Mono
}

func newBenchOpLatencies(n int) *benchOpLatencies {
	return &benchOpLatencies{ns: make([]int64, 0, n)}
}

func (l *benchOpLatencies) begin() { l.start = crtime.NowMono() }

func (l *benchOpLatencies) end() { l.ns = append(l.ns, int64(l.start.Elapsed())) }

// report reports the p50, p99 and maximum op latencies as the p50-ns, p99-ns
// and max-ns metrics.
func (l *benchOpLatencies) report(b *testing.B) {
	if len(l.ns) == 0 {
		return
	}
	slices.Sort(l.ns)
	at := func(q float64) float64 {
		return float64(l.ns[min(len(l.ns)-1, int(q*float64(len(l.ns))))])
	}
	b.ReportMetric(at(0.50), "p50-ns")
	b.ReportMetric(at(0.99), "p99-ns")
	b.ReportMetric(float64(l.ns[len(l.ns)-1]), "max-ns")
}

// rangeDelBenchBase is a memtable holding n disjoint range deletions, and the
// fragments of its skiplist.
type rangeDelBenchBase struct {
	m     *memTable
	spans []keyspan.Span
}

var rangeDelBenchBases struct {
	sync.Mutex
	m map[int]*rangeDelBenchBase
}

// getRangeDelBenchBase returns a memtable holding n disjoint range deletions,
// built once per process and shared by the benchmarks, which must restore its
// version and chunk size before use. Range deletion i covers [2i, 2i+1) in
// rangeDelBenchKey's key space.
func getRangeDelBenchBase(b *testing.B, n int) *rangeDelBenchBase {
	rangeDelBenchBases.Lock()
	defer rangeDelBenchBases.Unlock()
	if rangeDelBenchBases.m == nil {
		rangeDelBenchBases.m = make(map[int]*rangeDelBenchBase)
	}
	if bb, ok := rangeDelBenchBases.m[n]; ok {
		return bb
	}
	m := newRangeDelBenchMemTable(b, n, "disjoint")
	bb := &rangeDelBenchBase{m: m, spans: m.tombstones.version.Load().spans()}
	rangeDelBenchBases.m[n] = bb
	return bb
}

// useChunkSize makes the memtable's current version one of bb's fragments in
// chunks of chunkSize, and returns it.
func (bb *rangeDelBenchBase) useChunkSize(chunkSize int) *rangeDelVersion {
	bb.m.tombstones.chunkSize = chunkSize
	v := &rangeDelVersion{chunks: chunkRangeDelSpans(bb.spans, chunkSize), n: len(bb.spans)}
	bb.m.tombstones.version.Store(v)
	return v
}

// rangeDelBenchSpliceBatches returns batches that each hold one range deletion
// [2i, 2i+2) for a random i < n, which overlaps range deletion i of a
// rangeDelBenchBase and the gap after it, at sequence numbers above every
// range deletion of the base.
func rangeDelBenchSpliceBatches(n int) [][]rangeDelTombstone {
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
	return batches
}

// benchRangeDelSplice runs the op of the splice benchmarks: every op restores
// v0 and splices one of batches into it. It reports allocations and the op
// latency percentiles.
func benchRangeDelSplice(
	b *testing.B, m *memTable, v0 *rangeDelVersion, batches [][]rangeDelTombstone,
) {
	lat := newBenchOpLatencies(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.tombstones.version.Store(v0)
		lat.begin()
		m.tombstones.add(batches[i%len(batches)])
		lat.end()
	}
	b.StopTimer()
	lat.report(b)
	b.ReportMetric(float64(v0.n), "frags")
}

// rangeDelBenchChunkSizes are the chunk sizes the splice benchmarks compare.
var rangeDelBenchChunkSizes = []int{64, 128, 256}

// BenchmarkMemTableRangeDelSpliceChunkSize measures splicing a batch holding
// one range deletion into memtables of 100,000 and 1,000,000 disjoint
// fragments with several chunk sizes. Every op starts from the same version.
func BenchmarkMemTableRangeDelSpliceChunkSize(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000} {
		for _, chunkSize := range rangeDelBenchChunkSizes {
			b.Run(fmt.Sprintf("frags=%d/chunk=%d", n, chunkSize), func(b *testing.B) {
				bb := getRangeDelBenchBase(b, n)
				v0 := bb.useChunkSize(chunkSize)
				benchRangeDelSplice(b, bb.m, v0, rangeDelBenchSpliceBatches(n))
			})
		}
	}
}

// benchGCPressure holds a live heap of small objects that hold pointers, and
// runs the GC in a loop, so that the GC is marking for most of a benchmark.
var benchGCPressure struct {
	once sync.Once
	live []*[8]*int
}

// startBenchGCPressure allocates about liveBytes of live heap the first time
// it's called in a process, and keeps it for the life of the process. It runs
// runtime.GC in a loop on another goroutine until the returned function is
// called.
func startBenchGCPressure(liveBytes int) (stop func()) {
	benchGCPressure.once.Do(func() {
		benchGCPressure.live = make([]*[8]*int, liveBytes/64)
		for i := range benchGCPressure.live {
			benchGCPressure.live[i] = new([8]*int)
		}
	})
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				runtime.GC()
			}
		}
	}()
	return func() {
		close(done)
		wg.Wait()
	}
}

// BenchmarkMemTableRangeDelSpliceGC is BenchmarkMemTableRangeDelSpliceChunkSize
// while another goroutine holds a live heap of 1 GiB and runs the GC in a loop,
// so that most splices run while the GC is marking and pay its write barriers.
// The first run in a process allocates the live heap, which then stays live, so
// this benchmark should run in a process of its own.
func BenchmarkMemTableRangeDelSpliceGC(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000} {
		for _, chunkSize := range rangeDelBenchChunkSizes {
			b.Run(fmt.Sprintf("frags=%d/chunk=%d", n, chunkSize), func(b *testing.B) {
				bb := getRangeDelBenchBase(b, n)
				v0 := bb.useChunkSize(chunkSize)
				batches := rangeDelBenchSpliceBatches(n)
				stop := startBenchGCPressure(1 << 30)
				defer stop()
				benchRangeDelSplice(b, bb.m, v0, batches)
			})
		}
	}
}

// BenchmarkMemTableRangeDelSpliceWide measures splices that rewrite many
// fragments, in a memtable that also holds 100,000 disjoint fragments.
//
// The staircase cases hold the range deletions [q, q_j) for j < k of one queue
// q, as repeated deletions from a queue's start up to an advancing ack level
// leave them, and every op splices [q, q_k): it rewrites the queue's k
// fragments, which hold about k*k/2 keys. The full-cover case splices one
// range deletion over all 100,000 fragments and the gaps between them.
func BenchmarkMemTableRangeDelSpliceWide(b *testing.B) {
	const n = 100_000
	queueKey := func(j int) []byte { return fmt.Appendf(nil, "q/%08d", j) }
	for _, k := range []int{100, 300, 1000, 3000} {
		b.Run(fmt.Sprintf("staircase/k=%d", k), func(b *testing.B) {
			m := newMemTable(memTableOptions{
				Options:                      incrementalRangeDelOptions(true),
				size:                         64 << 20,
				releaseAccountingReservation: func() {},
			})
			defer m.free()
			seqNum := base.SeqNum(1)
			for i := 0; i < n; i++ {
				ik := base.MakeInternalKey(rangeDelBenchKey(2*i), seqNum, InternalKeyKindRangeDelete)
				require.NoError(b, m.rangeDelSkl.Add(ik, rangeDelBenchKey(2*i+1)))
				seqNum++
			}
			for j := 1; j < k; j++ {
				ik := base.MakeInternalKey(queueKey(0), seqNum, InternalKeyKindRangeDelete)
				require.NoError(b, m.rangeDelSkl.Add(ik, queueKey(j)))
				seqNum++
			}
			m.tombstones.mu.count = n + k - 1
			m.tombstones.build()
			v0 := m.tombstones.version.Load()
			batches := [][]rangeDelTombstone{{{
				start:   queueKey(0),
				end:     queueKey(k),
				trailer: base.MakeTrailer(seqNum, InternalKeyKindRangeDelete),
			}}}
			benchRangeDelSplice(b, m, v0, batches)
		})
	}
	b.Run("full-cover", func(b *testing.B) {
		bb := getRangeDelBenchBase(b, n)
		v0 := bb.useChunkSize(rangeDelChunkSize)
		batches := [][]rangeDelTombstone{{{
			start:   rangeDelBenchKey(0),
			end:     rangeDelBenchKey(2 * n),
			trailer: base.MakeTrailer(base.SeqNum(n+1), InternalKeyKindRangeDelete),
		}}}
		benchRangeDelSplice(b, bb.m, v0, batches)
	})
}

// BenchmarkMemTableRangeDelIter compares rangeDelChunkIter over the chunks of
// 100,000 fragments with a keyspan.Iter over the same fragments in one slice.
// Next and Prev step one fragment per op and cross a chunk boundary about
// every 256 ops.
func BenchmarkMemTableRangeDelIter(b *testing.B) {
	const n = 100_000
	for _, iter := range []string{"chunked", "flat"} {
		b.Run("iter="+iter, func(b *testing.B) {
			bb := getRangeDelBenchBase(b, n)
			v := bb.useChunkSize(rangeDelChunkSize)
			newIter := func() keyspan.FragmentIterator {
				if iter == "flat" {
					return keyspan.NewIter(bb.m.cmp, bb.spans)
				}
				it := bb.m.newRangeDelIter(nil)
				if _, ok := it.(*rangeDelChunkIter); !ok || len(v.chunks) < 2 {
					b.Fatalf("expected a rangeDelChunkIter over several chunks, got %T", it)
				}
				return it
			}
			b.Run("op=SeekGE", func(b *testing.B) {
				it := newIter()
				defer it.Close()
				rng := rand.New(rand.NewPCG(0, 0))
				keys := make([][]byte, 4096)
				for i := range keys {
					keys[i] = rangeDelBenchKey(rng.IntN(2 * n))
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := it.SeekGE(keys[i%len(keys)]); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("op=Next", func(b *testing.B) {
				it := newIter()
				defer it.Close()
				if s, _ := it.First(); s == nil {
					b.Fatal("no fragments")
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if s, _ := it.Next(); s == nil {
						_, _ = it.First()
					}
				}
			})
			b.Run("op=Prev", func(b *testing.B) {
				it := newIter()
				defer it.Close()
				if s, _ := it.Last(); s == nil {
					b.Fatal("no fragments")
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if s, _ := it.Prev(); s == nil {
						_, _ = it.Last()
					}
				}
			})
		})
	}
}

// BenchmarkMemTableRangeDelLiveHeap reports the live heap that a memtable's
// fragments take at 1,000,000 fragments, and how much more it takes when an
// iterator opened before 10,000 splices pins the version it read. It measures
// the heap once; run it with -benchtime=1x. The metrics are in MiB:
// frags-MiB is the fragments' heap, pinned-extra-MiB the extra heap while the
// iterator is open, and unpinned-extra-MiB the extra heap after it's closed.
func BenchmarkMemTableRangeDelLiveHeap(b *testing.B) {
	const n = 1_000_000
	liveHeap := func() float64 {
		runtime.GC()
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return float64(ms.HeapAlloc) / (1 << 20)
	}
	for i := 0; i < b.N; i++ {
		m := newRangeDelBenchMemTable(b, n, "disjoint")
		m.tombstones.version.Store(nil)
		empty := liveHeap()
		m.tombstones.build()
		built := liveHeap()
		it := m.newRangeDelIter(nil)
		batches := rangeDelBenchSpliceBatches(n)
		rng := rand.New(rand.NewPCG(1, 1))
		for j := 0; j < 10_000; j++ {
			t := batches[j%len(batches)][0]
			// Splice at random positions, at sequence numbers above every
			// earlier splice's.
			pos := rng.IntN(n)
			t.start, t.end = rangeDelBenchKey(2*pos), rangeDelBenchKey(2*pos+2)
			t.trailer = base.MakeTrailer(base.SeqNum(2*n+j), InternalKeyKindRangeDelete)
			m.tombstones.add([]rangeDelTombstone{t})
		}
		pinned := liveHeap()
		it.Close()
		unpinned := liveHeap()
		b.ReportMetric(built-empty, "frags-MiB")
		b.ReportMetric(pinned-built, "pinned-extra-MiB")
		b.ReportMetric(unpinned-built, "unpinned-extra-MiB")
		m.free()
	}
}

// BenchmarkOpenReplayRangeDels measures opening a DB whose WAL holds 100,000
// batches of one range deletion each and reading one key, with incremental
// range-deletion fragments off and on. Unless the DB is read-only, Open
// flushes the replayed memtables before it returns. The read makes each mode
// build the replayed memtables' fragments before the op ends: with the option
// off, a read-only Open leaves that to the first read.
func BenchmarkOpenReplayRangeDels(b *testing.B) {
	const batches = 100_000
	src := vfs.NewMem()
	func() {
		opts := &Options{
			FS:                          src,
			Logger:                      base.NoopLoggerAndTracer{},
			MemTableSize:                64 << 20,
			MemTableStopWritesThreshold: 100,
		}
		d, err := Open("db", opts)
		require.NoError(b, err)
		// Block flushes so that every batch is still in a WAL when the DB closes.
		d.mu.Lock()
		d.mu.compact.flushing = true
		d.mu.Unlock()
		for i := 0; i < batches; i++ {
			from, to := fmt.Appendf(nil, "d/%09d/a", i), fmt.Appendf(nil, "d/%09d/z", i)
			require.NoError(b, d.DeleteRange(from, to, nil))
		}
		require.NoError(b, d.Set([]byte("r/key"), []byte("v"), nil))
		d.mu.Lock()
		d.mu.compact.flushing = false
		d.mu.Unlock()
		require.NoError(b, d.Close())
	}()
	for _, readOnly := range []bool{false, true} {
		for _, incremental := range []bool{false, true} {
			b.Run(fmt.Sprintf("read-only=%t/incremental=%t", readOnly, incremental), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					fs := vfs.NewMem()
					_, err := vfs.Clone(src, fs, "db", "db")
					require.NoError(b, err)
					opts := &Options{
						FS:           fs,
						Logger:       base.NoopLoggerAndTracer{},
						MemTableSize: 64 << 20,
						ReadOnly:     readOnly,
					}
					opts.Experimental.IncrementalRangeDelFragments = func() bool { return incremental }
					b.StartTimer()
					d, err := Open("db", opts)
					require.NoError(b, err)
					v, closer, err := d.Get([]byte("r/key"))
					require.NoError(b, err)
					require.Equal(b, "v", string(v))
					require.NoError(b, closer.Close())
					b.StopTimer()
					require.NoError(b, d.Close())
					b.StartTimer()
				}
			})
		}
	}
}
