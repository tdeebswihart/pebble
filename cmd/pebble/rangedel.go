// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/sstable/tablefilters/bloom"
	"github.com/spf13/cobra"
)

const (
	// rangeDelShapeCycle is the original workload: DeleteRanges cycle through a
	// fixed set of spans, so after the first pass every new tombstone overlaps an
	// old one.
	rangeDelShapeCycle = "cycle"
	// rangeDelShapeUnique gives every DeleteRange a fresh span that no earlier
	// tombstone covers, except for the fraction that re-delete from a queue's
	// start.
	rangeDelShapeUnique = "unique"

	dbOptionsDefault        = "default"
	dbOptionsProdWriteHeavy = "prod-writeheavy"
)

// rangeDelBenchConfig configures the range-del benchmark.
type rangeDelBenchConfig struct {
	Readers        int
	ReaderInterval time.Duration
	Writers        int
	WriterInterval time.Duration

	// Shape is rangeDelShapeCycle or rangeDelShapeUnique. Queues, OverlapFrac,
	// RangeDelsPerBatch and SetsPerBatch only apply to rangeDelShapeUnique.
	Shape string
	// Queues is the number of independent queues that DeleteRanges are spread
	// across. Each queue has its own key prefix and an ack level that advances
	// with every DeleteRange, so a queue's tombstones abut one another.
	Queues int
	// OverlapFrac is the fraction of DeleteRanges that instead delete from the
	// start of a queue up to its current ack level, overlapping the queue's
	// earlier tombstones.
	OverlapFrac float64
	// RangeDelsPerBatch is the number of DeleteRanges in each batch.
	RangeDelsPerBatch int
	// SetsPerBatch is the number of point Sets added to each batch. If negative,
	// the writer alternates as the cycle shape does: a batch of only DeleteRanges,
	// then a lone Set.
	SetsPerBatch int
	// IterFrac is the fraction of reads that open an iterator and scan instead
	// of doing a Get. If negative, reads alternate between the two.
	IterFrac float64
	// DBOptions is dbOptionsDefault or dbOptionsProdWriteHeavy.
	DBOptions string
}

// defaultRangeDelConfig returns defaults approximating a read-heavy workload
// that consistently invalidates the range-del fragment cache.
func defaultRangeDelConfig() rangeDelBenchConfig {
	return rangeDelBenchConfig{
		Readers:        4,
		ReaderInterval: 1 * time.Millisecond,
		Writers:        1,
		WriterInterval: 10 * time.Millisecond,

		Shape:             rangeDelShapeCycle,
		Queues:            1024,
		OverlapFrac:       0,
		RangeDelsPerBatch: 1,
		SetsPerBatch:      -1,
		IterFrac:          -1,
		DBOptions:         dbOptionsDefault,
	}
}

// legacyWorkload returns true if the config reproduces the original benchmark,
// in which case nothing beyond the original output is printed.
func (c *rangeDelBenchConfig) legacyWorkload() bool {
	return c.Shape == rangeDelShapeCycle && c.DBOptions == dbOptionsDefault && c.IterFrac < 0
}

func (c *rangeDelBenchConfig) validate() error {
	if c.Readers <= 0 && c.Writers <= 0 {
		return errors.New("rangedel: --readers and --writers cannot both be zero")
	}
	switch c.DBOptions {
	case dbOptionsDefault, dbOptionsProdWriteHeavy:
	default:
		return errors.Newf("rangedel: unknown --db-options %q (want %s or %s)",
			c.DBOptions, dbOptionsDefault, dbOptionsProdWriteHeavy)
	}
	if c.IterFrac > 1 {
		return errors.Newf("rangedel: --iter-frac %v must be negative or at most 1", c.IterFrac)
	}
	switch c.Shape {
	case rangeDelShapeCycle:
		d := defaultRangeDelConfig()
		if c.OverlapFrac != d.OverlapFrac || c.RangeDelsPerBatch != d.RangeDelsPerBatch ||
			c.SetsPerBatch != d.SetsPerBatch || c.Queues != d.Queues {
			return errors.New("rangedel: --rangedel-queues, --rangedel-overlap-frac, " +
				"--rangedels-per-batch and --sets-per-batch require --rangedel-shape=unique")
		}
	case rangeDelShapeUnique:
		if c.OverlapFrac < 0 || c.OverlapFrac > 1 {
			return errors.Newf("rangedel: --rangedel-overlap-frac %v must be in [0, 1]", c.OverlapFrac)
		}
		if c.RangeDelsPerBatch < 1 {
			return errors.New("rangedel: --rangedels-per-batch must be at least 1")
		}
		if c.Queues < 1 || c.Queues < c.Writers {
			return errors.New("rangedel: --rangedel-queues must be at least 1 and at least --writers")
		}
	default:
		return errors.Newf("rangedel: unknown --rangedel-shape %q (want cycle or unique)", c.Shape)
	}
	return nil
}

var rangeDelConfig = defaultRangeDelConfig()

var rangeDelCmd = &cobra.Command{
	Use:   "rangedel <dir>",
	Short: "benchmark reads/writes intermixed with range deletions",
	Long: `
Runs a workload that performs point reads, scanning reads and point writes
concurrently with range deletions. Read and write key ranges do not overlap
deletion key ranges. The workload is intended to evaluate the effectiveness of
memtable range delete optimization that avoids rebuilding fragments when reads
do not overlap with deleted ranges.
	`,
	Args: cobra.ExactArgs(1),
	RunE: runRangeDelCmd,
}

func init() {
	f := rangeDelCmd.Flags()
	f.IntVar(&rangeDelConfig.Readers, "readers", rangeDelConfig.Readers,
		"number of concurrent readers")
	f.DurationVar(&rangeDelConfig.ReaderInterval, "reader-interval", rangeDelConfig.ReaderInterval,
		"duration between reads (0 to unthrottle)")
	f.IntVar(&rangeDelConfig.Writers, "writers", rangeDelConfig.Writers,
		"number of concurrent writers")
	f.DurationVar(&rangeDelConfig.WriterInterval, "writer-interval", rangeDelConfig.WriterInterval,
		"duration between writes (0 to unthrottle)")
	f.StringVar(&rangeDelConfig.Shape, "rangedel-shape", rangeDelConfig.Shape,
		"DeleteRange workload: cycle (spans reused, so tombstones overlap) or unique "+
			"(every DeleteRange covers a fresh span, spread across --rangedel-queues queues)")
	f.IntVar(&rangeDelConfig.Queues, "rangedel-queues", rangeDelConfig.Queues,
		"unique shape: number of queues, each with its own key prefix and advancing ack level")
	f.Float64Var(&rangeDelConfig.OverlapFrac, "rangedel-overlap-frac", rangeDelConfig.OverlapFrac,
		"unique shape: fraction of DeleteRanges that re-delete from a queue's start, "+
			"overlapping its earlier tombstones")
	f.IntVar(&rangeDelConfig.RangeDelsPerBatch, "rangedels-per-batch", rangeDelConfig.RangeDelsPerBatch,
		"unique shape: DeleteRanges per batch")
	f.IntVar(&rangeDelConfig.SetsPerBatch, "sets-per-batch", rangeDelConfig.SetsPerBatch,
		"unique shape: point Sets per batch (negative: alternate a DeleteRange-only batch "+
			"with a lone Set, as the cycle shape does)")
	f.Float64Var(&rangeDelConfig.IterFrac, "iter-frac", rangeDelConfig.IterFrac,
		"fraction of reads that open an iterator and scan instead of doing a Get "+
			"(negative: alternate between the two)")
	f.StringVar(&rangeDelConfig.DBOptions, "db-options", rangeDelConfig.DBOptions,
		"DB options: default (the benchmark's usual options) or prod-writeheavy (a write-heavy "+
			"production configuration: 10s range-delete flush delay, 256 MiB memtables, "+
			"32 KiB blocks, 10-bit bloom filters, fastest compression; the comparer and "+
			"key schema stay the benchmark's)")
}

func runRangeDelCmd(cmd *cobra.Command, args []string) error {
	return runRangeDel(args[0], &rangeDelConfig)
}

// runRangeDel runs the range-del benchmark.
func runRangeDel(dir string, cfg *rangeDelBenchConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if cfg.DBOptions == dbOptionsProdWriteHeavy {
		dbOptionsHook = applyProdWriteHeavyDBOptions
	}
	if !cfg.legacyWorkload() {
		fmt.Printf("rangedel: shape=%s queues=%d overlap-frac=%v rangedels-per-batch=%d "+
			"sets-per-batch=%d iter-frac=%v db-options=%s gomaxprocs=%d\n",
			cfg.Shape, cfg.Queues, cfg.OverlapFrac, cfg.RangeDelsPerBatch,
			cfg.SetsPerBatch, cfg.IterFrac, cfg.DBOptions, runtime.GOMAXPROCS(0))
	}

	var reads, writes atomic.Uint64
	var counts rangeDelCounters
	var benchDB *pebble.DB
	reg := newHistogramRegistry()
	readLatency := reg.Register("read")

	// Reads (Gets and scans) and point Sets target "r/*"; range deletions
	// target "d/*". Reads do not overlap with deletion ranges. All indices
	// are bounded % 1000 so the key space stays fixed.
	runTest(dir, test{
		init: func(d DB, wg *sync.WaitGroup) {
			pd := d.(pebbleDB).d
			benchDB = pd
			// Pre-populate the read range so cold scans/Gets have content.
			for i := 0; i < 1000; i++ {
				key := cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "r/%04d", i), 0, 0)
				if err := pd.Set(key, []byte("v"), pebble.NoSync); err != nil {
					log.Fatal(err)
				}
			}

			limiter := maxOpsPerSec.newRateLimiter()

			for w := 0; w < cfg.Writers; w++ {
				w := w
				wg.Add(1)
				go func() {
					defer wg.Done()
					var ticker *time.Ticker
					if cfg.WriterInterval > 0 {
						ticker = time.NewTicker(cfg.WriterInterval)
						defer ticker.Stop()
					}
					var uw *uniqueWriter
					if cfg.Shape == rangeDelShapeUnique {
						uw = newUniqueWriter(pd, cfg, w, &counts)
					}
					for i := uint64(0); ; i++ {
						if ticker != nil {
							<-ticker.C
						}
						wait(limiter)
						if uw != nil {
							uw.write(i)
							writes.Add(1)
							continue
						}
						// Alternate DeleteRanges with point Sets.
						slot := i % 1000
						if i%2 == 0 {
							delFrom := cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "d/%d/a", slot), 0, 0)
							delTo := cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "d/%d/z", slot), 0, 0)
							if err := pd.DeleteRange(delFrom, delTo, pebble.NoSync); err != nil {
								log.Fatalf("rangedel writer %d: DeleteRange: %v", w, err)
							}
							counts.batches.Add(1)
							counts.rangeDels.Add(1)
						} else {
							setKey := cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "r/%04d", slot), 0, 0)
							if err := pd.Set(setKey, []byte("v"), pebble.NoSync); err != nil {
								log.Fatalf("rangedel writer %d: Set: %v", w, err)
							}
							counts.sets.Add(1)
						}
						writes.Add(1)
					}
				}()
			}
			for r := 0; r < cfg.Readers; r++ {
				r := r
				wg.Add(1)
				go func() {
					defer wg.Done()
					var ticker *time.Ticker
					if cfg.ReaderInterval > 0 {
						ticker = time.NewTicker(cfg.ReaderInterval)
						defer ticker.Stop()
					}
					var rng *rand.Rand
					if cfg.IterFrac >= 0 {
						rng = rand.New(rand.NewPCG(rangeDelSeed, uint64(cfg.Writers+r)))
					}
					for i := uint64(0); ; i++ {
						if ticker != nil {
							<-ticker.C
						}
						wait(limiter)
						// Alternate point Gets and range scans, unless --iter-frac
						// picks the mix.
						useIter := i%2 == 1
						if rng != nil {
							useIter = rng.Float64() < cfg.IterFrac
						}
						start := time.Now()
						if !useIter {
							key := cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "r/%04d", i%1000), 0, 0)
							_, closer, err := pd.Get(key)
							readLatency.Record(time.Since(start))
							if err != nil {
								log.Fatalf("rangedel reader: Get: %v", err)
							}
							_ = closer.Close()
						} else {
							lower := i % 1000
							iter, err := pd.NewIter(&pebble.IterOptions{
								LowerBound: cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "r/%04d", lower), 0, 0),
								UpperBound: cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "r/%04d", lower+10), 0, 0),
							})
							if err != nil {
								log.Fatalf("rangedel reader: NewIter: %v", err)
							}
							for valid := iter.First(); valid; valid = iter.Next() {
							}
							readLatency.Record(time.Since(start))
							if err := iter.Close(); err != nil {
								log.Fatalf("rangedel reader: iter Close: %v", err)
							}
						}
						reads.Add(1)
					}
				}()
			}
		},
		tick: func(elapsed time.Duration, i int) {
			if i%20 == 0 {
				fmt.Println("____elapsed______reads/s_____writes/s___p50(µs)___p95(µs)___p99(µs)___pMax(µs)")
			}
			reg.Tick(func(tick histogramTick) {
				h := tick.Hist
				fmt.Printf("%9.1fs %12d %12d %9.1f %9.1f %9.1f %10.1f\n",
					elapsed.Seconds(),
					reads.Swap(0),
					writes.Swap(0),
					usFromNs(h.ValueAtQuantile(50)),
					usFromNs(h.ValueAtQuantile(95)),
					usFromNs(h.ValueAtQuantile(99)),
					usFromNs(h.ValueAtQuantile(100)),
				)
			})
		},
		done: func(elapsed time.Duration) {
			fmt.Println("\n____elapsed___read_ops____read_ops/s___p50(µs)___p95(µs)___p99(µs)___pMax(µs)")
			reg.Tick(func(tick histogramTick) {
				h := tick.Cumulative
				fmt.Printf("%9.1fs %10d %13.1f %9.1f %9.1f %9.1f %10.1f\n",
					elapsed.Seconds(),
					h.TotalCount(),
					float64(h.TotalCount())/elapsed.Seconds(),
					usFromNs(h.ValueAtQuantile(50)),
					usFromNs(h.ValueAtQuantile(95)),
					usFromNs(h.ValueAtQuantile(99)),
					usFromNs(h.ValueAtQuantile(100)),
				)
			})
			if !cfg.legacyWorkload() {
				fmt.Printf("\nrangedel_summary: elapsed=%.1fs rangedel_batches=%d "+
					"rangedel_batches/s=%.1f rangedel_ops=%d overlapping_rangedel_ops=%d "+
					"point_sets=%d flushes=%d\n",
					elapsed.Seconds(),
					counts.batches.Load(),
					float64(counts.batches.Load())/elapsed.Seconds(),
					counts.rangeDels.Load(),
					counts.overlaps.Load(),
					counts.sets.Load(),
					benchDB.Metrics().Flush.Count,
				)
			}
		},
	})
	return nil
}

// rangeDelSeed seeds the pseudo-random choices of the unique shape and of
// --iter-frac, so runs of different binaries issue the same operations.
const rangeDelSeed = 0x72616e6765646c31

// rangeDelCounters counts the writer's operations for the end-of-run summary.
type rangeDelCounters struct {
	// batches counts batches that contain at least one DeleteRange.
	batches atomic.Uint64
	// rangeDels counts DeleteRanges, and overlaps counts the ones that re-delete
	// from a queue's start.
	rangeDels atomic.Uint64
	overlaps  atomic.Uint64
	// sets counts point Sets.
	sets atomic.Uint64
}

// uniqueWriter issues the writes of rangeDelShapeUnique on behalf of one
// writer goroutine. Writer w owns the queues q where q % Writers == w. Each
// queue is a key prefix "d/q<queue>/" with an ack level that only advances.
// A DeleteRange deletes [ack, ack+step) and advances the ack level to ack+step,
// so the tombstones of a queue abut without overlapping, like a task queue that
// deletes everything below a new ack level. Each DeleteRange picks a random
// queue, so the tombstones arrive scattered across the key space.
type uniqueWriter struct {
	db     *pebble.DB
	cfg    *rangeDelBenchConfig
	counts *rangeDelCounters
	rng    *rand.Rand
	// queues holds the ids of the queues that this writer owns, and acks holds
	// their ack levels.
	queues []int
	acks   []uint64
	// setSlot cycles through the point-Set keys.
	setSlot uint64
}

func newUniqueWriter(
	db *pebble.DB, cfg *rangeDelBenchConfig, writer int, counts *rangeDelCounters,
) *uniqueWriter {
	uw := &uniqueWriter{
		db:     db,
		cfg:    cfg,
		counts: counts,
		rng:    rand.New(rand.NewPCG(rangeDelSeed, uint64(writer))),
	}
	for q := writer; q < cfg.Queues; q += cfg.Writers {
		uw.queues = append(uw.queues, q)
	}
	uw.acks = make([]uint64, len(uw.queues))
	return uw
}

func queueKey(queue int, ack uint64) []byte {
	return cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "d/q%06d/%012d", queue, ack), 0, 0)
}

// write performs iteration i of the writer loop.
func (uw *uniqueWriter) write(i uint64) {
	sets := uw.cfg.SetsPerBatch
	if sets < 0 {
		// Alternate as the cycle shape does: a batch of only DeleteRanges, then a
		// lone Set.
		if i%2 == 1 {
			if err := uw.db.Set(uw.setKey(), []byte("v"), pebble.NoSync); err != nil {
				log.Fatalf("rangedel writer: Set: %v", err)
			}
			uw.counts.sets.Add(1)
			return
		}
		sets = 0
	}

	b := uw.db.NewBatch()
	for k := 0; k < uw.cfg.RangeDelsPerBatch; k++ {
		qi := uw.rng.IntN(len(uw.queues))
		queue, ack := uw.queues[qi], uw.acks[qi]
		var from, to uint64
		if ack > 0 && uw.rng.Float64() < uw.cfg.OverlapFrac {
			// Re-delete from the start of the queue, overlapping every earlier
			// tombstone of the queue.
			from, to = 0, ack
			uw.counts.overlaps.Add(1)
		} else {
			// Delete [previous ack level, new ack level).
			from, to = ack, ack+1+uw.rng.Uint64N(1000)
			uw.acks[qi] = to
		}
		if err := b.DeleteRange(queueKey(queue, from), queueKey(queue, to), nil); err != nil {
			log.Fatalf("rangedel writer: DeleteRange: %v", err)
		}
	}
	for s := 0; s < sets; s++ {
		if err := b.Set(uw.setKey(), []byte("v"), nil); err != nil {
			log.Fatalf("rangedel writer: batch Set: %v", err)
		}
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		log.Fatalf("rangedel writer: Commit: %v", err)
	}
	if err := b.Close(); err != nil {
		log.Fatalf("rangedel writer: batch Close: %v", err)
	}
	uw.counts.batches.Add(1)
	uw.counts.rangeDels.Add(uint64(uw.cfg.RangeDelsPerBatch))
	uw.counts.sets.Add(uint64(sets))
}

// setKey returns the next point-Set key. The keys are the read range's, as in
// the cycle shape.
func (uw *uniqueWriter) setKey() []byte {
	slot := uw.setSlot % 1000
	uw.setSlot++
	return cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "r/%04d", slot), 0, 0)
}

// dbOptionsHook, if set, adjusts the options that newPebbleDB is about to open
// the DB with, before it fills in the defaults.
var dbOptionsHook func(opts *pebble.Options)

// applyProdWriteHeavyDBOptions overrides the options with a write-heavy
// production configuration: a 10s range-delete flush delay, 256 MiB memtables,
// a 512 MiB base level, 32 KiB data blocks with 256 KiB index blocks, 10-bit
// bloom filters and fastest compression at every level, a 10 MiB MANIFEST
// rollover, and compaction concurrency of 1 to min(3, GOMAXPROCS-1). The
// comparer and key schema stay the benchmark's. The benchmark's value
// separation and garbage-compaction overrides are dropped, leaving those
// options at the Pebble defaults, as the production configuration does.
func applyProdWriteHeavyDBOptions(opts *pebble.Options) {
	opts.FormatMajorVersion = pebble.FormatIngestBlobFiles
	opts.CompactionConcurrencyRange = func() (lower, upper int) {
		return 1, min(3, max(runtime.GOMAXPROCS(0)-1, 1))
	}
	opts.L0CompactionThreshold = 2
	opts.L0StopWritesThreshold = 1000
	opts.MemTableStopWritesThreshold = 4
	opts.FlushDelayDeleteRange = 10 * time.Second
	opts.FlushDelayRangeKey = 10 * time.Second
	opts.MemTableSize = 256 << 20
	opts.LBaseMaxBytes = 512 << 20
	opts.Experimental.L0CompactionConcurrency = 2
	opts.Experimental.ValueSeparationPolicy = nil
	opts.Experimental.CompactionGarbageFractionForMaxConcurrency = nil
	opts.MaxManifestFileSize = 10 << 20

	opts.Levels[0] = pebble.LevelOptions{
		BlockSize:         32 << 10,
		IndexBlockSize:    256 << 10,
		TableFilterPolicy: func() pebble.TableFilterPolicy { return bloom.FilterPolicy(10) },
	}
	opts.Levels[0].EnsureL0Defaults()
	for i := 1; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		*l = pebble.LevelOptions{
			BlockSize:         32 << 10,
			IndexBlockSize:    256 << 10,
			TableFilterPolicy: func() pebble.TableFilterPolicy { return bloom.FilterPolicy(10) },
		}
		l.EnsureL1PlusDefaults(&opts.Levels[i-1])
	}
	opts.ApplyCompressionSettings(func() pebble.DBCompressionSettings {
		return pebble.DBCompressionFastest
	})
}

func usFromNs(ns int64) float64 {
	return float64(ns) / 1000
}
