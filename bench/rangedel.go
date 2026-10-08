// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bench

import (
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/rate"
	"github.com/cockroachdb/pebble/sstable/tablefilters/bloom"
)

const (
	// rangeDelShapeCycle cycles through fixed spans, so later tombstones overlap
	// earlier ones.
	rangeDelShapeCycle = "cycle"
	// rangeDelShapeUnique gives every DeleteRange a fresh span that no earlier
	// tombstone covers, except for the fraction that re-delete from a queue's
	// start.
	rangeDelShapeUnique = "unique"

	dbOptionsDefault        = "default"
	dbOptionsProdWriteHeavy = "prod-writeheavy"
)

// RangeDelConfig configures the range-del benchmark.
type RangeDelConfig struct {
	Readers        int
	ReaderInterval time.Duration
	Writers        int
	WriterInterval time.Duration

	// Shape is rangeDelShapeCycle or rangeDelShapeUnique. Queues, OverlapFrac,
	// RangeDelsPerBatch and SetsPerBatch only apply to rangeDelShapeUnique.
	Shape string
	// Queues is the number of key prefixes with independent advancing ack levels.
	Queues int
	// OverlapFrac is the fraction of DeleteRanges that re-delete from a queue's
	// start to its current ack level.
	OverlapFrac float64
	// RangeDelsPerBatch is the number of DeleteRanges in each batch.
	RangeDelsPerBatch int
	// SetsPerBatch is the number of point Sets per batch. If negative, writes
	// alternate between a batch of DeleteRanges and a lone Set.
	SetsPerBatch int
	// IterFrac is the fraction of reads that open an iterator and scan instead
	// of doing a Get. If negative, reads alternate between the two.
	IterFrac float64
	// DBOptions is dbOptionsDefault or dbOptionsProdWriteHeavy.
	DBOptions string
	// IncrementalMemTableRangeDels sets
	// pebble.Options.IncrementalMemTableRangeDels.
	IncrementalMemTableRangeDels bool
}

// DefaultRangeDelConfig returns the default range-delete benchmark configuration.
func DefaultRangeDelConfig() RangeDelConfig {
	return RangeDelConfig{
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

func (c *RangeDelConfig) validate() error {
	if c.Readers < 0 || c.Writers < 0 || c.ReaderInterval < 0 || c.WriterInterval < 0 {
		return errors.New("rangedel: worker counts and intervals must be nonnegative")
	}
	if c.Readers <= 0 && c.Writers <= 0 {
		return errors.New("rangedel: --readers and --writers cannot both be zero")
	}
	switch c.DBOptions {
	case dbOptionsDefault, dbOptionsProdWriteHeavy:
	default:
		return errors.Newf("rangedel: unknown --db-options %q (want %s or %s)",
			c.DBOptions, dbOptionsDefault, dbOptionsProdWriteHeavy)
	}
	if math.IsNaN(c.IterFrac) || math.IsInf(c.IterFrac, 0) || c.IterFrac > 1 {
		return errors.Newf("rangedel: --iter-frac %v must be negative or at most 1", c.IterFrac)
	}
	switch c.Shape {
	case rangeDelShapeCycle:
		d := DefaultRangeDelConfig()
		if c.OverlapFrac != d.OverlapFrac || c.RangeDelsPerBatch != d.RangeDelsPerBatch ||
			c.SetsPerBatch != d.SetsPerBatch || c.Queues != d.Queues {
			return errors.New("rangedel: --rangedel-queues, --rangedel-overlap-frac, " +
				"--rangedels-per-batch and --sets-per-batch require --rangedel-shape=unique")
		}
	case rangeDelShapeUnique:
		if math.IsNaN(c.OverlapFrac) || c.OverlapFrac < 0 || c.OverlapFrac > 1 {
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

func rangeDelCommonConfig(common *CommonConfig, cfg *RangeDelConfig) CommonConfig {
	c := *common
	callerHook := c.OptionsHook
	c.OptionsHook = func(opts *pebble.Options) {
		if callerHook != nil {
			callerHook(opts)
		}
		if cfg.DBOptions == dbOptionsProdWriteHeavy {
			applyProdWriteHeavyDBOptions(opts)
		}
		incremental := cfg.IncrementalMemTableRangeDels
		opts.IncrementalMemTableRangeDels = func() bool { return incremental }
	}
	return c
}

// RunRangeDel runs the range-del benchmark.
func RunRangeDel(dir string, common *CommonConfig, cfg *RangeDelConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	c := rangeDelCommonConfig(common, cfg)
	var arenaSize uint64
	hook := c.OptionsHook
	c.OptionsHook = func(opts *pebble.Options) {
		if hook != nil {
			hook(opts)
		}
		arenaSize = opts.MemTableSize
	}
	fmt.Printf("rangedel: shape=%s queues=%d overlap-frac=%v rangedels-per-batch=%d "+
		"sets-per-batch=%d iter-frac=%v db-options=%s incremental-memtable-rangedels=%t "+
		"gomaxprocs=%d\n",
		cfg.Shape, cfg.Queues, cfg.OverlapFrac, cfg.RangeDelsPerBatch,
		cfg.SetsPerBatch, cfg.IterFrac, cfg.DBOptions, cfg.IncrementalMemTableRangeDels,
		runtime.GOMAXPROCS(0))

	var reads, writes atomic.Uint64
	var counts rangeDelCounters
	var benchDB *pebble.DB
	stop := make(chan struct{})
	var workers *sync.WaitGroup
	reg := newHistogramRegistry()
	readLatency := reg.Register("read")
	commits := newRangeDelLatencies(cfg.WriterInterval, "rangedel", "point")
	readerTails := newRangeDelLatencies(cfg.ReaderInterval, "iter", "get")

	// Reads (Gets and scans) and point Sets target "r/*"; range deletions
	// target "d/*". Reads do not overlap with deletion ranges.
	RunTest(dir, &c, Test{
		Init: func(d DB) {
			pd := d.(pebbleDB).d
			benchDB = pd
			fmt.Printf("rangedel_config: arena_bytes=%d readers=%d writers=%d reader_interval=%s writer_interval=%s\n",
				arenaSize, cfg.Readers, cfg.Writers, cfg.ReaderInterval, cfg.WriterInterval)
			// Pre-populate the read range so cold scans/Gets have content.
			for i := 0; i < 1000; i++ {
				key := cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "r/%04d", i), 0, 0)
				if err := pd.Set(key, []byte("v"), pebble.NoSync); err != nil {
					log.Fatal(err)
				}
			}
		},
		Run: func(d DB, wg *sync.WaitGroup) {
			workers = wg
			pd := d.(pebbleDB).d
			tokens := rangeDelRateTokens(c.RateLimiter, stop)

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
						uw = newUniqueWriter(pd, cfg, w, &counts, commits)
					}
					for i := uint64(0); ; i++ {
						if !rangeDelWait(stop, ticker, tokens) {
							return
						}
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
							start := time.Now()
							if err := pd.DeleteRange(delFrom, delTo, pebble.NoSync); err != nil {
								log.Fatalf("rangedel writer %d: DeleteRange: %v", w, err)
							}
							commits.record(start, true /* rangeDel */)
							counts.batches.Add(1)
							counts.rangeDels.Add(1)
						} else {
							setKey := cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "r/%04d", slot), 0, 0)
							start := time.Now()
							if err := pd.Set(setKey, []byte("v"), pebble.NoSync); err != nil {
								log.Fatalf("rangedel writer %d: Set: %v", w, err)
							}
							commits.record(start, false /* rangeDel */)
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
						if !rangeDelWait(stop, ticker, tokens) {
							return
						}
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
							if err != nil {
								log.Fatalf("rangedel reader: Get: %v", err)
							}
							if err := closer.Close(); err != nil {
								log.Fatalf("rangedel reader: Get Close: %v", err)
							}
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
							if err := iter.Error(); err != nil {
								log.Fatalf("rangedel reader: iter: %v", err)
							}
							if err := iter.Close(); err != nil {
								log.Fatalf("rangedel reader: iter Close: %v", err)
							}
						}
						latency := time.Since(start)
						readLatency.Record(latency)
						readerTails.recordElapsed(latency, useIter)
						reads.Add(1)
					}
				}()
			}
		},
		Tick: func(elapsed time.Duration, i int) {
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
		Done: func(elapsed time.Duration) {
			drainStart := time.Now()
			close(stop)
			workers.Wait()
			elapsed += time.Since(drainStart)
			readerTails.print("read", elapsed)
			commits.print("commit", elapsed)
			m := benchDB.Metrics()
			fmt.Printf("rangedel_summary: elapsed=%.3fs rangedel_batches=%d rangedel_ops=%d "+
				"overlapping_rangedel_ops=%d point_sets=%d flushes=%d memtable_bytes=%d "+
				"memtable_count=%d cumulative_flushable_mem_bytes=%d\n",
				elapsed.Seconds(), counts.batches.Load(), counts.rangeDels.Load(), counts.overlaps.Load(),
				counts.sets.Load(), m.Flush.Count, m.MemTable.Size, m.MemTable.Count,
				m.MemTable.CumulativeFlushableMemBytes)
		},
	})
	return nil
}

func rangeDelWait(stop <-chan struct{}, ticker *time.Ticker, tokens <-chan struct{}) bool {
	select {
	case <-stop:
		return false
	default:
	}
	if ticker != nil {
		select {
		case <-stop:
			return false
		case <-ticker.C:
		}
	}
	if tokens != nil {
		select {
		case <-stop:
			return false
		case <-tokens:
		}
	}
	return true
}

func rangeDelRateTokens(limiter *rate.Limiter, stop <-chan struct{}) <-chan struct{} {
	if limiter == nil {
		return nil
	}
	tokens := make(chan struct{})
	// Limiter.Wait cannot be cancelled. Keep that wait outside the DB workers
	// so shutdown can drain them even at rate zero. This pump has CLI lifetime.
	go func() {
		for {
			limiter.Wait(1)
			select {
			case <-stop:
				return
			case tokens <- struct{}{}:
			}
		}
	}()
	return tokens
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
	sets      atomic.Uint64
}

type rangeDelLatencies struct {
	mu             sync.Mutex
	interval       time.Duration
	names          [3]string
	raw, corrected [3]*hdrhistogram.Histogram
	clipped        [3]uint64
}

func newRangeDelLatencies(interval time.Duration, first, second string) *rangeDelLatencies {
	c := &rangeDelLatencies{interval: interval, names: [3]string{"all", first, second}}
	for i := range c.raw {
		c.raw[i] = hdrhistogram.New(1, maxLatency.Nanoseconds(), 3)
		if interval > 0 {
			c.corrected[i] = hdrhistogram.New(1, maxLatency.Nanoseconds(), 3)
		}
	}
	return c
}

func (c *rangeDelLatencies) record(start time.Time, first bool) {
	c.recordElapsed(time.Since(start), first)
}

func (c *rangeDelLatencies) recordElapsed(elapsed time.Duration, first bool) {
	d := max(1, min(elapsed.Nanoseconds(), maxLatency.Nanoseconds()))
	c.mu.Lock()
	defer c.mu.Unlock()
	class := 2
	if first {
		class = 1
	}
	for _, i := range []int{0, class} {
		if elapsed > maxLatency {
			c.clipped[i]++
		}
		if err := c.raw[i].RecordValue(d); err != nil {
			panic(err)
		}
		// Synthetic samples assume a fixed arrival interval for each worker.
		// They estimate coordinated omission; they are not completed operations.
		if c.corrected[i] != nil {
			if err := c.corrected[i].RecordCorrectedValue(d, c.interval.Nanoseconds()); err != nil {
				panic(err)
			}
		}
	}
}

func (c *rangeDelLatencies) print(kind string, elapsed time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, name := range c.names {
		for _, measurement := range []struct {
			label string
			h     *hdrhistogram.Histogram
		}{{"service", c.raw[i]}, {"corrected", c.corrected[i]}} {
			if measurement.h == nil {
				continue
			}
			h := measurement.h
			fmt.Printf("rangedel_latency: kind=%s class=%s measurement=%s samples=%d "+
				"completed_ops=%d ops/s=%.1f clipped_ops=%d expected_interval_ns=%d p50_us=%.3f p95_us=%.3f "+
				"p99_us=%.3f p99.9_us=%.3f max_us=%.3f\n",
				kind, name, measurement.label, h.TotalCount(), c.raw[i].TotalCount(),
				float64(c.raw[i].TotalCount())/elapsed.Seconds(), c.clipped[i], c.interval.Nanoseconds(),
				usFromNs(h.ValueAtQuantile(50)), usFromNs(h.ValueAtQuantile(95)),
				usFromNs(h.ValueAtQuantile(99)), usFromNs(h.ValueAtQuantile(99.9)), usFromNs(h.Max()))
		}
	}
}

// uniqueWriter owns queues where q % Writers equals its writer index. Each
// queue has a key prefix "d/q<queue>/" and an advancing ack level. DeleteRanges
// choose a queue at random and abut within it, except for re-deletions from zero.
type uniqueWriter struct {
	db      *pebble.DB
	cfg     *RangeDelConfig
	counts  *rangeDelCounters
	commits *rangeDelLatencies
	rng     *rand.Rand
	// queues holds the ids of the queues that this writer owns, and acks holds
	// their ack levels.
	queues  []int
	acks    []uint64
	setSlot uint64
}

func newUniqueWriter(
	db *pebble.DB,
	cfg *RangeDelConfig,
	writer int,
	counts *rangeDelCounters,
	commits *rangeDelLatencies,
) *uniqueWriter {
	uw := &uniqueWriter{
		db:      db,
		cfg:     cfg,
		counts:  counts,
		commits: commits,
		rng:     rand.New(rand.NewPCG(rangeDelSeed, uint64(writer))),
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

func (uw *uniqueWriter) write(i uint64) {
	sets := uw.cfg.SetsPerBatch
	if sets < 0 {
		// Alternate as the cycle shape does: a batch of only DeleteRanges, then a
		// lone Set.
		if i%2 == 1 {
			key := uw.setKey()
			start := time.Now()
			if err := uw.db.Set(key, []byte("v"), pebble.NoSync); err != nil {
				log.Fatalf("rangedel writer: Set: %v", err)
			}
			uw.commits.record(start, false /* rangeDel */)
			uw.counts.sets.Add(1)
			return
		}
		sets = 0
	}

	b := uw.db.NewBatch()
	var overlaps uint64
	for k := 0; k < uw.cfg.RangeDelsPerBatch; k++ {
		from, to, overlap := uw.nextRange()
		if overlap {
			overlaps++
		}
		if err := b.DeleteRange(from, to, nil); err != nil {
			log.Fatalf("rangedel writer: DeleteRange: %v", err)
		}
	}
	for s := 0; s < sets; s++ {
		if err := b.Set(uw.setKey(), []byte("v"), nil); err != nil {
			log.Fatalf("rangedel writer: batch Set: %v", err)
		}
	}
	start := time.Now()
	if err := b.Commit(pebble.NoSync); err != nil {
		log.Fatalf("rangedel writer: Commit: %v", err)
	}
	uw.commits.record(start, true /* rangeDel */)
	if err := b.Close(); err != nil {
		log.Fatalf("rangedel writer: batch Close: %v", err)
	}
	uw.counts.batches.Add(1)
	uw.counts.rangeDels.Add(uint64(uw.cfg.RangeDelsPerBatch))
	uw.counts.overlaps.Add(overlaps)
	uw.counts.sets.Add(uint64(sets))
}

func (uw *uniqueWriter) nextRange() (start, end []byte, overlap bool) {
	qi := uw.rng.IntN(len(uw.queues))
	queue, ack := uw.queues[qi], uw.acks[qi]
	if ack > 0 && uw.rng.Float64() < uw.cfg.OverlapFrac {
		return queueKey(queue, 0), queueKey(queue, ack), true
	}
	next := ack + 1 + uw.rng.Uint64N(1000)
	uw.acks[qi] = next
	return queueKey(queue, ack), queueKey(queue, next), false
}

// setKey returns the next point-Set key in the read range.
func (uw *uniqueWriter) setKey() []byte {
	slot := uw.setSlot % 1000
	uw.setSlot++
	return cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "r/%04d", slot), 0, 0)
}

// applyProdWriteHeavyDBOptions applies a write-heavy production configuration,
// preserving the benchmark's comparer and key schema.
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
	opts.L0CompactionConcurrency = 2
	opts.ValueSeparationPolicy = nil
	opts.CompactionGarbageFractionForMaxConcurrency = nil
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
