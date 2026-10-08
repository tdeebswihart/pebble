// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bench

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/ackseq"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/randvar"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// BenchmarkYCSB mirrors the YCSB roachtest in
// pkg/cmd/roachtest/tests/pebble_ycsb.go but runs in-process so the I/O paths
// can be profiled with the standard Go profilers. For each value size, it
// builds a 10M-key fixture once (cached on disk and reused across runs) and
// then takes a hard-link Checkpoint per workload so each workload starts from
// the same prepopulated state.
//
// Run example:
//
//	go test -run=^$ -bench=BenchmarkYCSB \
//	    ./bench -timeout=0 -benchtime=1000000x
//
// b.N controls the number of workload operations per sub-benchmark.
func BenchmarkYCSB(b *testing.B) {
	initialKeys := *ycsbBenchInitialKeys
	if initialKeys < 1 || *ycsbBenchConcurrency < 1 || *ycsbBenchWarmupOps < 0 {
		b.Fatal("YCSB fixture keys and concurrency must be positive; warmup must be nonnegative")
	}
	const fixtureCacheBytes = 4 << 30

	rootDir := *ycsbBenchFixtureDir
	formatVer := int(pebble.FormatNewest)

	for _, size := range ycsbBenchSizes {
		b.Run(fmt.Sprintf("values=%d", size), func(b *testing.B) {
			fixtureDir := filepath.Join(rootDir,
				fmt.Sprintf("values=%d", size),
				fmt.Sprintf("keys=%d", initialKeys),
				fmt.Sprintf("format=%d", formatVer),
				"fixture")

			if err := ensureYCSBFixture(b, fixtureDir, size, initialKeys, fixtureCacheBytes); err != nil {
				b.Fatal(err)
			}

			for _, workload := range []string{"A", "B", "C", "D", "E", "F"} {
				b.Run(workload, func(b *testing.B) {
					runYCSBWorkload(b, fixtureDir, workload, size, initialKeys)
				})
			}
		})
	}
}

var ycsbBenchSizes = []int{64, 1024}

var ycsbBenchFixtureDir = flag.String("ycsb-bench-fixture-dir", defaultYCSBFixtureDir(),
	"directory in which YCSB benchmark fixtures are cached")

var ycsbBenchInitialKeys = flag.Int("ycsb-bench-initial-keys", 10_000_000,
	"number of keys in the YCSB benchmark fixture")

var ycsbBenchConcurrency = flag.Int("ycsb-bench-concurrency", 256,
	"maximum number of YCSB benchmark workers")

var ycsbBenchWarmupOps = flag.Int("ycsb-bench-warmup-ops", 10_000,
	"untimed YCSB operations before each measured workload")

// Candidate options apply to the workload checkpoint, never the cached fixture.
var ycsbBenchOptionsHook func(*pebble.Options)

func defaultYCSBFixtureDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), "pebble-bench", "ycsb")
	}
	return filepath.Join(home, ".cache", "pebble-bench", "ycsb")
}

// ensureYCSBFixture builds the prepopulated fixture at fixtureDir if a
// `<fixtureDir>.ready` marker is missing. The marker is written only after
// the load phase finishes cleanly so a half-built fixture from a killed run
// is not silently reused.
func ensureYCSBFixture(
	b *testing.B, fixtureDir string, valueSize, initialKeys int, cacheBytes int64,
) error {
	marker := fixtureDir + ".ready"
	if _, err := os.Stat(marker); err == nil {
		b.Logf("reusing YCSB fixture at %s", fixtureDir)
		return nil
	}
	b.Logf("building YCSB fixture at %s (values=%d, initial-keys=%d). This may take a while",
		fixtureDir, valueSize, initialKeys)

	if err := os.RemoveAll(fixtureDir); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(fixtureDir), 0o755); err != nil {
		return err
	}

	// Mirror the roachtest load phase with --concurrency=1 and the requested
	// value size. Workload weights are unused by ycsb.init; pick the default.
	common := &CommonConfig{
		CacheSize:   cacheBytes,
		Concurrency: 1,
		Logger:      base.NoopLoggerAndTracer{},
	}
	cfg := DefaultYCSBConfig()
	cfg.InitialKeys = initialKeys
	cfg.Values = randvar.NewBytesFlag(fmt.Sprintf("%d", valueSize))

	weights, err := ycsbParseWorkload(cfg.Workload)
	if err != nil {
		return err
	}
	keyDist, err := ycsbParseKeyDist(cfg.Keys, &cfg)
	if err != nil {
		return err
	}

	db := NewPebbleDB(fixtureDir, common)
	y := newYcsb(common, &cfg, weights, keyDist, cfg.Batch, cfg.Scans, cfg.Values)
	y.init(db)

	if err := db.Close(); err != nil {
		return err
	}

	f, err := os.Create(marker)
	if err != nil {
		return err
	}
	return f.Close()
}

// runYCSBWorkload checkpoints the fixture into b.TempDir(), opens the
// checkpoint, and runs the requested workload for b.N operations.
func runYCSBWorkload(b *testing.B, fixtureDir, workload string, valueSize, initialKeys int) {
	common := &CommonConfig{
		CacheSize:   4 << 30,
		Concurrency: *ycsbBenchConcurrency,
		DisableWAL:  false,
		Logger:      base.NoopLoggerAndTracer{},
		OptionsHook: ycsbBenchOptionsHook,
	}
	cfg := DefaultYCSBConfig()
	cfg.Workload = workload
	cfg.Keys = "zipf"
	if workload == "D" {
		cfg.Keys = "uniform"
	}
	cfg.InitialKeys = 0
	cfg.PrepopulatedKeys = initialKeys
	cfg.Values = randvar.NewBytesFlag(fmt.Sprintf("%d", valueSize))

	// Open the fixture with compactions disabled and checkpoint it. The
	// checkpoint hard-links sstables so this is effectively free in time and
	// disk; disabling compactions keeps the cached fixture from being mutated
	// between runs.
	srcCfg := *common
	srcCfg.OptionsHook = nil
	srcCfg.DisableAutoCompactions = true
	srcDB := NewPebbleDB(fixtureDir, &srcCfg).(pebbleDB)
	ckptDir := filepath.Join(b.TempDir(), "ckpt")
	ckptErr := srcDB.d.Checkpoint(ckptDir)
	if closeErr := srcDB.Close(); ckptErr == nil {
		ckptErr = closeErr
	}
	if ckptErr != nil {
		b.Fatal(ckptErr)
	}

	db := NewPebbleDB(ckptDir, common)
	defer func() { _ = db.Close() }()

	weights, err := ycsbParseWorkload(cfg.Workload)
	if err != nil {
		b.Fatal(err)
	}
	keyDist, err := ycsbParseKeyDist(cfg.Keys, &cfg)
	if err != nil {
		b.Fatal(err)
	}

	y := newYcsb(common, &cfg, weights, keyDist, cfg.Batch, cfg.Scans, cfg.Values)
	y.db = db
	y.keyNum = ackseq.New(uint64(initialKeys))
	warmup := newYCSBBenchmarkRun(y, uint64(*ycsbBenchWarmupOps), common.Concurrency)
	warmup.run(db)
	y.readAmpCount.Store(0)
	y.readAmpSum.Store(0)
	run := newYCSBBenchmarkRun(y, uint64(b.N), common.Concurrency)
	// Keep the warmup's buffers and RNG streams, but exclude its latency samples.
	for i := range min(len(run.workers), len(warmup.workers)) {
		run.workers[i].buf = warmup.workers[i].buf
		run.workers[i].ops = randvar.NewWeighted(run.workers[i].buf.rng, y.weights...)
	}

	b.ReportAllocs()
	b.ResetTimer()
	run.run(db)
	b.StopTimer()
	run.report(b)
	b.ReportMetric(float64(*ycsbBenchWarmupOps), "warmup-ops")
}

const ycsbBenchMaxLatency = time.Minute

type ycsbBenchmarkWorker struct {
	buf        ycsbBuf
	ops        *randvar.Weighted
	quota      uint64
	completed  uint64
	histograms [ycsbNumOps]*hdrhistogram.Histogram
	clipped    [ycsbNumOps]uint64
}

type ycsbBenchmarkRun struct {
	y       *ycsb
	workers []ycsbBenchmarkWorker
}

func newYCSBBenchmarkHistogram() *hdrhistogram.Histogram {
	return hdrhistogram.New(100, ycsbBenchMaxLatency.Nanoseconds(), 2)
}

// The CLI checks its global limit after each operation and may exceed b.N.
// Fixed worker quotas execute exactly b.N operations using the same operations.
// The benchmark excludes the CLI's one-second read-amp sampler, which can delay
// completion after the workers finish. Histogram allocation precedes the timer.
func newYCSBBenchmarkRun(y *ycsb, operations uint64, concurrency int) *ycsbBenchmarkRun {
	n := min(uint64(concurrency), operations)
	r := &ycsbBenchmarkRun{y: y, workers: make([]ycsbBenchmarkWorker, int(n))}
	for i := range r.workers {
		w := &r.workers[i]
		w.buf.rng = rand.New(rand.NewPCG(6207, uint64(i)))
		w.ops = randvar.NewWeighted(w.buf.rng, y.weights...)
		w.quota = operations / n
		if uint64(i) < operations%n {
			w.quota++
		}
		for op := range ycsbNumOps {
			if y.weights.get(op) > 0 {
				w.histograms[op] = newYCSBBenchmarkHistogram()
			}
		}
	}
	return r
}

func (r *ycsbBenchmarkRun) run(db DB) {
	var wg sync.WaitGroup
	for i := range r.workers {
		w := &r.workers[i]
		wg.Go(func() {
			for range w.quota {
				op := w.ops.Int()
				start := time.Now()
				switch op {
				case ycsbInsert:
					r.y.insert(db, &w.buf)
				case ycsbRead:
					r.y.read(db, &w.buf)
				case ycsbScan:
					r.y.scan(db, &w.buf, false)
				case ycsbReverseScan:
					r.y.scan(db, &w.buf, true)
				case ycsbUpdate:
					r.y.update(db, &w.buf)
				default:
					panic("unknown YCSB operation")
				}
				latency := time.Since(start)
				if latency > ycsbBenchMaxLatency {
					w.clipped[op]++
				}
				if err := w.histograms[op].RecordValue(max(100, min(latency.Nanoseconds(), ycsbBenchMaxLatency.Nanoseconds()))); err != nil {
					panic(err)
				}
				w.completed++
			}
		})
	}
	wg.Wait()
}

func (r *ycsbBenchmarkRun) completed() uint64 {
	var count uint64
	for _, w := range r.workers {
		count += w.completed
	}
	return count
}

func (r *ycsbBenchmarkRun) report(b *testing.B) {
	completed := r.completed()
	if completed != uint64(b.N) {
		b.Fatalf("completed %d operations, expected %d", completed, b.N)
	}
	b.ReportMetric(float64(completed), "completed-ops")
	b.ReportMetric(float64(len(r.workers)), "workers")
	b.ReportMetric(float64(completed)/b.Elapsed().Seconds(), "ops/s")
	all := newYCSBBenchmarkHistogram()
	for op, name := range []string{"insert", "read", "scan", "rscan", "update"} {
		h := newYCSBBenchmarkHistogram()
		var clipped uint64
		for _, w := range r.workers {
			if w.histograms[op] != nil {
				if dropped := h.Merge(w.histograms[op]); dropped != 0 {
					b.Fatalf("dropped %d histogram samples", dropped)
				}
				clipped += w.clipped[op]
			}
		}
		if h.TotalCount() == 0 {
			continue
		}
		b.ReportMetric(float64(h.TotalCount()), name+"-ops")
		b.ReportMetric(float64(clipped), name+"-clipped-ops")
		for _, q := range []float64{50, 95, 99, 99.9, 100} {
			b.ReportMetric(float64(h.ValueAtQuantile(q)), fmt.Sprintf("%s-p%g-ns", name, q))
		}
		if dropped := all.Merge(h); dropped != 0 {
			b.Fatalf("dropped %d combined histogram samples", dropped)
		}
	}
	if all.TotalCount() != int64(completed) {
		b.Fatalf("histogram has %d samples for %d completed operations", all.TotalCount(), completed)
	}
	b.ReportMetric(float64(all.ValueAtQuantile(99)), "p99-ns")
	b.ReportMetric(float64(all.ValueAtQuantile(99.9)), "p99.9-ns")
}

func TestYCSBBenchmarkOperations(t *testing.T) {
	for _, workload := range []string{"A", "B", "C", "D", "E", "F"} {
		t.Run(workload, func(t *testing.T) {
			common := &CommonConfig{CacheSize: 1 << 20, DisableWAL: true, Logger: base.NoopLoggerAndTracer{},
				OptionsHook: func(o *pebble.Options) { o.FS = vfs.NewMem() },
			}
			db := NewPebbleDB("db", common)
			defer func() { require.NoError(t, db.Close()) }()
			cfg := DefaultYCSBConfig()
			cfg.Workload, cfg.InitialKeys = workload, 100
			weights, err := ycsbParseWorkload(workload)
			require.NoError(t, err)
			keys, err := ycsbParseKeyDist(cfg.Keys, &cfg)
			require.NoError(t, err)
			y := newYcsb(common, &cfg, weights, keys, cfg.Batch, cfg.Scans, cfg.Values)
			y.init(db)
			y.db, y.keyNum = db, ackseq.New(100)
			for _, operations := range []uint64{0, 1, 7, 257} {
				run := newYCSBBenchmarkRun(y, operations, 256)
				run.run(db)
				require.Equal(t, operations, run.completed())
				var samples int64
				for _, w := range run.workers {
					require.Equal(t, w.quota, w.completed)
					for _, h := range w.histograms {
						if h != nil {
							samples += h.TotalCount()
						}
					}
				}
				require.EqualValues(t, operations, samples)
			}
		})
	}
}
