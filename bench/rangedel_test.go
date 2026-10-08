// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bench

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/sstable/tablefilters/bloom"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestRangeDelInvalidConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*RangeDelConfig)
	}{
		{"no-workers", func(c *RangeDelConfig) { c.Readers, c.Writers = 0, 0 }},
		{"negative-workers", func(c *RangeDelConfig) { c.Readers = -1 }},
		{"negative-interval", func(c *RangeDelConfig) { c.WriterInterval = -time.Second }},
		{"unknown-profile", func(c *RangeDelConfig) { c.DBOptions = "unknown" }},
		{"unknown-shape", func(c *RangeDelConfig) { c.Shape = "unknown" }},
		{"iterator-fraction", func(c *RangeDelConfig) { c.IterFrac = 2 }},
		{"nan-iterator-fraction", func(c *RangeDelConfig) { c.IterFrac = math.NaN() }},
		{"cycle-queues", func(c *RangeDelConfig) { c.Queues++ }},
		{"unique-overlap", func(c *RangeDelConfig) { c.Shape, c.OverlapFrac = rangeDelShapeUnique, 2 }},
		{"nan-overlap", func(c *RangeDelConfig) { c.Shape, c.OverlapFrac = rangeDelShapeUnique, math.NaN() }},
		{"unique-batch", func(c *RangeDelConfig) { c.Shape, c.RangeDelsPerBatch = rangeDelShapeUnique, 0 }},
		{"unique-queues", func(c *RangeDelConfig) { c.Shape, c.Queues = rangeDelShapeUnique, 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultRangeDelConfig()
			tc.edit(&cfg)
			require.Error(t, RunRangeDel(t.TempDir(), &CommonConfig{}, &cfg))
		})
	}
}

func TestRangeDelUniqueStream(t *testing.T) {
	for _, overlapFrac := range []float64{0, 0.5, 1} {
		cfg := DefaultRangeDelConfig()
		cfg.Shape, cfg.Writers, cfg.Queues, cfg.OverlapFrac = rangeDelShapeUnique, 3, 17, overlapFrac
		for writer := range cfg.Writers {
			first := newUniqueWriter(nil, &cfg, writer, nil, nil)
			second := newUniqueWriter(nil, &cfg, writer, nil, nil)
			for _, q := range first.queues {
				require.Equal(t, writer, q%cfg.Writers)
			}
			lastEnd := make(map[string][]byte)
			for range 100 {
				start, end, overlap := first.nextRange()
				a, z, duplicate := second.nextRange()
				require.Equal(t, start, a)
				require.Equal(t, end, z)
				require.Equal(t, overlap, duplicate)
				require.Less(t, cockroachkvs.Comparer.Compare(start, end), 0)
				// The queue prefix precedes the fixed-width ack and MVCC suffix.
				prefix := string(start[:10])
				if overlap {
					require.NotNil(t, lastEnd[prefix])
					require.Equal(t, lastEnd[prefix], end)
				} else {
					if previous := lastEnd[prefix]; previous != nil {
						require.Equal(t, previous, start)
					}
					lastEnd[prefix] = end
				}
				readKey := cockroachkvs.EncodeMVCCKey(nil, []byte("r/0000"), 0, 0)
				require.Less(t, cockroachkvs.Comparer.Compare(end, readKey), 0)
			}
		}
	}
}

func TestRangeDelLatencies(t *testing.T) {
	c := newRangeDelLatencies(10*time.Millisecond, "rangedel", "point")
	c.recordElapsed(100*time.Millisecond, true)
	c.recordElapsed(5*time.Millisecond, false)
	require.EqualValues(t, 2, c.raw[0].TotalCount())
	require.EqualValues(t, 1, c.raw[1].TotalCount())
	require.EqualValues(t, 1, c.raw[2].TotalCount())
	require.EqualValues(t, 11, c.corrected[0].TotalCount())
	require.EqualValues(t, 10, c.corrected[1].TotalCount())
	require.EqualValues(t, 1, c.corrected[2].TotalCount())

	unpaced := newRangeDelLatencies(0, "iter", "get")
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				unpaced.recordElapsed(time.Microsecond, false)
			}
		})
	}
	wg.Wait()
	require.EqualValues(t, 800, unpaced.raw[0].TotalCount())
	require.EqualValues(t, 800, unpaced.raw[2].TotalCount())
	require.Nil(t, unpaced.corrected[0])
	unpaced.recordElapsed(15*time.Second, true)
	require.EqualValues(t, 1, unpaced.clipped[0])
	require.EqualValues(t, 1, unpaced.clipped[1])
	require.Zero(t, unpaced.clipped[2])
}

func TestRangeDelStop(t *testing.T) {
	stop := make(chan struct{})
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	close(stop)
	require.False(t, rangeDelWait(stop, ticker, nil))
	require.False(t, rangeDelWait(stop, nil, make(chan struct{})))

	stop = make(chan struct{})
	done := make(chan bool)
	go func() { done <- rangeDelWait(stop, nil, make(chan struct{})) }()
	close(stop)
	require.False(t, <-done)
}

func TestRangeDelOptions(t *testing.T) {
	defaults := &pebble.Options{}
	defaults.EnsureDefaults()
	var openedOptions *pebble.Options
	hookCalls := 0
	common := CommonConfig{
		CacheSize: 1 << 20,
		Logger:    testutils.Logger{T: t},
		OptionsHook: func(opts *pebble.Options) {
			hookCalls++
			opts.FS = vfs.NewMem()
			opts.MemTableSize = 8 << 20
			openedOptions = opts
		},
	}
	for _, profile := range []string{dbOptionsProdWriteHeavy, dbOptionsDefault} {
		t.Run(profile, func(t *testing.T) {
			cfg := DefaultRangeDelConfig()
			cfg.DBOptions = profile
			c := rangeDelCommonConfig(&common, &cfg)
			callsBefore := hookCalls
			d := NewPebbleDB("db", &c)
			t.Cleanup(func() { require.NoError(t, d.Close()) })
			require.Equal(t, callsBefore+1, hookCalls)
			require.Equal(t, &cockroachkvs.Comparer, openedOptions.Comparer)
			require.Equal(t, cockroachkvs.KeySchema.Name, openedOptions.KeySchema)
			if profile == dbOptionsProdWriteHeavy {
				require.Equal(t, uint64(256<<20), openedOptions.MemTableSize)
				require.Equal(t, int64(512<<20), openedOptions.LBaseMaxBytes)
				require.Equal(t, 10*time.Second, openedOptions.FlushDelayDeleteRange)
				require.Equal(t, 10*time.Second, openedOptions.FlushDelayRangeKey)
				require.Equal(t, int64(10<<20), openedOptions.MaxManifestFileSize)
				require.Equal(t, 2, openedOptions.L0CompactionConcurrency)
				require.Equal(t, pebble.FormatIngestBlobFiles, openedOptions.FormatMajorVersion)
				require.Equal(t, defaults.ValueSeparationPolicy(), openedOptions.ValueSeparationPolicy())
				require.Equal(t, 0.4, openedOptions.CompactionGarbageFractionForMaxConcurrency())
				for i := range openedOptions.Levels {
					require.Equal(t, 32<<10, openedOptions.Levels[i].BlockSize)
					require.Equal(t, 256<<10, openedOptions.Levels[i].IndexBlockSize)
					require.Equal(t, bloom.FilterPolicy(10), openedOptions.Levels[i].TableFilterPolicy())
					require.Equal(t, pebble.DBCompressionFastest.Levels[i], openedOptions.Levels[i].Compression())
				}
			} else {
				require.Equal(t, uint64(8<<20), openedOptions.MemTableSize)
				require.Zero(t, openedOptions.FlushDelayDeleteRange)
				require.Equal(t, -1.0, openedOptions.CompactionGarbageFractionForMaxConcurrency())
				require.Equal(t, 512, openedOptions.ValueSeparationPolicy().MinimumSize)
			}
		})
	}
	probe := &pebble.Options{}
	common.OptionsHook(probe)
	require.Equal(t, uint64(8<<20), probe.MemTableSize)

	for _, enabled := range []bool{false, true} {
		cfg := DefaultRangeDelConfig()
		cfg.IncrementalMemTableRangeDels = enabled
		c := rangeDelCommonConfig(&common, &cfg)
		opts := &pebble.Options{}
		c.OptionsHook(opts)
		require.Equal(t, enabled, opts.IncrementalMemTableRangeDels())
	}
	require.False(t, DefaultRangeDelConfig().IncrementalMemTableRangeDels)
}
