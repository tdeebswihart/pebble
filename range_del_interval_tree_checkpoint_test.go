// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/metrics"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/stretchr/testify/require"
)

// rangeDelIndexResult is one checkpoint measurement. The structural
// counters are only collected when Invariants is true; the timing and heap
// fields are only representative when it is false.
type rangeDelIndexResult struct {
	Layout                                                               string
	Invariants                                                           bool
	Tombstones, Batch                                                    int
	ArenaCapacity, ArenaUsed                                             uint32
	IntervalSize, RegistrySize, StateSize, IndexSize, OpSize             uintptr
	Comparisons, Visits, Allocations, Pushes, Rotations                  uint64
	PushCopies                                                           uint64
	EpochExhausted                                                       bool
	Reused, PushReused                                                   uint64
	MaxBatchAllocationRatio                                              float64
	RegistryVisits, RegistryAllocations, RegistryRotations, Publications uint64
	Height                                                               uint8
	Intervals, Covered                                                   uint64
	ReachableIntervals, ReachableVersions, ReachablePayload              uint64
	HeapAllocated, HeapRetained, ObservedPeakHeap                        uint64
	ScratchPeak                                                          uint64
	BuildNS, GCMarkCPUSeconds                                            float64
}

func rangeDelGCMarkCPU() float64 {
	samples := []metrics.Sample{{Name: "/cpu/classes/gc/mark/assist:cpu-seconds"}, {Name: "/cpu/classes/gc/mark/dedicated:cpu-seconds"}, {Name: "/cpu/classes/gc/mark/idle:cpu-seconds"}}
	metrics.Read(samples)
	var total float64
	for _, s := range samples {
		if s.Value.Kind() != metrics.KindFloat64 {
			panic("missing GC mark CPU metric: " + s.Name)
		}
		total += s.Value.Float64()
	}
	return total
}

func rangeDelReachable(i *rangeDelIntervalIndex) (intervals, versions, payload uint64) {
	seenIntervals := make(map[*rangeDelIntervalNode]bool)
	seenVersions := make(map[*rangeDelVersionNode]bool)
	var walkIntervals func(*rangeDelIntervalNode)
	walkIntervals = func(n *rangeDelIntervalNode) {
		if n == nil || seenIntervals[n] {
			return
		}
		seenIntervals[n] = true
		walkIntervals(n.left)
		walkIntervals(n.right)
	}
	var walkVersions func(*rangeDelVersionNode)
	walkVersions = func(n *rangeDelVersionNode) {
		if n == nil || seenVersions[n] {
			return
		}
		seenVersions[n] = true
		walkIntervals(n.root)
		walkVersions(n.left)
		walkVersions(n.right)
	}
	s := i.state.Load()
	walkVersions(s.versions)
	walkVersions(s.latest)
	intervals, versions = uint64(len(seenIntervals)), uint64(len(seenVersions))
	payload = intervals*uint64(unsafe.Sizeof(rangeDelIntervalNode{})) + versions*uint64(unsafe.Sizeof(rangeDelVersionNode{})) + uint64(unsafe.Sizeof(*i)) + uint64(unsafe.Sizeof(*s))
	return
}

// This dedicated pass measures GC and retention separately from benchmark
// timings. The fixture/arena remains live across the baseline and final GC.
func measureRangeDelIndex(t *testing.T, layout string, n, batch int) rangeDelIndexResult {
	f := makeRangeDelIndexFixture(layout, n)
	a, ops := rangeDelIndexInput(t, f)
	runtime.GC()
	var before, after, retained runtime.MemStats
	runtime.ReadMemStats(&before)
	markBefore := rangeDelGCMarkCPU()
	peak := before.HeapAlloc
	start := time.Now()
	lastSample := 0
	var previousIntervals, previousAllocations uint64
	var maxBatchAllocationRatio float64
	i := newRangeDelIntervalIndex(a, f.comparer.Compare)
	for j := 0; j < n; j += batch {
		end := min(j+batch, n)
		require.NoError(t, i.publish(base.SeqNum(j+1), base.SeqNum(end+1), ops[j:end]))
		r := float64(end - j)
		bound := r * (1 + math.Log2(1+float64(previousIntervals)/r))
		if invariants.Enabled {
			allocated := i.counters.allocations - previousAllocations
			maxBatchAllocationRatio = max(maxBatchAllocationRatio, float64(allocated)/bound)
			previousAllocations = i.counters.allocations
		}
		if root := i.state.Load().latest.root; root != nil {
			previousIntervals = root.fragmentCount
		}
		if end-lastSample >= 1024 {
			var sample runtime.MemStats
			runtime.ReadMemStats(&sample)
			peak = max(peak, sample.HeapAlloc)
			lastSample = end
		}
	}
	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)
	peak = max(peak, after.HeapAlloc)
	runtime.GC()
	runtime.ReadMemStats(&retained)
	markAfter := rangeDelGCMarkCPU()
	s := i.state.Load()
	var c rangeDelTreeCounters
	if invariants.Enabled {
		c = *i.counters
	}
	root := s.latest.root
	ni, nv, payload := rangeDelReachable(i)
	var scratchPeak uint64
	for j := 0; j < n; j += batch {
		scratch := append([]rangeDelIndexOp(nil), ops[j:min(j+batch, n)]...)
		scratchPeak = max(scratchPeak, uint64(cap(scratch))*uint64(unsafe.Sizeof(rangeDelIndexOp{})))
	}
	result := rangeDelIndexResult{
		Layout: layout, Invariants: invariants.Enabled, Tombstones: n, Batch: batch, ArenaCapacity: a.Capacity(), ArenaUsed: a.Size(),
		IntervalSize: unsafe.Sizeof(rangeDelIntervalNode{}), RegistrySize: unsafe.Sizeof(rangeDelVersionNode{}), StateSize: unsafe.Sizeof(*s), IndexSize: unsafe.Sizeof(*i), OpSize: unsafe.Sizeof(rangeDelIndexOp{}),
		Comparisons: c.comparisons, Visits: c.visits, Allocations: c.allocations, Pushes: c.pushes, Rotations: c.rotations,
		PushCopies: c.pushCopies, EpochExhausted: i.epochExhausted,
		Reused: c.reused, PushReused: c.pushReused,
		MaxBatchAllocationRatio: maxBatchAllocationRatio,
		RegistryVisits:          c.registryVisits, RegistryAllocations: c.registryAllocations, RegistryRotations: c.registryRotations, Publications: c.publications,
		Height: root.height, Intervals: root.fragmentCount, Covered: root.coveredCount,
		ReachableIntervals: ni, ReachableVersions: nv, ReachablePayload: payload,
		HeapAllocated: after.TotalAlloc - before.TotalAlloc, ObservedPeakHeap: peak - before.HeapAlloc,
		ScratchPeak: scratchPeak, BuildNS: float64(elapsed.Nanoseconds()), GCMarkCPUSeconds: markAfter - markBefore,
	}
	if retained.HeapAlloc >= before.HeapAlloc {
		result.HeapRetained = retained.HeapAlloc - before.HeapAlloc
	}
	runtime.KeepAlive(i)
	runtime.KeepAlive(f)
	runtime.KeepAlive(ops)
	return result
}

func TestRangeDelIndexCheckpoint(t *testing.T) {
	if os.Getenv("PEBBLE_RANGEDEL_CHECKPOINT") == "" {
		t.Skip("run scripts/rangedel-prototype.sh")
	}
	for _, layout := range rangeDelIndexLayouts {
		for _, n := range []int{100, 1000, 10_000, 100_000} {
			for _, batch := range rangeDelIndexBatchSizes(n) {
				result := measureRangeDelIndex(t, layout, n, batch)
				data, err := json.Marshal(result)
				require.NoError(t, err)
				fmt.Printf("prototype %s\n", data)
			}
		}
	}
}
