// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
)

// rangeDelGetAfterBatchOptions returns the options of the memtables that
// BenchmarkMemTableRangeDelGetAfterBatch reads: they splice range deletions
// into their fragments as batches are applied.
func rangeDelGetAfterBatchOptions() *Options { return incrementalRangeDelOptions(true) }

// BenchmarkMemTableRangeDelGetAfterBatch measures applying a batch that holds
// one range deletion to a memtable, and then seeking the memtable's range
// deletions to a key that none of them covers, as a Get right after the batch
// does. The memtable starts with frags disjoint fragments and is replaced, off
// the clock, once window batches have grown it, so it always holds at most
// 256 fragments, which is two chunks of the default size.
func BenchmarkMemTableRangeDelGetAfterBatch(b *testing.B) {
	for _, c := range []struct{ frags, window int }{{16, 240}, {128, 128}} {
		b.Run(fmt.Sprintf("frags=%d", c.frags), func(b *testing.B) {
			delKey := func(i int, suffix string) []byte {
				return fmt.Appendf(nil, "d/%08d/%s", i, suffix)
			}
			readKey := []byte("r/key")
			newBatchAt := func(i int) *Batch {
				batch := newBatch(nil)
				if err := batch.DeleteRange(delKey(i, "a"), delKey(i, "z"), nil); err != nil {
					b.Fatal(err)
				}
				return batch
			}
			var m *memTable
			var batches []*Batch
			reset := func() {
				if m != nil {
					m.free()
				}
				for _, batch := range batches {
					_ = batch.Close()
				}
				m = newMemTable(memTableOptions{
					Options:                      rangeDelGetAfterBatchOptions(),
					size:                         4 << 20,
					releaseAccountingReservation: func() {},
				})
				for i := 0; i < c.frags; i++ {
					batch := newBatchAt(i)
					if err := m.apply(batch, base.SeqNum(i+1)); err != nil {
						b.Fatal(err)
					}
					_ = batch.Close()
				}
				batches = batches[:0]
				for i := 0; i < c.window; i++ {
					batches = append(batches, newBatchAt(c.frags+i))
				}
			}
			reset()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w := i % c.window
				if w == 0 && i > 0 {
					b.StopTimer()
					reset()
					b.StartTimer()
				}
				if err := m.apply(batches[w], base.SeqNum(c.frags+w+1)); err != nil {
					b.Fatal(err)
				}
				it := m.newRangeDelIter(nil)
				if s, err := it.SeekGE(readKey); err != nil || s != nil {
					b.Fatalf("SeekGE(%s) = %v, %v", readKey, s, err)
				}
				it.Close()
			}
			b.StopTimer()
			m.free()
			for _, batch := range batches {
				_ = batch.Close()
			}
		})
	}
}
