// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manual"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/internal/treesteps"
	"github.com/prometheus/client_golang/prometheus"
)

func memTableEntrySize(keyBytes, valueBytes int) uint64 {
	return arenaskl.MaxNodeSize(uint32(keyBytes)+8, uint32(valueBytes))
}

// memTableEmptySize is the amount of allocated space in the arena when the
// memtable is empty.
var memTableEmptySize = func() uint32 {
	var pointSkl arenaskl.Skiplist
	var rangeDelSkl arenaskl.Skiplist
	var rangeKeySkl arenaskl.Skiplist
	arena := arenaskl.NewArena(make([]byte, 16<<10 /* 16 KB */))
	pointSkl.Reset(arena, bytes.Compare)
	rangeDelSkl.Reset(arena, bytes.Compare)
	rangeKeySkl.Reset(arena, bytes.Compare)
	return arena.Size()
}()

// A memTable implements an in-memory layer of the LSM. A memTable is mutable,
// but append-only. Records are added, but never removed. Deletion is supported
// via tombstones, but it is up to higher level code (see Iterator) to support
// processing those tombstones.
//
// A memTable is implemented on top of a lock-free arena-backed skiplist. An
// arena is a fixed size contiguous chunk of memory (see
// Options.MemTableSize). A memTable's memory consumption is thus fixed at the
// time of creation (with the exception of the cached fragmented range
// tombstones). The arena-backed skiplist provides both forward and reverse
// links which makes forward and reverse iteration the same speed.
//
// A batch is "applied" to a memTable in a two step process: prepare(batch) ->
// apply(batch). memTable.prepare() is not thread-safe and must be called with
// external synchronization. Preparation reserves space in the memTable for the
// batch. Note that we pessimistically compute how much space a batch will
// consume in the memTable (see memTableEntrySize and
// Batch.memTableSize). Preparation is an O(1) operation. Applying a batch to
// the memTable can be performed concurrently with other apply
// operations. Applying a batch is an O(n logm) operation where N is the number
// of records in the batch and M is the number of records in the memtable. The
// commitPipeline serializes batch preparation, and allows batch application to
// proceed concurrently.
//
// It is safe to call get, apply, newIter, and newRangeDelIter concurrently.
type memTable struct {
	cmp         Compare
	formatKey   base.FormatKey
	equal       Equal
	arenaBuf    manual.Buf
	skl         arenaskl.Skiplist
	rangeDelSkl arenaskl.Skiplist
	rangeKeySkl arenaskl.Skiplist
	// reserved tracks the amount of space used by the memtable, both by actual
	// data stored in the memtable as well as inflight batch commit
	// operations. This value is incremented pessimistically by prepare() in
	// order to account for the space needed by a batch.
	reserved uint32
	// writerRefs tracks the write references on the memtable. The two sources of
	// writer references are the memtable being on DB.mu.mem.queue and from
	// inflight mutations that have reserved space in the memtable but not yet
	// applied. The memtable cannot be flushed to disk until the writer refs
	// drops to zero.
	writerRefs atomic.Int32
	tombstones rangeDelFragments
	rangeKeys  keySpanCache
	// The current logSeqNum at the time the memtable was created. This is
	// guaranteed to be less than or equal to any seqnum stored in the memtable.
	logSeqNum                    base.SeqNum
	releaseAccountingReservation func()
}

func (m *memTable) free() {
	if m != nil {
		m.releaseAccountingReservation()
		manual.Free(manual.MemTable, m.arenaBuf)
		m.arenaBuf = manual.Buf{}
	}
}

// memTableOptions holds configuration used when creating a memTable. All of
// the fields are optional and will be filled with defaults if not specified
// which is used by tests.
type memTableOptions struct {
	*Options
	arenaBuf                     manual.Buf
	size                         int
	logSeqNum                    base.SeqNum
	releaseAccountingReservation func()
	// rangeDelCacheStats, if non-nil, records statistics about invalidations
	// and rebuilds of the memtable's cache of fragmented range deletions.
	rangeDelCacheStats *keySpanCacheStats
}

func checkMemTable(obj interface{}) {
	m := obj.(*memTable)
	if m.arenaBuf.Data() != nil {
		fmt.Fprintf(os.Stderr, "%v: memTable buffer was not freed\n", m.arenaBuf)
		os.Exit(1)
	}
}

// newMemTable returns a new MemTable of the specified size. If size is zero,
// Options.MemTableSize is used instead.
func newMemTable(opts memTableOptions) *memTable {
	if opts.Options == nil {
		opts.Options = &Options{}
	}
	opts.Options.EnsureDefaults()
	m := new(memTable)
	m.init(opts)
	return m
}

func (m *memTable) init(opts memTableOptions) {
	if opts.size == 0 {
		opts.size = int(opts.MemTableSize)
	}
	*m = memTable{
		cmp:                          opts.Comparer.Compare,
		formatKey:                    opts.Comparer.FormatKey,
		equal:                        opts.Comparer.Equal,
		arenaBuf:                     opts.arenaBuf,
		logSeqNum:                    opts.logSeqNum,
		releaseAccountingReservation: opts.releaseAccountingReservation,
	}
	m.writerRefs.Store(1)
	m.tombstones = rangeDelFragments{
		cmp:       m.cmp,
		formatKey: m.formatKey,
		skl:       &m.rangeDelSkl,
		stats:     opts.rangeDelCacheStats,
		chunkSize: rangeDelChunkSize,
	}
	m.rangeKeys = keySpanCache{
		cmp:           m.cmp,
		formatKey:     m.formatKey,
		skl:           &m.rangeKeySkl,
		constructSpan: rangekey.Decode,
	}

	if m.arenaBuf.Data() == nil {
		m.arenaBuf = manual.New(manual.MemTable, uintptr(opts.size))
	}

	arena := arenaskl.NewArena(m.arenaBuf.Slice())
	m.skl.Reset(arena, m.cmp)
	m.rangeDelSkl.Reset(arena, m.cmp)
	m.rangeKeySkl.Reset(arena, m.cmp)
	m.reserved = arena.Size()
}

func (m *memTable) writerRef() {
	switch v := m.writerRefs.Add(1); {
	case v <= 1:
		panic(errors.AssertionFailedf("pebble: inconsistent reference count: %d", errors.Safe(v)))
	}
}

// writerUnref drops a ref on the memtable. Returns true if this was the last ref.
func (m *memTable) writerUnref() (wasLastRef bool) {
	switch v := m.writerRefs.Add(-1); {
	case v < 0:
		panic(errors.AssertionFailedf("pebble: inconsistent reference count: %d", errors.Safe(v)))
	case v == 0:
		return true
	default:
		return false
	}
}

// readyForFlush is part of the flushable interface.
func (m *memTable) readyForFlush() bool {
	return m.writerRefs.Load() == 0
}

// Prepare reserves space for the batch in the memtable and references the
// memtable preventing it from being flushed until the batch is applied. Note
// that prepare is not thread-safe, while apply is. The caller must call
// writerUnref() after the batch has been applied.
func (m *memTable) prepare(batch *Batch) error {
	avail := m.availBytes()
	if batch.memTableSize > uint64(avail) {
		return arenaskl.ErrArenaFull
	}
	m.reserved += uint32(batch.memTableSize)

	m.writerRef()
	return nil
}

func (m *memTable) apply(batch *Batch, seqNum base.SeqNum) error {
	if seqNum < m.logSeqNum {
		return base.CorruptionErrorf("pebble: batch seqnum %d is less than memtable creation seqnum %d",
			errors.Safe(seqNum), errors.Safe(m.logSeqNum))
	}

	var ins arenaskl.Inserter
	var rangeKeyCount uint32
	// rangeDels collects the batch's range deletions as they're added to
	// rangeDelSkl. They're spliced into m.tombstones before apply returns, even
	// if it returns an error, so that the fragments hold every range deletion in
	// rangeDelSkl once the applies that added them have returned.
	var rangeDelsBuf [4]rangeDelTombstone
	rangeDels := rangeDelsBuf[:0]
	defer func() {
		if len(rangeDels) > 0 {
			m.tombstones.add(rangeDels)
		}
	}()
	startSeqNum := seqNum
	for r := batch.Reader(); ; seqNum++ {
		kind, ukey, value, ok, err := r.Next()
		if !ok {
			if err != nil {
				return err
			}
			break
		}
		ikey := base.MakeInternalKey(ukey, seqNum, kind)
		switch kind {
		case InternalKeyKindRangeDelete:
			// The batch's buffer may be reused once the commit completes, so
			// the fragments' bounds must point at the copies in the arena.
			var start, end []byte
			start, end, err = m.rangeDelSkl.AddAndGetSlices(ikey, value)
			if err == nil {
				rangeDels = append(rangeDels,
					rangeDelTombstone{start: start, end: end, trailer: ikey.Trailer})
			}
		case InternalKeyKindRangeKeySet, InternalKeyKindRangeKeyUnset, InternalKeyKindRangeKeyDelete:
			err = m.rangeKeySkl.Add(ikey, value)
			rangeKeyCount++
		case InternalKeyKindLogData:
			// Don't increment seqNum for LogData, since these are not applied
			// to the memtable.
			seqNum--
		case InternalKeyKindIngestSST, InternalKeyKindIngestSSTWithBlobs, InternalKeyKindExcise:
			panic("pebble: cannot apply ingested sstable or excise kind keys to memtable")
		default:
			err = ins.Add(&m.skl, ikey, value)
		}
		if err != nil {
			return err
		}
	}
	if seqNum != startSeqNum+base.SeqNum(batch.Count()) {
		return base.CorruptionErrorf("pebble: inconsistent batch count: %d vs %d",
			errors.Safe(seqNum), errors.Safe(startSeqNum+base.SeqNum(batch.Count())))
	}
	if rangeKeyCount != 0 {
		m.rangeKeys.invalidate(rangeKeyCount)
	}
	return nil
}

// newIter is part of the flushable interface. It returns an iterator that is
// unpositioned (Iterator.Valid() will return false). The iterator can be
// positioned via a call to SeekGE, SeekLT, First or Last.
func (m *memTable) newIter(o *IterOptions) internalIterator {
	return m.skl.NewIter(o.GetLowerBound(), o.GetUpperBound())
}

// newFlushIter is part of the flushable interface.
func (m *memTable) newFlushIter(o *IterOptions) internalIterator {
	return m.skl.NewFlushIter()
}

// newRangeDelIter is part of the flushable interface.
func (m *memTable) newRangeDelIter(*IterOptions) keyspan.FragmentIterator {
	v := m.tombstones.version.Load()
	if v == nil {
		return nil
	}
	if len(v.chunks) == 1 {
		return keyspan.NewIter(m.cmp, v.chunks[0].spans)
	}
	return &rangeDelChunkIter{cmp: m.cmp, chunks: v.chunks, ci: -1}
}

// newRangeKeyIter is part of the flushable interface.
func (m *memTable) newRangeKeyIter(*IterOptions) keyspan.FragmentIterator {
	rangeKeys := m.rangeKeys.get()
	if rangeKeys == nil {
		return nil
	}
	return keyspan.NewIter(m.cmp, rangeKeys)
}

// containsRangeKeys is part of the flushable interface.
func (m *memTable) containsRangeKeys() bool {
	return m.rangeKeys.count.Load() > 0
}

func (m *memTable) availBytes() uint32 {
	a := m.skl.Arena()
	if m.writerRefs.Load() == 1 {
		// Note that one ref is maintained as long as the memtable is the
		// current mutable memtable, so when evaluating whether the current
		// mutable memtable has sufficient space for committing a batch, it is
		// guaranteed that m.writerRefs() >= 1. This means a writerRefs() of 1
		// indicates there are no other concurrent apply operations.
		//
		// If there are no other concurrent apply operations, we can update the
		// reserved bytes setting to accurately reflect how many bytes of been
		// allocated vs the over-estimation present in memTableEntrySize.
		m.reserved = a.Size()
	}
	return a.Capacity() - m.reserved
}

// inuseBytes is part of the flushable interface.
func (m *memTable) inuseBytes() uint64 {
	return uint64(m.skl.Size() - memTableEmptySize)
}

// totalBytes is part of the flushable interface.
func (m *memTable) totalBytes() uint64 {
	return uint64(m.skl.Arena().Capacity())
}

// empty returns whether the MemTable has no key/value pairs.
func (m *memTable) empty() bool {
	return m.skl.Size() == memTableEmptySize
}

// computePossibleOverlaps is part of the flushable interface.
func (m *memTable) computePossibleOverlaps(fn func(bounded) shouldContinue, bounded ...bounded) {
	computePossibleOverlapsGenericImpl[*memTable](m, m.cmp, fn, bounded)
}

// A keySpanFrags holds a set of fragmented keyspan.Spans with a particular key
// kind at a particular moment for a memtable.
//
// When a new span of a particular kind is added to the memtable, it may overlap
// with other spans of the same kind. Instead of performing the fragmentation
// whenever an iterator requires it, fragments are cached within a keySpanCache
// type. The keySpanCache uses keySpanFrags to hold the cached fragmented spans.
//
// The count of keys (and keys of any given kind) in a memtable only
// monotonically increases. The count of key spans of a particular kind is used
// as a stand-in for a 'sequence number'. A keySpanFrags represents the
// fragmented state of the memtable's keys of a given kind at the moment while
// there existed `count` keys of that kind in the memtable.
//
// It's currently only used to contain fragmented range keys. Range deletions
// are kept up to date as they're applied by rangeDelFragments instead.
type keySpanFrags struct {
	count uint32
	once  sync.Once
	// built is set once the spans have been populated. It lets get skip the
	// timing and bookkeeping that surround once.Do on the slow path.
	built atomic.Bool
	spans []keyspan.Span
}

type constructSpan func(ik base.InternalKey, v []byte, keysDst []keyspan.Key) (keyspan.Span, error)

func rangeDelConstructSpan(
	ik base.InternalKey, v []byte, keysDst []keyspan.Key,
) (keyspan.Span, error) {
	return rangedel.Decode(ik, v, keysDst), nil
}

// get retrieves the fragmented spans, populating them if necessary. Note that
// the populated span fragments may be built from more than f.count memTable
// spans, but that is ok for correctness. All we're requiring is that the
// memTable contains at least f.count keys of the configured kind. This
// situation can occur if there are multiple concurrent additions of the key
// kind and a concurrent reader. The reader can load a keySpanFrags and populate
// it even though is has been invalidated (i.e. replaced with a newer
// keySpanFrags).
//
// If bypassDisjoint is set, the spans are populated by populateBypassingDisjoint.
func (f *keySpanFrags) get(
	skl *arenaskl.Skiplist,
	cmp Compare,
	formatKey base.FormatKey,
	constructSpan constructSpan,
	bypassDisjoint bool,
	stats *keySpanCacheStats,
) []keyspan.Span {
	if f.built.Load() {
		return f.spans
	}
	var start crtime.Mono
	if stats != nil {
		start = crtime.NowMono()
	}
	ran := false
	f.once.Do(func() {
		ran = true
		stats.rebuildStarted(f.count)
		if bypassDisjoint {
			f.populateBypassingDisjoint(skl, cmp, formatKey, constructSpan)
			return
		}
		frag := &keyspan.Fragmenter{
			Cmp:    cmp,
			Format: formatKey,
			Emit: func(fragmented keyspan.Span) {
				f.spans = append(f.spans, fragmented)
			},
		}
		it := skl.NewIter(nil, nil)
		var keysDst []keyspan.Key
		for kv := it.First(); kv != nil; kv = it.Next() {
			s, err := constructSpan(kv.K, kv.InPlaceValue(), keysDst)
			if err != nil {
				panic(err)
			}
			frag.Add(s)
			keysDst = s.Keys[len(s.Keys):]
		}
		frag.Finish()
	})
	if ran {
		f.built.Store(true)
		stats.rebuildFinished(start, len(f.spans))
	} else {
		stats.readerWaited(start)
	}
	return f.spans
}

// populateBypassingDisjoint populates f.spans with the same fragments as
// passing every span in skl through one keyspan.Fragmenter, but only runs the
// Fragmenter over spans that overlap another span.
//
// Spans arrive in start key order. A cluster is a run of spans in which each
// span starts before the largest end key of the spans before it in the run. No
// fragment crosses a cluster boundary, so a cluster of one span is emitted
// as-is. This requires that the Fragmenter leave a lone span's keys in the
// order they were decoded, which holds for range deletions since each decodes
// to a single key.
func (f *keySpanFrags) populateBypassingDisjoint(
	skl *arenaskl.Skiplist, cmp Compare, formatKey base.FormatKey, constructSpan constructSpan,
) {
	emit := func(s keyspan.Span) {
		if f.spans == nil {
			f.spans = make([]keyspan.Span, 0, f.count)
		}
		f.spans = append(f.spans, s)
	}
	frag := keyspan.Fragmenter{Cmp: cmp, Format: formatKey, Emit: emit}

	// first is the first span of the current cluster. It's added to frag only
	// once a second span joins the cluster.
	var first keyspan.Span
	var clusterLen int
	var clusterEnd []byte
	emitFirst := func() {
		// Keys points into keysDst. Cap it so an append to the emitted span
		// can't overwrite the keys of the span decoded after it.
		n := len(first.Keys)
		emit(keyspan.Span{Start: first.Start, End: first.End, Keys: first.Keys[:n:n]})
	}

	// The skiplist may hold more than f.count spans if there were concurrent
	// applies, in which case decoding the extra spans allocates.
	keysDst := make([]keyspan.Key, 0, f.count)
	it := skl.NewIter(nil, nil)
	for kv := it.First(); kv != nil; kv = it.Next() {
		s, err := constructSpan(kv.K, kv.InPlaceValue(), keysDst)
		if err != nil {
			panic(err)
		}
		keysDst = s.Keys[len(s.Keys):]
		if cmp(s.Start, s.End) >= 0 {
			// The Fragmenter would drop this empty span.
			continue
		}
		if clusterLen > 0 && cmp(s.Start, clusterEnd) < 0 {
			if clusterLen == 1 {
				frag.Add(first)
			}
			frag.Add(s)
			clusterLen++
			if cmp(s.End, clusterEnd) > 0 {
				clusterEnd = s.End
			}
			continue
		}
		// s begins a new cluster, so emit the current one before anything
		// after it. Every fragment of a multi-span cluster ends at or before
		// s.Start, so truncating at s.Start flushes all of them.
		if clusterLen == 1 {
			emitFirst()
		} else if clusterLen > 1 {
			frag.Truncate(s.Start)
		}
		first, clusterLen, clusterEnd = s, 1, s.End
	}
	if clusterLen == 1 {
		emitFirst()
	}
	frag.Finish()
}

// A keySpanCache is used to cache a set of fragmented spans. The cache is
// invalidated whenever a key of the same kind is added to a memTable, and
// populated when empty when a span iterator of that key kind is created.
type keySpanCache struct {
	count         atomic.Uint32
	frags         atomic.Pointer[keySpanFrags]
	cmp           Compare
	formatKey     base.FormatKey
	constructSpan constructSpan
	skl           *arenaskl.Skiplist
	// stats, if non-nil, records invalidations and rebuilds of the cache. It is
	// owned by the DB and shared by the caches of all its memtables.
	stats *keySpanCacheStats
}

// Invalidate the current set of cached spans, indicating the number of
// spans that were added.
func (c *keySpanCache) invalidate(count uint32) {
	c.stats.invalidated()
	newCount := c.count.Add(count)
	var frags *keySpanFrags

	for {
		oldFrags := c.frags.Load()
		if oldFrags != nil && oldFrags.count >= newCount {
			// Someone else invalidated the cache before us and their invalidation
			// subsumes ours.
			break
		}
		if frags == nil {
			frags = &keySpanFrags{count: newCount}
		}
		if c.frags.CompareAndSwap(oldFrags, frags) {
			// We successfully invalidated the cache.
			break
		}
		// Someone else invalidated the cache. Loop and try again.
	}
}

func (c *keySpanCache) get() []keyspan.Span {
	frags := c.frags.Load()
	if frags == nil {
		return nil
	}
	// A RangeKeySet decodes to several keys that share a trailer, and the
	// Fragmenter sorts keys with an unstable sort, so emitting a lone range key
	// span as-is could change the order of its keys.
	return frags.get(c.skl, c.cmp, c.formatKey, c.constructSpan, false /* bypassDisjoint */, c.stats)
}

// rangeDelTombstone is a range deletion [start, end)#trailer that apply has
// added to a memtable's rangeDelSkl. start and end point into the memtable's
// arena.
type rangeDelTombstone struct {
	start, end []byte
	trailer    base.InternalKeyTrailer
}

// rangeDelFragments holds a memtable's fragmented range deletions, and keeps
// them up to date as batches are applied rather than rebuilding them on a read.
//
// The fragments are an immutable rangeDelVersion behind an atomic pointer. A
// reader loads the pointer and iterates over the version's chunks.
// memTable.apply adds a batch's range deletions to rangeDelSkl, then, holding
// mu, splices each of them into a new version, which it publishes before it
// returns. The commit pipeline publishes a batch's sequence numbers only after
// apply returns, so a reader that can see a batch loads a version that holds
// the batch's range deletions. Writers serialize on mu, so each version builds
// on the one before, even though batches may reach mu out of sequence number
// order. Readers never take mu.
type rangeDelFragments struct {
	cmp       Compare
	formatKey base.FormatKey
	// skl is the memtable's rangeDelSkl. It's only read by the invariants
	// check in add.
	skl *arenaskl.Skiplist
	// stats, if non-nil, records the splices. It is owned by the DB and shared
	// by all its memtables.
	stats *keySpanCacheStats
	// chunkSize is the number of fragments a chunk holds after a split (see
	// rangeDelVersion). It's rangeDelChunkSize, except in tests.
	chunkSize int
	// version holds the current fragments. It's nil until the first non-empty
	// range deletion is spliced in.
	version atomic.Pointer[rangeDelVersion]
	mu      struct {
		sync.Mutex
		// count is the number of range deletions passed to add, including the
		// empty ones that it dropped.
		count int
	}
}

// rangeDelChunkSize is the number of fragments that a rangeDelVersion's chunks
// hold after a split.
const rangeDelChunkSize = 128

// A rangeDelVersion is a set of fragmented range deletions: spans that are
// sorted by start key and don't overlap, each holding keys sorted by trailer
// descending. The spans are split into chunks, in order. Each chunk holds 1 to
// 2*chunkSize spans, so that a splice copies the chunk index and the chunks
// that the range deletion overlaps rather than every span. A version with at
// most 2*chunkSize spans has a single chunk.
//
// Nothing in a published version is ever modified, since readers may hold
// pointers to its spans (see keyspan.Iter). A later version shares the chunks
// that its splices didn't touch, and the Keys slices of the spans it copies
// unchanged. Neither the chunk index, nor any chunk's spans, nor any span's
// Keys has spare capacity, so an append by a reader can't write into memory
// that another version shares.
type rangeDelVersion struct {
	chunks []*rangeDelChunk
	// n is the number of spans in all chunks.
	n int
}

// A rangeDelChunk holds a run of a rangeDelVersion's spans.
type rangeDelChunk struct {
	spans []keyspan.Span
}

// add splices a batch's range deletions, which the caller has added to skl,
// into the fragments, and publishes the result.
func (f *rangeDelFragments) add(tombstones []rangeDelTombstone) {
	f.stats.invalidated()
	f.mu.Lock()
	var start crtime.Mono
	if f.stats != nil {
		start = crtime.NowMono()
	}
	var chunks []*rangeDelChunk
	var n int
	if v := f.version.Load(); v != nil {
		chunks, n = v.chunks, v.n
	}
	// copied is set once chunks is a copy of the published chunk index, which
	// later splices in the batch may then modify.
	copied := false
	for i := range tombstones {
		t := tombstones[i]
		if f.cmp(t.start, t.end) >= 0 {
			// A keyspan.Fragmenter drops an empty or inverted span too.
			continue
		}
		lo, hi, repl, added, touched := spliceRangeDelChunks(f.cmp, f.chunkSize, chunks, t)
		if copied {
			chunks = slices.Replace(chunks, lo, hi, repl...)
		} else {
			chunks = slices.Concat(chunks[:lo], repl, chunks[hi:])
			copied = true
		}
		n += added
		f.stats.tombstoneSpliced(touched)
	}
	if copied {
		f.version.Store(&rangeDelVersion{chunks: slices.Clip(chunks), n: n})
	}
	f.mu.count += len(tombstones)
	var elapsed time.Duration
	if f.stats != nil {
		elapsed = start.Elapsed()
	}
	if invariants.Enabled {
		v := f.version.Load()
		if err := checkRangeDelVersion(f.cmp, v, f.chunkSize); err != nil {
			panic(err)
		}
		checkRangeDelFragments(f.skl, f.cmp, f.formatKey, v, f.mu.count)
	}
	f.mu.Unlock()
	f.stats.batchSpliced(elapsed, n)
}

// spliceRangeDelChunks splices the non-empty range deletion t into the chunks
// of a rangeDelVersion, which it doesn't modify. It returns the chunks that
// replace chunks[lo:hi], the number of spans that the splice added, and the
// number of spans it wrote, as spliceRangeDel counts them.
//
// Only the chunks in [lo, hi) overlap t: every chunk before lo ends at or
// before t.start, and every chunk from hi on starts at or after t.end. So
// splicing t into the spans of those chunks alone gives the same spans as
// splicing it into every span of the version. If t overlaps no chunk, [lo, hi)
// is the chunk that follows t, or the last chunk if t follows every chunk.
func spliceRangeDelChunks(
	cmp Compare, chunkSize int, chunks []*rangeDelChunk, t rangeDelTombstone,
) (lo, hi int, repl []*rangeDelChunk, added, touched int) {
	if len(chunks) == 0 {
		var spans []keyspan.Span
		spans, touched = spliceRangeDel(cmp, nil, t)
		return 0, 0, []*rangeDelChunk{{spans: spans}}, len(spans), touched
	}
	// lo is the first chunk whose last span ends after t.start.
	lo, hi = 0, len(chunks)
	for lo < hi {
		h := int(uint(lo+hi) >> 1)
		spans := chunks[h].spans
		if cmp(spans[len(spans)-1].End, t.start) <= 0 {
			lo = h + 1
		} else {
			hi = h
		}
	}
	// hi is the first chunk from lo on whose first span starts at or after
	// t.end. The chunk before lo ends at or before t.start, so it starts before
	// t.end, and hi can't be less than lo.
	hi = len(chunks)
	for j := lo; j < hi; {
		h := int(uint(j+hi) >> 1)
		if cmp(chunks[h].spans[0].Start, t.end) < 0 {
			j = h + 1
		} else {
			hi = h
		}
	}
	if lo == hi {
		// t lies in a gap between chunks, or before or after all of them.
		if lo == len(chunks) {
			lo--
		}
		hi = lo + 1
	}

	var in []keyspan.Span
	if hi-lo == 1 {
		in = chunks[lo].spans
	} else {
		var numSpans int
		for _, c := range chunks[lo:hi] {
			numSpans += len(c.spans)
		}
		in = make([]keyspan.Span, 0, numSpans)
		for _, c := range chunks[lo:hi] {
			in = append(in, c.spans...)
		}
	}
	out, touched := spliceRangeDel(cmp, in, t)
	return lo, hi, chunkRangeDelSpans(out, chunkSize), len(out) - len(in), touched
}

// chunkRangeDelSpans returns spans as chunks for a rangeDelVersion. Up to
// 2*chunkSize spans make a single chunk that holds spans itself, with its
// capacity clipped. More are split into pieces of chunkSize to 2*chunkSize-1
// spans, and each piece is copied into an array of its own, so that no chunk
// keeps a larger array alive or shares one with another chunk.
func chunkRangeDelSpans(spans []keyspan.Span, chunkSize int) []*rangeDelChunk {
	if len(spans) <= 2*chunkSize {
		return []*rangeDelChunk{{spans: slices.Clip(spans)}}
	}
	chunks := make([]*rangeDelChunk, len(spans)/chunkSize)
	for p := range chunks {
		piece := spans[len(spans)*p/len(chunks) : len(spans)*(p+1)/len(chunks)]
		c := &rangeDelChunk{spans: make([]keyspan.Span, len(piece))}
		copy(c.spans, piece)
		chunks[p] = c
	}
	return chunks
}

// spliceRangeDel returns the fragments of spans and the range deletion t
// together, and the number of the result's fragments that it wrote rather than
// copied from spans. spans must be fragments as a rangeDelVersion holds them.
// spans isn't modified. The result shares the Keys of every fragment of spans
// that t doesn't cover (including the parts of a fragment that t splits), and
// every Keys slice it allocates, as well as the result, has no spare capacity.
// If t is empty or inverted, spliceRangeDel returns spans and 0, as a
// keyspan.Fragmenter drops such a span.
func spliceRangeDel(
	cmp Compare, spans []keyspan.Span, t rangeDelTombstone,
) (_ []keyspan.Span, touched int) {
	if cmp(t.start, t.end) >= 0 {
		return spans, 0
	}
	// Find i, the first fragment that ends after t.start, and j, the first
	// fragment that starts at or after t.end. Fragments [i, j) overlap t, and
	// the others are copied as they are.
	i, hi := 0, len(spans)
	for i < hi {
		h := int(uint(i+hi) >> 1)
		if cmp(spans[h].End, t.start) <= 0 {
			i = h + 1
		} else {
			hi = h
		}
	}
	j := i
	hi = len(spans)
	for j < hi {
		h := int(uint(j+hi) >> 1)
		if cmp(spans[h].Start, t.end) < 0 {
			j = h + 1
		} else {
			hi = h
		}
	}
	overlap := spans[i:j]

	// Each overlapping fragment is replaced by at most itself, a gap before it,
	// and one piece of it before or after t, and a gap may follow the last one.
	// The keys of each covered part and gap get one new key.
	numKeys := 2*len(overlap) + 1
	for k := range overlap {
		numKeys += len(overlap[k].Keys)
	}
	keys := make([]keyspan.Key, numKeys)
	out := make([]keyspan.Span, i, len(spans)+len(overlap)+2)
	copy(out, spans[:i])

	newKey := keyspan.Key{Trailer: t.trailer}
	// pos is the end of the part of t already written.
	pos := t.start
	for k := range overlap {
		s := &overlap[k]
		coveredStart := s.Start
		if c := cmp(s.Start, pos); c < 0 {
			// Only the first overlapping fragment can start before t. Its part
			// before t keeps its keys.
			out = append(out, keyspan.Span{Start: s.Start, End: t.start, Keys: s.Keys})
			coveredStart = t.start
		} else if c > 0 {
			// t alone covers the gap before s.
			keys[0] = newKey
			out = append(out, keyspan.Span{Start: pos, End: s.Start, Keys: keys[:1:1]})
			keys = keys[1:]
		}
		coveredEnd := s.End
		endsAfter := cmp(s.End, t.end) > 0
		if endsAfter {
			coveredEnd = t.end
		}
		n := len(s.Keys) + 1
		coveredKeys := keys[:n:n]
		keys = keys[n:]
		insertRangeDelKey(coveredKeys, s.Keys, newKey)
		out = append(out, keyspan.Span{Start: coveredStart, End: coveredEnd, Keys: coveredKeys})
		if endsAfter {
			// Only the last overlapping fragment can end after t. Its part
			// after t keeps its keys.
			out = append(out, keyspan.Span{Start: t.end, End: s.End, Keys: s.Keys})
		}
		pos = s.End
	}
	if cmp(pos, t.end) < 0 {
		keys[0] = newKey
		out = append(out, keyspan.Span{Start: pos, End: t.end, Keys: keys[:1:1]})
	}
	touched = len(out) - i
	out = append(out, spans[j:]...)
	return out[:len(out):len(out)], touched
}

// insertRangeDelKey copies keys, which are sorted by trailer descending, into
// dst with k inserted in trailer order. len(dst) must be len(keys)+1.
func insertRangeDelKey(dst, keys []keyspan.Key, k keyspan.Key) {
	// Batches usually reach rangeDelFragments.add in sequence number order, so
	// k usually goes first.
	p := 0
	for p < len(keys) && keys[p].Trailer > k.Trailer {
		p++
	}
	copy(dst, keys[:p])
	dst[p] = k
	copy(dst[p+1:], keys[p:])
}

// spans returns the version's spans in a single slice, or nil if v is nil.
func (v *rangeDelVersion) spans() []keyspan.Span {
	if v == nil {
		return nil
	}
	spans := make([]keyspan.Span, 0, v.n)
	for _, c := range v.chunks {
		spans = append(spans, c.spans...)
	}
	return spans
}

// checkRangeDelVersion returns an error if v isn't structured as a
// rangeDelVersion must be for the given chunk size: its chunks must hold 1 to
// 2*chunkSize spans, the last span of each must end at or before the first
// span of the next starts, n must count the spans, and neither the chunk index
// nor any chunk's spans may have spare capacity. A nil version is valid.
func checkRangeDelVersion(cmp Compare, v *rangeDelVersion, chunkSize int) error {
	if v == nil {
		return nil
	}
	if len(v.chunks) == 0 || cap(v.chunks) != len(v.chunks) {
		return errors.AssertionFailedf("pebble: range deletion version has %d chunks and capacity %d",
			errors.Safe(len(v.chunks)), errors.Safe(cap(v.chunks)))
	}
	n := 0
	for i, c := range v.chunks {
		if len(c.spans) == 0 || len(c.spans) > 2*chunkSize || cap(c.spans) != len(c.spans) {
			return errors.AssertionFailedf(
				"pebble: range deletion chunk %d has %d spans and capacity %d (chunk size %d)",
				errors.Safe(i), errors.Safe(len(c.spans)), errors.Safe(cap(c.spans)), errors.Safe(chunkSize))
		}
		if i > 0 {
			prev := v.chunks[i-1].spans
			if cmp(prev[len(prev)-1].End, c.spans[0].Start) > 0 {
				return errors.AssertionFailedf("pebble: range deletion chunk %d starts at %s, before %s",
					errors.Safe(i), c.spans[0].Start, prev[len(prev)-1].End)
			}
		}
		n += len(c.spans)
	}
	if n != v.n {
		return errors.AssertionFailedf("pebble: range deletion version counts %d spans but holds %d",
			errors.Safe(v.n), errors.Safe(n))
	}
	return nil
}

// maxCheckedRangeDelFragments is the largest number of fragments a memtable
// can hold for invariants builds to check them against a full rebuild after
// every splice.
const maxCheckedRangeDelFragments = 4096

// checkRangeDelFragments panics if the spans of v differ from the fragments of
// the range deletions in skl. n is the number of range deletions spliced into
// v. The check is skipped if v holds more than maxCheckedRangeDelFragments
// spans, or if skl doesn't hold exactly n range deletions, as happens while
// other applies have added range deletions to skl that they haven't spliced
// yet. Every range deletion spliced into v was added to skl before this is
// called, so a skl iteration that finds exactly n of them found those.
func checkRangeDelFragments(
	skl *arenaskl.Skiplist, cmp Compare, formatKey base.FormatKey, v *rangeDelVersion, n int,
) {
	if v != nil && v.n > maxCheckedRangeDelFragments {
		return
	}
	var want []keyspan.Span
	frag := keyspan.Fragmenter{
		Cmp:    cmp,
		Format: formatKey,
		Emit:   func(s keyspan.Span) { want = append(want, s) },
	}
	found := 0
	it := skl.NewIter(nil, nil)
	for kv := it.First(); kv != nil && found <= n; kv = it.Next() {
		found++
		frag.Add(rangedel.Decode(kv.K, kv.InPlaceValue(), nil))
	}
	_ = it.Close()
	if found != n {
		return
	}
	frag.Finish()
	if spans := v.spans(); !rangeDelSpansEqual(cmp, want, spans) {
		panic(errors.AssertionFailedf(
			"pebble: memtable range deletion fragments differ from a rebuild\nwant: %s\ngot:  %s",
			want, spans))
	}
}

// rangeDelSpansEqual returns whether a and b hold the same fragments: bounds
// that compare equal, and identical KeysOrder and Keys.
func rangeDelSpansEqual(cmp Compare, a, b []keyspan.Span) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if cmp(a[i].Start, b[i].Start) != 0 || cmp(a[i].End, b[i].End) != 0 ||
			a[i].KeysOrder != b[i].KeysOrder || len(a[i].Keys) != len(b[i].Keys) {
			return false
		}
		for k := range a[i].Keys {
			ka, kb := &a[i].Keys[k], &b[i].Keys[k]
			if ka.Trailer != kb.Trailer || !bytes.Equal(ka.Suffix, kb.Suffix) ||
				!bytes.Equal(ka.Value, kb.Value) {
				return false
			}
		}
	}
	return true
}

// rangeDelChunkIter is a keyspan.FragmentIterator over the spans of a
// rangeDelVersion with more than one chunk. It behaves as a keyspan.Iter over
// the version's spans in a single slice would, and the spans it returns point
// into the chunks, so they stay valid for as long as the version is reachable.
type rangeDelChunkIter struct {
	cmp    Compare
	chunks []*rangeDelChunk
	// ci is the current chunk, and i the current span within it. ci is -1
	// before the first span and len(chunks) after the last, where i is unused.
	ci, i int
}

var _ keyspan.FragmentIterator = (*rangeDelChunkIter)(nil)

// SeekGE implements keyspan.FragmentIterator.
func (it *rangeDelChunkIter) SeekGE(key []byte) (*keyspan.Span, error) {
	// The span sought, the first that ends after key, is in the first chunk
	// whose last span ends after key.
	ci, hi := 0, len(it.chunks)
	for ci < hi {
		h := int(uint(ci+hi) >> 1)
		spans := it.chunks[h].spans
		if it.cmp(key, spans[len(spans)-1].End) >= 0 {
			ci = h + 1
		} else {
			hi = h
		}
	}
	it.ci = ci
	if ci == len(it.chunks) {
		return nil, nil
	}
	spans := it.chunks[ci].spans
	i, hi := 0, len(spans)-1
	for i < hi {
		h := int(uint(i+hi) >> 1)
		if it.cmp(key, spans[h].End) >= 0 {
			i = h + 1
		} else {
			hi = h
		}
	}
	it.i = i
	return &spans[i], nil
}

// SeekLT implements keyspan.FragmentIterator.
func (it *rangeDelChunkIter) SeekLT(key []byte) (*keyspan.Span, error) {
	// The span sought, the last that starts before key, is in the last chunk
	// whose first span starts before key.
	ci, hi := 0, len(it.chunks)
	for ci < hi {
		h := int(uint(ci+hi) >> 1)
		if it.cmp(key, it.chunks[h].spans[0].Start) > 0 {
			ci = h + 1
		} else {
			hi = h
		}
	}
	ci--
	it.ci = ci
	if ci < 0 {
		return nil, nil
	}
	spans := it.chunks[ci].spans
	i, hi := 1, len(spans)
	for i < hi {
		h := int(uint(i+hi) >> 1)
		if it.cmp(key, spans[h].Start) > 0 {
			i = h + 1
		} else {
			hi = h
		}
	}
	it.i = i - 1
	return &spans[it.i], nil
}

// First implements keyspan.FragmentIterator.
func (it *rangeDelChunkIter) First() (*keyspan.Span, error) {
	it.ci, it.i = 0, 0
	return &it.chunks[0].spans[0], nil
}

// Last implements keyspan.FragmentIterator.
func (it *rangeDelChunkIter) Last() (*keyspan.Span, error) {
	it.ci = len(it.chunks) - 1
	it.i = len(it.chunks[it.ci].spans) - 1
	return &it.chunks[it.ci].spans[it.i], nil
}

// Next implements keyspan.FragmentIterator.
func (it *rangeDelChunkIter) Next() (*keyspan.Span, error) {
	switch {
	case it.ci >= len(it.chunks):
		return nil, nil
	case it.ci < 0:
		it.ci, it.i = 0, 0
	case it.i+1 < len(it.chunks[it.ci].spans):
		it.i++
	default:
		it.ci, it.i = it.ci+1, 0
		if it.ci == len(it.chunks) {
			return nil, nil
		}
	}
	return &it.chunks[it.ci].spans[it.i], nil
}

// Prev implements keyspan.FragmentIterator.
func (it *rangeDelChunkIter) Prev() (*keyspan.Span, error) {
	switch {
	case it.ci < 0:
		return nil, nil
	case it.ci >= len(it.chunks):
		return it.Last()
	case it.i > 0:
		it.i--
	default:
		it.ci--
		if it.ci < 0 {
			return nil, nil
		}
		it.i = len(it.chunks[it.ci].spans) - 1
	}
	return &it.chunks[it.ci].spans[it.i], nil
}

// SetContext implements keyspan.FragmentIterator.
func (it *rangeDelChunkIter) SetContext(ctx context.Context) {}

// Close implements keyspan.FragmentIterator.
func (it *rangeDelChunkIter) Close() {}

// WrapChildren implements keyspan.FragmentIterator.
func (it *rangeDelChunkIter) WrapChildren(wrap keyspan.WrapFn) {}

// TreeStepsNode implements keyspan.FragmentIterator.
func (it *rangeDelChunkIter) TreeStepsNode() treesteps.NodeInfo {
	return treesteps.NodeInfof(it, "%T(%p)", it, it)
}

func (it *rangeDelChunkIter) String() string {
	return "rangeDelChunkIter"
}

// keySpanCacheStats records how often keySpanCaches are invalidated and
// rebuilt, how long readers wait on a rebuild, and what splicing range
// deletions into rangeDelFragments costs. A single instance is owned by the DB
// and shared by the range deletion fragments of all its memtables. Those
// splice rather than rebuild, and the range key caches have no stats, so a DB
// records no rebuilds; only a keySpanFrags.get that is passed the stats does.
// All methods are safe to call on a nil receiver, in which case they do
// nothing.
type keySpanCacheStats struct {
	// The duration histograms observe float64(time.Duration), i.e. nanoseconds.
	rebuildDuration        prometheus.Histogram
	readerWait             prometheus.Histogram
	rebuildTombstones      prometheus.Histogram
	rebuildFragments       prometheus.Histogram
	concurrentRebuilds     prometheus.Histogram
	spliceDuration         prometheus.Histogram
	spliceFragmentsTouched prometheus.Histogram
	spliceVersionFragments prometheus.Histogram

	invalidations atomic.Uint64
	// rebuildsInFlight is the number of rebuilds currently running.
	rebuildsInFlight atomic.Int64
}

func newKeySpanCacheStats() *keySpanCacheStats {
	return &keySpanCacheStats{
		rebuildDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Buckets: rangeDelCacheDurationBuckets,
		}),
		readerWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Buckets: rangeDelCacheDurationBuckets,
		}),
		rebuildTombstones: prometheus.NewHistogram(prometheus.HistogramOpts{
			Buckets: rangeDelCacheCountBuckets,
		}),
		rebuildFragments: prometheus.NewHistogram(prometheus.HistogramOpts{
			Buckets: rangeDelCacheCountBuckets,
		}),
		concurrentRebuilds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Buckets: rangeDelCacheConcurrencyBuckets,
		}),
		spliceDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Buckets: rangeDelCacheDurationBuckets,
		}),
		spliceFragmentsTouched: prometheus.NewHistogram(prometheus.HistogramOpts{
			Buckets: rangeDelCacheCountBuckets,
		}),
		spliceVersionFragments: prometheus.NewHistogram(prometheus.HistogramOpts{
			Buckets: rangeDelCacheCountBuckets,
		}),
	}
}

// tombstoneSpliced records that splicing a range deletion into
// rangeDelFragments wrote the given number of fragments.
func (s *keySpanCacheStats) tombstoneSpliced(fragmentsTouched int) {
	if s == nil {
		return
	}
	s.spliceFragmentsTouched.Observe(float64(fragmentsTouched))
}

// batchSpliced records that splicing a batch's range deletions into
// rangeDelFragments took d, not counting the wait for the lock, and left
// numFragments fragments.
func (s *keySpanCacheStats) batchSpliced(d time.Duration, numFragments int) {
	if s == nil {
		return
	}
	s.spliceDuration.Observe(float64(d))
	s.spliceVersionFragments.Observe(float64(numFragments))
}

// invalidated records a call to keySpanCache.invalidate, or a batch of range
// deletions passed to rangeDelFragments.add.
func (s *keySpanCacheStats) invalidated() {
	if s == nil {
		return
	}
	s.invalidations.Add(1)
}

// rebuildStarted records the start of a rebuild of a keySpanFrags whose
// memtable held the given number of tombstones when the frags were created.
func (s *keySpanCacheStats) rebuildStarted(tombstones uint32) {
	if s == nil {
		return
	}
	s.concurrentRebuilds.Observe(float64(s.rebuildsInFlight.Add(1)))
	s.rebuildTombstones.Observe(float64(tombstones))
}

// rebuildFinished records the end of a rebuild that began at start and produced
// numFragments fragments.
func (s *keySpanCacheStats) rebuildFinished(start crtime.Mono, numFragments int) {
	if s == nil {
		return
	}
	s.rebuildsInFlight.Add(-1)
	s.rebuildDuration.Observe(float64(start.Elapsed()))
	s.rebuildFragments.Observe(float64(numFragments))
}

// readerWaited records that a reader spent the time since start inside
// sync.Once.Do without running the rebuild itself.
func (s *keySpanCacheStats) readerWaited(start crtime.Mono) {
	if s == nil {
		return
	}
	s.readerWait.Observe(float64(start.Elapsed()))
}

// metrics returns the exported view of the stats. The returned histograms are
// the live ones, not copies.
func (s *keySpanCacheStats) metrics() MemTableRangeDelCacheMetrics {
	if s == nil {
		return MemTableRangeDelCacheMetrics{}
	}
	return MemTableRangeDelCacheMetrics{
		RebuildDuration:        s.rebuildDuration,
		ReaderWait:             s.readerWait,
		RebuildTombstones:      s.rebuildTombstones,
		RebuildFragments:       s.rebuildFragments,
		ConcurrentRebuilds:     s.concurrentRebuilds,
		SpliceDuration:         s.spliceDuration,
		SpliceFragmentsTouched: s.spliceFragmentsTouched,
		SpliceVersionFragments: s.spliceVersionFragments,
		Invalidations:          s.invalidations.Load(),
	}
}
