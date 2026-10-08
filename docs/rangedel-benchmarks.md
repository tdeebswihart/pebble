# Range Deletion Baseline Benchmarks

These tools implement stage 1 of the interval tree proposal. They measure
upstream behavior and provide inputs for later index work. No candidate index,
budget checkpoint, or performance acceptance result is included.

The executable baseline is Git commit
`ee9323c5e2f74518b0432e071e26971c23940ff0`. Its upstream base is
`e0818dd07ec580eb229e8291b4eefdb135cd5a55`. The cycle/unique workload comes from
[PR #6207](https://github.com/cockroachdb/pebble/pull/6207), at source commit
`89efad0e3938eed69207f2ae3249503648aaf73f`; this port excludes that PR's
production cache changes and metrics.

## Build Once per Revision

Use [scripts/rangedel-baselines.sh](../scripts/rangedel-baselines.sh) to build,
smoke-test, and compare revisions. Run `scripts/rangedel-baselines.sh --help`
for arguments and environment settings. The script creates JJ workspaces,
builds release binaries once per revision, and saves revision/toolchain/host
metadata. Use a fresh output root when building another pair of revisions.

For baseline only, run
`scripts/rangedel-baselines.sh build /tmp/pebble-rangedel-baselines`.
For a comparison, supply both SHAs:
`scripts/rangedel-baselines.sh build /tmp/pebble-rangedel-baselines ee9323c5e2f74518b0432e071e26971c23940ff0 <candidate-sha>`.
Keep the same benchmark code, toolchain, and settings on both sides.
An option-off candidate and option-on candidate must share an operation stream
and differ only in the intended option/configuration.
The incremental-index option and its benchmark flag binding belong to later
stages; they do not exist in this baseline.

Performance binaries omit `invariants`. Correctness tests use it. The script
records macOS version/CPU details or Linux `lscpu` output when available.
Record GOMAXPROCS, worker counts, cache/WAL settings, value and
fixture sizes, warmup count, configured and allocated arenas, and any future
index budget or override with each sample.

## Fixed-Size Memtable and DB Epochs

An epoch applies one finite input to fresh state. The input size is independent
of Go's benchmark iteration count. Batch sizes are 1, 16, and 1,000; a short
final batch is allowed. Both bytewise and Cockroach comparers are exercised.

| Benchmark Suffix | Timed Work | Untimed Work |
| --- | --- | --- |
| `apply` | Prepare, apply, writer unref for the input | Fixture/batch generation, arena allocation and release, shape accounting |
| `history-cold` | `newRangeDelIter`, including upstream's eager history rebuild | Fresh memtable population, history traversal/counting, iterator close and release |
| `history-warm` | Constructor against one fixed populated cache | Initial population/rebuild, traversal/counting, iterator close |
| DB `get` or `seek` | Apply each batch, then Get or fresh NewIter/SeekGE in the separate read region, including close | DB setup, input copies, metrics and DB close |

`ns/op`, `B/op`, and `allocs/op` describe an epoch; `ns/tombstone` divides the
epoch time by its input count. `fragments/epoch` counts covered elementary
intervals, `intervals/epoch` also counts gaps, and `history-keys/epoch` sums
covering histories. `configured-arena-bytes` is 4 MiB. DB cases separately report
`allocated-arena-bytes`: upstream starts with smaller arenas. They assert one
memtable and no flush after the input. These are fixed-state measurements, not
sustained default-budget rotation measurements.

Arena setup and fresh DB/batch creation are outside the timer. Their garbage and
background work may still affect timed GC. Report allocation units together
with retained/peak heap and GC profiles when comparing implementations. Constructor
history benchmarks exclude traversal; they do not measure a future lazy
iterator's complete materialization cost.

Run `scripts/rangedel-baselines.sh smoke /tmp/pebble-rangedel-baselines`
first. It checks 100-record, batch-16 memtable and DB epochs, small YCSB inputs,
and one-second cycle/unique CLI runs. Supply `candidate` as the final argument
to check that revision. Each invocation saves output and the executed commands
in a fresh results directory. Epochs default to GOMAXPROCS=1; other workloads
use Go's default unless GOMAXPROCS is set explicitly.

The generators cover disjoint, touching, concentric, shared-start, staircase,
repeated-boundary, random-overlap, shallow-then-full-cover, invalid, empty-start,
and comparer-equivalent endpoints. Tests generate 100,000 records without
expanding histories. Timed memtable cases use 100, 1,000, and 10,000 records;
quadratic history layouts stop at 1,000. DB epochs stop at 1,000. Rebuilding after
every single-record batch makes deep DB epochs particularly expensive. Use
`-test.benchtime=1x` or `2x` for those checks, and retain that iteration count in
the results. Do not treat one or two epochs as tail-latency evidence.

For practical microbenchmarks, take a pilot sample, then run
`scripts/rangedel-baselines.sh compare /tmp/pebble-rangedel-baselines micro`.
It alternates ten roughly one-second samples per revision, measures input
application for disjoint/concentric layouts at 1,000 records and batch 16, and
runs `benchstat`. Set `SAMPLES` and `BENCHTIME` for a pilot. Set `MICRO_FILTER`
to select other cases, including `history-cold` or `history-warm` for history
cost. Each invocation saves fresh result files.

The script runs binaries from their corresponding checkout for relative testdata.
Use separate result files per arena, budget, comparer, layout, batch size, and
concurrency setting. Deep fixed-state results cannot satisfy the proposal's
sustained DB throughput and stall gates.

## YCSB without Range Deletions

The existing A-F workloads issue no DeleteRange operations. They use 64-byte
and 1,024-byte values. The default cached fixture has 10 million keys per value
size, a 4 GiB cache limit, and 256 workers. Fixture preparation/checkpointing are
untimed. Candidate hooks apply to the workload checkpoint, never fixture opens.

Each run performs 10,000 untimed warmup operations by default, preserves its
database/distribution state and worker buffers, then measures exactly `b.N`
operations. Workers have fixed quotas; runs below the concurrency limit use
fewer workers. Histogram allocation and merging are untimed; worker startup and
operation recording are timed. The write-only read-amp sampler is excluded.

Output includes completed counts, workers, throughput, allocations, and per-type
p50/p95/p99/p99.9/p100 service-time quantiles in nanoseconds. Histograms use two
significant digits and cap observations at one minute, with clipping counts.
These are closed-loop service times, not arrival-to-completion latency under an
external request schedule. Small smoke runs cannot establish p99.9.

The script's `smoke` mode checks all 12 cases with 1,000 fixture keys, eight
workers, 100 warmup operations, and 257 measured operations.

For performance sampling, run
`scripts/rangedel-baselines.sh compare /tmp/pebble-rangedel-baselines ycsb`.
It uses the full fixture, a common fixture directory, and the same worker/warmup
settings. `INITIAL_KEYS`, `WORKERS`, and `WARMUP` override these settings for
both revisions. Pilot longer runs with `BENCHTIME` and increase warmup until the
measured workload is stable. The script alternates samples and runs `benchstat`.
Compare throughput and per-operation tails; a mixed-workload
aggregate p99 can hide read regressions. A-F retains the existing harness's
workload definitions, including F as insert-only. Worker streams have fixed
seeds; concurrent insertion order still depends on scheduling.

## Cycle/Unique DB Workload

`pebble bench rangedel` reads and sets `r/*`, while deleting `d/*`. Cycle writers
reuse their original slot sequence, including the same deletion spans across
writers. Unique writers own disjoint queue sets; fresh ranges advance each
queue's ack level, while the overlap fraction re-deletes from zero to that ack.
Preserve the shape, queue count, rates, batch sizes, reader mix, and writer count
between revisions. Use a fresh DB directory for each sample.

The script's `smoke` mode checks both shapes. Its CLI profiles use a 16 MiB
cache and four readers with 1 ms intervals. Cycle uses one writer at 10 ms.
Unique uses two writers at 1 ms, 1,024 queues, 0.1 overlap fraction, 16 range
deletions and one set per batch, and a 0.5 iterator fraction.

These one-second runs check execution only. Run
`scripts/rangedel-baselines.sh compare /tmp/pebble-rangedel-baselines cycle`
or use `unique` as the final argument for ten alternating 60-second samples.
Set `DURATION` and `SAMPLES` for pilots, and confirm runs reach rotation and
compaction before using them for sustained measurements. The script preserves
raw CLI output; it does not send these summaries to `benchstat`.
For additional CLI runs, sweep readers 1/8/32, writers 1/2/4, unique batches
1/16/1,000, overlap 0/0.01/0.1/1, and Get/scan mixes. Record GOMAXPROCS explicitly
for each DB run. The default profile has a 64 MiB memtable limit;
`--db-options=prod-writeheavy` selects the PR's 256 MiB profile with ten-second
range-deletion flush delay. Preserve WAL and cache settings across revisions.

Save unmodified stdout. `rangedel_latency` separates Get/iterator service times
and point/RANGEDEL commit times, with p99.9. Positive worker intervals also produce
corrected estimates for coordinated omission. Correction assumes each worker's
configured fixed arrival interval; samples it synthesizes are not completed
operations. Mixed operation classes and a shared `--rate` limiter change that
assumption. Use service and corrected summaries together, and do not interpret
correction as a measured open-loop load test. Observations cap at ten seconds;
`clipped_ops` exposes longer operations. Clipped tails cannot establish a pass.

DB workers stop before terminal statistics, and elapsed includes their drain
time. The summary records completed ranges, overlaps and sets, flushes,
allocated memtable bytes/count, and cumulative flushable memory. These counters
do not expose the current memtable's exact tombstone count, depth, or index
budget. A slower writer changes the realized deletion load, so compare completed
operations and occupancy with reader tails. CLI summaries are not benchstat
input. `--rate=0` still terminates at the requested duration; its token pump has
process lifetime because the shared limiter's wait cannot be cancelled.

## Stage 1 Verification Record

Smoke checks ran on Apple M5 Max, macOS 26.7.1 (25G241), arm64, Go 1.27.1.
Memtable smoke checks used GOMAXPROCS=1; DB and YCSB smoke checks used the host
default of 18. All 372 memtable cases, 96 two-epoch DB cases, and 12 YCSB cases
completed. The YCSB cases used 1,000 fixture keys, eight workers, 100 warmup
operations, and exactly 257 measured operations. These are execution checks;
there is no baseline/candidate performance comparison yet.

Focused invariants/race tests, build, normal lint, invariant-tag vet, and
format-check passed. Two root-suite failures reproduce before these changes:
`TestLSMViewURL` and `TestTreeSteps` disagree with encoded-URL fixtures. The rest
of the root suite passed with those two tests excluded. Forcing
`GOFLAGS=-tags=invariants` into every lint subprocess also reproduces baseline
Staticcheck (`treesteps_test.go:149`, SA4006) and GC inlining assertion failures
in `cockroachkvs` and `sstable/colblk`. No unrelated fixture or production edits
were made to suppress them.

The interval/registry structural counters, 4 MiB and 64 MiB memory/rotation
checkpoint, retained heap and GC mark measurements, and the later DB checkpoint
remain stage 3 and stage 7 work. Run before/after samples with these same tools
when those implementations exist.
