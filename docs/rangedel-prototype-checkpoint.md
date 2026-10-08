# Range Deletion Prototype Checkpoint

Stage three implements the interval tree and version registry described in
the interval tree proposal. On October 4, 2026, the user removed the proposed
heap-based rotation policy: preserve master's existing admission, reservations,
rotation, and cache accounting. Heap allocation and projected history remain
diagnostics only. No index-budget option or rotation trigger will be added.

The measurements below were collected under the previous policy and are retained
as historical evidence. Its 25% raw-capacity and 50% history-only bars no longer
gate integration. The failed counts do not predict production rotation under the
current policy. Structural complexity and correctness checks remain required;
DB integration and sustained performance validation remain pending.

Interval nodes, registry nodes, publication states, and collectors allocate on
the Go heap. They reference key bytes in the arena through offsets. Their heap
allocations do not consume arena slots or affect master admission checks. The
100,000-record sweep builds one simulated memtable index; at the reported rate
of 10 range deletions per second it would take about 2.8 hours without rotation.
Production relevance depends on deletions per memtable and point-write volume.

## Verification of the Corrected Policy

The prototype no longer exposes `rotationCharge`, the synthetic capacity-limit
helper, or its memory-bar failure. The runner stops on actual checkpoint/test
errors. At the time of this verification the JSON reported `AllocatedPayload`,
`ProjectedHistory`, and `ScratchAllocated` as separate diagnostics, with timing
metric `allocated-payload/epoch`. Existing production admission and rotation
code was not modified.

Later on October 4, 2026 the byte-charge accounting was removed from the index:
the power-of-two payload charges, saturating arithmetic, projected-history
estimate, and operation collector. The JSON no longer has `AllocatedPayload`,
`ProjectedHistory`, or `ScratchAllocated`, and the benchmark no longer reports
`allocated-payload/epoch`. `ScratchPeak` is now the capacity of one bulk `append`
of a batch, so it can differ from the collector's doubling in the tables below. Memory is
reported from structural counters, reachable-node payload, and runtime heap
measurements. Charge-based figures in the sections below ("index budget",
"history", "payload/charge") describe the removed accounting.

The corrected runner completed all 132 measurement cases with exit code 0:

```text
--- PASS: TestRangeDelPrototypeCheckpoint (43.24s)
PASS
checkpoint exit=0
sample 1 exit=0
```

This verification used Go 1.27.1 and one 100ms timing sample of disjoint,
single-record batches at all four sizes. It verifies runner behavior, not a new
ten-sample performance comparison. Evidence is in
`/tmp/pebble-master-rotation-checkpoint`.

| Command | Exit code | Output |
| --- | --- | --- |
| `go test -tags invariants -run RangeDel -count=1 .` | 0 | `ok github.com/cockroachdb/pebble 11.973s` |
| `go test -race -tags invariants -run RangeDel -count=1 .` | 0 | `ok github.com/cockroachdb/pebble 68.296s` |
| `go build -tags invariants ./...` | 0 | No output |
| `make lint TAGS=invariants` | 0 | `ok github.com/cockroachdb/pebble/internal/lint 15.232s` |
| `go test -tags invariants ./...` | 0 | All packages passed; root 40.529s, lint 22.748s, metamorphic 42.556s |
| `make format-check` | 0 | `Git repository is clean.` |

Lint used Go 1.25.12, a GOPATH symlink to this repository at
`/tmp/pebble-rotation-gopath/src/github.com/cockroachdb/pebble`, and writable
staticcheck cache `/tmp/pebble-rotation-staticcheck-cache`, as required by the
existing lint environment described below. The full suite used the same Go 1.25.12
environment; its output is in `/tmp/pebble-rotation-full-tests.log`. Formatting ran
in a clean colocated Git/JJ copy at `/tmp/pebble-stage3-clean` and left the source
unchanged. Build and focused/race tests used Go 1.27.1.

## Historical Measurements

All remaining measurements and gate transcripts in this report describe the
original stage-three implementation and its removed heap-budget rotation policy.
Their numerical data is preserved unchanged.

## Implementation and Measurement

The prototype uses immutable 128-byte interval nodes with arena offsets,
height-sensitive AVL joins, boundary-pivot concatenation, and recursive splits.
It preserves every endpoint and explicit gap. Covered subtrees receive a lazy
trailer assignment and depth increment. Structural operations expose tags onto
fresh child copies before attaching subtrees. Readers carry inherited tags and
never modify nodes.

The persistent version registry has 48-byte nodes. An atomic pointer publishes
a 32-byte state containing the registry and latest entry. The fixed index object
is 104 bytes, including structural counters. Operation records are 24 bytes.
Publication validates increasing batch labels and trailers, retains a registry
entry for all-invalid batches, and rejects thresholds inside registered batches.
Point-only batches have no entry. This is a standalone single-writer prototype;
memtable ordering tasks, reader iterators, rotation, and replay are later stages.

Allocation charges include every interval and registry copy, publication state,
fixed index object, and every collector backing-array growth. Nonzero payloads
are rounded up to a power of two with a minimum of 16 bytes. Checked arithmetic
saturates at MaxUint64. Projected history uses actual 80-byte Span and 56-byte Key
layouts, with active fragment count and summed depth from the latest root.

Run `scripts/rangedel-prototype.sh OUTPUT_DIRECTORY` from the repository root.
The script builds a release test binary once, records revision and host metadata,
and runs the retention pass and ten timing samples. The complete matrix has
11 layouts, four sizes (100, 1,000, 10,000, 100,000), and three batch sizes
(1, 16, 1,000). Unique workloads use 1,024 queues and overlap fractions
0, 0.01, 0.1, and 1. The queue rule matches the single unique writer in
bench/rangedel.go, using independent PCG seed (6207, 1); it reproduces the layout
rather than that CLI's exact random stream. Cycle uses the existing single writer's 500 even deletion
slots. Only unique fractions 0 and 0.01 are classified as low overlap for the
25% bar; cycle and higher fractions are reported without an invented pass bar.

Measurements use Go 1.27.1, darwin/arm64, Apple M5 Max, macOS 26.7.1 (25G241),
and GOMAXPROCS=1. Timing binaries omit invariants. Arena insertion and fixture
creation precede the timer, and the same immutable arena is reused across epochs.
Each timed epoch constructs a new index and collectors. Time includes allocation
and GC caused by construction. There is no DB baseline comparison or tail claim.

Retention uses a separate pass. The fixture, input operations, and arena remain
live across baseline and final GC. Runtime TotalAlloc and HeapAlloc deltas include
runtime allocation overhead; graph traversal separately counts unique reachable
interval and registry nodes across every retained version, including the latest
entry pointer. Its maps are allocated after the heap measurements. GC mark CPU
is the sum of assist, dedicated, and idle mark CPU deltas during construction and
the final forced collection. It is a process-wide measurement, not a CPU profile
attribution to individual tree methods. The sampled heap maximum is an observed
lower bound, sampled every 1,024 records and after construction, not a precise
peak. Scratch peak records the largest backing-array capacity; cumulative scratch
charge includes replaced arrays. Transient old and new arrays can coexist during
growth and contribute to the observed heap.

The largest simultaneous old/new scratch payload during growth is derived from
the collector capacities: 24 bytes for batch size 1, 576 bytes for batch size 16,
and 36,864 bytes for batch size 1,000 (4,608 bytes when the input has only 100
records). These bounds count both backing arrays at the largest growth step;
allocator rounding and arrays awaiting collection are included in runtime heap
measurements and cumulative scratch charges, respectively.

Fixed-size arenas are large enough for the input using MaxNodeSize reservations;
their actual capacity and occupancy are recorded per case. These runs override
the soft budget by continuing to index the entire input. They do not describe
sustained default-budget DB behavior. Task/channel allocations are absent from
this standalone stage and would add costs during integration.

## Structural Bounds

Let H be the maximum pre-update or post-update interval height plus three. A join
descends the taller inner spine until the heights differ by at most one. Each
step exposes one node; balancing exposes at most three more nodes through double
rotations. A split exposes a search path and reconstructs with joins whose inner
spine height differences telescope along that path, as in Theorem 7 of
[Joinable Parallel Balanced Binary Trees](https://www.cs.cmu.edu/~blelloch/papers/3512769.pdf).
On each side, descending joins increase the height of the accumulated result;
their total descent steps are bounded by H. Other joins have input height
differences of at most one because the accumulated result does not exceed the
original sibling's height. Across both sides there are at most 2H descent steps,
H+2 base joins (including split interval pieces), H search exposures, and
3(H+2) rotation exposures. This is at most 7H+8 exposures, within the asserted
16H split allowance for H >= 3. A concat uses one boundary
extraction and one join, at most 8H exposed nodes. An insertion uses two splits
and at most four concats (two domain extensions and two final concats), giving
64H. The asserted 128H allowance also covers height changes in intermediate
trees. Each exposure allocates one node plus at most two tagged child copies;
up to five interval/tag creations add constant work. The asserted allocation
bound is 400H. Comparisons are bounded by 8H. Pushes are bounded by visits,
and rotations by twice visits. These constants were set before the sweep.

Registry insertion visits at most its previous height plus one nodes, allocates
at most that height plus six nodes, and rotates at most twice. AVL height is
bounded by twice the ceiling of log2(N+1), from its Fibonacci minimum-node
recurrence. Summing the per-update bounds gives O(t log(t+1)) structural work
and cumulative allocation, with B <= t for the registry. Retained interval roots
can consume O(t log t) memory. The budget charges cumulative allocation, including
copies that GC can reclaim, so it can be much larger than retained heap.

The correctness sweep checks small histories, all retained roots after each
update, tag ordering, effective trailer/depth, AVL invariants, aggregates,
contiguous coverage, endpoint provenance, cached active bounds, tagged splits,
unequal-height joins, pivot extraction, registry thresholds, and accounting
overflow. Geometric complexity sweeps cover disjoint, concentric, shared-start,
staircase, random, and shallow-then-full-cover layouts through 100,000 tombstones.
The large full-cover insertion covers approximately 200,000 elementary intervals
without expanding them. Reference expansion is confined to small correctness
tests and is excluded from release measurements and complexity counters.

## Historical Rotation Counts Under the Removed Policy

Each capacity run initializes the three upstream skiplists in one actual 4 MiB
or 64 MiB arena. Before each single-record insertion it checks the upstream
MaxNodeSize reservation against actual remaining arena capacity, matching
sequential prepare/availBytes behavior. Successful insertion offsets feed the
prototype. The same ordered stream supplies raw occupancy and index accounting.
Index construction stops after the first completed batch crossing the budget;
raw insertion continues to reservation exhaustion. This accepts one-batch
overshoot. Raw runs do not retain index roots beyond the index limit.

Concentric history-only charge is (2t-1)*80 + t*t*56; shared-start charge is
t*80 + t*(t+1)/2*56. These exact formulas avoid quadratic history expansion and
are checked against root aggregates in the fixed-size sweep. History-only runs
also count the first batch crossing the limit. Endpoint generation fixes the
input sweep before insertion and extends it far enough to reach raw capacity.
Skiplist tower heights are randomized; raw capacities and occupancy have small
run-to-run variation. Counts predict rotation for one sequential writer;
actual DB flushes, stalls, and L0 effects are unmeasured.

| Arena | Layout | Baseline records | Index records | Fraction | Amplification | Bar | Result |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 4 MiB | disjoint | 70,279 | 498 | 0.71% | 141.12x | 25% | FAIL |
| 4 MiB | unique-0 | 48,972 | 456 | 0.93% | 107.39x | 25% | FAIL |
| 4 MiB | unique-0.01 | 48,962 | 457 | 0.93% | 107.14x | 25% | FAIL |
| 4 MiB | concentric | 273 | 214 | 78.39% | 1.28x | 50% | pass |
| 4 MiB | shared-start | 386 | 261 | 67.62% | 1.48x | 50% | pass |
| 64 MiB | disjoint | 1,124,884 | 5,861 | 0.52% | 191.93x | 25% | FAIL |
| 64 MiB | unique-0 | 783,536 | 5,398 | 0.69% | 145.15x | 25% | FAIL |
| 64 MiB | unique-0.01 | 783,561 | 5,406 | 0.69% | 144.94x | 25% | FAIL |
| 64 MiB | concentric | 1,094 | 1,011 | 92.41% | 1.08x | 50% | pass |
| 64 MiB | shared-start | 1,547 | 1,349 | 87.20% | 1.15x | 50% | pass |

The failed shallow bars predict 107x-192x more rotations under the index budget
for these streams, far above the allowed 4x. This is a count ratio, not a measured
DB stall or flush amplification. At the 4 MiB disjoint limit the raw arena holds
only 30,937 bytes (0.74% occupancy); the index budget is 4,128,128 bytes and
projected history 67,728 bytes. At 64 MiB the corresponding values are 351,382
bytes (0.52%), 66,323,840 bytes, and 797,096 bytes. More GC does not reduce the
cumulative allocation budget. Larger batches reclaim more private copies, but
the single-record pass bars deliberately expose retention and allocation costs.

## Retained Memory at 100,000 Tombstones

| Layout | Batch | Allocated B/t | Retained B/t | Retained B/active | Index budget MiB | History MiB | Observed heap max MiB | Mark CPU seconds |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| disjoint | 1 | 14426 | 2440 | 2440 | 1403.56 | 12.97 | 464.70 | 0.433 |
| disjoint | 16 | 13611 | 392 | 392 | 1300.82 | 12.97 | 143.97 | 0.227 |
| disjoint | 1000 | 13571 | 258 | 258 | 1295.79 | 12.97 | 116.54 | 0.171 |
| concentric | 1 | 15763 | 4128 | 2064 | 1531.00 | 534072.88 | 772.20 | 0.687 |
| concentric | 16 | 14947 | 494 | 247 | 1428.25 | 534072.88 | 178.38 | 0.230 |
| concentric | 1000 | 14907 | 260 | 130 | 1423.22 | 534072.88 | 130.95 | 0.158 |
| shared-start | 1 | 19916 | 7337 | 7337 | 1927.08 | 267039.11 | 1143.08 | 0.780 |
| shared-start | 16 | 19100 | 575 | 575 | 1824.34 | 267039.11 | 168.31 | 0.278 |
| shared-start | 1000 | 19060 | 135 | 135 | 1819.31 | 267039.11 | 102.39 | 0.124 |
| staircase | 1 | 24796 | 9409 | 4705 | 2392.52 | 534072.88 | 1406.58 | 1.198 |
| staircase | 16 | 23981 | 828 | 414 | 2289.77 | 534072.88 | 260.19 | 0.720 |
| staircase | 1000 | 23941 | 265 | 133 | 2284.74 | 534072.88 | 143.64 | 0.451 |
| random | 1 | 29994 | 12129 | 8857 | 2888.24 | 147096.33 | 1701.95 | 2.273 |
| random | 16 | 29179 | 9109 | 6652 | 2785.50 | 147096.33 | 1379.79 | 1.371 |
| random | 1000 | 29139 | 3330 | 2432 | 2780.47 | 147096.33 | 669.74 | 1.021 |
| full-cover | 1 | 14426 | 2440 | 1220 | 1403.56 | 31.28 | 490.78 | 0.446 |
| full-cover | 16 | 13611 | 392 | 196 | 1300.82 | 31.28 | 118.18 | 0.191 |
| full-cover | 1000 | 13571 | 258 | 129 | 1295.79 | 31.28 | 129.68 | 0.140 |
| cycle | 1 | 10121 | 2287 | 457383 | 992.96 | 5.38 | 423.50 | 0.404 |
| cycle | 16 | 9305 | 478 | 95599 | 890.22 | 5.38 | 135.95 | 0.170 |
| cycle | 1000 | 9265 | 128 | 25584 | 885.19 | 5.38 | 97.65 | 0.061 |
| unique-0 | 1 | 15970 | 3668 | 3668 | 1550.81 | 12.97 | 657.59 | 0.653 |
| unique-0 | 16 | 15155 | 2623 | 2623 | 1448.07 | 12.97 | 550.75 | 0.574 |
| unique-0 | 1000 | 15115 | 969 | 969 | 1443.04 | 12.97 | 279.15 | 0.300 |
| unique-0.01 | 1 | 15966 | 3683 | 3721 | 1550.36 | 15.48 | 707.68 | 0.752 |
| unique-0.01 | 16 | 15150 | 2635 | 2662 | 1447.61 | 15.48 | 513.67 | 0.531 |
| unique-0.01 | 1000 | 15110 | 977 | 987 | 1442.58 | 15.48 | 297.81 | 0.319 |
| unique-0.1 | 1 | 15929 | 3806 | 4221 | 1546.88 | 34.95 | 719.35 | 0.757 |
| unique-0.1 | 16 | 15114 | 2756 | 3057 | 1444.14 | 34.95 | 526.28 | 0.538 |
| unique-0.1 | 1000 | 15074 | 1037 | 1150 | 1439.11 | 34.95 | 301.62 | 0.336 |
| unique-1 | 1 | 11508 | 2623 | 256104 | 1125.24 | 5.42 | 532.38 | 0.474 |
| unique-1 | 16 | 10692 | 1582 | 154481 | 1022.49 | 5.42 | 354.39 | 0.300 |
| unique-1 | 1000 | 10652 | 241 | 23521 | 1017.47 | 5.42 | 135.28 | 0.087 |

B/t means bytes per input tombstone. Retained B/active uses latest active
fragments as the denominator, while the numerator includes all retained roots.
Cycle therefore has a large B/active ratio: it repeats only 500 active spans.

## Historical Runner and Allocator Verification

All ten timing subprocesses completed with exit code 0 and 132 cases each. Their
files were checked for complete case coverage and terminal PASS before computing
the matrix medians. The original mutable runner exited 2 after sampling because
an edit shifted its shell read position; that wrapper exit is not a passing gate.
A frozen copy of the final runner was then executed with one final-source timing
case. It recorded checkpoint exit 1, preserved all failed-bar evidence, completed
the timing case with exit 0, and exited 1 as required. The numerical failure now
blocks automated integration instead of merely appearing in JSON output.

The final-source timing snapshot is `8a8fac0cae35a3628e0df14c8f5ba7417854949a`.
Its disjoint 100,000-record, single-record-batch check measured 7,838 ns/tombstone,
105.6 interval allocations/tombstone, and the same 1,485,340,928-byte rotation
charge as the original matrix. This single follow-up is a verification sample,
not a replacement for the ten-sample timing snapshot.

The release allocator probe produced this output with exit code 0:

```text
payload/charge/runtime interval=128/128/128 registry=48/64/48 state=32/32/32
--- PASS: TestRangeDelIntervalAllocatorLayout (0.00s)
PASS
```

The rounded payload allowance covers these measured object size classes on this
toolchain. Runtime heap measurements remain separate from the budget, including
slice allocation rounding and runtime/GC overhead.

Raw local evidence is in `/tmp/pebble-stage3-checkpoint` (ten samples, initial
retention/capacity pass, host metadata, and combined benchstat output) and
`/tmp/pebble-stage3-final-checkpoint` (frozen final runner and enforced failure).
The matrix below preserves the numerical data in this repository; the local
directories can be regenerated with the runner.

## Correctness Gates

The final source was checked in an isolated JJ workspace with Go 1.27.1.

| Command | Exit code | Result |
| --- | --- | --- |
| `go test -tags invariants -run RangeDel -count=1 .` | 0 | Passed, 10.599s |
| `go test -race -tags invariants -run RangeDel -count=1 .` | 0 | Passed, 73.241s |
| `go test -tags invariants ./internal/arenaskl` | 0 | Passed, 0.868s |
| `go test -tags invariants ./...` | 1 | Existing compressed-URL goldens and lint environment failures |
| `go test -tags invariants ./internal/metamorphic -count=1` | 0 | Passed, 103.748s |
| `go build -tags invariants ./...` | 0 | No output |
| `go vet -tags invariants ./...` | 0 | No output |
| `go test -tags invariants ./internal/lint` | 1 | GOPATH lookup and staticcheck cache permission failures |
| `make format-check` | 2 | Git cleanliness check found the ancestor main worktree |

The URL and lint failures were reproduced at the stage-2 baseline,
`c5f64d4264016d694bcdadc0fab13c28bbeadb4e`. All 12 actual/expected URL pairs in
the failed suite decoded to identical payloads; their compressed bytes differed.
The failing packages were the root package, `internal/lsmview`,
`internal/treesteps`, `sstable`, and `tool`. The lint failures were package lookup
through `go/build` and denied creation of the default staticcheck cache.

The same final source was then copied to a clean, colocated Git/JJ checkout at
`/tmp/pebble-stage3-clean`, with the manifest fixture retained. No source changes
were made for this retry. Go 1.25.12 was selected with these environment settings:

```sh
export PATH=/Users/timods/.local/share/mise/installs/go/1.25.12/bin:/Users/timods/go/bin:$PATH
export GOPATH=/tmp/pebble-stage3-gopath
export GOMODCACHE=/Users/timods/go/pkg/mod
export GOBIN=/Users/timods/go/bin
export STATICCHECK_CACHE=/tmp/pebble-stage3-staticcheck-cache
```

`$GOPATH/src/github.com/cockroachdb/pebble` points to that checkout. The following
gates were run directly and returned exit code 0:

| Command | Output |
| --- | --- |
| `go test -tags invariants ./...` | All packages passed; root 111.314s, lint 98.203s, metamorphic 63.822s |
| `go test -tags invariants ./internal/lint` | `ok github.com/cockroachdb/pebble/internal/lint 11.521s` |
| `make format-check` | `Git repository is clean.` |

Formatting left the verification checkout unchanged. The original gate logs and
exit codes are in `/tmp/pebble-stage3-gates.tsv`; baseline and retry logs use the
`/tmp/pebble-stage3-baseline-*` and `/tmp/pebble-stage3-normalized-*` prefixes.

## Complete Fixed-Size Matrix

Times and timed allocated bytes per tombstone are medians of ten samples
from captured Git snapshot `dedc39d5bff44be8245d661b7d3b4bba05613133`.
The final source adds a batch-bit validation check and checkpoint exit enforcement;
those final changes are verified separately above. The structural algorithms,
node layouts, collectors, and charges are unchanged from the timing snapshot.

All counts and byte values are totals per epoch unless `/t` is shown.
Interval counters are comparisons/visits/allocations/pushes/rotations.
Registry counters are visits/allocations/rotations/publications.
Bounds include actual arena capacity and raw occupancy, F/K is active fragments/history keys,
and reachable is interval nodes/version nodes/payload bytes.
Index/history/total are budget bytes. Heap is allocated/retained/sampled maximum bytes.
Scratch is largest backing capacity bytes/cumulative charged bytes. GC is mark CPU seconds.

| Layout | t | Batch | ns/t | Timed allocated B/t | Arena capacity/used | Height; F/K | Interval counters | Registry counters | Reachable | Index/history/total | Heap | Scratch | GC |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- | --- | --- | --- | ---: |
| disjoint | 100 | 1 | 2544 | 6265 | 21724/6379 | 8; 100/100 | 1745/4165/4563/99/239 | 573/766/93/100 | 864/100/115528 | 639616/13600/653216 | 636024/86448/636024 | 24/3200 | 0.000222 |
| disjoint | 100 | 16 | 2191 | 5902 | 21724/6419 | 8; 100/100 | 1745/4165/4563/99/239 | 14/25/4/7 | 240/7/31192 | 592192/13600/605792 | 590360/28736/590360 | 384/6176 | 0.000172 |
| disjoint | 100 | 1000 | 2085 | 5904 | 21724/6251 | 8; 100/100 | 1745/4165/4563/99/239 | 0/1/0/1 | 199/1/25656 | 592448/13600/606048 | 590504/23248/590504 | 3072/8160 | 0.000188 |
| disjoint | 1000 | 1 | 4058 | 8945 | 208024/60063 | 11; 1000/1000 | 23953/61330/65328/999/2486 | 8977/10967/990/1000 | 11965/1000/1579656 | 9128000/136000/9264000 | 8944640/1577200/2658696 | 24/32000 | 0.002987 |
| disjoint | 1000 | 16 | 3015 | 8431 | 208024/60279 | 11; 1000/1000 | 23953/61330/65328/999/2486 | 315/435/57/63 | 2618/63/338264 | 8453952/136000/8589952 | 8431608/335808/3010624 | 384/61984 | 0.001104 |
| disjoint | 1000 | 1000 | 2807 | 8411 | 208024/60143 | 11; 1000/1000 | 23953/61330/65328/999/2486 | 0/1/0/1 | 1999/1/256056 | 8427712/136000/8563712 | 8411432/253600/1373472 | 24576/65504 | 0.000948 |
| disjoint | 10000 | 1 | 5276 | 11711 | 2071024/596727 | 15; 10000/10000 | 307233/816658/856656/9999/24982 | 123617/143603/9986/10000 | 153601/10000/20141064 | 119482688/1360000/120842688 | 117105152/20141072/42965288 | 24/320000 | 0.026504 |
| disjoint | 10000 | 16 | 4046 | 11045 | 2071024/597735 | 15; 10000/10000 | 307233/816658/856656/9999/24982 | 5227/6467/615/625 | 28336/625/3657144 | 110705984/1360000/112065984 | 110447624/3654560/13877576 | 384/620000 | 0.011661 |
| disjoint | 10000 | 1000 | 3997 | 11015 | 2071024/598175 | 15; 10000/10000 | 307233/816658/856656/9999/24982 | 25/41/6/10 | 20119/10/2575848 | 110310080/1360000/111670080 | 110145776/2575856/13744512 | 24576/655040 | 0.011260 |
| disjoint | 100000 | 1 | 8981 | 14426 | 20701024/5964687 | 18; 100000/100000 | 3737857/10163521/10563519/99999/249979 | 1568929/1768912/99983/100000 | 1868910/100000/244020616 | 1471740928/13600000/1485340928 | 1442638448/244020624/487274432 | 24/3200000 | 0.432744 |
| disjoint | 100000 | 16 | 6943 | 13611 | 20701024/5969695 | 18; 100000/100000 | 3737857/10163521/10563519/99999/249979 | 73059/85546/6237/6250 | 304290/6250/39249256 | 1364005504/13600000/1377605504 | 1361086880/39246672/150959280 | 384/6200000 | 0.227098 |
| disjoint | 100000 | 1000 | 6829 | 13571 | 20701024/5961711 | 18; 100000/100000 | 3737857/10163521/10563519/99999/249979 | 573/766/93/100 | 201653/100/25816520 | 1358733184/13600000/1372333184 | 1357083440/25816528/122198400 | 24576/6550400 | 0.170500 |
| concentric | 100 | 1 | 2825 | 6346 | 21724/6363 | 8; 199/10000 | 1122/4129/4626/100/160 | 573/766/93/100 | 1191/100/157384 | 647680/575920/1223600 | 634736/154816/634736 | 24/3200 | 0.000381 |
| concentric | 100 | 16 | 2313 | 5983 | 21724/6251 | 8; 199/10000 | 1122/4129/4626/100/160 | 14/25/4/7 | 260/7/33752 | 600256/575920/1176176 | 598424/31312/598424 | 384/6176 | 0.000232 |
| concentric | 100 | 1000 | 2068 | 5984 | 21724/6371 | 8; 199/10000 | 1122/4129/4626/100/160 | 0/1/0/1 | 199/1/25656 | 600512/575920/1176432 | 598568/23216/598568 | 3072/8160 | 0.000193 |
| concentric | 1000 | 1 | 3842 | 9424 | 208024/59927 | 11; 1999/1000000 | 14476/64076/69073/1000/1729 | 8977/10967/990/1000 | 18446/1000/2409224 | 9607360/56159920/65767280 | 9424000/2406768/5537272 | 24/32000 | 0.002440 |
| concentric | 1000 | 16 | 3048 | 8911 | 208024/59695 | 11; 1999/1000000 | 14476/64076/69073/1000/1729 | 315/435/57/63 | 2995/63/386520 | 8933312/56159920/65093232 | 8910968/384064/2445440 | 384/61984 | 0.001035 |
| concentric | 1000 | 1000 | 3113 | 8891 | 208024/60223 | 11; 1999/1000000 | 14476/64076/69073/1000/1729 | 0/1/0/1 | 1999/1/256056 | 8907072/56159920/65066992 | 8890792/253600/2332320 | 24576/65504 | 0.000901 |
| concentric | 10000 | 1 | 6666 | 12621 | 2071024/597103 | 15; 19999/100000000 | 178616/877798/927795/10000/17471 | 123617/143603/9986/10000 | 252222/10000/32764552 | 128588480/5601599920/5730188400 | 126210944/32764560/59468560 | 24/320000 | 0.047856 |
| concentric | 10000 | 16 | 4861 | 11955 | 2071024/596991 | 15; 19999/100000000 | 178616/877798/927795/10000/17471 | 5227/6467/615/625 | 34187/625/4406072 | 119811776/5601599920/5721411696 | 119553416/4403488/15328848 | 384/620000 | 0.014153 |
| concentric | 10000 | 1000 | 4745 | 11925 | 2071024/595231 | 15; 19999/100000000 | 178616/877798/927795/10000/17471 | 25/41/6/10 | 20203/10/2586600 | 119415872/5601599920/5721015792 | 119251568/2586608/14846592 | 24576/655040 | 0.011042 |
| concentric | 100000 | 1 | 10537 | 15763 | 20701024/5966079 | 18; 199999/10000000000 | 2118928/11107476/11607473/100000/174965 | 1568929/1768912/99983/100000 | 3187843/100000/412844040 | 1605367040/560015999920/561621366960 | 1576264560/412842840/809715016 | 24/3200000 | 0.686835 |
| concentric | 100000 | 16 | 7702 | 14947 | 20701024/5967847 | 18; 199999/10000000000 | 2118928/11107476/11607473/100000/174965 | 73059/85546/6237/6250 | 383598/6250/49400680 | 1497631616/560015999920/561513631536 | 1494712992/49398096/187043240 | 384/6200000 | 0.230248 |
| concentric | 100000 | 1000 | 7249 | 14907 | 20701024/5967751 | 18; 199999/10000000000 | 2118928/11107476/11607473/100000/174965 | 573/766/93/100 | 202911/100/25977544 | 1492359296/560015999920/561508359216 | 1490709552/25977552/137312136 | 24576/6550400 | 0.157753 |
| shared-start | 100 | 1 | 3033 | 6752 | 21724/6395 | 7; 100/5050 | 1879/3216/4943/984/116 | 573/766/93/100 | 1726/100/225864 | 688256/290800/979056 | 675312/223296/675312 | 24/3200 | 0.000237 |
| shared-start | 100 | 16 | 2482 | 6389 | 21724/6323 | 7; 100/5050 | 1879/3216/4943/984/116 | 14/25/4/7 | 208/7/27096 | 640832/290800/931632 | 639000/24656/639000 | 384/6176 | 0.000180 |
| shared-start | 100 | 1000 | 2109 | 6390 | 21724/6323 | 7; 100/5050 | 1879/3216/4943/984/116 | 0/1/0/1 | 100/1/12984 | 641088/290800/931888 | 639144/10544/639144 | 3072/8160 | 0.000168 |
| shared-start | 1000 | 1 | 4458 | 11082 | 208024/60039 | 10; 1000/500500 | 28452/51628/82025/16444/1238 | 8977/10967/990/1000 | 30396/1000/3938824 | 11265216/28108000/39373216 | 11081856/3936368/6918064 | 24/32000 | 0.002482 |
| shared-start | 1000 | 16 | 3874 | 10569 | 208024/59783 | 10; 1000/500500 | 28452/51628/82025/16444/1238 | 315/435/57/63 | 2816/63/363608 | 10591168/28108000/38699168 | 10568824/361152/1940848 | 384/61984 | 0.001203 |
| shared-start | 1000 | 1000 | 4366 | 10549 | 208024/59983 | 10; 1000/500500 | 28452/51628/82025/16444/1238 | 0/1/0/1 | 1000/1/128184 | 10564928/28108000/38672928 | 10548648/125728/1595808 | 24576/65504 | 0.000984 |
| shared-start | 10000 | 1 | 10699 | 15531 | 2071024/596671 | 14; 10000/50005000 | 384991/717444/1155117/231332/12484 | 123617/143603/9986/10000 | 437672/10000/56502152 | 157685696/2801080000/2958765696 | 155308160/56502160/90090768 | 24/320000 | 0.052110 |
| shared-start | 10000 | 16 | 6928 | 14865 | 2071024/597151 | 14; 10000/50005000 | 384991/717444/1155117/231332/12484 | 5227/6467/615/625 | 36410/625/4690616 | 148908992/2801080000/2949988992 | 148650632/4688032/19747680 | 384/620000 | 0.014444 |
| shared-start | 10000 | 1000 | 5726 | 14835 | 2071024/596783 | 14; 10000/50005000 | 384991/717444/1155117/231332/12484 | 25/41/6/10 | 10382/10/1329512 | 148513088/2801080000/2949593088 | 148348784/1329520/12081280 | 24576/655040 | 0.009233 |
| shared-start | 100000 | 1 | 15357 | 19916 | 20701024/5966239 | 17; 100000/5000050000 | 4841305/9157563/14852220/2972324/124981 | 1568929/1768912/99983/100000 | 5694656/100000/733716104 | 2020694656/280010800000/282031494656 | 1991592176/733716112/1198602272 | 24/3200000 | 0.780472 |
| shared-start | 100000 | 16 | 10230 | 19100 | 20701024/5967551 | 17; 100000/5000050000 | 4841305/9157563/14852220/2972324/124981 | 73059/85546/6237/6250 | 446534/6250/57456488 | 1912959232/280010800000/281923759232 | 1910040608/57453904/176489944 | 384/6200000 | 0.277962 |
| shared-start | 100000 | 1000 | 9090 | 19060 | 20701024/5970863 | 17; 100000/5000050000 | 4841305/9157563/14852220/2972324/124981 | 573/766/93/100 | 105496/100/13508424 | 1907686912/280010800000/281918486912 | 1906037168/13505840/107363520 | 24576/6550400 | 0.124055 |
| staircase | 100 | 1 | 4481 | 9053 | 21724/6323 | 8; 199/10000 | 2173/4816/6741/984/232 | 573/766/93/100 | 2341/100/304584 | 918400/575920/1494320 | 905456/302016/905456 | 24/3200 | 0.000251 |
| staircase | 100 | 16 | 3859 | 8690 | 21724/6379 | 8; 199/10000 | 2173/4816/6741/984/232 | 14/25/4/7 | 342/7/44248 | 870976/575920/1446896 | 869144/41808/869144 | 384/6176 | 0.000199 |
| staircase | 100 | 1000 | 3375 | 8692 | 21724/6275 | 8; 199/10000 | 2173/4816/6741/984/232 | 0/1/0/1 | 199/1/25656 | 871232/575920/1447152 | 869288/23216/869288 | 3072/8160 | 0.000180 |
| staircase | 1000 | 1 | 5819 | 14240 | 208024/60071 | 11; 1999/1000000 | 31446/74305/106700/16444/2476 | 8977/10967/990/1000 | 39862/1000/5150472 | 14423616/56159920/70583536 | 14240256/5148016/8523592 | 24/32000 | 0.004097 |
| staircase | 1000 | 16 | 4801 | 13727 | 208024/60447 | 11; 1999/1000000 | 31446/74305/106700/16444/2476 | 315/435/57/63 | 4372/63/562776 | 13749568/56159920/69909488 | 13727224/560320/2991136 | 384/61984 | 0.001873 |
| staircase | 1000 | 1000 | 5154 | 13707 | 208024/60079 | 11; 1999/1000000 | 31446/74305/106700/16444/2476 | 0/1/0/1 | 1999/1/256056 | 13723328/56159920/69883248 | 13707048/253600/2715040 | 24576/65504 | 0.001592 |
| staircase | 10000 | 1 | 13066 | 19559 | 2071024/596935 | 15; 19999/100000000 | 414985/1012143/1469814/231332/24968 | 123617/143603/9986/10000 | 566274/10000/72963208 | 197966912/5601599920/5799566832 | 195589376/72963216/120090712 | 24/320000 | 0.079510 |
| staircase | 10000 | 16 | 9299 | 18893 | 2071024/595767 | 15; 19999/100000000 | 414985/1012143/1469814/231332/24968 | 5227/6467/615/625 | 54122/625/6957752 | 189190208/5601599920/5790790128 | 188931848/6955168/23279168 | 384/620000 | 0.021900 |
| staircase | 10000 | 1000 | 8039 | 18863 | 2071024/598047 | 15; 19999/100000000 | 414985/1012143/1469814/231332/24968 | 25/41/6/10 | 20492/10/2623592 | 188794304/5601599920/5790394224 | 188630000/2621008/15315936 | 24576/655040 | 0.015029 |
| staircase | 100000 | 1 | 21392 | 24796 | 20701024/5968431 | 18; 199999/10000000000 | 5141299/12770380/18665035/2972324/249962 | 1568929/1768912/99983/100000 | 7313567/100000/940936712 | 2508734976/560015999920/562524734896 | 2479632496/940936720/1474904912 | 24/3200000 | 1.198367 |
| staircase | 100000 | 16 | 13679 | 23981 | 20701024/5965335 | 18; 199999/10000000000 | 5141299/12770380/18665035/2972324/249962 | 73059/85546/6237/6250 | 644575/6250/82805736 | 2400999552/560015999920/562416999472 | 2398080928/82803152/272826328 | 384/6200000 | 0.720338 |
| staircase | 100000 | 1000 | 11774 | 23941 | 20701024/5964199 | 18; 199999/10000000000 | 5141299/12770380/18665035/2972324/249962 | 573/766/93/100 | 207050/100/26507336 | 2395727232/560015999920/562411727152 | 2394077488/26504752/150614512 | 24576/6550400 | 0.451026 |
| random | 100 | 1 | 4102 | 7671 | 21724/6299 | 9; 135/2941 | 1842/4196/5661/790/276 | 573/766/93/100 | 1932/100/252232 | 780160/175496/955656 | 767216/249648/767216 | 24/3200 | 0.000304 |
| random | 100 | 16 | 4286 | 7308 | 21724/6339 | 9; 135/2941 | 1842/4196/5661/790/276 | 14/25/4/7 | 534/7/68824 | 732736/175496/908232 | 730904/66368/730904 | 384/6176 | 0.000227 |
| random | 100 | 1000 | 4107 | 7309 | 21724/6363 | 9; 135/2941 | 1842/4196/5661/790/276 | 0/1/0/1 | 135/1/17464 | 732992/175496/908488 | 731048/15008/731048 | 3072/8160 | 0.000190 |
| random | 1000 | 1 | 9633 | 14468 | 208024/60375 | 13; 1376/286572 | 28560/69990/108481/20400/4647 | 8977/10967/990/1000 | 41995/1000/5423496 | 14651584/16158112/30809696 | 14468224/5421040/8852112 | 24/32000 | 0.005563 |
| random | 1000 | 16 | 8082 | 13955 | 208024/60215 | 13; 1376/286572 | 28560/69990/108481/20400/4647 | 315/435/57/63 | 21007/63/2692056 | 13977536/16158112/30135648 | 13955192/2689600/5283368 | 384/61984 | 0.002522 |
| random | 1000 | 1000 | 6616 | 13935 | 208024/60079 | 13; 1376/286572 | 28560/69990/108481/20400/4647 | 0/1/0/1 | 1376/1/176312 | 13951296/16158112/30109408 | 13935016/173856/2821920 | 24576/65504 | 0.001530 |
| random | 10000 | 1 | 19594 | 22095 | 2071024/596439 | 17; 13637/27773712 | 384965/978220/1667920/357980/63130 | 123617/143603/9986/10000 | 678639/10000/87345928 | 223324480/1556418832/1779743312 | 220946944/87345936/142132496 | 24/320000 | 0.197083 |
| random | 10000 | 16 | 18460 | 21429 | 2071024/597759 | 17; 13637/27773712 | 384965/978220/1667920/357980/63130 | 5227/6467/615/625 | 448807/625/57477432 | 214547776/1556418832/1770966608 | 214289416/57474848/96114688 | 384/620000 | 0.152468 |
| random | 10000 | 1000 | 13531 | 21399 | 2071024/597279 | 17; 13637/27773712 | 384965/978220/1667920/357980/63130 | 25/41/6/10 | 74415/10/9525736 | 214151872/1556418832/1770570704 | 213987568/9523152/25078880 | 24576/655040 | 0.032943 |
| random | 100000 | 1 | 38945 | 29994 | 20701024/5966135 | 20; 136937/2754120037 | 4878819/12579675/22726041/5204289/784978 | 1568929/1768912/99983/100000 | 9438134/100000/1212881288 | 3028543744/154241677032/157270220776 | 2999441264/1212881296/1784628184 | 24/3200000 | 2.272815 |
| random | 100000 | 16 | 24979 | 29179 | 20701024/5966471 | 20; 136937/2754120037 | 4878819/12579675/22726041/5204289/784978 | 73059/85546/6237/6250 | 7113694/6250/910852968 | 2920808320/154241677032/157162485352 | 2917889696/910850384/1446812264 | 384/6200000 | 1.371066 |
| random | 100000 | 1000 | 27593 | 29139 | 20701024/5969831 | 20; 136937/2754120037 | 4878819/12579675/22726041/5204289/784978 | 573/766/93/100 | 2601719/100/333024968 | 2915536000/154241677032/157157213032 | 2913886256/333022384/702276920 | 24576/6550400 | 1.021333 |
| full-cover | 100 | 1 | 3421 | 6260 | 21724/6395 | 8; 198/297 | 1751/4163/4559/99/239 | 573/766/93/100 | 870/100/116296 | 639104/32472/671576 | 626160/113712/626160 | 24/3200 | 0.000270 |
| full-cover | 100 | 16 | 3015 | 5897 | 21724/6475 | 8; 198/297 | 1751/4163/4559/99/239 | 14/25/4/7 | 245/7/31832 | 591680/32472/624152 | 589848/29376/589848 | 384/6176 | 0.000200 |
| full-cover | 100 | 1000 | 2851 | 5899 | 21724/6267 | 8; 198/297 | 1751/4163/4559/99/239 | 0/1/0/1 | 198/1/25528 | 591936/32472/624408 | 589992/23072/589992 | 3072/8160 | 0.000200 |
| full-cover | 1000 | 1 | 5475 | 8944 | 208024/60055 | 11; 1998/2997 | 23962/61328/65324/999/2486 | 8977/10967/990/1000 | 11974/1000/1580808 | 9127488/327672/9455160 | 8944128/1578352/4395680 | 24/32000 | 0.001701 |
| full-cover | 1000 | 16 | 4577 | 8431 | 208024/60015 | 11; 1998/2997 | 23962/61328/65324/999/2486 | 315/435/57/63 | 2627/63/339416 | 8453440/327672/8781112 | 8431096/336960/1937912 | 384/61984 | 0.001056 |
| full-cover | 1000 | 1000 | 4444 | 8411 | 208024/60295 | 11; 1998/2997 | 23962/61328/65324/999/2486 | 0/1/0/1 | 1998/1/255928 | 8427200/327672/8754872 | 8410920/253472/1866272 | 24576/65504 | 0.000957 |
| full-cover | 10000 | 1 | 7480 | 11710 | 2071024/595807 | 15; 19998/29997 | 307245/816654/856650/9999/24982 | 123617/143603/9986/10000 | 153613/10000/20142600 | 119481920/3279672/122761592 | 117104384/20142608/42701464 | 24/320000 | 0.023588 |
| full-cover | 10000 | 16 | 5590 | 11045 | 2071024/597591 | 15; 19998/29997 | 307245/816654/856650/9999/24982 | 5227/6467/615/625 | 28348/625/3658680 | 110705216/3279672/113984888 | 110446856/3656096/17202344 | 384/620000 | 0.010962 |
| full-cover | 10000 | 1000 | 5268 | 11014 | 2071024/596407 | 15; 19998/29997 | 307245/816654/856650/9999/24982 | 25/41/6/10 | 20131/10/2577384 | 110309312/3279672/113588984 | 110145008/2577392/14402368 | 24576/655040 | 0.009584 |
| full-cover | 100000 | 1 | 11623 | 14426 | 20701024/5961407 | 18; 199998/299997 | 3737873/10163519/10563515/99999/249979 | 1568929/1768912/99983/100000 | 1868926/100000/244022664 | 1471740416/32799672/1504540088 | 1442637936/244021592/514624696 | 24/3200000 | 0.445699 |
| full-cover | 100000 | 16 | 8967 | 13611 | 20701024/5968015 | 18; 199998/299997 | 3737873/10163519/10563515/99999/249979 | 73059/85546/6237/6250 | 304306/6250/39251304 | 1364004992/32799672/1396804664 | 1361086368/39248720/123917072 | 384/6200000 | 0.191004 |
| full-cover | 100000 | 1000 | 7255 | 13571 | 20701024/5966591 | 18; 199998/299997 | 3737873/10163519/10563515/99999/249979 | 573/766/93/100 | 201669/100/25818568 | 1358732672/32799672/1391532344 | 1357082928/25818576/135983968 | 24576/6550400 | 0.139680 |
| cycle | 100 | 1 | 3691 | 6506 | 21214/5805 | 8; 100/100 | 1600/4268/4751/99/253 | 573/766/93/100 | 1031/100/136904 | 663680/13600/677280 | 650736/134320/650736 | 24/3200 | 0.000228 |
| cycle | 100 | 16 | 3066 | 6143 | 21214/5917 | 8; 100/100 | 1600/4268/4751/99/253 | 14/25/4/7 | 268/7/34776 | 616256/13600/629856 | 614424/32320/614424 | 384/6176 | 0.000179 |
| cycle | 100 | 1000 | 2821 | 6144 | 21214/5829 | 8; 100/100 | 1600/4268/4751/99/253 | 0/1/0/1 | 199/1/25656 | 616512/13600/630112 | 614568/23200/614568 | 3072/8160 | 0.000187 |
| cycle | 1000 | 1 | 5355 | 9328 | 203804/55235 | 11; 500/1000 | 21904/65348/68321/999/2722 | 8977/10967/990/1000 | 15662/1000/2052872 | 9511104/96000/9607104 | 9327744/2050416/4861328 | 24/32000 | 0.002061 |
| cycle | 1000 | 16 | 4456 | 8815 | 203804/55779 | 11; 500/1000 | 21904/65348/68321/999/2722 | 315/435/57/63 | 3311/63/426968 | 8837056/96000/8933056 | 8814712/424512/2369792 | 384/61984 | 0.001087 |
| cycle | 1000 | 1000 | 4171 | 8794 | 203804/55963 | 11; 500/1000 | 21904/65348/68321/999/2722 | 0/1/0/1 | 999/1/128056 | 8810816/96000/8906816 | 8794536/125600/2170656 | 24576/65504 | 0.000898 |
| cycle | 10000 | 1 | 6401 | 9918 | 2028824/556135 | 11; 500/10000 | 231910/704672/716645/9999/28480 | 123617/143603/9986/10000 | 173252/10000/22656392 | 101561280/600000/102161280 | 99183744/22656400/48411200 | 24/320000 | 0.028852 |
| cycle | 10000 | 16 | 4697 | 9253 | 2028824/555007 | 11; 500/10000 | 231910/704672/716645/9999/28480 | 5227/6467/615/625 | 36759/625/4735288 | 92784576/600000/93384576 | 92526216/4732448/17240224 | 384/620000 | 0.010572 |
| cycle | 10000 | 1000 | 4162 | 9222 | 2028824/555919 | 11; 500/10000 | 231910/704672/716645/9999/28480 | 25/41/6/10 | 9990/10/1279336 | 92388672/600000/92988672 | 92224368/1279344/10051160 | 24576/655040 | 0.006340 |
| cycle | 100000 | 1 | 8854 | 10121 | 20279024/5545151 | 11; 500/100000 | 2331970/7097912/7199885/99999/286060 | 1568929/1768912/99983/100000 | 1749152/100000/228691592 | 1041195776/5640000/1046835776 | 1012093296/228691600/444071800 | 24/3200000 | 0.403932 |
| cycle | 100000 | 16 | 5962 | 9305 | 20279024/5542807 | 11; 500/100000 | 2331970/7097912/7199885/99999/286060 | 73059/85546/6237/6250 | 371109/6250/47802088 | 933460352/5640000/939100352 | 930541728/47799504/142554200 | 384/6200000 | 0.169972 |
| cycle | 100000 | 1000 | 3786 | 9265 | 20279024/5541999 | 11; 500/100000 | 2331970/7097912/7199885/99999/286060 | 573/766/93/100 | 99900/100/12792136 | 928188032/5640000/933828032 | 926538288/12792144/102390320 | 24576/6550400 | 0.061068 |
| unique-0 | 100 | 1 | 3641 | 6800 | 24324/8987 | 9; 100/100 | 1839/4495/4981/99/267 | 573/766/93/100 | 1097/100/145352 | 693120/13600/706720 | 680176/142768/680176 | 24/3200 | 0.000243 |
| unique-0 | 100 | 16 | 3460 | 6437 | 24324/8891 | 9; 100/100 | 1839/4495/4981/99/267 | 14/25/4/7 | 467/7/60248 | 645696/13600/659296 | 643864/57792/643864 | 384/6176 | 0.000202 |
| unique-0 | 100 | 1000 | 3595 | 6439 | 24324/8979 | 9; 100/100 | 1839/4495/4981/99/267 | 0/1/0/1 | 196/1/25272 | 645952/13600/659552 | 644008/22816/644008 | 3072/8160 | 0.000179 |
| unique-0 | 1000 | 1 | 7272 | 9982 | 234024/86407 | 13; 1000/1000 | 25972/69145/73436/999/3284 | 8977/10967/990/1000 | 17058/1000/2231560 | 10165824/136000/10301824 | 9982464/2229104/4429104 | 24/32000 | 0.002231 |
| unique-0 | 1000 | 16 | 6167 | 9469 | 234024/86503 | 13; 1000/1000 | 25972/69145/73436/999/3284 | 315/435/57/63 | 9534/63/1223512 | 9491776/136000/9627776 | 9469432/1221056/3158440 | 384/61984 | 0.001593 |
| unique-0 | 1000 | 1000 | 4945 | 9449 | 234024/86063 | 13; 1000/1000 | 25972/69145/73436/999/3284 | 0/1/0/1 | 1652/1/211640 | 9465536/136000/9601536 | 9449256/209184/2904864 | 24576/65504 | 0.001199 |
| unique-0 | 10000 | 1 | 9649 | 12821 | 2331024/858415 | 16; 10000/10000 | 319258/911394/943420/9999/33162 | 123617/143603/9986/10000 | 224917/10000/29269512 | 130588480/1360000/131948480 | 128210944/29268424/56713024 | 24/320000 | 0.038124 |
| unique-0 | 10000 | 16 | 8627 | 12155 | 2331024/856495 | 16; 10000/10000 | 319258/911394/943420/9999/33162 | 5227/6467/615/625 | 146764/625/18815928 | 121811776/1360000/123171776 | 121553416/18813344/39546832 | 384/620000 | 0.026877 |
| unique-0 | 10000 | 1000 | 6667 | 12125 | 2331024/857271 | 16; 10000/10000 | 319258/911394/943420/9999/33162 | 25/41/6/10 | 38460/10/4923496 | 121415872/1360000/122775872 | 121251568/4920912/18278112 | 24576/655040 | 0.012492 |
| unique-0 | 100000 | 1 | 13322 | 15970 | 23301024/8564503 | 19; 100000/100000 | 3773666/11467864/11769804/99999/322009 | 1568929/1768912/99983/100000 | 2828454/100000/366842248 | 1626145408/13600000/1639745408 | 1597042928/366842256/689530824 | 24/3200000 | 0.653194 |
| unique-0 | 100000 | 16 | 11566 | 15155 | 23301024/8567031 | 19; 100000/100000 | 3773666/11467864/11769804/99999/322009 | 73059/85546/6237/6250 | 2047035/6250/262320616 | 1518409984/13600000/1532009984 | 1515491360/262318032/577504328 | 384/6200000 | 0.573837 |
| unique-0 | 100000 | 1000 | 10140 | 15115 | 23301024/8561951 | 19; 100000/100000 | 3773666/11467864/11769804/99999/322009 | 573/766/93/100 | 757108/100/96914760 | 1513137664/13600000/1526737664 | 1511487920/96912176/292714272 | 24576/6550400 | 0.300424 |
| unique-0.01 | 100 | 1 | 3442 | 6800 | 24324/8955 | 9; 100/100 | 1839/4495/4981/99/267 | 573/766/93/100 | 1097/100/145352 | 693120/13600/706720 | 680176/142768/680176 | 24/3200 | 0.000266 |
| unique-0.01 | 100 | 16 | 2959 | 6437 | 24324/8859 | 9; 100/100 | 1839/4495/4981/99/267 | 14/25/4/7 | 467/7/60248 | 645696/13600/659296 | 643864/57792/643864 | 384/6176 | 0.000205 |
| unique-0.01 | 100 | 1000 | 2991 | 6439 | 24324/8939 | 9; 100/100 | 1839/4495/4981/99/267 | 0/1/0/1 | 196/1/25272 | 645952/13600/659552 | 644008/22816/644008 | 3072/8160 | 0.000187 |
| unique-0.01 | 1000 | 1 | 6502 | 9975 | 234024/86263 | 13; 996/1000 | 25959/69097/73378/999/3259 | 8977/10967/990/1000 | 17043/1000/2229640 | 10158400/135680/10294080 | 9975040/2227184/4413856 | 24/32000 | 0.002195 |
| unique-0.01 | 1000 | 16 | 5199 | 9462 | 234024/85647 | 13; 996/1000 | 25959/69097/73378/999/3259 | 315/435/57/63 | 9498/63/1218904 | 9484352/135680/9620032 | 9462008/1216448/3159848 | 384/61984 | 0.001547 |
| unique-0.01 | 1000 | 1000 | 4812 | 9442 | 234024/86031 | 13; 996/1000 | 25959/69097/73378/999/3259 | 0/1/0/1 | 1647/1/211000 | 9458112/135680/9593792 | 9441832/208544/2896160 | 24576/65504 | 0.001171 |
| unique-0.01 | 10000 | 1 | 9045 | 12809 | 2331024/857119 | 16; 9906/10482 | 319001/910212/942458/10223/33206 | 123617/143603/9986/10000 | 224978/10000/29277320 | 130465344/1379472/131844816 | 128087808/29276232/56884936 | 24/320000 | 0.038647 |
| unique-0.01 | 10000 | 16 | 9285 | 12143 | 2331024/857215 | 16; 9906/10482 | 319001/910212/942458/10223/33206 | 5227/6467/615/625 | 146767/625/18816312 | 121688640/1379472/123068112 | 121430280/18813728/44063056 | 384/620000 | 0.031960 |
| unique-0.01 | 10000 | 1000 | 7511 | 12113 | 2331024/856279 | 16; 9906/10482 | 319001/910212/942458/10223/33206 | 25/41/6/10 | 38601/10/4941544 | 121292736/1379472/122672208 | 121128432/4938960/16772064 | 24576/655040 | 0.012404 |
| unique-0.01 | 100000 | 1 | 23126 | 15966 | 23301024/8564055 | 19; 98989/148474 | 3773459/11451574/11766070/107518/322038 | 1568929/1768912/99983/100000 | 2839816/100000/368296584 | 1625667456/16233664/1641901120 | 1596564976/368296592/742057096 | 24/3200000 | 0.752007 |
| unique-0.01 | 100000 | 16 | 13067 | 15150 | 23301024/8570887 | 19; 98989/148474 | 3773459/11451574/11766070/107518/322038 | 73059/85546/6237/6250 | 2056510/6250/263533416 | 1517932032/16233664/1534165696 | 1515013408/263530832/538626448 | 384/6200000 | 0.531339 |
| unique-0.01 | 100000 | 1000 | 10254 | 15110 | 23301024/8570175 | 19; 98989/148474 | 3773459/11451574/11766070/107518/322038 | 573/766/93/100 | 763307/100/97708232 | 1512659712/16233664/1528893376 | 1511009968/97705648/312278160 | 24576/6550400 | 0.318813 |
| unique-0.1 | 100 | 1 | 3802 | 6800 | 24324/8915 | 9; 100/100 | 1839/4495/4981/99/267 | 573/766/93/100 | 1097/100/145352 | 693120/13600/706720 | 680176/142768/680176 | 24/3200 | 0.000259 |
| unique-0.1 | 100 | 16 | 3460 | 6437 | 24324/8939 | 9; 100/100 | 1839/4495/4981/99/267 | 14/25/4/7 | 467/7/60248 | 645696/13600/659296 | 643864/57792/643864 | 384/6176 | 0.000212 |
| unique-0.1 | 100 | 1000 | 3530 | 6439 | 24324/8923 | 9; 100/100 | 1839/4495/4981/99/267 | 0/1/0/1 | 196/1/25272 | 645952/13600/659552 | 644008/22816/644008 | 3072/8160 | 0.000213 |
| unique-0.1 | 1000 | 1 | 7564 | 9979 | 234024/85799 | 13; 962/1006 | 25975/69184/73407/1000/3329 | 8977/10967/990/1000 | 17064/1000/2232328 | 10162112/133296/10295408 | 9978752/2229872/4380920 | 24/32000 | 0.002259 |
| unique-0.1 | 1000 | 16 | 5838 | 9466 | 234024/86047 | 13; 962/1006 | 25975/69184/73407/1000/3329 | 315/435/57/63 | 9466/63/1214808 | 9488064/133296/9621360 | 9465720/1212352/3164328 | 384/61984 | 0.001552 |
| unique-0.1 | 1000 | 1000 | 5328 | 9445 | 234024/85551 | 13; 962/1006 | 25975/69184/73407/1000/3329 | 0/1/0/1 | 1615/1/206904 | 9461824/133296/9595120 | 9445544/204448/2913440 | 24576/65504 | 0.001219 |
| unique-0.1 | 10000 | 1 | 10011 | 12737 | 2331024/859255 | 16; 9087/13652 | 318779/903413/936857/11815/34430 | 123617/143603/9986/10000 | 226590/10000/29483656 | 129748416/1491472/131239888 | 127370880/29482568/52930472 | 24/320000 | 0.038637 |
| unique-0.1 | 10000 | 16 | 8818 | 12071 | 2331024/856575 | 16; 9087/13652 | 318779/903413/936857/11815/34430 | 5227/6467/615/625 | 148260/625/19007416 | 120971712/1491472/122463184 | 120713352/19004832/45163792 | 384/620000 | 0.031599 |
| unique-0.1 | 10000 | 1000 | 6849 | 12041 | 2331024/857535 | 16; 9087/13652 | 318779/903413/936857/11815/34430 | 25/41/6/10 | 38317/10/4905192 | 120575808/1491472/122067280 | 120411504/4902608/18058848 | 24576/655040 | 0.012370 |
| unique-0.1 | 100000 | 1 | 17414 | 15929 | 23301024/8564047 | 19; 90157/525582 | 3771511/11319618/11737619/171259/328096 | 1568929/1768912/99983/100000 | 2935881/100000/380592904 | 1622025728/36645152/1658670880 | 1592923248/380592912/754292664 | 24/3200000 | 0.756955 |
| unique-0.1 | 100000 | 16 | 16652 | 15114 | 23301024/8566015 | 19; 90157/525582 | 3771511/11319618/11737619/171259/328096 | 73059/85546/6237/6250 | 2151091/6250/275639784 | 1514290304/36645152/1550935456 | 1511371680/275637200/551843264 | 384/6200000 | 0.537883 |
| unique-0.1 | 100000 | 1000 | 10692 | 15074 | 23301024/8564263 | 19; 90157/525582 | 3771511/11319618/11737619/171259/328096 | 573/766/93/100 | 810095/100/103697096 | 1509017984/36645152/1545663136 | 1507368240/103694512/316269784 | 24576/6550400 | 0.335553 |
| unique-1 | 100 | 1 | 3637 | 6715 | 24324/8979 | 9; 97/100 | 1830/4436/4914/99/254 | 573/766/93/100 | 1074/100/142408 | 684544/13360/697904 | 671600/139824/671600 | 24/3200 | 0.000279 |
| unique-1 | 100 | 16 | 3109 | 6352 | 24324/9035 | 9; 97/100 | 1830/4436/4914/99/254 | 14/25/4/7 | 451/7/58200 | 637120/13360/650480 | 635288/55744/635288 | 384/6176 | 0.000260 |
| unique-1 | 100 | 1000 | 3160 | 6353 | 24324/8971 | 9; 97/100 | 1830/4436/4914/99/254 | 0/1/0/1 | 193/1/24888 | 637376/13360/650736 | 635432/22432/635432 | 3072/8160 | 0.000198 |
| unique-1 | 1000 | 1 | 6825 | 9697 | 234024/86271 | 12; 626/1000 | 25978/67712/71204/999/3491 | 8977/10967/990/1000 | 16584/1000/2170888 | 9880128/106080/9986208 | 9696768/2168432/4838472 | 24/32000 | 0.002219 |
| unique-1 | 1000 | 16 | 6615 | 9184 | 234024/86247 | 12; 626/1000 | 25978/67712/71204/999/3491 | 315/435/57/63 | 9028/63/1158744 | 9206080/106080/9312160 | 9183736/1156288/2864336 | 384/61984 | 0.001541 |
| unique-1 | 1000 | 1000 | 5435 | 9163 | 234024/85591 | 12; 626/1000 | 25978/67712/71204/999/3491 | 0/1/0/1 | 1251/1/160312 | 9179840/106080/9285920 | 9163560/157856/2617376 | 24576/65504 | 0.001190 |
| unique-1 | 10000 | 1 | 8874 | 11167 | 2331024/856391 | 13; 1024/10000 | 301235/800106/814190/9999/39583 | 123617/143603/9986/10000 | 196732/10000/25661832 | 114047040/641920/114688960 | 111669504/25660760/52580368 | 24/320000 | 0.036675 |
| unique-1 | 10000 | 16 | 6908 | 10501 | 2331024/856791 | 13; 1024/10000 | 301235/800106/814190/9999/39583 | 5227/6467/615/625 | 118625/625/15214136 | 105270336/641920/105912256 | 105011976/15211552/38824328 | 384/620000 | 0.023766 |
| unique-1 | 10000 | 1000 | 5091 | 10471 | 2331024/857879 | 13; 1024/10000 | 301235/800106/814190/9999/39583 | 25/41/6/10 | 17947/10/2297832 | 104874432/641920/105516352 | 104710128/2297840/12387584 | 24576/655040 | 0.007431 |
| unique-1 | 100000 | 1 | 9469 | 11508 | 23301024/8567831 | 13; 1024/100000 | 3070001/8179406/8283490/99999/406189 | 1568929/1768912/99983/100000 | 2011331/100000/262250504 | 1179897216/5681920/1185579136 | 1150794736/262250512/558238584 | 24/3200000 | 0.474458 |
| unique-1 | 100000 | 16 | 8302 | 10692 | 23301024/8565263 | 13; 1024/100000 | 3070001/8179406/8283490/99999/406189 | 73059/85546/6237/6250 | 1233526/6250/158191464 | 1072161792/5681920/1077843712 | 1069243168/158188880/371607688 | 384/6200000 | 0.300351 |
| unique-1 | 100000 | 1000 | 5787 | 10652 | 23301024/8566655 | 13; 1024/100000 | 3070001/8179406/8283490/99999/406189 | 573/766/93/100 | 188148/100/24087880 | 1066889472/5681920/1072571392 | 1065239728/24085296/141856160 | 24576/6550400 | 0.087391 |
