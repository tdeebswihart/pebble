# Range-Deletion Benchmark Results

Raw output from the interval-tree benchmark runs, kept for later comparison.
Test binaries were left out; Go profiles (`*.prof`) are included and carry
their own symbols.

| Directory | What it is |
| --- | --- |
| `step0-20261004/` | Step 0 checkpoint microbenchmarks |
| `method1-20261004/` | Method 1 vs. baseline microbenchmarks |
| `method2-20261005/` | Method 2 vs. method 1 microbenchmarks |
| `dbbench/20261005-fourway-v2/` | Four-way DB benchmark (master, P1, method 1, methods 1+2) |
| `dbbench/20261005-fourway-prod-flush-v1/` | Four-way run with the production write-heavy profile |
| `dbbench/20261005-fourway-prod-point-v1/` | Four-way production-profile run with alternating point-Set writers |
| `dbbench/rangedel-db-fixeddepth/20261005T183721Z/` | Fixed-depth method 1 vs. method 2 DB benchmark |
| `dbbench/20261005-method12-write-profile/` | Methods 1+2 writer-only CPU and heap profiles, 10% overlap |
| `dbbench/20261005-method12-overlap-profile/` | Methods 1+2 writer-only CPU and heap profiles, 50% overlap |

Each directory records its own commands and metadata (`commands.*`,
`metadata.*`, `meta.txt`, or `run.sh`).
