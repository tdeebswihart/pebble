#!/usr/bin/env bash
WS=rangedel-db-fixeddepth
RUN_ID=20261005T183721Z
BASE_SHA=9c60a475d2e6fb440b64980b46f75b682acbe7ee
CAND_SHA=48e86e8f9676ff16206b14c119ac195f05bce451
PKG=.
SLUG=root
REGEX=\^BenchmarkRangeDelWriteReadBaseline\$/.\*/\^concentric\$/\^n=1000\$/\^batch=16\$/\^get\$
COUNT=10
BENCHTIME=1s
GMP=1
BENCHFLAGS=''
# One flock around the whole sample loop.
if [ -z "${BENCH_LOCKED:-}" ]; then
  BENCH_LOCKED=1 exec flock "$HOME/bench/.lock" bash "$0"
fi
BENCH="$HOME/bench"
RUN_DIR="$BENCH/$WS/runs/$RUN_ID"
cd "$RUN_DIR" || exit 1
: > baseline.txt; : > candidate.txt; : > bench.log; : > commands.txt
read -r -a extra <<< "$BENCHFLAGS"
rc=0
for ((i = 1; i <= COUNT; i++)); do
  for arm in baseline candidate; do
    if [ "$arm" = baseline ]; then sha=$BASE_SHA; else sha=$CAND_SHA; fi
    cmd=(env)
    [ -n "$GMP" ] && cmd+=("GOMAXPROCS=$GMP")
    cmd+=("$BENCH/$WS/bin/$sha-$SLUG.test" -test.run='^$' "-test.bench=$REGEX"
      -test.count=1 "-test.benchtime=$BENCHTIME" -test.benchmem "${extra[@]}")
    { printf '(cd %q && ' "$BENCH/$WS/src/$sha/$PKG"; printf '%q ' "${cmd[@]}"
      printf '>> %s.txt)\n' "$arm"; } >> commands.txt
    r=0
    (cd "$BENCH/$WS/src/$sha/$PKG" && "${cmd[@]}" >> "$RUN_DIR/$arm.txt" 2>> "$RUN_DIR/bench.log") || r=$?
    [ "$rc" -eq 0 ] && rc=$r
  done
done
echo "$rc" > exit_code
touch done
