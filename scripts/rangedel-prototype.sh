#!/usr/bin/env bash
set -euo pipefail

# Keep the release binary and raw checkpoint evidence together.
if [[ $# != 1 ]]; then
  echo "usage: scripts/rangedel-prototype.sh OUTPUT_DIRECTORY" >&2
  exit 2
fi
output=$1
mkdir -p "$output"
output=$(cd "$output" && pwd)
if [[ -e "$output/pebble.test" ]]; then
  echo "output directory already contains a prototype binary" >&2
  exit 2
fi
export GOMAXPROCS=1
{
  jj log -r @ -T 'commit_id ++ "\n"' --no-graph
  go version
  uname -a
  if [[ $(uname) == Darwin ]]; then
    sysctl -n machdep.cpu.brand_string
    sw_vers
  fi
  echo "GOMAXPROCS=$GOMAXPROCS"
  echo "SAMPLES=${SAMPLES:-10} BENCHTIME=${BENCHTIME:-1s}"
  echo "build: go test -c -o $output/pebble.test ."
} > "$output/metadata.txt"
go test -c -o "$output/pebble.test" .
set +e
PEBBLE_RANGEDEL_CHECKPOINT=1 "$output/pebble.test" -test.run '^TestRangeDelIndexCheckpoint$' -test.v -test.timeout 2h > "$output/checkpoint.log" 2>&1
status=$?
set -e
echo "checkpoint exit=$status" >> "$output/commands.log"
if [[ $status != 0 ]]; then
  cat "$output/checkpoint.log" >&2
  exit "$status"
fi
for ((sample=1; sample<=${SAMPLES:-10}; sample++)); do
  echo "sample $sample" >&2
  filter=${BENCH_FILTER:-^BenchmarkRangeDelIntervalIndex$}
  echo "GOMAXPROCS=1 pebble.test -test.run ^$ -test.bench $filter -test.benchtime ${BENCHTIME:-1s} -test.timeout 2h" >> "$output/commands.log"
  set +e
  "$output/pebble.test" -test.run '^$' -test.bench "$filter" -test.benchtime "${BENCHTIME:-1s}" -test.timeout 2h > "$output/benchmark-$sample.txt" 2>&1
  status=$?
  set -e
  echo "sample $sample exit=$status" >> "$output/commands.log"
  if [[ $status != 0 ]]; then
    cat "$output/benchmark-$sample.txt" >&2
    exit "$status"
  fi
done
if command -v benchstat > /dev/null; then
  cat "$output"/benchmark-*.txt > "$output/benchmark-combined.txt"
  benchstat "$output/benchmark-combined.txt" > "$output/benchstat.txt"
fi
