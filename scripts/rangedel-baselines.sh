#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  rangedel-baselines.sh build ROOT [BASELINE_SHA [CANDIDATE_SHA]]
  rangedel-baselines.sh smoke ROOT [baseline|candidate]
  rangedel-baselines.sh compare ROOT [micro|ycsb|cycle|unique]

Build creates JJ workspaces and release binaries. The default baseline is
ee9323c5e2f74518b0432e071e26971c23940ff0. Omit CANDIDATE_SHA for baseline only.
Smoke checks reduced inputs. Compare alternates baseline/candidate samples.

Compare settings (environment variables):
  SAMPLES=10 BENCHTIME=1s DURATION=60s
  INITIAL_KEYS=10000000 WORKERS=256 WARMUP=10000
  MICRO_FILTER='^BenchmarkMemTableRangeDelBaseline$/.*/(disjoint|concentric)/n=1000$/batch=16$/apply$'
  GOMAXPROCS defaults to 1 for memtable/DB epochs; YCSB/CLI use Go's default.

Results and command records go into a fresh directory under ROOT for each run.
Compare requires benchstat for micro/YCSB. CLI output is saved without benchstat.
EOF
}

if [[ ${1:-} == --help || ${1:-} == -h ]]; then
  usage
  exit 0
fi
if (( $# < 2 )); then
  usage >&2
  exit 1
fi

mode=$1
bench_root=$2
shift 2
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

metadata() {
  go version
  go env GOOS GOARCH GOFLAGS
  uname -a
  if [[ $(uname -s) == Darwin ]]; then
    sw_vers
    sysctl -n machdep.cpu.brand_string
  elif command -v lscpu >/dev/null; then
    lscpu
  fi
  printf 'GOMAXPROCS=%s\n' "${GOMAXPROCS:-Go default (epochs use 1)}"
  printf 'SAMPLES=%s BENCHTIME=%s DURATION=%s\n' "${SAMPLES:-10}" "${BENCHTIME:-1s}" "${DURATION:-60s}"
  printf 'INITIAL_KEYS=%s WORKERS=%s WARMUP=%s\n' "${INITIAL_KEYS:-10000000}" "${WORKERS:-256}" "${WARMUP:-10000}"
}

run_logged() {
  local checkout=$1 output=$2
  shift 2
  {
    printf '(cd %q && env ' "$bench_root/$checkout"
    if [[ -n ${GOMAXPROCS:-} ]]; then
      printf '%q ' "GOMAXPROCS=$GOMAXPROCS"
    else
      printf '%s ' '-u GOMAXPROCS'
    fi
    printf '%q ' "$@"
    printf ')\n'
  } > "$output.command"
  (cd "$bench_root/$checkout" && "$@") | tee "$output"
}

micro() {
  local checkout=$1 output=$2 filter=$3 benchtime=$4
  GOMAXPROCS=${GOMAXPROCS:-1} run_logged "$checkout" "$output" \
    "$bench_root/$checkout-root.test" -test.run='^$' -test.bench="$filter" \
    -test.benchtime="$benchtime" -test.count=1 -test.benchmem
}

ycsb() {
  local checkout=$1 output=$2 benchtime=$3 keys=$4 workers=$5 warmup=$6
  run_logged "$checkout" "$output" "$bench_root/$checkout-bench.test" \
    -test.run='^$' -test.bench='^BenchmarkYCSB$' -test.benchtime="$benchtime" \
    -test.count=1 -test.benchmem -ycsb-bench-initial-keys="$keys" \
    -ycsb-bench-fixture-dir="$bench_root/ycsb-fixtures" \
    -ycsb-bench-concurrency="$workers" -ycsb-bench-warmup-ops="$warmup"
}

rangedel() {
  local checkout=$1 output=$2 shape=$3 duration=$4
  local args=(--duration="$duration" --cache=16777216 --readers=4 --reader-interval=1ms)
  if [[ $shape == cycle ]]; then
    args+=(--writers=1 --writer-interval=10ms)
  else
    args+=(--writers=2 --writer-interval=1ms --rangedel-shape=unique
      --rangedel-queues=1024 --rangedel-overlap-frac=0.1
      --rangedels-per-batch=16 --sets-per-batch=1 --iter-frac=0.5)
  fi
  run_logged "$checkout" "$output" "$bench_root/$checkout-pebble" bench rangedel \
    "$output.db" --wipe "${args[@]}"
}

case $mode in
  build)
    if (( $# > 2 )); then usage >&2; exit 1; fi
    baseline_sha=${1:-ee9323c5e2f74518b0432e071e26971c23940ff0}
    candidate_sha=${2:-}
    mkdir -p "$bench_root"
    bench_root=$(cd "$bench_root" && pwd)
    checkouts=(baseline)
    if [[ -n $candidate_sha ]]; then checkouts+=(candidate); fi
    for checkout in "${checkouts[@]}"; do
      revision=$baseline_sha
      if [[ $checkout == candidate ]]; then revision=$candidate_sha; fi
      jj --repository "$repo_root" workspace add --name "rangedel-benchmark-$checkout-$$" \
        --revision "$revision" "$bench_root/$checkout"
      (
        cd "$bench_root/$checkout"
        GOFLAGS='' go test -c -o "$bench_root/$checkout-root.test" .
        GOFLAGS='' go test -c -o "$bench_root/$checkout-bench.test" ./bench
        GOFLAGS='' go build -o "$bench_root/$checkout-pebble" ./cmd/pebble
        jj log -r '@-' --no-graph -T 'commit_id ++ " " ++ change_id ++ "\n"' \
          > "$bench_root/$checkout-revision.txt"
      )
    done
    metadata > "$bench_root/build-metadata.txt"
    ;;
  smoke|compare)
    if (( $# > 1 )); then usage >&2; exit 1; fi
    bench_root=$(cd "$bench_root" && pwd)
    profile=${1:-micro}
    if [[ $mode == smoke ]]; then
      checkout=${1:-baseline}
      case $checkout in baseline|candidate) ;; *) usage >&2; exit 1 ;; esac
      result_dir=$(mktemp -d "$bench_root/smoke-$checkout.XXXXXX")
      metadata > "$result_dir/metadata.txt"
      printf 'Smoke: epochs=1x/2x YCSB=257x INITIAL_KEYS=1000 WORKERS=8 WARMUP=100 CLI=1s\n' \
        >> "$result_dir/metadata.txt"
      micro "$checkout" "$result_dir/memtable.txt" \
        '^BenchmarkMemTableRangeDelBaseline$/.*/.*/n=100$/batch=16$' 1x
      micro "$checkout" "$result_dir/db.txt" \
        '^BenchmarkRangeDelWriteReadBaseline$/.*/.*/n=100$/batch=16$' 2x
      ycsb "$checkout" "$result_dir/ycsb.txt" 257x 1000 8 100
      rangedel "$checkout" "$result_dir/cycle.txt" cycle 1s
      rangedel "$checkout" "$result_dir/unique.txt" unique 1s
    else
      case $profile in
        micro|ycsb) command -v benchstat >/dev/null ;;
        cycle|unique) ;;
        *) usage >&2; exit 1 ;;
      esac
      samples=${SAMPLES:-10}
      if [[ ! $samples =~ ^[1-9][0-9]*$ ]]; then
        printf 'SAMPLES must be a positive integer\n' >&2
        exit 1
      fi
      result_dir=$(mktemp -d "$bench_root/compare-$profile.XXXXXX")
      metadata > "$result_dir/metadata.txt"
      for (( sample=1; sample<=samples; sample++ )); do
        for checkout in baseline candidate; do
          output="$result_dir/$checkout-$sample.txt"
          case $profile in
            micro)
              micro "$checkout" "$output" \
                "${MICRO_FILTER:-^BenchmarkMemTableRangeDelBaseline$/.*/(disjoint|concentric)/n=1000$/batch=16$/apply$}" \
                "${BENCHTIME:-1s}"
              ;;
            ycsb)
              ycsb "$checkout" "$output" "${BENCHTIME:-1s}" \
                "${INITIAL_KEYS:-10000000}" "${WORKERS:-256}" "${WARMUP:-10000}"
              ;;
            cycle|unique) rangedel "$checkout" "$output" "$profile" "${DURATION:-60s}" ;;
          esac
          cat "$output" >> "$result_dir/$checkout.txt"
        done
      done
      if [[ $profile == micro || $profile == ycsb ]]; then
        benchstat "$result_dir/baseline.txt" "$result_dir/candidate.txt" | tee "$result_dir/benchstat.txt"
      fi
    fi
    printf '\nResults: %s\n' "$result_dir"
    ;;
  *) usage >&2; exit 1 ;;
esac
