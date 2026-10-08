#!/usr/bin/env bash
set -euo pipefail
run_dir="$HOME/bench/rangedel-db-4way/runs/20261005-method12-write-profile"
mkdir -p "$run_dir"
cd "$run_dir"
trap 'rc=$?; printf "%s\n" "$rc" > exit_code; touch done' EXIT
bin="$HOME/bench/rangedel-db-4way/bin/48e86e8f9676ff16206b14c119ac195f05bce451-pebble"
cmd=(env GOMAXPROCS=4 "$bin" bench rangedel "$run_dir/db-method12" --wipe
  --duration=60s --cache=16777216 --readers=0 --db-options=prod-writeheavy
  --writers=2 --writer-interval=1ms --rangedel-shape=unique
  --rangedel-queues=1024 --rangedel-overlap-frac=0.1
  --rangedels-per-batch=16 --sets-per-batch=1)
printf '%q ' /usr/bin/time -v -o "$run_dir/method12.time" "${cmd[@]}" > commands.txt
printf '> %q 2> %q\n' "$run_dir/method12.out" "$run_dir/method12.err" >> commands.txt
/usr/bin/time -v -o "$run_dir/method12.time" "${cmd[@]}" > method12.out 2> method12.err
