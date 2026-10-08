#!/usr/bin/env bash
set -euo pipefail
ws=rangedel-db-4way
run_id=20261005-fourway-prod-point-v1
run_dir="$HOME/bench/$ws/runs/$run_id"
mkdir -p "$run_dir"
cd "$run_dir"
trap 'rc=$?; printf "%s\n" "$rc" > exit_code; touch done' EXIT
cat > meta.txt <<'EOF'
study=Pebble range deletion DB comparison with point-only commits and timed flush
master_harness=197f5ab769fa7905bc0370313d468a9db6eaa114
p1=23ea42e87e3b2d95c208b0c66a878cd99b14fbfd
method1=b2b36d2c7cc2c9b77816c0cdcfa91e5d35a21139
method12=48e86e8f9676ff16206b14c119ac195f05bce451
count=5
duration=60s
gomaxprocs=4
db_options=prod-writeheavy
writer_interval=2ms
sets_per_batch=-1
EOF
/home/coder/.local/share/mise/installs/go/1.27.0/bin/go version >> meta.txt
uname -a >> meta.txt
lscpu | grep -m1 'Model name' >> meta.txt
touch commands.txt
arms=(master p1 method1 method12)
declare -A sha=(
  [master]=197f5ab769fa7905bc0370313d468a9db6eaa114
  [p1]=23ea42e87e3b2d95c208b0c66a878cd99b14fbfd
  [method1]=b2b36d2c7cc2c9b77816c0cdcfa91e5d35a21139
  [method12]=48e86e8f9676ff16206b14c119ac195f05bce451
)
for ((sample=3; sample<=5; sample++)); do
  for ((slot=0; slot<4; slot++)); do
    idx=$(((slot+sample-1)%4))
    arm=${arms[$idx]}
    bin="$HOME/bench/$ws/bin/${sha[$arm]}-pebble"
    dbdir="$run_dir/db-unique-$sample-$arm"
    cmd=(env GOMAXPROCS=4 "$bin" bench rangedel "$dbdir" --wipe
      --duration=60s --cache=16777216 --readers=4 --reader-interval=1ms
      --db-options=prod-writeheavy --writers=2 --writer-interval=2ms
      --rangedel-shape=unique --rangedel-queues=1024
      --rangedel-overlap-frac=0.1 --rangedels-per-batch=16
      --sets-per-batch=-1 --iter-frac=0.5)
    output="unique-$sample-$arm"
    printf '%q ' /usr/bin/time -v -o "$run_dir/$output.time" "${cmd[@]}" >> commands.txt
    printf '> %q 2> %q\n' "$run_dir/$output.out" "$run_dir/$output.err" >> commands.txt
    /usr/bin/time -v -o "$run_dir/$output.time" "${cmd[@]}" \
      > "$run_dir/$output.out" 2> "$run_dir/$output.err"
    echo "done sample=$sample arm=$arm" >> progress.txt
  done
done
