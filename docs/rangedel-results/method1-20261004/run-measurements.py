import json
import os
from pathlib import Path
import subprocess
import time

root = Path('/tmp/pebble-method1-20261004')
repo = '/Users/timods/git/cockroachdb/pebble'
baseline_repo = '/tmp/pebble-method1-baseline-20261004'
env = dict(os.environ, GOMAXPROCS='1')
filter_arg = '^BenchmarkRangeDelIntervalPrototype$/./n=100000/'

def capture(argv, cwd=repo):
    return subprocess.check_output(argv, cwd=cwd, env=env, text=True).strip()

metadata = {
    'baseline_parent': '991cea2e60d728fea07de1868a99f1451551b4fc',
    'baseline_workspace': capture(['jj', 'log', '-r', '@', '--no-graph', '-T', 'commit_id'], baseline_repo),
    'candidate': capture(['jj', 'log', '-r', '@', '--no-graph', '-T', 'commit_id']),
    'go': capture(['go', 'version']),
    'host': capture(['uname', '-a']),
    'cpu': capture(['sysctl', '-n', 'machdep.cpu.brand_string']),
    'os': capture(['sw_vers']),
    'GOMAXPROCS': 1,
    'samples': 10,
    'benchtime': '1x',
    'filter': filter_arg,
    'start_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
}
(root / 'metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')

def run(label, argv, out, extra_env=None):
    started = time.monotonic()
    command_env = dict(env, **(extra_env or {}))
    print(f'{label}: started', flush=True)
    with out.open('w') as output:
        result = subprocess.run(argv, cwd=repo, env=command_env, stdout=output,
                                stderr=subprocess.STDOUT, timeout=7200)
    record = {'label': label, 'argv': argv, 'environment': extra_env or {},
              'exit': result.returncode, 'seconds': time.monotonic() - started,
              'output': str(out)}
    with (root / 'commands.jsonl').open('a') as log:
        log.write(json.dumps(record) + '\n')
    print(f'{label}: exit={result.returncode} elapsed={record["seconds"]:.1f}s', flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)

for variant in ['baseline', 'candidate']:
    folder = root / variant
    run(f'{variant} checkpoint', [str(folder / 'pebble.test'),
        '-test.run', '^TestRangeDelPrototypeCheckpoint$', '-test.v', '-test.timeout', '2h'],
        folder / 'checkpoint.log', {'PEBBLE_RANGEDEL_CHECKPOINT': '1'})

for sample in range(1, 11):
    order = ['baseline', 'candidate'] if sample % 2 else ['candidate', 'baseline']
    for variant in order:
        folder = root / variant
        run(f'{variant} sample {sample}', [str(folder / 'pebble.test'), '-test.run', '^$',
            '-test.bench', filter_arg, '-test.benchtime', '1x', '-test.timeout', '2h'],
            folder / f'benchmark-{sample}.txt')

for variant in ['baseline', 'candidate']:
    folder = root / variant
    (folder / 'benchmark-combined.txt').write_text(''.join(
        (folder / f'benchmark-{sample}.txt').read_text() for sample in range(1, 11)))
run('benchstat', ['/Users/timods/go/bin/benchstat',
    str(root / 'baseline/benchmark-combined.txt'),
    str(root / 'candidate/benchmark-combined.txt')], root / 'benchstat.txt')
