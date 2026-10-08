import json
import os
from pathlib import Path
import subprocess
import time

root = Path('/tmp/pebble-method2-20261005')
repo = '/Users/timods/git/cockroachdb/pebble'
env = dict(os.environ, GOMAXPROCS='1')
filter_arg = '^BenchmarkRangeDelIntervalPrototype$/./n=100000/'

def capture(argv):
    return subprocess.check_output(argv, cwd=repo, env=env, text=True).strip()

metadata = {
    'baseline_parent': '78f59f40141f94513490bda672dd8128d7f3acb1',
    'baseline_metadata': (root / 'baseline/metadata.txt').read_text(),
    'candidate_metadata': (root / 'candidate/metadata.txt').read_text(),
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

def run(label, argv, out):
    started = time.monotonic()
    with out.open('w') as output:
        result = subprocess.run(argv, cwd=repo, env=env, stdout=output,
                                stderr=subprocess.STDOUT, timeout=7200)
    record = {'label': label, 'argv': argv, 'exit': result.returncode,
              'seconds': time.monotonic() - started, 'output': str(out)}
    with (root / 'commands.jsonl').open('a') as log:
        log.write(json.dumps(record) + '\n')
    print(f'{label}: exit={result.returncode} elapsed={record["seconds"]:.1f}s', flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)

for sample in range(1, 11):
    order = ['baseline', 'candidate'] if sample % 2 else ['candidate', 'baseline']
    for variant in order:
        folder = root / variant
        run(f'{variant} sample {sample}', [str(folder / 'pebble.test'), '-test.run', '^$',
            '-test.bench', filter_arg, '-test.benchtime', '1x', '-test.timeout', '2h'],
            folder / f'comparison-{sample}.txt')

for variant in ['baseline', 'candidate']:
    folder = root / variant
    (folder / 'comparison-combined.txt').write_text(''.join(
        (folder / f'comparison-{sample}.txt').read_text() for sample in range(1, 11)))
run('benchstat', ['/Users/timods/go/bin/benchstat',
    str(root / 'baseline/comparison-combined.txt'),
    str(root / 'candidate/comparison-combined.txt')], root / 'benchstat.txt')
