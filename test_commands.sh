#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
make -s
run() { echo "TEST: $*"; ./campuscompute "$@" > /tmp/campuscompute_test_output.txt; }
contains() { grep -q "$1" /tmp/campuscompute_test_output.txt; }
run help; contains "Usage:"
for policy in fcfs rr priority compare; do
  if [[ "$policy" = rr || "$policy" = compare ]]; then run "$policy" --quantum 3 --file samples/jobs.csv
  else run "$policy" --file samples/jobs.csv; fi
  contains "utilization\|CPU util"
done
run banker safe; contains "SAFE: sequence"
run banker unsafe; contains "UNSAFE:"
run banker samples/banker_safe.txt; contains "RESULT: SAFE"
run banker samples/banker_unsafe.txt; contains "RESULT: UNSAFE"
run logger; contains "produced=12 consumed=12 pending=0"
test "$(wc -l < campuscompute.log)" -eq 12
run ipc; contains "Shared-memory object unlinked"
run worker normal; contains "status 0"
run worker fail; contains "SIMULATED FAILURE"
run limit; contains "SIGXFSZ"
run all; contains "Failures: 0"
printf '0\n' | ./campuscompute menu >/dev/null
for bad in 'rr --quantum 0' 'rr --quantum nope' 'wrongcmd' 'fcfs --file samples/jobs_invalid.csv'; do
  read -ra args <<< "$bad"
  if ./campuscompute "${args[@]}" >/dev/null 2>&1; then echo "FAIL: invalid input accepted: $bad" >&2;exit 1; fi
done
run cleanup; test ! -e campuscompute.log
printf 'ALL TESTS PASSED\n'
