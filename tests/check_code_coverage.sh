#!/usr/bin/env bash
# Run from an out-of-tree build configured with --enable-code-coverage.
set -euo pipefail

build_jobs=${JOBS:-2}

# Check the instrumentation itself: the HDR unit makes 4 * 10000 concurrent
# atomic recordings plus 12 clamping calls. Non-atomic profile counters can
# undercount these even when the unit passes, or make lcov reject the trace.
make -j"$build_jobs" hdr_histogram_test
find . -name '*.gcda' -delete
./hdr_histogram_test
make code-coverage-capture CODE_COVERAGE_IGNORE_PATTERN= \
  CODE_COVERAGE_OUTPUT_FILE=coverage-threaded.info \
  CODE_COVERAGE_OUTPUT_DIRECTORY=coverage-threaded
awk -F, '$1 ~ /^FNDA:/ && $2 == "hdr_record_value_capped_atomic" {
             seen=1; if ($1 != "FNDA:40012") bad=1
         }
         END { exit (!seen || bad) }' coverage-threaded.info

make -j"$build_jobs" check
./memtier_benchmark --version

# A deliberately absent exclusion must be harmless on both lcov 1 and 2.
# Keep the normal exclusions too, so a successful capture still filters them.
make code-coverage-capture \
  CODE_COVERAGE_OUTPUT_FILE=coverage-smoke.info \
  CODE_COVERAGE_OUTPUT_DIRECTORY=coverage-smoke \
  CODE_COVERAGE_IGNORE_PATTERN="'*/deps/hdr_histogram/*' '*/tests/*' '*/absent-coverage-input/*'"
test -s coverage-smoke/index.html
awk '/^SF:/ { in_main=($0 ~ /memtier_benchmark\.cpp$/) }
     in_main && /^DA:[0-9]+,[1-9][0-9]*$/ { main_hit=1 }
     END { exit !main_hit }' coverage-smoke.info
if grep -E '^SF:.*(/deps/hdr_histogram/|/tests/)' coverage-smoke.info; then
  echo "Excluded source files remain in coverage report" >&2
  exit 1
fi

# Empty exclusions mean retain every collected source, not an invalid lcov
# invocation with no --remove patterns.
make code-coverage-capture CODE_COVERAGE_IGNORE_PATTERN= \
  CODE_COVERAGE_OUTPUT_FILE=coverage-all.info \
  CODE_COVERAGE_OUTPUT_DIRECTORY=coverage-all
test -s coverage-all/index.html
grep -qE '^SF:.*/deps/hdr_histogram/' coverage-all.info

check_no_coverage_data() {
  if find . \( -name '*.gcda' -o -name '*.gcno' -o -name '*.gcov' \) -print -quit | grep -q .; then
    echo "Coverage data remains after $1" >&2
    exit 1
  fi
}
# Exercise both clean targets with actual generated notes/counters.
test -n "$(find . -name '*.gcno' -print -quit)"
test -n "$(find . -name '*.gcda' -print -quit)"
make clean
check_no_coverage_data clean
make -j"$build_jobs" memtier_benchmark
./memtier_benchmark --version
test -n "$(find . -name '*.gcno' -print -quit)"
test -n "$(find . -name '*.gcda' -print -quit)"
make distclean
check_no_coverage_data distclean
