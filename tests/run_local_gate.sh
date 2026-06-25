#!/usr/bin/env bash
# tests/run_local_gate.sh -- the one-command local gate (PLAN.md §11).
#
# Runs top-to-bottom against a never-configured srcdir using sibling VPATH
# builds for BOTH configure flavors (prometheus default-on and
# --disable-prometheus), the unit harness under each, the RLTest CLI +
# functional + redaction + statsd suites, the manual HTTP smoke (hardening,
# TTL/render-rate, run-boundary monotonicity, teardown race window), error
# paths + IPv6, the sanitizer cells (skippable via GATE_FAST=1), and the
# docs/completion/purity grep audits.  Any failed check aborts the gate.
#
# Usage:
#   ./tests/run_local_gate.sh           # full gate (incl. sanitizers)
#   GATE_FAST=1 ./tests/run_local_gate.sh   # skip the S9 sanitizer stage
set -euo pipefail
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"; cd "$SRC"
fail() { echo "GATE FAIL: $*" >&2; exit 1; }
assert_eq() { [ "$1" = "$2" ] || fail "$3 (got '$1', want '$2')"; }
PIDS=(); cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT
free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1])'; }
# promtool: native -> pinned docker (image must be present locally; pull is
# attempted once and degrades to a warn+skip when blocked/offline) -> FAIL.
PROM_DOCKER_IMAGE="prom/prometheus:v2.53.2"
promtool_check() {  # stdin: exposition text.
  if command -v promtool >/dev/null; then
    promtool check metrics
  elif command -v docker >/dev/null; then
    if ! docker image inspect "$PROM_DOCKER_IMAGE" >/dev/null 2>&1; then
      docker pull "$PROM_DOCKER_IMAGE" >/dev/null 2>&1 || true
    fi
    if docker image inspect "$PROM_DOCKER_IMAGE" >/dev/null 2>&1; then
      docker run --rm -i --entrypoint promtool "$PROM_DOCKER_IMAGE" check metrics
    else
      echo "WARN: $PROM_DOCKER_IMAGE unavailable (offline?); skipping promtool lint" >&2
      cat >/dev/null
    fi
  else
    fail "neither promtool nor docker available (gate prereq)"
  fi
}

# S0 prereqs + test venv.  PEP-668 makes a venv mandatory; this environment
# cannot bootstrap pip (no ensurepip), so we REUSE an existing venv that already
# carries RLTest + prometheus_client (.venv or .venv-tests) rather than creating
# a fresh one.  pip install runs only when pip is present.
for c in autoreconf clang-format redis-server curl python3 help2man docker; do
  command -v "$c" >/dev/null || fail "missing $c"
done
VENV=""
for cand in .venv .venv-tests; do
  if [ -f "$cand/bin/activate" ]; then VENV="$cand"; break; fi
done
[ -n "$VENV" ] || fail "no test venv found (.venv or .venv-tests); create one with RLTest + prometheus_client"
# shellcheck disable=SC1090
. "$VENV/bin/activate"
if python3 -m pip --version >/dev/null 2>&1; then
  python3 -m pip install -q -r tests/test_requirements.txt
else
  echo "NOTE: pip unavailable in $VENV; assuming deps preinstalled"
fi
python3 -c 'import RLTest, prometheus_client' \
  || fail "$VENV missing RLTest or prometheus_client"

# S1 srcdir hygiene (REQUIRED for VPATH; destroys any in-tree build).
if [ -f config.status ]; then echo "NOTE: distcleaning in-tree build for VPATH gate"; make distclean || true; fi
autoreconf -ivf

# S2 both flavors, sibling VPATH dirs; root symlink feeds tests/run_tests.sh.
mkdir -p build-prom build-noprom
(cd build-prom   && ../configure                      && make -j"$(nproc)")
(cd build-noprom && ../configure --disable-prometheus && make -j"$(nproc)")
ln -sf build-prom/memtier_benchmark ./memtier_benchmark
MB="$SRC/build-prom/memtier_benchmark"; export MEMTIER_BINARY="$MB"
export MEMTIER_NOPROM_BINARY="$SRC/build-noprom/memtier_benchmark"
export REDIS_SERVER="${REDIS_SERVER:-$(command -v redis-server)}"

# S3 format gate
make -C build-prom format-check

# S4 flavor asserts
grep -q 'define HAVE_EVHTTP 1' build-prom/config.h     || fail "default build lost HAVE_EVHTTP"
"$MB" --version | grep -q 'prometheus=yes'             || fail "--version token missing"
"$MB" --help >/tmp/help.out 2>&1 || true   # usage() exits 2 even for --help; capture, don't pipe
for f in --prometheus-port --prometheus-bind-addr --prometheus-run-label --prometheus-latency-buckets; do
  grep -q -- "$f" /tmp/help.out || fail "usage() missing $f"
done
grep -q '/\* #undef HAVE_EVHTTP \*/' build-noprom/config.h || fail "noprom build still has evhttp"
rc=0; "$MEMTIER_NOPROM_BINARY" --prometheus-port=9100 >/tmp/noprom.out 2>&1 || rc=$?
assert_eq "$rc" 2 "compiled-out flag exit code"
grep -q 'unrecognized option' /tmp/noprom.out          || fail "compiled-out flag message"

# S5 unit tests U1-U8, both flavors
make -C build-prom check; make -C build-noprom check

# S6 RLTest suites
TEST=test_cli_validation_prometheus.py OSS_STANDALONE=1 ./tests/run_tests.sh
TEST=test_prometheus.py               OSS_STANDALONE=1 ./tests/run_tests.sh
TEST=test_prometheus_redaction.py     OSS_STANDALONE=1 ./tests/run_tests.sh
TEST=test_statsd.py                   OSS_STANDALONE=1 ./tests/run_tests.sh
TEST=test_realtime_latencies.py       OSS_STANDALONE=1 ./tests/run_tests.sh
OSS_STANDALONE=0 OSS_CLUSTER=1 SHARDS=3 TEST=test_prometheus.py ./tests/run_tests.sh
if [ -f tests/tls/redis.crt ]; then
  TLS=1 TEST=test_prometheus_redaction.py OSS_STANDALONE=1 ./tests/run_tests.sh
else
  echo "NOTE: tests/tls absent; run ./tests/gen-test-certs.sh for the TLS redaction cell"
fi

# S7 manual smoke: ephemeral bind, hardening, TTL/render-rate, run-boundary
# monotonicity, teardown race window, promtool on the captured body.
RPORT=$(free_port); "$REDIS_SERVER" --port "$RPORT" --save '' --daemonize no >/dev/null 2>&1 & PIDS+=($!)
sleep 0.5
"$MB" -s 127.0.0.1 -p "$RPORT" --test-time=20 --run-count=2 --prometheus-port=0 \
  >/tmp/mb.out 2>/tmp/mb.err & MBPID=$!; PIDS+=($MBPID)
URL=""; for i in $(seq 1 100); do
  URL=$(grep -oP 'listening on \Khttp://[^ ]+/metrics' /tmp/mb.out || true); [ -n "$URL" ] && break; sleep 0.1
done; [ -n "$URL" ] || fail "no announce line"
curl -fsS "$URL" > /tmp/prom_scrape.txt
# Read the Content-Type via a GET with header dump (-D -), NOT curl -I: the
# exporter is GET-only and answers HEAD with 501 (hardening, F11), which would
# make -fsSI fail spuriously.
ct=$(curl -fsS -D - -o /dev/null "$URL" | tr -d '\r' | grep -i '^content-type: ' | cut -d' ' -f2-)
assert_eq "$ct" "text/plain; version=0.0.4; charset=utf-8" "Content-Type"
assert_eq "$(curl -s -o /dev/null -w '%{http_code}' -X POST "$URL")" 501 "non-GET status"
assert_eq "$(curl -s -o /dev/null -w '%{http_code}' "$URL?x=1")" 200 "query-string scrape"
assert_eq "$(curl -s -o /dev/null -w '%{http_code}' "$URL/")" 404 "trailing-slash path"
assert_eq "$(curl -s -o /dev/null -w '%{http_code}' "${URL%/metrics}/secret/x")" 404 "unknown path"
curl -s "${URL%/metrics}/secret/x" | grep -q secret && fail "404 echoes URI" || true
R0=$(curl -s "$URL" | awk '/^memtier_exporter_renders_total /{print $2}')
for i in 1 2 3; do curl -s -o /dev/null "$URL"; done
R1=$(curl -s "$URL" | awk '/^memtier_exporter_renders_total /{print $2}')
[ "$((R1 - R0))" -le 1 ] || fail "TTL clamp: >1 render across 5 scrapes in ~1s"
sleep 1.5
R2=$(curl -s "$URL" | awk '/^memtier_exporter_renders_total /{print $2}')
[ "$R2" -gt "$R1" ] || fail "cache never expired"
seq 1 64 | xargs -P64 -I{} curl -s -o /dev/null -w '%{http_code}\n' "$URL" > /tmp/storm.txt
grep -vqE '^(200|503)$' /tmp/storm.txt && fail "storm: unexpected status" || true
grep -q '^200$' /tmp/storm.txt || fail "storm: no successful scrape"
N2=0; iter=0
while kill -0 "$MBPID" 2>/dev/null; do
  iter=$((iter+1)); [ "$iter" -le 240 ] || fail "monotonicity poller timed out (120 s)"
  line=$(curl -s --max-time 2 "$URL" 2>/dev/null \
         | awk '/^memtier_ops_total /{o=$NF} /^memtier_run /{r=$NF} END{if (o != "") print o, r}' || true)
  [ -n "$line" ] && { echo "$line"; case "$line" in *' 2') N2=$((N2+1));; esac; }
  [ "$N2" -ge 3 ] && break
  sleep 0.5
done > /tmp/mono.txt
awk 'NR>1 && $1+0 < prev+0 {exit 1} {prev=$1}' /tmp/mono.txt || fail "ops_total went backwards"
awk '$2==2{f=1} END{exit !f}' /tmp/mono.txt                  || fail "smoke never observed run 2"
awk 'NR>1 && $2+0 < pr+0 {exit 1} {pr=$2}' /tmp/mono.txt     || fail "memtier_run went backwards"
awk '$2==1{l1=$1+0} $2==2 && !d{d=1; exit !($1+0 >= l1)}' /tmp/mono.txt || fail "run 2 dropped run-1 totals"
rc=0; wait "$MBPID" || rc=$?; assert_eq "$rc" 0 "memtier exit code"
promtool_check < /tmp/prom_scrape.txt                  || fail "promtool lint"
curl -s --max-time 2 "$URL" >/dev/null 2>&1 && fail "port not released after exit (F14)" || true
for i in $(seq 1 10); do
  timeout 30 "$MB" -s 127.0.0.1 -p "$RPORT" --verify-only \
    --data-import=tests/data-import-2-keys.txt --prometheus-port=0 >/tmp/mbv.out 2>&1 \
    || fail "verify-only teardown iter $i"
  grep -q 'Prometheus exporter listening on' /tmp/mbv.out || fail "no announce in verify-only"
done

# S8 error paths + IPv6 + 503-cap seam
PF=$(mktemp)
python3 -c 'import socket,sys,time
s=socket.socket(); s.bind(("127.0.0.1",0)); s.listen(1)
open(sys.argv[1],"w").write(str(s.getsockname()[1])); time.sleep(60)' "$PF" & SQUAT=$!; PIDS+=($SQUAT)
for i in $(seq 1 100); do [ -s "$PF" ] && break; sleep 0.05; done; SPORT=$(cat "$PF")
rc=0; "$MB" -s 127.0.0.1 -p "$RPORT" --test-time=2 --prometheus-port="$SPORT" >/tmp/inuse.out 2>&1 || rc=$?
assert_eq "$rc" 1 "EADDRINUSE exit code"
grep -q "failed to bind 127.0.0.1:$SPORT" /tmp/inuse.out || fail "EADDRINUSE message"
kill "$SQUAT"; wait "$SQUAT" 2>/dev/null || true
rc=0; "$MB" -s 127.0.0.1 -p "$RPORT" --test-time=2 --prometheus-port=0 \
  --prometheus-bind-addr=0.0.0.0 >/dev/null 2>/tmp/warn.err || rc=$?
assert_eq "$rc" 0 "non-loopback run exit code"
[ "$(grep -c 'not a loopback address' /tmp/warn.err)" -eq 1 ] || fail "W1 not exactly once"
rc=0; "$MB" -s 127.0.0.1 -p "$RPORT" --prometheus-run-label=a=1 >/tmp/dep.out 2>&1 || rc=$?
assert_eq "$rc" 2 "dependency-error exit code"
grep -q 'requires --prometheus-port' /tmp/dep.out || fail "dependency-error message"
if python3 -c 'import socket;socket.socket(socket.AF_INET6).bind(("::1",0))' 2>/dev/null; then
  "$MB" -s 127.0.0.1 -p "$RPORT" --test-time=4 --prometheus-port=0 --prometheus-bind-addr=::1 \
    >/tmp/mb6.out 2>&1 & P6=$!; PIDS+=($P6); sleep 1.5
  U6=$(grep -oP 'listening on \Khttp://\[::1\]:[0-9]+/metrics' /tmp/mb6.out) || fail "IPv6 announce not bracketed"
  curl -gfsS "$U6" | grep -q '^memtier_ops_total ' || fail "IPv6 scrape failed"; wait "$P6"
else echo "SKIP: no IPv6 loopback"; fi
MEMTIER_PROM_MAX_INFLIGHT=0 "$MB" -s 127.0.0.1 -p "$RPORT" --test-time=3 --prometheus-port=0 \
  >/tmp/mbcap.out 2>&1 & PCAP=$!; PIDS+=($PCAP)
CAPURL=""; for i in $(seq 1 100); do
  CAPURL=$(grep -oP 'listening on \Khttp://[^ ]+/metrics' /tmp/mbcap.out || true)
  [ -n "$CAPURL" ] && break; sleep 0.1
done; [ -n "$CAPURL" ] || fail "no announce line (cap seam)"
for i in 1 2 3; do
  assert_eq "$(curl -s -o /dev/null -w '%{http_code}' "$CAPURL")" 503 "cap=0 scrape #$i not rejected"
done
assert_eq "$(curl -s "$CAPURL")" "exporter busy" "cap=0 503 body"
assert_eq "$(curl -s -o /dev/null -w '%{http_code}' "${CAPURL%/metrics}/x")" 503 "cap=0 gencb not capped"
rc=0; wait "$PCAP" || rc=$?; assert_eq "$rc" 0 "cap-seam exit code"

# S9 sanitizers (GATE_FAST=1 skips)
if [ "${GATE_FAST:-0}" != 1 ]; then
  mkdir -p build-asan build-tsan
  (cd build-asan && ../configure --enable-sanitizers && make -j"$(nproc)")
  MEMTIER_BINARY="$SRC/build-asan/memtier_benchmark" ASAN_OPTIONS=detect_leaks=1 \
    TEST="test_cli_validation_prometheus.py test_prometheus.py" OSS_STANDALONE=1 ./tests/run_tests.sh
  (cd build-tsan && ../configure --enable-thread-sanitizer && make -j"$(nproc)")
  MEMTIER_BINARY="$SRC/build-tsan/memtier_benchmark" \
    TSAN_OPTIONS="suppressions=$SRC/tsan_suppressions.txt exitcode=66" \
    setarch "$(uname -m)" -R bash -c 'TEST=test_prometheus.py OSS_STANDALONE=1 ./tests/run_tests.sh'
fi

# S10 docs/completion/fuzz/grep audits
make -C build-prom rebuild-man && cp build-prom/memtier_benchmark.1 ./memtier_benchmark.1
for f in '\-\-prometheus\-port' '\-\-prometheus\-bind\-addr' \
         '\-\-prometheus\-run\-label' '\-\-prometheus\-latency\-buckets'; do
  grep -qF -- "$f" memtier_benchmark.1 || fail "man page stale: missing $f"
done
[ "$(grep -o -- '--prometheus-[a-z-]*' bash-completion/memtier_benchmark | sort -u | wc -l)" -eq 4 ] \
  || fail "completion stale"
for f in --prometheus-port --prometheus-bind-addr --prometheus-run-label --prometheus-latency-buckets; do
  grep -qF -- "\"$f\"" bash-completion/memtier_benchmark || fail "completion missing $f"
done
MEMTIER_FUZZ=1 MEMTIER_FUZZ_MAX_EXAMPLES=100 OSS_STANDALONE=1 ./tests/run_tests.sh
grep -nE '#include[[:space:]]*[<"](event2/|event\.h|evhttp)|#include[[:space:]]*"(config|version)\.h"|PACKAGE_VERSION|MEMTIER_GIT_SHA1' \
  prometheus_metrics.cpp prometheus_metrics.h tests/unit/prometheus_metrics_test.cpp \
  && fail "purity breach: config/version/libevent reference in the unconditional metrics layer" || true
grep -q -- '--disable-prometheus' DEVELOPMENT.md || fail "DEVELOPMENT.md missing --disable-prometheus"
grep -qE 'evhttp >= 2\.1\.1|libevent 2\.1\.1' DEVELOPMENT.md || fail "DEVELOPMENT.md missing the 2.1.1 floor"
grep -q 'make gate' DEVELOPMENT.md || fail "DEVELOPMENT.md missing make gate"
n=$(grep -c '1e6' prometheus_metrics.cpp prometheus_metrics.h | awk -F: '{s+=$2} END{print s}')
[ "$n" -eq 1 ] || fail "1e6 literal count != 1 (only the USECS_PER_SEC definition)"
[ "$(grep -c 'USECS_PER_SEC' prometheus_metrics.cpp)" -ge 3 ] || fail "conversion sites < 3"
grep -q 'LATENCY_HDR_RESULTS_MULTIPLIER' prometheus_metrics.cpp prometheus_metrics.h \
  && fail "ms constant leaked into exporter" || true
grep -qF 'int64_t capped = (value > h->highest_trackable_value) ? h->highest_trackable_value : value;' run_stats.cpp \
  && grep -qF 'capped = (capped < h->lowest_trackable_value) ? h->lowest_trackable_value : capped;' run_stats.cpp \
  || fail "capped-record clamp drifted from the U2 test replica"
tr -s '[:space:]' ' ' < prometheus_exporter.cpp \
  | grep -q 'hdr_init(LATENCY_HDR_MIN_VALUE, LATENCY_HDR_SEC_MAX_VALUE, LATENCY_HDR_SEC_SIGDIGTS, ' \
  || fail "exporter hdr_init drifted from :3058 triplet"
echo "GATE PASS"
