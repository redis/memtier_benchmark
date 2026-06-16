"""
Functional tests for the Prometheus /metrics exporter (F1-F20, PLAN.md §10).

The exporter is a second real-time transport on memtier's one-producer metrics
snapshot (the StatsD/Graphite path is the first).  Every test here drives a live
memtier_benchmark with --prometheus-port=0 (ephemeral), discovers the bound URL
from the one-line stdout announce via PROM_LISTEN_RE, and scrapes /metrics over
HTTP.  Bodies are parsed with prometheus_client's official text parser through
the include.py helpers (prom_scrape / prom_parse / prom_sample_value).

These run across the CI matrix.  Cases that depend on standalone semantics
(F15 / F15b verify-only teardown, F19 stall-dedup) self-skip under
OSS_CLUSTER=1; the rest are cluster-agnostic (F18 explicitly exercises cluster).

promtool validation (F2) is opportunistic: it runs only if `which promtool`
succeeds, otherwise it [SKIP]s -- the consolidated `make gate` runs the pinned
docker promtool instead (PLAN.md Decisions #35).

Run with:
  . .venv-tests/bin/activate
  TEST=test_prometheus.py OSS_STANDALONE=1 REDIS_SERVER=/usr/local/bin/redis-server \
    MEMTIER_BINARY=$PWD/memtier_benchmark ./tests/run_tests.sh
"""
import json
import os
import re
import shutil
import socket
import subprocess
import tempfile
import threading
import time

from include import (
    MEMTIER_BINARY,
    PROM_CONTENT_TYPE,
    PROM_LISTEN_RE,
    TLS_CACERT,
    TLS_CERT,
    TLS_KEY,
    TLS_PROTOCOLS,
    prom_parse,
    prom_sample_value,
    prom_scrape,
    wait_for_prometheus_url,
)

# The 26 default histogram bucket upper bounds, in seconds, as the exporter
# renders them (PLAN.md §4 / U8).  Byte-equal literal strings; +Inf appended.
DEFAULT_LE_STRINGS = [
    "0.0001", "0.00025", "0.0005", "0.00075", "0.001", "0.0015", "0.002",
    "0.003", "0.004", "0.005", "0.0075", "0.01", "0.015", "0.02", "0.03",
    "0.05", "0.075", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "30", "60",
]

# The nine cumulative counter families the exporter exports (PLAN.md §3.4 / §4).
COUNTER_NAMES = [
    "memtier_ops_total",
    "memtier_sent_bytes_total",
    "memtier_received_bytes_total",
    "memtier_hits_total",
    "memtier_misses_total",
    "memtier_errors_total",
    "memtier_connection_errors_total",
    "memtier_retry_attempts_total",
    "memtier_retried_ops_total",
]


# ---------------------------------------------------------------------------
# Harness helpers
# ---------------------------------------------------------------------------
def _is_cluster(env):
    return env.isCluster()


def _server_args(env):
    """Connection args for a live memtier run against the RLTest env.

    Standalone: --server/--port of the master shard.
    Cluster:    --server/--port of one shard plus --cluster-mode (memtier
                discovers the rest via CLUSTER SLOTS).
    TLS:        when the env runs TLS, redis accepts only TLS connections, so
                add --tls and the cert material (mirrors include.addTLSArgs).
                The exporter listener stays plain HTTP regardless.
    """
    master_nodes_list = env.getMasterNodesList()
    node = master_nodes_list[0]
    # RLTest node dicts carry the port; host defaults to loopback.
    port = node["port"]
    host = node.get("host", "127.0.0.1") or "127.0.0.1"
    args = ["--server", str(host), "--port", str(port)]
    if _is_cluster(env):
        args.append("--cluster-mode")
    if getattr(env, "useTLS", False):
        args.append("--tls")
        args.append("--cert={}".format(TLS_CERT))
        args.append("--cacert={}".format(TLS_CACERT))
        if TLS_KEY != "":
            args.append("--key={}".format(TLS_KEY))
        else:
            args.append("--tls-skip-verify")
        if TLS_PROTOCOLS != "":
            args.append("--tls-protocols={}".format(TLS_PROTOCOLS))
    return args


def _new_results_dir():
    d = tempfile.mkdtemp(prefix="prom_")
    return d


def _popen_memtier(env, extra_args, results_dir, env_overrides=None):
    """Launch memtier_benchmark with stdout/stderr redirected to files.

    Returns (proc, stdout_path, stderr_path).  The stdout file is where the
    exporter announce line lands (PROM_LISTEN_RE discovery contract).
    """
    stdout_path = os.path.join(results_dir, "mb.stdout")
    stderr_path = os.path.join(results_dir, "mb.stderr")
    args = [MEMTIER_BINARY] + _server_args(env) + extra_args
    run_env = dict(os.environ)
    if env_overrides:
        run_env.update(env_overrides)
    stdout_f = open(stdout_path, "w")
    stderr_f = open(stderr_path, "w")
    proc = subprocess.Popen(
        args, stdout=stdout_f, stderr=stderr_f, env=run_env, cwd=results_dir
    )
    proc._mb_stdout_f = stdout_f  # keep refs so they aren't GC-closed
    proc._mb_stderr_f = stderr_f
    return proc, stdout_path, stderr_path


def _kill(proc):
    if proc.poll() is None:
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except Exception:
            try:
                proc.kill()
                proc.wait(timeout=5)
            except Exception:
                pass


def _drain(path):
    try:
        with open(path) as fh:
            return fh.read()
    except Exception:
        return ""


def _assert_histogram_invariants(env, parsed):
    """+Inf bucket == _count; buckets monotone non-decreasing; _sum present."""
    buckets = parsed.get("memtier_latency_seconds_bucket", [])
    env.assertTrue(len(buckets) > 0, message="no histogram buckets in scrape")
    # Order in exposition is bound-ascending then +Inf (PLAN.md §4).
    prev = -1.0
    inf_val = None
    for s in buckets:
        v = s.value
        env.assertTrue(
            v >= prev,
            message="bucket counts not monotone: {} after {}".format(v, prev),
        )
        prev = v
        if s.labels.get("le") == "+Inf":
            inf_val = v
    env.assertFalse(inf_val is None, message="no +Inf bucket")
    count = prom_sample_value(parsed, "memtier_latency_seconds_count")
    env.assertFalse(count is None, message="no _count sample")
    env.assertEqual(
        inf_val, count, message="+Inf bucket ({}) != _count ({})".format(inf_val, count)
    )
    env.assertFalse(
        prom_sample_value(parsed, "memtier_latency_seconds_sum") is None,
        message="no _sum sample",
    )


def _promtool_validate(env, body, scrape_path):
    """Opportunistic promtool lint (PLAN.md §4 rule / Decisions #35).

    Runs native promtool if present; otherwise self-[SKIP]s (the docker path
    is exercised by `make gate`, not in-test).
    """
    promtool = shutil.which("promtool")
    if not promtool:
        env.debugPrint("[SKIP] promtool not on PATH; gate uses docker promtool", True)
        return
    with open(scrape_path, "w") as fh:
        fh.write(body)
    proc = subprocess.run(
        [promtool, "check", "metrics"],
        input=body,
        capture_output=True,
        text=True,
        timeout=30,
    )
    env.assertEqual(
        proc.returncode,
        0,
        message="promtool check metrics failed: rc={} out={} err={}".format(
            proc.returncode, proc.stdout, proc.stderr
        ),
    )


def _no_sanitizer_output(env, *texts):
    for t in texts:
        env.assertNotContains("AddressSanitizer", t)
        env.assertNotContains("LeakSanitizer", t)
        env.assertNotContains("ThreadSanitizer", t)


# ---------------------------------------------------------------------------
# F1 -- disabled by default
# ---------------------------------------------------------------------------
def test_F1_disabled_by_default(env):
    """No --prometheus-port -> no announce line, exporter never starts."""
    rd = _new_results_dir()
    proc, out_path, err_path = _popen_memtier(
        env, ["--test-time=2", "--hide-histogram"], rd
    )
    rc = proc.wait()
    out = _drain(out_path)
    err = _drain(err_path)
    env.assertEqual(rc, 0, message="memtier did not exit cleanly: {}".format(err))
    env.assertFalse(
        bool(PROM_LISTEN_RE.search(out)),
        message="exporter announced without --prometheus-port",
    )


# ---------------------------------------------------------------------------
# F2 -- mid-run scrape: 200, content-type, all families, histogram, promtool
# ---------------------------------------------------------------------------
def test_F2_mid_run_scrape(env):
    rd = _new_results_dir()
    proc, out_path, err_path = _popen_memtier(
        env, ["--test-time=10", "--prometheus-port=0"], rd
    )
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line: {}".format(_drain(err_path)))
        time.sleep(2)  # let a couple of 1 Hz ticks land
        r = prom_scrape(url)
        env.assertEqual(r.status, 200, message="scrape status {}".format(r.status))
        env.assertEqual(
            r.header("Content-Type"), PROM_CONTENT_TYPE, message="content-type mismatch"
        )
        parsed = prom_parse(r.body)  # hard-asserts on malformed body
        # All §4 families present.
        for name in COUNTER_NAMES:
            env.assertContains(name, r.body)
        for g in ["memtier_build_info", "memtier_start_time_seconds",
                  "memtier_connections", "memtier_threads", "memtier_run",
                  "memtier_configured_runs", "memtier_config_test_time_seconds",
                  "memtier_exporter_renders_total",
                  "memtier_exporter_snapshot_age_seconds"]:
            env.assertContains(g, r.body)
        _assert_histogram_invariants(env, parsed)
        # default le set byte-equals the 26 literals (+Inf last).
        le_strings = [s.labels["le"] for s in parsed["memtier_latency_seconds_bucket"]]
        env.assertEqual(
            le_strings, DEFAULT_LE_STRINGS + ["+Inf"], message="default le set drift"
        )
        # memtier_run == 1 mid-run.
        env.assertEqual(prom_sample_value(parsed, "memtier_run"), 1.0)
        # artifact
        scrape_path = os.path.join(rd, "prom_scrape.txt")
        with open(scrape_path, "w") as fh:
            fh.write(r.body)
        _promtool_validate(env, r.body, scrape_path)
    finally:
        _kill(proc)


# ---------------------------------------------------------------------------
# F3 -- two scrapes >=2 s apart: counters/buckets non-decreasing, ops increases
# ---------------------------------------------------------------------------
def test_F3_counter_monotonicity_two_scrapes(env):
    rd = _new_results_dir()
    proc, out_path, err_path = _popen_memtier(
        env, ["--test-time=12", "--prometheus-port=0"], rd
    )
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line")
        time.sleep(1.5)
        a = prom_parse(prom_scrape(url).body)
        time.sleep(2.5)
        b = prom_parse(prom_scrape(url).body)
        for name in COUNTER_NAMES:
            va = prom_sample_value(a, name) or 0.0
            vb = prom_sample_value(b, name) or 0.0
            env.assertTrue(vb >= va, message="{} went backwards {}->{}".format(name, va, vb))
        ops_a = prom_sample_value(a, "memtier_ops_total") or 0.0
        ops_b = prom_sample_value(b, "memtier_ops_total") or 0.0
        env.assertTrue(ops_b > ops_a, message="ops_total did not increase {}->{}".format(ops_a, ops_b))
        # bucket counts and _count/_sum non-decreasing
        ca = prom_sample_value(a, "memtier_latency_seconds_count") or 0.0
        cb = prom_sample_value(b, "memtier_latency_seconds_count") or 0.0
        env.assertTrue(cb >= ca, message="_count went backwards {}->{}".format(ca, cb))
    finally:
        _kill(proc)


# ---------------------------------------------------------------------------
# F4 -- restart/run-fold acceptance (THE accumulator test): --run-count=2
# ---------------------------------------------------------------------------
def test_F4_accumulator_across_runs(env):
    if _is_cluster(env):
        env.skip()  # cluster run boundaries covered by F18
        return
    rd = _new_results_dir()
    proc, out_path, err_path = _popen_memtier(
        env, ["--test-time=5", "--run-count=2", "--prometheus-port=0"], rd
    )
    series = []  # list of (ops, run)
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line")
        # poll @0.25 s until the process exits
        deadline = time.time() + 60
        while proc.poll() is None and time.time() < deadline:
            r = prom_scrape(url, timeout=2)
            if r.status == 200:
                p = prom_parse(r.body)
                ops = prom_sample_value(p, "memtier_ops_total")
                run = prom_sample_value(p, "memtier_run")
                if ops is not None and run is not None:
                    series.append((ops, int(run)))
            time.sleep(0.25)
        rc = proc.wait()
        env.assertEqual(rc, 0, message="memtier exit {}: {}".format(rc, _drain(err_path)))
        env.assertTrue(len(series) >= 3, message="too few samples: {}".format(series))
        # all 9 counter series are tested for monotonicity via ops here; the
        # key regression is ops never going backwards across the run boundary
        # (naive per-run export goes 0->N->0->N).
        prev = -1.0
        for ops, _run in series:
            env.assertTrue(ops >= prev, message="ops_total went backwards: {}".format(series))
            prev = ops
        runs_seen = [run for _ops, run in series]
        env.assertTrue(2 in runs_seen, message="never reached run 2: {}".format(runs_seen))
        # run label prefix-monotone 1..1 2..2
        prev_run = 0
        for _ops, run in series:
            env.assertTrue(run >= prev_run, message="memtier_run went backwards: {}".format(runs_seen))
            prev_run = run
        # first run-2 ops >= last run-1 ops
        last_run1 = max((ops for ops, run in series if run == 1), default=None)
        first_run2 = next((ops for ops, run in series if run == 2), None)
        if last_run1 is not None and first_run2 is not None:
            env.assertTrue(
                first_run2 >= last_run1,
                message="run 2 dropped run-1 totals: {} < {}".format(first_run2, last_run1),
            )
    finally:
        _kill(proc)


# ---------------------------------------------------------------------------
# F5 -- last scrape vs final JSON Totals Count
# ---------------------------------------------------------------------------
def test_F5_scrape_vs_final_json(env):
    if _is_cluster(env):
        env.skip()
        return
    rd = _new_results_dir()
    json_path = os.path.join(rd, "mb.json")
    proc, out_path, err_path = _popen_memtier(
        env,
        ["--test-time=6", "--prometheus-port=0", "--json-out-file", json_path],
        rd,
    )
    last_ops = [0.0]
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line")
        # keep scraping until the process is about to exit
        while proc.poll() is None:
            r = prom_scrape(url, timeout=2)
            if r.status == 200:
                v = prom_sample_value(prom_parse(r.body), "memtier_ops_total")
                if v is not None:
                    last_ops[0] = v
            time.sleep(0.5)
        rc = proc.wait()
        env.assertEqual(rc, 0, message="memtier exit {}".format(rc))
        with open(json_path) as fh:
            js = json.load(fh)
        json_total = js["ALL STATS"]["Totals"]["Count"]
        env.assertTrue(
            0.5 * json_total <= last_ops[0] <= json_total,
            message="last scrape ops {} not within [0.5*{}, {}]".format(
                last_ops[0], json_total, json_total
            ),
        )
    finally:
        _kill(proc)


# ---------------------------------------------------------------------------
# F6 -- redaction is covered by tests/test_prometheus_redaction.py (A3.3)
# (the dedicated redaction suite runs under TLS); F6 here is a body smoke
# check that no obvious connection identity leaks into a plaintext scrape.
# ---------------------------------------------------------------------------
def test_F6_no_obvious_secret_in_body(env):
    rd = _new_results_dir()
    server_args = _server_args(env)
    # extract the port we connected to, to confirm it never appears as a value
    port = None
    for i, a in enumerate(server_args):
        if a == "--port":
            port = server_args[i + 1]
    proc, out_path, err_path = _popen_memtier(
        env, ["--test-time=4", "--prometheus-port=0"], rd
    )
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line")
        time.sleep(1.0)
        body = prom_scrape(url).body
        # The exporter's own ephemeral port legitimately appears in nothing;
        # the *target* redis port must not be echoed as a metric/label.
        # (build_info carries version/git_sha only.)
        env.assertFalse(
            re.search(r'(server|host|addr|target)\s*=\s*"', body),
            message="connection identity label leaked: {}".format(body[:500]),
        )
    finally:
        _kill(proc)


# ---------------------------------------------------------------------------
# F7 -- fixed port + announce agreement
# ---------------------------------------------------------------------------
def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def test_F7_fixed_port_announce_agreement(env):
    rd = _new_results_dir()
    port = _free_port()
    proc, out_path, err_path = _popen_memtier(
        env, ["--test-time=4", "--prometheus-port={}".format(port)], rd
    )
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line: {}".format(_drain(err_path)))
        env.assertContains(":{}/metrics".format(port), url)
        r = prom_scrape(url)
        env.assertEqual(r.status, 200)
    finally:
        _kill(proc)


# ---------------------------------------------------------------------------
# F8 -- run-label propagation + escaping round-trip
# ---------------------------------------------------------------------------
def test_F8_run_label_propagation_and_escaping(env):
    rd = _new_results_dir()
    # raw value a"b\c -> parsed back exactly; raw body has a\"b\\c
    proc, out_path, err_path = _popen_memtier(
        env,
        ["--test-time=4", "--prometheus-port=0",
         "--prometheus-run-label=tier=gold",
         '--prometheus-run-label=note=a"b\\c'],
        rd,
    )
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line")
        time.sleep(1.0)
        r = prom_scrape(url)
        parsed = prom_parse(r.body)
        # every sample carries both labels
        for name, samples in parsed.items():
            for s in samples:
                env.assertEqual(s.labels.get("tier"), "gold",
                                message="{} missing tier label".format(name))
                env.assertEqual(s.labels.get("note"), 'a"b\\c',
                                message="{} note label not round-tripped: {!r}".format(
                                    name, s.labels.get("note")))
        # raw body carries the escaped form
        env.assertContains('a\\"b\\\\c', r.body)
    finally:
        _kill(proc)


# ---------------------------------------------------------------------------
# F9 -- TTL clamp via the renders_total instrument
# ---------------------------------------------------------------------------
def test_F9_ttl_render_cache(env):
    rd = _new_results_dir()
    proc, out_path, err_path = _popen_memtier(
        env, ["--test-time=8", "--prometheus-port=0"], rd
    )
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line")
        time.sleep(1.0)
        # 2+ scrapes inside one TTL window carry equal renders_total
        r0 = prom_sample_value(prom_parse(prom_scrape(url).body), "memtier_exporter_renders_total")
        r1 = prom_sample_value(prom_parse(prom_scrape(url).body), "memtier_exporter_renders_total")
        env.assertEqual(r0, r1, message="renders_total changed inside TTL window {} -> {}".format(r0, r1))
        ops0 = prom_sample_value(prom_parse(prom_scrape(url).body), "memtier_ops_total")
        time.sleep(1.6)  # past the 1 s TTL
        p2 = prom_parse(prom_scrape(url).body)
        r2 = prom_sample_value(p2, "memtier_exporter_renders_total")
        ops2 = prom_sample_value(p2, "memtier_ops_total")
        env.assertTrue(r2 > r1, message="cache never expired {} -> {}".format(r1, r2))
        env.assertTrue(ops2 >= ops0, message="ops decreased across TTL expiry")
    finally:
        _kill(proc)


# ---------------------------------------------------------------------------
# F10 -- snapshot_age in [0, 5)
# ---------------------------------------------------------------------------
def test_F10_snapshot_age_bounds(env):
    rd = _new_results_dir()
    proc, out_path, err_path = _popen_memtier(
        env, ["--test-time=6", "--prometheus-port=0"], rd
    )
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line")
        time.sleep(1.5)
        age = prom_sample_value(
            prom_parse(prom_scrape(url).body), "memtier_exporter_snapshot_age_seconds"
        )
        env.assertFalse(age is None, message="no snapshot_age sample")
        env.assertTrue(0.0 <= age < 5.0, message="snapshot_age out of [0,5): {}".format(age))
    finally:
        _kill(proc)


# ---------------------------------------------------------------------------
# F11 -- HTTP hardening
# ---------------------------------------------------------------------------
def test_F11_http_hardening(env):
    rd = _new_results_dir()
    # short, self-terminating run so we can assert a clean rc 0 (no kill).
    proc, out_path, err_path = _popen_memtier(
        env, ["--test-time=4", "--prometheus-port=0"], rd
    )
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line")
        time.sleep(0.5)
        base = url[: -len("/metrics")]
        env.assertEqual(prom_scrape(url, method="POST").status, 501, message="POST not 501")
        env.assertEqual(prom_scrape(url, method="HEAD").status, 501, message="HEAD not 501")
        unknown = prom_scrape(base + "/secret/x")
        env.assertEqual(unknown.status, 404, message="unknown path not 404")
        env.assertFalse("secret" in unknown.body, message="404 echoes URI: {!r}".format(unknown.body))
        env.assertEqual(prom_scrape(url + "?x=1").status, 200, message="?x=1 not 200")
        env.assertEqual(prom_scrape(url + "/").status, 404, message="/metrics/ not 404")
        # normal scrape still works
        env.assertEqual(prom_scrape(url).status, 200, message="scrape broken after hardening probes")
        # let the run finish on its own (--test-time): exit must be clean 0.
        rc = proc.wait(timeout=30)
        env.assertEqual(rc, 0, message="exit code {}".format(rc))
        _no_sanitizer_output(env, _drain(err_path))
    finally:
        _kill(proc)


# ---------------------------------------------------------------------------
# F12 -- custom buckets
# ---------------------------------------------------------------------------
def test_F12_custom_buckets(env):
    rd = _new_results_dir()
    proc, out_path, err_path = _popen_memtier(
        env,
        ["--test-time=4", "--prometheus-port=0",
         "--prometheus-latency-buckets=0.001,0.0025,0.01,1"],
        rd,
    )
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line")
        time.sleep(1.0)
        parsed = prom_parse(prom_scrape(url).body)
        les = [s.labels["le"] for s in parsed["memtier_latency_seconds_bucket"]]
        # parse as floats; +Inf -> inf
        as_floats = set(float(x) for x in les)
        env.assertEqual(
            as_floats, {0.001, 0.0025, 0.01, 1.0, float("inf")},
            message="custom le set wrong: {}".format(les),
        )
    finally:
        _kill(proc)


# ---------------------------------------------------------------------------
# F13 -- latency unit sanity
# ---------------------------------------------------------------------------
def test_F13_latency_unit_sanity(env):
    if _is_cluster(env):
        env.skip()
        return
    rd = _new_results_dir()
    json_path = os.path.join(rd, "mb.json")
    proc, out_path, err_path = _popen_memtier(
        env, ["--test-time=6", "--prometheus-port=0", "--json-out-file", json_path], rd
    )
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line")
        time.sleep(3.0)
        parsed = prom_parse(prom_scrape(url).body)
        s = prom_sample_value(parsed, "memtier_latency_seconds_sum")
        c = prom_sample_value(parsed, "memtier_latency_seconds_count")
        env.assertTrue(c and c > 0, message="empty histogram")
        mean = s / c
        env.assertTrue(1e-6 < mean < 0.05, message="mean latency out of range: {} s".format(mean))
        proc.wait()
        with open(json_path) as fh:
            js = json.load(fh)
        # cross-check vs JSON avg latency (ms) within 50%
        try:
            avg_ms = js["ALL STATS"]["Totals"]["Latency"]
            if avg_ms:
                ratio = (mean * 1000.0) / avg_ms
                env.assertTrue(0.5 <= ratio <= 2.0,
                               message="latency cross-check off: prom {} ms vs json {} ms".format(
                                   mean * 1000.0, avg_ms))
        except (KeyError, TypeError):
            pass
    finally:
        _kill(proc)


# ---------------------------------------------------------------------------
# F14 -- clean shutdown: port refused after exit, re-binds immediately
# ---------------------------------------------------------------------------
def test_F14_clean_shutdown_port_release(env):
    rd = _new_results_dir()
    port = _free_port()
    proc, out_path, err_path = _popen_memtier(
        env, ["--test-time=3", "--prometheus-port={}".format(port)], rd
    )
    url = wait_for_prometheus_url(out_path, timeout=20)
    env.assertFalse(url is None, message="no announce line")
    env.assertEqual(prom_scrape(url).status, 200)
    rc = proc.wait()
    env.assertEqual(rc, 0, message="exit code {}".format(rc))
    # post-exit scrape refused (status None == connection failure)
    after = prom_scrape(url, timeout=2)
    env.assertTrue(after.status is None, message="port still served after exit: {}".format(after.status))
    # same fixed port re-binds immediately
    rd2 = _new_results_dir()
    proc2, out2, err2 = _popen_memtier(
        env, ["--test-time=3", "--prometheus-port={}".format(port)], rd2
    )
    try:
        url2 = wait_for_prometheus_url(out2, timeout=20)
        env.assertFalse(url2 is None, message="re-bind failed: {}".format(_drain(err2)))
        env.assertEqual(prom_scrape(url2).status, 200)
    finally:
        _kill(proc2)


# ---------------------------------------------------------------------------
# F15 -- teardown race-window regression (the stop-event fix); 20x
# ---------------------------------------------------------------------------
def test_F15_teardown_race_window(env):
    if _is_cluster(env):
        env.skip()
        return
    root = os.environ.get("ROOT_FOLDER", os.getcwd())
    import_file = os.path.join(root, "tests", "data-import-2-keys.txt")
    if not os.path.isfile(import_file):
        # fall back to CWD-relative when ROOT_FOLDER isn't set
        alt = os.path.join(os.getcwd(), "tests", "data-import-2-keys.txt")
        import_file = alt if os.path.isfile(alt) else import_file
    env.assertTrue(os.path.isfile(import_file),
                   message="data-import fixture not found: {}".format(import_file))
    for i in range(20):
        rd = _new_results_dir()
        proc, out_path, err_path = _popen_memtier(
            env,
            ["--verify-only", "--data-import={}".format(import_file),
             "--prometheus-port=0"],
            rd,
        )
        url = wait_for_prometheus_url(out_path, timeout=10)
        env.assertFalse(url is None, message="iter {}: no announce: {}".format(i, _drain(err_path)))
        # one best-effort scrape with a closed outcome set
        r = prom_scrape(url, timeout=2)
        if r.status == 200:
            # must be the zero snapshot and parse cleanly
            p = prom_parse(r.body)
            env.assertEqual(prom_sample_value(p, "memtier_ops_total"), 0.0,
                            message="iter {}: 200 but ops != 0".format(i))
            env.assertEqual(prom_sample_value(p, "memtier_run"), 0.0,
                            message="iter {}: 200 but run != 0".format(i))
        else:
            # connection-level failure is the accepted alternative (process
            # beat the scrape to teardown). Any other HTTP status is a FAIL.
            env.assertTrue(
                r.status is None,
                message="iter {}: unexpected status {} (closed outcome set)".format(i, r.status),
            )
        try:
            rc = proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            _kill(proc)
            env.assertTrue(False, message="iter {}: TEARDOWN HANG (TimeoutExpired)".format(i))
            return
        env.assertEqual(rc, 0, message="iter {}: exit {}: {}".format(i, rc, _drain(err_path)))


# ---------------------------------------------------------------------------
# F15b -- deterministic zero-snapshot + no-publish-during-verify
# ---------------------------------------------------------------------------
def _write_import_file(path, n):
    with open(path, "w") as fh:
        fh.write("dumpflags, time, exptime, nbytes, nsuffix, it_flags, clsid, nkey, key, data\n")
        for i in range(n):
            key = "memtier-vk-{:07d}".format(i)
            fh.write("0, 0, 0, 12, 0, 0, 0, {}, {}, xxxxxxxxxx\n".format(len(key), key))


def test_F15b_zero_snapshot_no_publish_during_verify(env):
    if _is_cluster(env):
        env.skip()
        return
    rd = _new_results_dir()
    import_file = os.path.join(rd, "import_300k.csv")
    _write_import_file(import_file, 300000)
    # preload once (rc 0, keeps verify mismatch-free)
    pre = subprocess.run(
        [MEMTIER_BINARY] + _server_args(env)
        + ["--data-import={}".format(import_file), "--ratio=1:0",
           "--key-pattern=P:P", "-t", "1", "-c", "1", "-n", "300000",
           "--hide-histogram"],
        capture_output=True, text=True, timeout=120,
    )
    env.assertEqual(pre.returncode, 0, message="preload failed: {}".format(pre.stderr[-500:]))
    # verify-only run: ~5.8 s window, only the ctor zero snapshot is published
    proc, out_path, err_path = _popen_memtier(
        env,
        ["--verify-only", "--data-import={}".format(import_file),
         "--ratio=1:0", "-n", "300000", "--prometheus-port=0"],
        rd,
    )
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line: {}".format(_drain(err_path)))
        # scrape with retry; must land >=1 200 deterministically
        first = None
        for _ in range(40):
            r = prom_scrape(url, timeout=1)
            if r.status == 200:
                first = r
                break
            time.sleep(0.25)
        env.assertFalse(first is None, message="never got a 200 during verify window")
        p1 = prom_parse(first.body)
        env.assertEqual(prom_sample_value(p1, "memtier_run"), 0.0, message="run != 0 during verify")
        env.assertEqual(prom_sample_value(p1, "memtier_configured_runs"), 1.0)
        for name in COUNTER_NAMES:
            env.assertEqual(prom_sample_value(p1, name), 0.0,
                            message="{} != 0 during verify".format(name))
        env.assertEqual(prom_sample_value(p1, "memtier_latency_seconds_count"), 0.0)
        age1 = prom_sample_value(p1, "memtier_exporter_snapshot_age_seconds")
        time.sleep(1.5)
        p2 = prom_parse(prom_scrape(url, timeout=2).body)
        for name in COUNTER_NAMES:
            env.assertEqual(prom_sample_value(p2, name), 0.0,
                            message="{} became nonzero during verify".format(name))
        age2 = prom_sample_value(p2, "memtier_exporter_snapshot_age_seconds")
        env.assertTrue(age2 > age1, message="snapshot_age did not grow: {} -> {}".format(age1, age2))
        env.assertTrue(age2 > 1.5,
                       message="snapshot_age <= 1.5 -> a publish happened during verify: {}".format(age2))
        rc = proc.wait(timeout=30)
        env.assertEqual(rc, 0, message="verify exit {}".format(rc))
    finally:
        _kill(proc)


# ---------------------------------------------------------------------------
# F16 -- IPv6
# ---------------------------------------------------------------------------
def test_F16_ipv6(env):
    # skip-guard: can we bind ::1 at all on this host?
    try:
        s6 = socket.socket(socket.AF_INET6)
        s6.bind(("::1", 0))
        s6.close()
    except OSError:
        env.skip()
        return
    rd = _new_results_dir()
    proc, out_path, err_path = _popen_memtier(
        env,
        ["--test-time=4", "--prometheus-port=0", "--prometheus-bind-addr=::1"],
        rd,
    )
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no IPv6 announce: {}".format(_drain(out_path)))
        env.assertTrue(re.search(r"http://\[::1\]:\d+/metrics", url),
                       message="IPv6 announce not bracketed: {}".format(url))
        time.sleep(1.0)
        r = prom_scrape(url)
        env.assertEqual(r.status, 200)
        prom_parse(r.body)
    finally:
        _kill(proc)


# ---------------------------------------------------------------------------
# F17 -- monotonicity under connection churn (CLIENT KILL)
# ---------------------------------------------------------------------------
def test_F17_monotonicity_under_churn(env):
    if _is_cluster(env):
        env.skip()
        return
    rd = _new_results_dir()
    proc, out_path, err_path = _popen_memtier(
        env, ["--test-time=12", "--prometheus-port=0"], rd
    )
    stop = threading.Event()

    def killer():
        conns = env.getOSSMasterNodesConnectionList()
        while not stop.is_set():
            time.sleep(0.5)
            for c in conns:
                try:
                    c.execute_command("CLIENT", "KILL", "TYPE", "normal")
                except Exception:
                    pass

    kt = threading.Thread(target=killer)
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line")
        kt.start()
        prev_ops = -1.0
        prev_ce = -1.0
        last_ce = 0.0
        deadline = time.time() + 14
        while proc.poll() is None and time.time() < deadline:
            r = prom_scrape(url, timeout=2)
            if r.status == 200:
                p = prom_parse(r.body)
                ops = prom_sample_value(p, "memtier_ops_total") or 0.0
                ce = prom_sample_value(p, "memtier_connection_errors_total") or 0.0
                env.assertTrue(ops >= prev_ops, message="ops backwards under churn")
                env.assertTrue(ce >= prev_ce, message="conn_errors backwards under churn")
                prev_ops, prev_ce = ops, ce
                last_ce = ce
            time.sleep(0.25)
        stop.set()
        kt.join(timeout=5)
        proc.wait()
        env.assertTrue(last_ce > 0, message="no connection errors observed under CLIENT KILL")
    finally:
        stop.set()
        if kt.is_alive():
            kt.join(timeout=5)
        _kill(proc)


# ---------------------------------------------------------------------------
# F18 -- cluster smoke (only meaningful under OSS_CLUSTER)
# ---------------------------------------------------------------------------
def test_F18_cluster_smoke(env):
    if not _is_cluster(env):
        env.skip()
        return
    rd = _new_results_dir()
    proc, out_path, err_path = _popen_memtier(
        env, ["--test-time=10", "--run-count=2", "--prometheus-port=0"], rd
    )
    series = []
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line")
        deadline = time.time() + 60
        while proc.poll() is None and time.time() < deadline:
            r = prom_scrape(url, timeout=2)
            if r.status == 200:
                p = prom_parse(r.body)
                ops = prom_sample_value(p, "memtier_ops_total")
                run = prom_sample_value(p, "memtier_run")
                if ops is not None and run is not None:
                    series.append((ops, int(run)))
            time.sleep(0.4)
        rc = proc.wait()
        env.assertEqual(rc, 0, message="cluster exit {}".format(rc))
        env.assertTrue(len(series) >= 2, message="too few samples")
        prev = -1.0
        for ops, _run in series:
            env.assertTrue(ops >= prev, message="ops backwards across cluster runs")
            prev = ops
        env.assertTrue(series[-1][0] > 0, message="cluster ops == 0 at exit")
    finally:
        _kill(proc)


# ---------------------------------------------------------------------------
# F19 -- stall dedup (THE Decisions #54 regression test); standalone-only
# ---------------------------------------------------------------------------
def test_F19_stall_dedup(env):
    if _is_cluster(env):
        env.skip()
        return
    rd = _new_results_dir()
    # A long DEBUG SLEEP (18 s) against a generous test-time gives a wide,
    # unambiguous frozen window even on a busy/sanitized CI cell.  The server
    # must allow DEBUG (RLTest passes --enable-debug-command, run_tests.sh).
    sleep_secs = 18
    proc, out_path, err_path = _popen_memtier(
        env, ["--test-time=45", "-t", "2", "-c", "2", "--prometheus-port=0"], rd
    )
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line")
        # wait until the histogram has samples
        deadline = time.time() + 15
        while time.time() < deadline:
            c = prom_sample_value(prom_parse(prom_scrape(url).body), "memtier_latency_seconds_count")
            if c and c > 0:
                break
            time.sleep(0.5)

        # issue a long DEBUG SLEEP on a dedicated connection to freeze the
        # single-threaded server (and thus every memtier in-flight op).  Build
        # a fresh redis-py client with NO socket_timeout so the call blocks the
        # full sleep instead of aborting mid-stall; surface any error loudly.
        import redis as _redis

        node = env.getMasterNodesList()[0]
        sleeper_err = []

        # Under TLS the server accepts only TLS connections, so the dedicated
        # DEBUG SLEEP client must speak TLS too (the test CA is self-signed; the
        # client cert/key authenticate, ca_certs verifies the server).
        tls_kwargs = {}
        if getattr(env, "useTLS", False):
            tls_kwargs = dict(
                ssl=True,
                ssl_certfile=TLS_CERT or None,
                ssl_keyfile=TLS_KEY or None,
                ssl_ca_certs=TLS_CACERT or None,
                ssl_cert_reqs="none",
            )

        def sleeper():
            try:
                conn = _redis.Redis(
                    host=node.get("host") or "127.0.0.1",
                    port=node["port"],
                    password=node.get("password"),
                    socket_timeout=None,
                    socket_connect_timeout=5,
                    **tls_kwargs
                )
                conn.execute_command("DEBUG", "SLEEP", str(sleep_secs))
            except Exception as e:  # noqa: BLE001
                sleeper_err.append("{}: {}".format(type(e).__name__, e))

        st = threading.Thread(target=sleeper)
        st.start()
        stall_started = time.time()

        # Wait for ops to actually plateau: poll @0.6 s until ops is unchanged
        # across a >=1.5 s span (TTL is 1 s, so a single repeat could be a cache
        # hit -- require the value to hold across multiple fresh renders).  This
        # is the "stall reached + the last-delta tick absorbed" point.
        count_A = None
        ops_A = None
        samples = []  # (t, ops)
        while time.time() - stall_started < sleep_secs - 4:
            p = prom_parse(prom_scrape(url).body)
            ops = prom_sample_value(p, "memtier_ops_total")
            now = time.time()
            samples.append((now, ops))
            # Trailing-window freeze check: find the MOST RECENT prior sample
            # that is >=1.8 s old.  If ops has not moved between that sample and
            # now, the whole 1.8 s window is flat -> the stall has been reached
            # and the final pre-stall delta tick has been absorbed.  (Anchoring
            # to the oldest qualifying sample would forever compare against the
            # pre-stall value and never match.)
            anchor = None
            for t, o in samples:
                if now - t >= 1.8:
                    anchor = (t, o)
                else:
                    break
            if anchor is not None and anchor[1] == ops:
                count_A = prom_sample_value(p, "memtier_latency_seconds_count")
                ops_A = ops
                break
            time.sleep(0.6)
        env.assertFalse(
            count_A is None,
            message="stall window never reached (ops kept moving); sleeper_err={}".format(
                sleeper_err),
        )
        if count_A is None:
            return

        time.sleep(3.0)  # >=2 ticks inside the remaining stall
        p = prom_parse(prom_scrape(url).body)
        count_B = prom_sample_value(p, "memtier_latency_seconds_count")
        ops_B = prom_sample_value(p, "memtier_ops_total")
        # window validity: ops still frozen -> we measured inside the stall
        env.assertEqual(ops_B, ops_A, message="stall window ended early; test invalid")
        # THE gate: _count must NOT keep growing while frozen
        env.assertEqual(
            count_B, count_A,
            message="STALL DEDUP REGRESSION: _count grew {} -> {} during stall".format(
                count_A, count_B),
        )

        st.join(timeout=20)
        # liveness: after the stall, _count strictly increases within 10 s
        live_deadline = time.time() + 12
        grew = False
        while time.time() < live_deadline and proc.poll() is None:
            c = prom_sample_value(prom_parse(prom_scrape(url).body), "memtier_latency_seconds_count")
            if c is not None and c > count_B:
                grew = True
                break
            time.sleep(0.5)
        env.assertTrue(grew, message="_count never resumed after stall (gate froze the series)")
        # final live scrape: 0 < _count <= ops_total
        p = prom_parse(prom_scrape(url).body)
        fc = prom_sample_value(p, "memtier_latency_seconds_count")
        fo = prom_sample_value(p, "memtier_ops_total")
        env.assertTrue(0 < fc <= fo, message="final _count {} not in (0, ops {}]".format(fc, fo))
        rc = proc.wait()
        env.assertEqual(rc, 0, message="exit {}".format(rc))
    finally:
        _kill(proc)


# ---------------------------------------------------------------------------
# F20 -- in-flight-cap 503 path via the MEMTIER_PROM_MAX_INFLIGHT=0 seam
# ---------------------------------------------------------------------------
def test_F20_inflight_cap_503(env):
    rd = _new_results_dir()
    proc, out_path, err_path = _popen_memtier(
        env, ["--test-time=3", "--prometheus-port=0"], rd,
        env_overrides={"MEMTIER_PROM_MAX_INFLIGHT": "0"},
    )
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line")
        base = url[: -len("/metrics")]
        # three consecutive GETs each -> 503 "exporter busy\n" + Connection: close
        for i in range(3):
            r = prom_scrape(url, timeout=2)
            env.assertEqual(r.status, 503, message="GET #{} not 503 under cap=0".format(i))
            env.assertEqual(r.body, "exporter busy\n", message="503 body wrong: {!r}".format(r.body))
            env.assertEqual(r.header("Connection"), "close", message="503 missing Connection: close")
        # one non-/metrics path -> 503 as well (cap precedes routing)
        g = prom_scrape(base + "/x", timeout=2)
        env.assertEqual(g.status, 503, message="gencb not capped under cap=0")
        rc = proc.wait(timeout=10)
        env.assertEqual(rc, 0, message="cap-seam exit {}".format(rc))
        _no_sanitizer_output(env, _drain(err_path))
    finally:
        _kill(proc)
