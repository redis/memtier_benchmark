"""
Regression tests for issue #426 Phase 1 — the --connection-stage-timeout
supervisor that bounds the connection-setup retry / parse loop.

On master prior to this fix every scenario below ran until SIGKILLed: the
worker thread enters a tight AUTH-fail / SELECT-fail / CLUSTER-SLOTS-fail /
HELLO-fail / response-parse-fail / WAIT-never-returns loop that ignores
--test-time. The fix tracks the first connection-stage failure in a streak
on the main thread and aborts the process with rc=2 (plus a diagnostic) if
the streak — or, for the WAIT case, the simple absence of a steady-state
hand-off — exceeds --connection-stage-timeout seconds. A disconnected
worker now fails immediately with rc=1 instead of restarting; the oversized
bulk case checks that path explicitly. The remaining setup stalls assert:

  * the supervisor's exit code 2 (not a parser rejection or a signal)
  * stderr contains the new diagnostic prefix
  * the process exits well within --connection-stage-timeout + 5 s
    wall-budget — i.e. the supervisor really bounded the loop, the test
    didn't just trip its own outer timeout

Run subset:
  TEST=test_cli_fuzz_phase1.py ./tests/run_tests.sh
"""

import os
import subprocess
import tempfile
import time

from include import MEMTIER_BINARY, addTLSArgs


# Short timeout so the regression suite finishes in seconds, not 30 s per
# case. The supervisor's *default* is 30; we pass an explicit override to
# the CLI in every test so we don't grow the suite by a minute.
#
# Sanitizer cells (TSAN especially) slow memtier startup + libevent
# scheduling enough that a 3-s supervisor + 5-s outer budget is too tight:
# TSAN added ~3 s of overhead per case on the OSS-TLS cells, tipping
# case #8 (--data-size-range with the reconnect-loop server reply) past
# the budget. Detect sanitizer instrumentation via the runtime env hooks
# the CI workflows set (`ASAN_OPTIONS`, `TSAN_OPTIONS`, `UBSAN_OPTIONS`)
# and double the supervisor + outer budgets when any of them is present.
_SANITIZER_ACTIVE = any(
    os.environ.get(k) for k in ("ASAN_OPTIONS", "TSAN_OPTIONS", "UBSAN_OPTIONS")
)
SUPERVISOR_TIMEOUT_SECS = 6 if _SANITIZER_ACTIVE else 3
WALL_BUDGET_SECS = SUPERVISOR_TIMEOUT_SECS + (10 if _SANITIZER_ACTIVE else 5)

DIAGNOSTIC_PREFIX = "memtier_benchmark: aborting after"
DIAGNOSTIC_FLAG_HINT = "See --connection-stage-timeout."


def _run_memtier(args):
    """Run memtier with a wall-budget guard and capture stderr.

    Returns (returncode, stderr_text, wall_elapsed_secs).

    The wall-budget guard is *outside* SUPERVISOR_TIMEOUT_SECS so we can
    distinguish "the supervisor fired and the process exited cleanly" from
    "the supervisor failed, our outer timer killed the process".
    """
    with tempfile.TemporaryDirectory() as td:
        stdout_path = os.path.join(td, "mb.stdout")
        stderr_path = os.path.join(td, "mb.stderr")
        with open(stdout_path, "w") as out, open(stderr_path, "w") as err:
            start = time.monotonic()
            try:
                proc = subprocess.Popen(args, stdout=out, stderr=err)
                # Wait at most wall_budget; on overshoot, kill so the test
                # framework's per-test deadline is preserved.
                try:
                    rc = proc.wait(timeout=WALL_BUDGET_SECS)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                    elapsed = time.monotonic() - start
                    with open(stderr_path) as f:
                        stderr_text = f.read()
                    return None, stderr_text, elapsed
            finally:
                pass
            elapsed = time.monotonic() - start
            with open(stderr_path) as f:
                stderr_text = f.read()
            return rc, stderr_text, elapsed


def _assert_supervisor_tripped(env, args, label):
    """Common assertions for a single fuzz scenario."""
    rc, stderr_text, elapsed = _run_memtier(args)
    # Non-None return code = process exited under its own steam.
    env.assertIsNotNone(
        rc,
        message=(
            f"[{label}] memtier did not exit within {WALL_BUDGET_SECS}s — "
            f"--connection-stage-timeout supervisor did NOT bound the loop. "
            f"stderr tail:\n{stderr_text[-2000:]}"
        ),
    )
    env.assertEqual(
        rc, 2,
        message=(
            f"[{label}] expected supervisor exit 2; got rc={rc}. "
            f"stderr tail:\n{stderr_text[-2000:]}"
        ),
    )
    # Diagnostic must mention the supervisor — both the abort prefix and
    # the --connection-stage-timeout hint so the operator can self-serve.
    env.assertTrue(
        DIAGNOSTIC_PREFIX in stderr_text,
        message=(
            f"[{label}] expected '{DIAGNOSTIC_PREFIX}' in stderr; got:\n"
            f"{stderr_text[-2000:]}"
        ),
    )
    env.assertTrue(
        DIAGNOSTIC_FLAG_HINT in stderr_text,
        message=(
            f"[{label}] expected '{DIAGNOSTIC_FLAG_HINT}' in stderr; got:\n"
            f"{stderr_text[-2000:]}"
        ),
    )
    # Wall budget: must exit well within SUPERVISOR_TIMEOUT + 5 s. This
    # is the key invariant the fix promises — on master prior to #426 fix
    # every case ran for the full WALL_BUDGET (and longer; the outer guard
    # would kill it).
    env.assertTrue(
        elapsed < WALL_BUDGET_SECS,
        message=(
            f"[{label}] memtier took {elapsed:.1f}s to exit, expected < "
            f"{WALL_BUDGET_SECS}s. The supervisor likely didn't fire on time."
        ),
    )


def _assert_worker_failed(env, args, label):
    """A server disconnect must fail the worker without waiting for retries."""
    rc, stderr_text, elapsed = _run_memtier(args)
    env.assertEqual(rc, 1, message=f"[{label}] expected worker failure, got {rc}: {stderr_text[-2000:]}")
    env.assertTrue("client(s) stopped before reaching their stop condition" in stderr_text,
                   message=f"[{label}] missing worker failure diagnostic: {stderr_text[-2000:]}")
    env.assertTrue("the results of this run are not valid" in stderr_text)
    env.assertFalse(DIAGNOSTIC_PREFIX in stderr_text, message="disconnect waited for the supervisor")
    env.assertFalse("Restarting thread" in stderr_text)
    env.assertLess(elapsed, WALL_BUDGET_SECS, message=f"[{label}] worker failure took {elapsed:.1f}s")


def _base_args(env, requests=None):
    """Build the common argv prefix; subtests append scenario-specific flags."""
    if env.isUnixSocket():
        env.skip()
        return None
    if env.isCluster():
        # These tests intentionally target a *standalone* Redis to reproduce
        # the connection-stage retry loops — running them in cluster mode
        # changes (or hides) the failure modes we're regressing.
        env.skip()
        return None

    master_nodes_list = env.getMasterNodesList()
    port = master_nodes_list[0]["port"]
    args = [
        MEMTIER_BINARY,
        "-s", "127.0.0.1",
        "-p", str(port),
        "-c", "1",
        "-t", "1",
        f"--connection-stage-timeout={SUPERVISOR_TIMEOUT_SECS}",
        "--test-time=1" if requests is None else f"--requests={requests}",
        "--hide-histogram",
    ]
    # Otherwise TLS cells only exercise a plaintext connection reset, hiding
    # the AUTH/SELECT/parser/WAIT condition each test is supposed to reach.
    addTLSArgs({"args": args}, env)
    return args


# ---------------------------------------------------------------------------
# Issue #426 item 1: --authenticate '' against no-auth Redis hangs.
# ---------------------------------------------------------------------------
def test_426_1_authenticate_empty_password_against_no_auth(env):
    args = _base_args(env)
    if args is None:
        return
    args.append("--authenticate=")
    _assert_supervisor_tripped(env, args, "#1 --authenticate ''")


# ---------------------------------------------------------------------------
# Issue #426 item 2: --cluster-mode against standalone hangs in CLUSTER SLOTS.
# ---------------------------------------------------------------------------
def test_426_2_cluster_mode_against_standalone(env):
    args = _base_args(env)
    if args is None:
        return
    args.append("--cluster-mode")
    _assert_supervisor_tripped(env, args, "#2 --cluster-mode vs standalone")


# ---------------------------------------------------------------------------
# Issue #426 item 3: memcache_{text,binary} protocol against a Redis server
# (response parser never aligns → tight spin in process_response).
# ---------------------------------------------------------------------------
def test_426_3_memcache_text_against_redis(env):
    args = _base_args(env)
    if args is None:
        return
    args.extend(["--protocol", "memcache_text"])
    _assert_supervisor_tripped(env, args, "#3 --protocol memcache_text vs redis")


def test_426_3_memcache_binary_against_redis(env):
    args = _base_args(env)
    if args is None:
        return
    args.extend(["--protocol", "memcache_binary"])
    _assert_supervisor_tripped(env, args, "#3 --protocol memcache_binary vs redis")


# ---------------------------------------------------------------------------
# Issue #426 item 8: --data-size-range 1-9999999999 → server replies
# "-ERR Protocol error: invalid bulk length" and resets the connection;
# without recovery, memtier must fail the worker rather than retry forever.
#
# This is the most server-dependent of the six; it relies on the server
# actually rejecting the oversized bulk. We temporarily lower
# proto-max-bulk-len so the rejection is deterministic across CI images.
# ---------------------------------------------------------------------------
def test_426_8_data_size_range_too_large(env):
    # Serializing the oversized SET can exceed a one-second test window on
    # sanitizer builds. Keep work unfinished when Redis rejects that request,
    # even though the error reply itself counts as a completed operation.
    args = _base_args(env, requests=100)
    if args is None:
        return

    master_connections = env.getOSSMasterNodesConnectionList()
    original_max = None
    try:
        original_max = master_connections[0].config_get("proto-max-bulk-len").get(
            "proto-max-bulk-len"
        )
        # Clamp below the data-size-range upper bound so the server rejects.
        for c in master_connections:
            c.config_set("proto-max-bulk-len", 100000000)
        args.extend(["--pipeline=1", "--ratio=1:0", "--data-size-range=1-9999999999"])
        _assert_worker_failed(env, args, "#8 --data-size-range 1-9999999999")
    finally:
        if original_max is not None:
            for c in master_connections:
                try:
                    c.config_set("proto-max-bulk-len", original_max)
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# Issue #426 item 11: --select-db beyond the configured DB count → every
# SELECT comes back with "DB index is out of range", connection stays
# half-open, worker loops.
# ---------------------------------------------------------------------------
def test_426_11_select_db_out_of_range(env):
    args = _base_args(env)
    if args is None:
        return
    # 100 is comfortably above the Redis default of 16 DBs.
    args.append("--select-db=100")
    _assert_supervisor_tripped(env, args, "#11 --select-db 100")


# ---------------------------------------------------------------------------
# Issue #426 item 17: --wait-ratio 0:1 --num-slaves 1-10 against a
# standalone (no replicas) → WAIT never returns; first response never
# arrives so we never reach steady state.
# ---------------------------------------------------------------------------
def test_426_17_wait_ratio_unsatisfiable(env):
    args = _base_args(env)
    if args is None:
        return
    args.extend(["--wait-ratio=0:1", "--num-slaves=1-10"])
    _assert_supervisor_tripped(env, args, "#17 --wait-ratio 0:1 --num-slaves 1-10")
