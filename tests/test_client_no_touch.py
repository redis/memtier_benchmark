"""
Functional test for --client-no-touch.

Verifies the actual server-side effect (not just that the wire command was
sent): with --client-no-touch, a GET-only memtier run against a single key
must NOT reset that key's LRU access recency (OBJECT IDLETIME keeps growing).
Without the flag, the same GET-only run resets it back to ~0, which also
serves as the negative control proving the test methodology is sound.

Run with:
  TEST=test_client_no_touch.py OSS_STANDALONE=1 ./tests/run_tests.sh
"""
import tempfile
import time

from include import (
    get_default_memtier_config,
    add_required_env_arguments,
    addTLSArgs,
    ensure_clean_benchmark_folder,
    debugPrintMemtierOnError,
)
from mb import Benchmark, RunConfig

_KEY_PREFIX = "nt-test-"
_KEY = _KEY_PREFIX + "1"


def _wait_for_idle_time(master_connection, key, until, timeout=5.0, interval=0.2):
    """Poll OBJECT IDLETIME until `until(idle)` is true or `timeout` elapses.

    Redis's idle-time clock (server.lruclock) is a 1-second-quantized value
    refreshed by serverCron, not a live timestamp -- see LRU_CLOCK() in
    evict.c. On a loaded/throttled CI host a cron tick can lag by a second
    or more, so a single snapshot right after a fixed sleep (or right after
    a benchmark run) can observe a stale value in either direction: too low
    ("idle time should have accrued" firing early) or too high (a real
    reset not yet reflected). Poll for the expected condition instead of
    asserting on one snapshot; a genuine regression still fails once the
    timeout is exhausted.
    """
    deadline = time.time() + timeout
    idle = master_connection.execute_command("OBJECT", "IDLETIME", key)
    while not until(idle) and time.time() < deadline:
        time.sleep(interval)
        idle = master_connection.execute_command("OBJECT", "IDLETIME", key)
    return idle


def _run_get_only(env, test_dir, name, extra_args, test_time=3):
    config = get_default_memtier_config(threads=1, clients=1, test_time=test_time)
    config['memtier_benchmark']['requests'] = None

    master_nodes_list = env.getMasterNodesList()

    args = [
        '--command', 'GET __key__',
        '--key-minimum', '1',
        '--key-maximum', '1',
        '--key-prefix', _KEY_PREFIX,
    ] + extra_args

    benchmark_specs = {"name": name, "args": args}
    addTLSArgs(benchmark_specs, env)
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    run_config = RunConfig(test_dir, name, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)

    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(run_config, env)
    env.assertTrue(memtier_ok == True)


def test_client_no_touch_preserves_idle_time(env):
    """--client-no-touch: GET traffic must not reset OBJECT IDLETIME."""
    env.skipOnCluster()
    # CLIENT NO-TOUCH is Redis 7.2+ (deps/commands_json/client-no-touch.json:
    # "since": "7.2.0"); older servers reject it and memtier fails the
    # connection loudly (working as intended), which isn't what this test
    # is checking.
    env.skipOnVersionSmaller("7.2")

    master_connection = env.getOSSMasterNodesConnectionList()[0]
    # OBJECT IDLETIME is unsupported under LFU eviction policies; pin to a
    # policy that tracks it so the test is robust to the env's defaults.
    master_connection.config_set("maxmemory-policy", "noeviction")

    master_connection.set(_KEY, "v")
    master_connection.get(_KEY)  # reset idle time to 0
    idle_before = _wait_for_idle_time(master_connection, _KEY, lambda idle: idle >= 2)
    env.assertTrue(idle_before >= 2,
                   message="idle time should have accrued before the run; got {}".format(idle_before))

    test_dir = tempfile.mkdtemp()
    _run_get_only(env, test_dir, env.testName, ['--client-no-touch'], test_time=3)

    idle_after = master_connection.execute_command("OBJECT", "IDLETIME", _KEY)
    env.assertTrue(
        idle_after >= idle_before,
        message="--client-no-touch should not reset idle time; before={} after={}".format(
            idle_before, idle_after),
    )


def test_without_client_no_touch_resets_idle_time(env):
    """Negative control: without the flag, GET traffic resets OBJECT IDLETIME.

    Proves the test methodology above actually detects the effect: absent
    --client-no-touch, ordinary reads DO update recency.
    """
    env.skipOnCluster()
    # CLIENT NO-TOUCH is Redis 7.2+ (deps/commands_json/client-no-touch.json:
    # "since": "7.2.0"); older servers reject it and memtier fails the
    # connection loudly (working as intended), which isn't what this test
    # is checking.
    env.skipOnVersionSmaller("7.2")

    master_connection = env.getOSSMasterNodesConnectionList()[0]
    master_connection.config_set("maxmemory-policy", "noeviction")

    master_connection.set(_KEY, "v")
    master_connection.get(_KEY)  # reset idle time to 0
    idle_before = _wait_for_idle_time(master_connection, _KEY, lambda idle: idle >= 2)
    env.assertTrue(idle_before >= 2,
                   message="idle time should have accrued before the run; got {}".format(idle_before))

    test_dir = tempfile.mkdtemp()
    _run_get_only(env, test_dir, env.testName, [], test_time=3)

    idle_after = _wait_for_idle_time(master_connection, _KEY, lambda idle: idle < idle_before)
    env.assertTrue(
        idle_after < idle_before,
        message="without --client-no-touch, idle time should reset; before={} after={}".format(
            idle_before, idle_after),
    )
