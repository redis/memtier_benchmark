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
import json
import os
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
    return run_config


def _get_count(run_config):
    """Read ALL STATS.Gets.Count from mb.json -- the number of GET ops the
    run actually completed, independent of OBJECT IDLETIME."""
    with open(os.path.join(run_config.results_dir, "mb.json")) as f:
        data = json.load(f)
    return data["ALL STATS"]["Gets"]["Count"]


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
    # Restored afterward -- this connects to the shared master, and other
    # tests running later in the same env should not inherit the override.
    orig_policy = master_connection.config_get("maxmemory-policy")["maxmemory-policy"]
    master_connection.config_set("maxmemory-policy", "noeviction")
    try:
        master_connection.set(_KEY, "v")
        master_connection.get(_KEY)  # reset idle time to 0
        idle_before = _wait_for_idle_time(master_connection, _KEY, lambda idle: idle >= 2)
        env.assertTrue(idle_before >= 2,
                       message="idle time should have accrued before the run; got {}".format(idle_before))

        test_dir = tempfile.mkdtemp()
        run_config = _run_get_only(env, test_dir, env.testName, ['--client-no-touch'], test_time=3)

        # Without this, the test would pass vacuously if the run sent zero
        # GETs against _KEY for any reason: idle time only ever grows on its
        # own, so "idle_after >= idle_before" alone doesn't prove any GET
        # traffic actually happened under --client-no-touch specifically.
        get_count = _get_count(run_config)
        env.assertTrue(get_count > 0,
                       message="expected GET traffic against {}; got Count={}".format(_KEY, get_count))

        idle_after = master_connection.execute_command("OBJECT", "IDLETIME", _KEY)
        env.assertTrue(
            idle_after >= idle_before,
            message="--client-no-touch should not reset idle time; before={} after={}".format(
                idle_before, idle_after),
        )
    finally:
        master_connection.config_set("maxmemory-policy", orig_policy)


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
    orig_policy = master_connection.config_get("maxmemory-policy")["maxmemory-policy"]
    master_connection.config_set("maxmemory-policy", "noeviction")
    try:
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
    finally:
        master_connection.config_set("maxmemory-policy", orig_policy)


def test_client_no_touch_rejected_by_server_fails_loudly(env):
    """--client-no-touch must fail the connection loudly if the server
    rejects CLIENT NO-TOUCH, not silently run with the flag doing nothing.

    Exercises the "unsupported/erroring server fails loudly" claim from
    this feature's design without needing an actual pre-7.2 server: an ACL
    user with 'client|no-touch' denied gets the identical -NOPERM rejection
    an old server's -ERR unknown command would produce, on a real 7.2+
    server (the same one every other test in this file already runs
    against).
    """
    env.skipOnCluster()
    env.skipOnVersionSmaller("7.2")

    master_connection = env.getOSSMasterNodesConnectionList()[0]
    user = "nt-no-perm-user"
    password = "nt-no-perm-pw"
    master_connection.execute_command(
        "ACL", "SETUSER", user, "on", ">" + password, "~*", "+@all", "-client|no-touch"
    )
    try:
        config = get_default_memtier_config(threads=1, clients=1, test_time=3)
        config['memtier_benchmark']['requests'] = None

        args = [
            '--authenticate', '{}:{}'.format(user, password),
            '--client-no-touch',
            '--max-reconnect-attempts', '1',
            '--connection-stage-timeout', '3',
        ]
        benchmark_specs = {"name": env.testName, "args": args}
        addTLSArgs(benchmark_specs, env)
        add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())

        test_dir = tempfile.mkdtemp()
        run_config = RunConfig(test_dir, env.testName, config, {})
        ensure_clean_benchmark_folder(run_config.results_dir)
        benchmark = Benchmark.from_json(run_config, benchmark_specs)

        memtier_ok = benchmark.run()
        env.assertTrue(
            memtier_ok == False,
            message="expected memtier to fail loudly when the server rejects CLIENT NO-TOUCH, "
                   "but it exited successfully",
        )

        stderr_path = os.path.join(run_config.results_dir, "mb.stderr")
        with open(stderr_path) as f:
            stderr = f.read()
        env.assertTrue(
            "CLIENT NO-TOUCH failed" in stderr,
            message="expected 'CLIENT NO-TOUCH failed' in stderr; got: {!r}".format(stderr),
        )
    finally:
        master_connection.execute_command("ACL", "DELUSER", user)
