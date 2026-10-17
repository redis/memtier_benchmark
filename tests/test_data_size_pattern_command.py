import os
import tempfile
import redis
from include import *
from mb import Benchmark, RunConfig

# --data-size-pattern=S sizes a value by its key index, spreading
# --data-size-range evenly over [--key-minimum, --key-maximum]. Each __data__
# in --command and in __monitor_line@__ replay must follow the most recent
# __key__ before it, like the default SET path; a __data__ with no __key__
# before it gets the minimum size (issue #672).

SIZE_MIN = 10
SIZE_MAX = 500


def expected_size(key_index, key_min, key_max):
    # Same arithmetic as object_generator::get_value().
    a = (key_index - key_min) / float(key_max - key_min)
    return int((SIZE_MAX - SIZE_MIN) * a + SIZE_MIN)


def key_range_args(key_min, key_max):
    args = ["--key-maximum={}".format(key_max)]
    # Key 0 is only reachable through the default; --key-minimum must be >= 1.
    if key_min > 0:
        args.append("--key-minimum={}".format(key_min))
    return args


def run_memtier(env, args, requests, results_dirs=None):
    test_dir = tempfile.mkdtemp()
    benchmark_specs = {
        "name": env.testName,
        # --command-key-pattern applies to the --command before it, so it
        # stays in `args` next to its --command.
        "args": args + [
            "--data-size-range={}-{}".format(SIZE_MIN, SIZE_MAX),
            "--data-size-pattern=S",
            "--key-prefix=memtier-",
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    # One connection, so a P pattern walks the key range exactly once.
    config = get_default_memtier_config(threads=1, clients=1, requests=requests)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    memtier_ok = benchmark.run()
    if results_dirs is not None:
        results_dirs.append(config.results_dir)
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok)
    return memtier_ok


def run_key_range(env, args, key_min, key_max, keys_per_request=1):
    requests = (key_max - key_min + keys_per_request) // keys_per_request
    return run_memtier(env, args + key_range_args(key_min, key_max), requests)


def is_moved(e):
    # redis-py raises MOVED as MovedError (prefix stripped) where it maps it,
    # or as a plain ResponseError starting with "MOVED" otherwise.
    moved = getattr(redis.exceptions, "MovedError", None)
    return (moved is not None and isinstance(e, moved)) or str(e).startswith("MOVED")


def strlen(env, key):
    # Returns None when no shard holds the key.
    for conn in env.getOSSMasterNodesConnectionList():
        try:
            if conn.execute_command("EXISTS", key):
                return conn.execute_command("STRLEN", key)
        except redis.exceptions.ResponseError as e:
            # MOVED: the key lives on another shard. Anything else (and any
            # connection error) is a real failure.
            if not is_moved(e):
                raise
    return None


def assert_sizes_follow_key_index(env, key_min, key_max):
    sizes = {}
    for k in range(key_min, key_max + 1):
        n = strlen(env, "memtier-{}".format(k))
        if n is not None:
            sizes[k] = n
    # One connection walks the whole range, so every key is written
    # (standalone and cluster alike).
    env.assertEqual(len(sizes), key_max - key_min + 1)
    mismatches = [(k, n, expected_size(k, key_min, key_max))
                  for k, n in sorted(sizes.items())
                  if n != expected_size(k, key_min, key_max)]
    env.assertEqual((len(mismatches), mismatches[:5]), (0, []))
    # The range is actually spread, not stuck at the minimum.
    env.assertGreater(len(set(sizes.values())), 10)
    env.assertEqual(sizes.get(key_min), SIZE_MIN)
    env.assertEqual(sizes.get(key_max), SIZE_MAX)


def write_monitor_file(line):
    test_dir = tempfile.mkdtemp()
    monitor_file = os.path.join(test_dir, "monitor.txt")
    with open(monitor_file, "w") as f:
        f.write('[ proxy49 ] 1764031576.604009 [0 172.16.10.147:51682] {}\n'.format(line))
    return monitor_file


def monitor_args(line):
    # __monitor_line@__ picks the line at runtime and goes through
    # resolve_monitor_placeholders()/write_monitor_command_args();
    # __monitor_line1__ would be expanded into a plain --command at startup.
    return ["--monitor-input={}".format(write_monitor_file(line)), "--command=__monitor_line@__"]


def test_command_data_size_pattern_S(env):
    # Default --key-minimum (0): every value used to get the minimum size.
    env.flush()
    key_min, key_max = 0, 1000
    args = ["--command=SET __key__ __data__", "--command-key-pattern=P"]
    if run_key_range(env, args, key_min, key_max):
        assert_sizes_follow_key_index(env, key_min, key_max)


def test_command_data_size_pattern_S_key_minimum(env):
    # --key-minimum > 0 used to underflow the size computation and abort on
    # assert(value_len > 0).
    env.flush()
    key_min, key_max = 100, 1000
    args = ["--command=SET __key__ __data__", "--command-key-pattern=P"]
    if run_key_range(env, args, key_min, key_max):
        assert_sizes_follow_key_index(env, key_min, key_max)


def test_monitor_line_data_size_pattern_S_key_minimum(env):
    env.flush()
    key_min, key_max = 100, 1000
    args = monitor_args('"SET" "__key__" "__data__"') + ["--command-key-pattern=P"]
    if run_key_range(env, args, key_min, key_max):
        assert_sizes_follow_key_index(env, key_min, key_max)


# Two keys per command: each __data__ is sized by the __key__ just before it,
# on both the --command and the __monitor_line@__ paths.
def test_command_mset_data_size_pattern_S(env):
    env.skipOnCluster()  # two keys per MSET would cross slots
    env.flush()
    key_min, key_max = 100, 1000
    args = ["--command=MSET __key__ __data__ __key__ __data__", "--command-key-pattern=P"]
    if run_key_range(env, args, key_min, key_max, keys_per_request=2):
        assert_sizes_follow_key_index(env, key_min, key_max)


def test_monitor_line_mset_data_size_pattern_S(env):
    env.skipOnCluster()  # two keys per MSET would cross slots
    env.flush()
    key_min, key_max = 100, 1000
    args = monitor_args('"MSET" "__key__" "__data__" "__key__" "__data__"') + ["--command-key-pattern=P"]
    if run_key_range(env, args, key_min, key_max, keys_per_request=2):
        assert_sizes_follow_key_index(env, key_min, key_max)


# A __data__ with no __key__ before it gets the minimum size, and must not
# abort with --key-minimum > 0.
def test_command_data_without_key(env):
    env.skipOnCluster()
    env.flush()
    if run_memtier(env, ["--command=RPUSH memtier-list __data__"] + key_range_args(100, 1000), 50):
        values = env.getConnection().execute_command("LRANGE", "memtier-list", 0, -1)
        env.assertEqual(len(values), 50)
        env.assertEqual(set(len(v) for v in values), {SIZE_MIN})


def test_command_data_without_key_default_key_maximum(env):
    # With the default --key-maximum the wrapped index did not abort: the
    # undefined conversion gave a multi-GB size, so memtier sent a huge bulk
    # length and Redis replied "-ERR Protocol error: invalid bulk length".
    env.skipOnCluster()
    env.flush()
    results_dirs = []
    if run_memtier(env, ["--command=ECHO __data__", "--key-minimum=100"], 20, results_dirs):
        for name in ("mb.stdout", "mb.stderr"):
            with open(os.path.join(results_dirs[0], name), errors="replace") as f:
                env.assertNotContains("Protocol error", f.read())


def test_command_data_before_key(env):
    env.skipOnCluster()  # the fixed key and __key__ would cross slots
    env.flush()
    key_min, key_max = 100, 1000
    args = ["--command=MSET memtier-fixed __data__ __key__ __data__", "--command-key-pattern=P"]
    if run_key_range(env, args, key_min, key_max):
        env.assertEqual(strlen(env, "memtier-fixed"), SIZE_MIN)
        assert_sizes_follow_key_index(env, key_min, key_max)


def test_monitor_line_data_before_key(env):
    env.skipOnCluster()  # the fixed key and __key__ would cross slots
    env.flush()
    key_min, key_max = 100, 1000
    args = monitor_args('"MSET" "memtier-fixed" "__data__" "__key__" "__data__"') + ["--command-key-pattern=P"]
    if run_key_range(env, args, key_min, key_max):
        env.assertEqual(strlen(env, "memtier-fixed"), SIZE_MIN)
        assert_sizes_follow_key_index(env, key_min, key_max)


def test_monitor_line_data_first_arg(env):
    # __data__ as argument 1 (the key slot) is filled in when the line is
    # resolved, with no __key__ before it: the value becomes the key name and
    # gets the minimum size.
    env.skipOnCluster()
    env.flush()
    if run_memtier(env, monitor_args('"SET" "__data__" "v"') + key_range_args(100, 1000), 20):
        # Every request writes the same key: without --random-data the value
        # buffer is identical each time, so there is one key of the minimum size.
        keys = env.getConnection().execute_command("KEYS", "*")
        env.assertEqual([len(k) for k in keys], [SIZE_MIN])


def test_transaction_rotation_data_size_pattern_S(env):
    # Both SETs in a --transaction rotation reuse one key; the second one,
    # which leaves the final value, must be sized by that key too.
    env.skipOnCluster()
    env.flush()
    key_min, key_max = 100, 1000
    args = ["--command=SET __key__ __data__", "--command-key-pattern=P",
            "--command=SET __key__ __data__", "--command-key-pattern=P",
            "--transaction"] + key_range_args(key_min, key_max)
    if run_memtier(env, args, (key_max - key_min + 1) * 2):
        assert_sizes_follow_key_index(env, key_min, key_max)


def test_default_set_single_key_range(env):
    # --key-minimum == --key-maximum used to divide 0 by 0 in get_value() and
    # abort on the default SET path.
    env.flush()
    args = ["--ratio=1:0", "--key-pattern=P:P"] + key_range_args(5, 5)
    if run_memtier(env, args, 20):
        env.assertEqual(strlen(env, "memtier-5"), SIZE_MIN)
