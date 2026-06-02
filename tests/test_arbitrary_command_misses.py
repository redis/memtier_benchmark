"""
Integration tests for the per-key arbitrary-command miss tracking feature.

Each test pre-populates a known subset of keys, runs an arbitrary command
that probes a wider key range, and asserts that the resulting `mb.json`
contains the expected aggregate Hits/Misses counters and per-key buckets
under the "Per-Key Misses" section nested in "ALL STATS".
"""

import json
import os
import tempfile

from include import (
    addTLSArgs,
    add_required_env_arguments,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig


# Keys 1..3 are pre-populated; --key-maximum=10 means ~30% hit rate on average.
_KEY_PREFIX = "memtier-"
_PRELOADED_KEYS = 3
_KEY_RANGE_MAX = 10
_REQUESTS = 200


def _preload_strings(env):
    """Populate keys memtier-1, memtier-2, memtier-3 (string values)."""
    env.flush()
    conn = env.getConnection()
    for i in range(1, _PRELOADED_KEYS + 1):
        conn.set("{}{}".format(_KEY_PREFIX, i), "v{}".format(i))


def _preload_sets(env, prefix):
    """Populate sets prefix-1, prefix-2, prefix-3 (each with 3 members)."""
    env.flush()
    conn = env.getConnection()
    for i in range(1, _PRELOADED_KEYS + 1):
        conn.sadd("{}{}".format(prefix, i), "a", "b", "c")


def _run_benchmark(env, command, miss_tracking="auto", key_prefix=_KEY_PREFIX, requests=_REQUESTS):
    """Run memtier with the given --command and return the parsed mb.json."""
    test_dir = tempfile.mkdtemp()
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--command={}".format(command),
            "--command-key-pattern=R",
            "--command-stats-breakdown=line",
            "--command-miss-tracking={}".format(miss_tracking),
            "--key-prefix={}".format(key_prefix),
            "--key-minimum=1",
            "--key-maximum={}".format(_KEY_RANGE_MAX),
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)

    config = get_default_memtier_config(threads=1, clients=2, requests=requests)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok)

    json_path = "{}/mb.json".format(config.results_dir)
    env.assertTrue(os.path.isfile(json_path))
    with open(json_path) as fh:
        return json.load(fh)


def test_arbitrary_get_single_null_bulk(env):
    """GET — SingleNullBulk shape; expect 1 key bucket and ~30% hit rate."""
    env.skipOnCluster()
    _preload_strings(env)

    result = _run_benchmark(env, "GET __key__")
    per_key = result["ALL STATS"].get("Per-Key Misses", {})
    env.assertContains("GET", per_key)
    cmd_stats = per_key["GET"]

    total_ops = cmd_stats["Total Hits"] + cmd_stats["Total Misses"]
    env.assertEqual(total_ops, _REQUESTS * 2)  # 2 clients
    # Exactly one bucket for GET.
    env.assertContains("key[0] Hits", cmd_stats)
    env.assertNotContains("key[1] Hits", cmd_stats)
    # Bucket sums match overall.
    env.assertEqual(cmd_stats["key[0] Hits"], cmd_stats["Total Hits"])
    env.assertEqual(cmd_stats["key[0] Misses"], cmd_stats["Total Misses"])
    # Hit rate should be in the right ballpark for a uniform random key in
    # [1, 10] with 3 pre-populated keys; allow generous slack to avoid flakes.
    hit_rate = cmd_stats["Total Hits"] / float(total_ops)
    env.assertTrue(0.15 < hit_rate < 0.50,
                   message="GET hit_rate={} outside [0.15, 0.50]".format(hit_rate))


def test_arbitrary_mget_per_element_nulls(env):
    """MGET — ArrayPerElementNulls; expect K=3 key buckets, no phantom k[3]."""
    env.skipOnCluster()
    _preload_strings(env)

    result = _run_benchmark(env, "MGET __key__ __key__ __key__")
    per_key = result["ALL STATS"].get("Per-Key Misses", {})
    env.assertContains("MGET", per_key)
    cmd_stats = per_key["MGET"]

    total_ops = cmd_stats["Total Hits"] + cmd_stats["Total Misses"]
    env.assertEqual(total_ops, _REQUESTS * 2 * 3)  # 2 clients × 3 keys per request

    for k in (0, 1, 2):
        env.assertContains("key[{}] Hits".format(k), cmd_stats)
    # No phantom 4th bucket from off-by-one in spec evaluation.
    env.assertNotContains("key[3] Hits", cmd_stats)

    # Sum of per-key matches totals.
    sum_hits = sum(cmd_stats["key[{}] Hits".format(k)] for k in (0, 1, 2))
    sum_misses = sum(cmd_stats["key[{}] Misses".format(k)] for k in (0, 1, 2))
    env.assertEqual(sum_hits, cmd_stats["Total Hits"])
    env.assertEqual(sum_misses, cmd_stats["Total Misses"])


def test_arbitrary_smembers_empty_collection(env):
    """SMEMBERS on missing key returns empty array; empty == miss heuristic."""
    env.skipOnCluster()
    set_prefix = "memtier-set-"
    _preload_sets(env, set_prefix)

    result = _run_benchmark(env, "SMEMBERS __key__", key_prefix=set_prefix)
    per_key = result["ALL STATS"].get("Per-Key Misses", {})
    env.assertContains("SMEMBERS", per_key)
    cmd_stats = per_key["SMEMBERS"]

    total_ops = cmd_stats["Total Hits"] + cmd_stats["Total Misses"]
    env.assertEqual(total_ops, _REQUESTS * 2)
    # Exactly one bucket.
    env.assertContains("key[0] Hits", cmd_stats)
    env.assertNotContains("key[1] Hits", cmd_stats)
    # Misses must be present (we query keys 4..10 which are empty/missing).
    env.assertTrue(cmd_stats["Total Misses"] > 0,
                   message="expected SMEMBERS misses on missing keys")


def test_arbitrary_exists_integer_membership(env):
    """EXISTS — IntegerMembership; integer reply N == hit count."""
    env.skipOnCluster()
    _preload_strings(env)

    result = _run_benchmark(env, "EXISTS __key__")
    per_key = result["ALL STATS"].get("Per-Key Misses", {})
    env.assertContains("EXISTS", per_key)
    cmd_stats = per_key["EXISTS"]

    total_ops = cmd_stats["Total Hits"] + cmd_stats["Total Misses"]
    env.assertEqual(total_ops, _REQUESTS * 2)
    env.assertContains("key[0] Hits", cmd_stats)
    env.assertNotContains("key[1] Hits", cmd_stats)


def test_arbitrary_miss_tracking_off_omits_per_key_section(env):
    """--command-miss-tracking=off: backward-compatible JSON shape."""
    env.skipOnCluster()
    _preload_strings(env)

    result = _run_benchmark(env, "GET __key__", miss_tracking="off")
    all_stats = result["ALL STATS"]

    # Per-Key Misses section absent entirely.
    env.assertNotContains("Per-Key Misses", all_stats)

    # Aggregate Hits/sec and Misses/sec must be exactly zero (no bookkeeping).
    # The "Gets" entry exists when --command-stats-breakdown=command (default).
    env.assertContains("Gets", all_stats)
    env.assertEqual(all_stats["Gets"]["Hits/sec"], 0.0)
    env.assertEqual(all_stats["Gets"]["Misses/sec"], 0.0)


def test_arbitrary_hmget_per_field_buckets(env):
    """HMGET has 1 Redis key but produces N reply elements (one per field).
    Bucket count must match the reply length (==fields), not the spec key
    count. Regression for the off-by-one Cursor Bugbot caught.
    """
    env.skipOnCluster()
    env.flush()
    conn = env.getConnection()
    # Populate hashes 1..3 with f1, f2 (but NOT f_missing).
    for i in range(1, _PRELOADED_KEYS + 1):
        conn.hset("{}{}".format(_KEY_PREFIX, i), mapping={"f1": "v1", "f2": "v2"})

    result = _run_benchmark(env, "HMGET __key__ f1 f2 f_missing")
    per_key = result["ALL STATS"].get("Per-Key Misses", {})
    env.assertContains("HMGET", per_key)
    cmd_stats = per_key["HMGET"]
    total_ops = cmd_stats["Total Hits"] + cmd_stats["Total Misses"]
    # 200 reqs * 2 clients * 3 fields per request
    env.assertEqual(total_ops, _REQUESTS * 2 * 3)
    # Exactly 3 buckets (one per reply element / field), not 1 (spec key count).
    for k in (0, 1, 2):
        env.assertContains("key[{}] Hits".format(k), cmd_stats)
    env.assertNotContains("key[3] Hits", cmd_stats)
    # Field f_missing (position 2) is never present anywhere — 0 hits.
    env.assertEqual(cmd_stats["key[2] Hits"], 0)
    env.assertEqual(cmd_stats["key[2] Misses"], _REQUESTS * 2)
    # Fields f1 and f2 (positions 0, 1) hit only when the hash key is one of
    # the 3 populated keys.
    env.assertTrue(cmd_stats["key[0] Hits"] > 0)
    env.assertTrue(cmd_stats["key[1] Hits"] > 0)


def test_arbitrary_geopos_nested_array_elements(env):
    """GEOPOS returns array of (nested-array | null); reply walker must not
    crash on nested-array elements. Regression for the as_bulk() assert that
    Cursor Bugbot caught — affects GEOPOS, COMMAND INFO, SORT_RO, etc.
    """
    env.skipOnCluster()
    env.flush()
    conn = env.getConnection()
    geo_prefix = "memtier-geo-"
    # Pre-populate 3 geo keys (1..3); query range 1..10 to exercise both
    # populated-key (nested-array element) and missing-key (null bulk) paths.
    # Use execute_command to stay compatible across redis-py 3.x/4.x signatures.
    for i in range(1, _PRELOADED_KEYS + 1):
        conn.execute_command("GEOADD", "{}{}".format(geo_prefix, i),
                             -122.0 - i, 37.0 + i, "loc")

    # Single member query; response is array of [lon, lat] or null per member.
    result = _run_benchmark(env, "GEOPOS __key__ loc", key_prefix=geo_prefix)
    per_key = result["ALL STATS"].get("Per-Key Misses", {})
    env.assertContains("GEOPOS", per_key)
    cmd_stats = per_key["GEOPOS"]
    total_ops = cmd_stats["Total Hits"] + cmd_stats["Total Misses"]
    env.assertEqual(total_ops, _REQUESTS * 2)
    # Both hits and misses must be observed (not 0/all) — proves the walker
    # didn't crash and correctly distinguished nested-array from null.
    env.assertTrue(cmd_stats["Total Hits"] > 0,
                   message="expected GEOPOS hits on populated geo keys")
    env.assertTrue(cmd_stats["Total Misses"] > 0,
                   message="expected GEOPOS misses on missing geo keys")


def test_arbitrary_set_not_missable_no_per_key_section(env):
    """SET has reply_shape NotMissable — no Per-Key entry should appear."""
    env.skipOnCluster()
    env.flush()

    result = _run_benchmark(env, "SET __key__ __data__")
    per_key = result["ALL STATS"].get("Per-Key Misses", {})
    # SET should not produce any per-key bookkeeping.
    env.assertNotContains("SET", per_key)


def test_arbitrary_spop_with_count_empty_array_is_miss(env):
    """SPOP key COUNT N returns *0 (empty array) on missing key, not null bulk.

    Regression for Bugbot finding #7: the SingleNullBulk null-sentinel set
    was {$-1, *-1} only; *0 was misclassified as a hit. With the fix and
    pre-loaded 3 keys × 5 members each, destructive SPOPs should drain
    populated sets quickly (~15 hits total) and report the rest as misses.
    Without the fix, every call would record a hit.
    """
    env.skipOnCluster()
    env.flush()
    conn = env.getConnection()
    # 3 keys pre-populated, each with 5 members. SPOP count=1 destroys
    # one member per successful call, so total successful pops = 15.
    for i in range(1, _PRELOADED_KEYS + 1):
        conn.sadd("{}{}".format(_KEY_PREFIX, i), "m1", "m2", "m3", "m4", "m5")

    result = _run_benchmark(env, "SPOP __key__ 1")
    per_key = result["ALL STATS"].get("Per-Key Misses", {})
    env.assertContains("SPOP", per_key)
    cmd_stats = per_key["SPOP"]

    total_ops = cmd_stats["Total Hits"] + cmd_stats["Total Misses"]
    env.assertEqual(total_ops, _REQUESTS * 2)
    # Misses MUST dominate. Without the *0 fix, Total Misses would be 0.
    env.assertTrue(cmd_stats["Total Misses"] > cmd_stats["Total Hits"],
                   message="expected SPOP key COUNT misses on empty/missing keys "
                           "(seeing {}/{} hits/misses; *0 may not be classified as miss)"
                           .format(cmd_stats["Total Hits"], cmd_stats["Total Misses"]))


def test_arbitrary_blpop_variadic_keys_single_bucket(env):
    """BLPOP key1 key2 ... timeout returns [winning_key, value] on success.

    Regression for Bugbot finding #6: the SingleNullBulk handler used
    response->get_hits() (which counts every non-null bulk recursively),
    causing BLPOP/BRPOP/BZPOPMAX/BZPOPMIN replies to inflate hit counts
    and report multiple per-key buckets. With the fix, BLPOP must always
    show exactly one bucket regardless of how many key slots the user
    supplied; we don't currently parse the winning key from the reply.
    """
    env.skipOnCluster()
    env.flush()
    conn = env.getConnection()
    # Preload memtier-1 with enough items so every BLPOP call returns
    # immediately (the test fixes key range to [1,1] below to guarantee that).
    for _ in range(_REQUESTS * 2 + 10):
        conn.rpush("{}1".format(_KEY_PREFIX), "v")

    test_dir = tempfile.mkdtemp()
    benchmark_specs = {
        "name": env.testName,
        # Three key slots + 1s timeout. Key range [1,1] makes every pick
        # land on the populated memtier-1 list, so BLPOP returns instantly.
        "args": [
            "--command=BLPOP __key__ __key__ __key__ 1",
            "--command-key-pattern=R",
            "--command-stats-breakdown=line",
            "--key-prefix={}".format(_KEY_PREFIX),
            "--key-minimum=1",
            "--key-maximum=1",
            "--hide-histogram",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=2, requests=_REQUESTS)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())
    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    benchmark = Benchmark.from_json(config, benchmark_specs)
    env.assertTrue(benchmark.run())
    debugPrintMemtierOnError(config, env)

    with open("{}/mb.json".format(config.results_dir)) as fh:
        result = json.load(fh)
    per_key = result["ALL STATS"].get("Per-Key Misses", {})
    env.assertContains("BLPOP", per_key)
    cmd_stats = per_key["BLPOP"]
    # Exactly one bucket, regardless of the 3 __key__ placeholders.
    # Without the fix, get_hits()=2 for [key,value] reply would mark two
    # buckets as hit and one as miss per call.
    env.assertContains("key[0] Hits", cmd_stats)
    env.assertNotContains("key[1] Hits", cmd_stats,
                          message="BLPOP should produce a single bucket; per-key "
                                  "attribution beyond hit/miss isn't extracted")


def test_arbitrary_xread_keyword_spec_evaluates_correctly(env):
    """XREAD COUNT N STREAMS key id uses Keyword begin_search.

    Regression for Bugbot finding #4 (Keyword/Keynum off-by-one in
    evaluate_key_spec). Before the fix, args[idx-1] always read the
    command name first, mis-locating the STREAMS keyword. With the fix
    XREAD's spec resolves correctly and miss tracking works against the
    SingleNullBulk shape (object reply on hit, null on miss).
    """
    env.skipOnCluster()
    env.flush()
    conn = env.getConnection()
    # Preload 3 streams (memtier-1..3) each with one entry.
    for i in range(1, _PRELOADED_KEYS + 1):
        conn.execute_command("XADD", "{}{}".format(_KEY_PREFIX, i), "*", "f", "v")

    result = _run_benchmark(env, "XREAD COUNT 1 STREAMS __key__ 0")
    per_key = result["ALL STATS"].get("Per-Key Misses", {})
    env.assertContains("XREAD", per_key)
    cmd_stats = per_key["XREAD"]
    total_ops = cmd_stats["Total Hits"] + cmd_stats["Total Misses"]
    env.assertEqual(total_ops, _REQUESTS * 2)
    # Both hits and misses must be observable, proving the Keyword spec
    # resolved correctly and SingleNullBulk routed status to is_null check.
    env.assertTrue(cmd_stats["Total Hits"] > 0,
                   message="expected XREAD hits on populated streams")
    env.assertTrue(cmd_stats["Total Misses"] > 0,
                   message="expected XREAD misses on missing streams")


def _server_supports_resp3(env):
    """Return True if the Redis server accepts HELLO 3 (Redis 6+)."""
    try:
        conn = env.getConnection()
        # HELLO 3 switches the connection to RESP3 and returns a server map.
        # Older servers return an error.  We reset back to RESP2 afterwards.
        resp = conn.execute_command("HELLO", "3")
        conn.execute_command("HELLO", "2")
        return resp is not None
    except Exception:
        return False


def test_arbitrary_get_resp3_nil_counted_as_miss(env):
    """GET under --protocol=resp3 returns '_\\r\\n' (RESP3 null) for missing
    keys.  Before the fix the SingleNullBulk sentinel did not include '_' so
    every RESP3 nil was counted as a hit, silently understating the miss rate.

    Gated by capability detection: the test is skipped when the test Redis
    does not support HELLO 3 (Redis < 6).
    """
    env.skipOnCluster()
    if not _server_supports_resp3(env):
        env.skip()
        return

    _preload_strings(env)

    test_dir = tempfile.mkdtemp()
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--command=GET __key__",
            "--command-key-pattern=R",
            "--command-stats-breakdown=line",
            "--command-miss-tracking=auto",
            "--key-prefix={}".format(_KEY_PREFIX),
            "--key-minimum=1",
            "--key-maximum={}".format(_KEY_RANGE_MAX),
            "--hide-histogram",
            "--protocol=resp3",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=2, requests=_REQUESTS)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok)

    with open("{}/mb.json".format(config.results_dir)) as fh:
        result = json.load(fh)

    per_key = result["ALL STATS"].get("Per-Key Misses", {})
    env.assertContains("GET", per_key,
                       message="Per-Key Misses section missing GET entry under resp3")
    cmd_stats = per_key["GET"]

    total_ops = cmd_stats["Total Hits"] + cmd_stats["Total Misses"]
    env.assertEqual(total_ops, _REQUESTS * 2)
    # Keys 4..10 do not exist.  With ~70% miss rate expected we insist on at
    # least one miss so that a zero-miss result (as produced by the pre-fix
    # code that counted RESP3 nil as a hit) is a test failure.
    env.assertTrue(
        cmd_stats["Total Misses"] > 0,
        message=(
            "GET under resp3: Total Misses=0 (RESP3 nil '_' not recognised "
            "as miss sentinel — fix regression in SingleNullBulk handler)"
        ),
    )
    # Basic sanity: total hits must also be non-zero (keys 1..3 do exist).
    env.assertTrue(
        cmd_stats["Total Hits"] > 0,
        message="GET under resp3: Total Hits=0 unexpectedly (keys 1..3 were pre-loaded)",
    )


def test_arbitrary_eval_keynum_spec_runs_cleanly(env):
    """EVAL has Keynum begin_search; verify resolve_command_meta doesn't crash.

    EVAL's reply_shape is NotMissable (script return value is opaque), so
    no Per-Key entry should appear — but the Keynum spec evaluation must
    not abort the run. Regression for Bugbot finding #4 (Keynum path
    previously read the wrong arg via args[idx-1] = "script body" instead
    of args[idx] = "numkeys", causing strtol to fail).
    """
    env.skipOnCluster()
    _preload_strings(env)

    # Minimal script returning 1; numkeys=1 followed by one __key__.
    result = _run_benchmark(env, "EVAL return 1 1 __key__")
    all_stats = result["ALL STATS"]
    per_key = all_stats.get("Per-Key Misses", {})
    # NotMissable -> no per-key bookkeeping for EVAL.
    env.assertNotContains("EVAL", per_key)
    # But the run must have produced ops — proves EVAL command was sent
    # and parsed without the Keynum evaluator aborting startup.
    eval_entry = all_stats.get("Evals", {})
    env.assertTrue(eval_entry.get("Count", 0) > 0,
                   message="expected EVAL to run (Keynum spec evaluation)")


def test_arbitrary_get_resp3_literal_underscore_not_counted_as_miss(env):
    """GET under --protocol=resp3 must NOT count a literal '_' value as a miss.

    Bugbot LOW on PR #435: the ArrayPerElementNulls walker used a content
    heuristic (value_len==1 && value[0]=='_') that could not distinguish
    a RESP3 null (parsed by single_type as strdup("_")) from a legitimate
    bulk string whose content is the single character '_' (parsed via
    blob_type from '$1\\r\\n_\\r\\n').  Both produced identical bulk_el
    value/value_len before the fix.

    The fix adds a bulk_el::is_resp3_null flag set only by single_type when
    the type byte is '_', so blob_type-constructed elements (real data) are
    never mistaken for nulls.

    This test pre-populates keys with the literal value '_', then runs GET
    under resp3 and asserts that Total Misses < total operations (i.e. at
    least the pre-populated keys are counted as hits, not as nulls).
    """
    env.skipOnCluster()
    if not _server_supports_resp3(env):
        env.skip()
        return

    # Populate ALL keys in the range with the literal value "_"
    # so that every GET in the benchmark hits an existing key.
    env.flush()
    conn = env.getConnection()
    for i in range(1, _KEY_RANGE_MAX + 1):
        conn.set("{}{}".format(_KEY_PREFIX, i), "_")

    test_dir = tempfile.mkdtemp()
    benchmark_specs = {
        "name": env.testName,
        "args": [
            "--command=GET __key__",
            "--command-key-pattern=R",
            "--command-stats-breakdown=line",
            "--command-miss-tracking=auto",
            "--key-prefix={}".format(_KEY_PREFIX),
            "--key-minimum=1",
            "--key-maximum={}".format(_KEY_RANGE_MAX),
            "--hide-histogram",
            "--protocol=resp3",
        ],
    }
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=1, clients=2, requests=_REQUESTS)
    master_nodes_list = env.getMasterNodesList()
    add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

    config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)

    benchmark = Benchmark.from_json(config, benchmark_specs)
    memtier_ok = benchmark.run()
    debugPrintMemtierOnError(config, env)
    env.assertTrue(memtier_ok)

    with open("{}/mb.json".format(config.results_dir)) as fh:
        result = json.load(fh)

    per_key = result["ALL STATS"].get("Per-Key Misses", {})
    env.assertContains("GET", per_key,
                       message="Per-Key Misses section missing GET entry under resp3")
    cmd_stats = per_key["GET"]

    total_ops = cmd_stats["Total Hits"] + cmd_stats["Total Misses"]
    env.assertEqual(total_ops, _REQUESTS * 2)

    # Every key in [1, KEY_RANGE_MAX] was populated with "_".  With the
    # content-heuristic bug, all of these would be counted as RESP3 nulls
    # (misses).  With the is_resp3_null flag fix, the blob_type-constructed
    # bulk_el carries is_resp3_null=false, so they are correctly counted as
    # hits.  Assert that Total Misses is strictly less than total_ops
    # (i.e. at least one hit was observed) and that Total Hits > 0.
    env.assertTrue(
        cmd_stats["Total Hits"] > 0,
        message=(
            "GET resp3 with literal '_' values: Total Hits=0 — the content "
            "heuristic is falsely counting real '_' values as RESP3 nulls "
            "(is_resp3_null flag fix not applied or not working)"
        ),
    )
    env.assertEqual(
        cmd_stats["Total Misses"],
        0,
        message=(
            "GET resp3 with literal '_' values: Total Misses={} but all keys "
            "are populated — '_' bulk values are being wrongly counted as "
            "RESP3 null misses".format(cmd_stats["Total Misses"])
        ),
    )
