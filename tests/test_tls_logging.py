"""
Tests for the one-shot negotiated-TLS log line.

When --tls is enabled, memtier_benchmark logs the agreed TLS protocol version
and ciphersuite exactly ONCE for the whole run (not per connection / thread /
shard), on the first completed handshake:

    TLS connection established: protocol TLSv1.3, cipher TLS_AES_256_GCM_SHA384

These tests run across the full CI matrix:
- TLS cells (standalone or cluster): assert the line appears EXACTLY once and
  carries a protocol + cipher, even with many connections.
- Plaintext cells: assert the line does NOT appear.
"""

import json
import os
import tempfile

from include import (
    add_required_env_arguments,
    addTLSArgs,
    debugPrintMemtierOnError,
    ensure_clean_benchmark_folder,
    get_default_memtier_config,
)
from mb import Benchmark, RunConfig

_TLS_LINE = "TLS connection established:"


def _run(env, threads, clients, requests=50):
    benchmark_specs = {"name": env.testName, "args": ["--hide-histogram"]}
    addTLSArgs(benchmark_specs, env)
    config = get_default_memtier_config(threads=threads, clients=clients, requests=requests)
    add_required_env_arguments(benchmark_specs, config, env, env.getMasterNodesList())

    test_dir = tempfile.mkdtemp()
    run_config = RunConfig(test_dir, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)
    benchmark = Benchmark.from_json(run_config, benchmark_specs)
    ok = benchmark.run()
    debugPrintMemtierOnError(run_config, env)

    stderr = ""
    path = "{}/mb.stderr".format(run_config.results_dir)
    if os.path.isfile(path):
        with open(path) as fh:
            stderr = fh.read()
    js = {}
    json_path = "{}/mb.json".format(run_config.results_dir)
    if os.path.isfile(json_path):
        with open(json_path) as fh:
            js = json.load(fh)
    return ok, run_config, stderr, js


def test_tls_negotiated_logged_once(env):
    """With --tls, the negotiated protocol+cipher is logged exactly once even
    across many connections (threads*clients, and every shard in cluster mode).
    Without TLS, the line is absent."""
    # Many connections to prove the one-shot dedup (not per-conn/thread/shard).
    ok, run_config, stderr, js = _run(env, threads=2, clients=4, requests=50)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier did not complete")
        lines = [l for l in stderr.splitlines() if _TLS_LINE in l]
        tls_json = js.get("TLS", {})
        if env.useTLS:
            # stderr: exactly one line carrying protocol + cipher.
            env.assertEqual(len(lines), 1,
                            message="expected exactly 1 TLS log line, got {}: {}".format(len(lines), lines))
            env.assertTrue("protocol" in lines[0] and "cipher" in lines[0],
                           message="TLS line missing protocol/cipher: {}".format(lines[0]))
            env.assertTrue("TLSv1" in lines[0],
                           message="TLS line missing a TLSvX protocol: {}".format(lines[0]))
            # JSON: the same info is preserved under the "TLS" object.
            env.assertContains("TLS", js)
            env.assertTrue(tls_json.get("negotiated_version", "").startswith("TLSv1"),
                           message="JSON TLS.negotiated_version missing/invalid: {}".format(tls_json))
            env.assertTrue(len(tls_json.get("negotiated_cipher", "")) > 0,
                           message="JSON TLS.negotiated_cipher missing: {}".format(tls_json))
        else:
            env.assertEqual(len(lines), 0,
                            message="TLS line should not appear without --tls: {}".format(lines))
            env.assertNotContains("TLS", js)
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)
