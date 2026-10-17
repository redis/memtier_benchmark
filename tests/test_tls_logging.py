"""
Tests for the one-shot negotiated-TLS log line and related JSON output hygiene.

When --tls is enabled, memtier_benchmark logs the agreed TLS protocol version
and ciphersuite exactly ONCE for the whole run (not per connection / thread /
shard), on the first completed handshake:

    TLS connection established: protocol TLSv1.3, cipher TLS_AES_256_GCM_SHA384

These tests run across the full CI matrix:
- TLS cells (standalone or cluster): assert the line appears EXACTLY once and
  carries a protocol + cipher, even with many connections.
- Plaintext cells: assert the line does NOT appear.

Path-redaction test (test_tls_paths_redacted_in_json):
- When --tls is used, the configuration block in mb.json must contain only the
  basename of cert/key/cacert paths, not the full absolute path.  This prevents
  directory-layout details (e.g. /etc/ssl/private/client.key) from leaking into
  benchmark artifacts that operators share.  Pre-existing in 2.3; fixed in 2.4.
"""

import json
import os
import tempfile

from include import (
    TLS_CACERT,
    TLS_CERT,
    TLS_KEY,
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


def test_tls_paths_redacted_in_json(env):
    """mb.json configuration.cert/key/cacert must be basename-only, not full paths.

    Absolute paths like /etc/ssl/private/client.key leak directory-layout
    details into every benchmark artifact operators share.  The fix (2.4)
    strips to basename before emitting JSON.  This test verifies that:

    - In TLS cells: the JSON values equal os.path.basename(TLS_CERT/KEY/CACERT)
      and contain no directory separator.
    - In plaintext cells: the configuration block emits empty strings for those
      fields (no path leakage in either case).
    """
    ok, run_config, _stderr, js = _run(env, threads=1, clients=2, requests=20)

    failed = env.getNumberOfFailedAssertion()
    try:
        env.assertTrue(ok, message="memtier did not complete")
        env.assertContains("configuration", js,
                           message="mb.json must contain a 'configuration' block")
        cfg = js["configuration"]

        if env.useTLS:
            # Each emitted value must be a pure basename (no slash of any kind).
            for field, full_path in (("cert", TLS_CERT),
                                     ("key", TLS_KEY if TLS_KEY else ""),
                                     ("cacert", TLS_CACERT)):
                emitted = cfg.get(field, None)
                env.assertFalse(emitted is None,
                                message="configuration.{} missing from mb.json".format(field))
                # No directory separator must appear in the emitted value.
                env.assertFalse("/" in emitted,
                                message="configuration.{} leaks a path separator: {!r}".format(
                                    field, emitted))
                env.assertFalse("\\" in emitted,
                                message="configuration.{} leaks a backslash path separator: {!r}".format(
                                    field, emitted))
                # When the full path is known, the basename must match exactly.
                if full_path:
                    expected = os.path.basename(full_path)
                    env.assertEqual(emitted, expected,
                                    message="configuration.{} is {!r}, expected basename {!r} "
                                            "(full path was {!r})".format(
                                                field, emitted, expected, full_path))
        else:
            # Without --tls the fields are emitted as empty strings; confirm no
            # accidental path leaks (e.g. from a misconfigured non-TLS build).
            for field in ("cert", "key", "cacert"):
                emitted = cfg.get(field, "")
                env.assertFalse("/" in emitted,
                                message="configuration.{} contains a slash in non-TLS run: {!r}".format(
                                    field, emitted))
    finally:
        if env.getNumberOfFailedAssertion() > failed:
            debugPrintMemtierOnError(run_config, env)
