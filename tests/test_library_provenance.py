"""libevent/OpenSSL provenance and failure handling.

- The JSON "run information" records the libevent (and, with TLS, OpenSSL)
  version memtier runs with and the one it was built against.
- A matched installation prints no version-mismatch warning; a runtime
  library from another release series than the build headers gets one
  warning on stderr, and stdout is unchanged.
- evthread_use_pthreads() failing is fatal (exit 1) for a run instead of a
  warning; --help and --version still work.
- bufferevent creation failing fails the connection (error exit) instead of
  aborting on an assert.

The last three use tests/library_failure_injector.c (LD_PRELOAD, Linux).
"""
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from include import *
from mb import Benchmark, RunConfig

_MISMATCH = "warning: memtier_benchmark was built with"


def _version_line():
    out = subprocess.run([MEMTIER_BINARY, "--version"], capture_output=True, text=True, timeout=30)
    return out.stdout.splitlines()[0] if out.stdout else ""


def _version_field(line, name):
    for word in line.split():
        if word.startswith(name + "="):
            return word[len(name) + 1:]
    return None


def _has_tls():
    return " openssl=" in _version_line()


def _shim_env(env, directory):
    """Build the preload shim and return a child environment using it, or
    skip the test where that cannot be done."""
    compiler = shlex.split(os.environ.get("CC", "cc"))
    if (sys.platform != "linux" or not compiler or not shutil.which(compiler[0])
            or not shutil.which("ldd")):
        print("SKIP: needs Linux, a C compiler and ldd")
        env.skip()
    binary = shutil.which(MEMTIER_BINARY) or MEMTIER_BINARY
    linked = subprocess.run(["ldd", binary], capture_output=True, text=True, timeout=60)
    if linked.returncode != 0 or "libevent" not in linked.stdout:
        print("SKIP: memtier_benchmark is not dynamically linked against libevent")
        env.skip()
    library = Path(directory) / "library_failure.so"
    try:
        subprocess.run(compiler + ["-shared", "-fPIC",
                                   str(Path(__file__).with_name("library_failure_injector.c")),
                                   "-o", str(library), "-ldl"],
                       check=True, capture_output=True, text=True, timeout=120)
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError) as e:
        print(f"SKIP: compiling the preload shim failed: {e}")
        env.skip()
    # Keep the sanitizer runtime first; its interceptors forward to the shim.
    runtimes = [line.split()[2] for line in linked.stdout.splitlines()
                if line.strip().startswith(("libasan.so", "libtsan.so", "libubsan.so"))
                and "=>" in line and len(line.split()) >= 3]
    preloads = runtimes + [str(library)]
    if os.environ.get("LD_PRELOAD"):
        preloads.append(os.environ["LD_PRELOAD"])
    child_env = dict(os.environ, LD_PRELOAD=":".join(preloads))
    for name in ("MEMTIER_TEST_FAIL_EVTHREAD", "MEMTIER_TEST_FAIL_BUFFEREVENT",
                 "MEMTIER_TEST_LIBEVENT_VERSION", "MEMTIER_TEST_OPENSSL_VERSION"):
        child_env.pop(name, None)
    child_env["MEMTIER_TEST_SHIM_MARKER"] = str(Path(directory) / "shim_marker.txt")
    return child_env


def _marker(child_env):
    try:
        with open(child_env["MEMTIER_TEST_SHIM_MARKER"]) as f:
            return f.read().splitlines()
    except OSError:
        return []


def _run(env, directory, extra_args=(), child_env=None, requests=50):
    """Run memtier against the test server with --json-out-file; return
    (returncode, stderr, stdout, run information or None)."""
    specs = {"name": env.testName, "args": list(extra_args)}
    addTLSArgs(specs, env)
    config = get_default_memtier_config(threads=1, clients=2, requests=requests)
    add_required_env_arguments(specs, config, env, env.getMasterNodesList())
    run_config = RunConfig(directory, env.testName, config, {})
    ensure_clean_benchmark_folder(run_config.results_dir)
    benchmark = Benchmark.from_json(run_config, specs)
    json_path = os.path.join(directory, "out.json")
    out_path = os.path.join(run_config.results_dir, "mb.stdout")
    args = benchmark.args + ["--json-out-file", json_path]
    proc = subprocess.run(args, capture_output=True, text=True, timeout=120, env=child_env)
    if proc.returncode < 0 or "ERROR: AddressSanitizer" in proc.stderr:
        print(proc.stderr[-4000:])
    stdout = proc.stdout
    if os.path.exists(out_path):
        with open(out_path) as f:
            stdout += f.read()
    info = None
    try:
        with open(json_path) as f:
            info = json.load(f).get("run information")
    except (OSError, ValueError):
        pass
    return proc.returncode, proc.stderr, stdout, info


def test_json_run_information_library_versions(env):
    line = _version_line()
    with tempfile.TemporaryDirectory() as directory:
        rc, stderr, _, info = _run(env, directory)
    env.assertEqual(rc, 0, message=stderr[-2000:])
    env.assertTrue(info is not None, message="no run information in the JSON output")
    info = info or {}
    # The runtime version is the one --version reports.
    env.assertEqual(info.get("libevent runtime version"), _version_field(line, "libevent"), message=info)
    env.assertTrue(bool(info.get("libevent build version")), message=info)
    if _has_tls():
        env.assertTrue(bool(info.get("OpenSSL runtime version")), message=info)
        # --version's openssl= field is the build-time OpenSSL text.
        env.assertContains(_version_field(line, "openssl"), info.get("OpenSSL build version", ""))
    else:
        env.assertFalse("OpenSSL runtime version" in info, message=info)
        env.assertFalse("OpenSSL build version" in info, message=info)
    # Existing keys keep their names.
    for key in ("Threads", "Connections per thread", "Runs completed", "Format version"):
        env.assertTrue(key in info, message=key)
    # A matched installation gets no warning.
    env.assertFalse(_MISMATCH in stderr, message=stderr[-2000:])


def test_libevent_series_mismatch_warns_on_stderr(env):
    with tempfile.TemporaryDirectory() as directory:
        child_env = _shim_env(env, directory)
        child_env["MEMTIER_TEST_LIBEVENT_VERSION"] = "2.99.1-fake:0x02630100"
        if _has_tls():
            child_env["MEMTIER_TEST_OPENSSL_VERSION"] = "OpenSSL 15.99.0-fake:0xf6300000"
        rc, stderr, stdout, info = _run(env, directory, child_env=child_env)
        version = subprocess.run([MEMTIER_BINARY, "--version"], capture_output=True, text=True, timeout=30,
                                 env=child_env)
    env.assertEqual(rc, 0, message=stderr[-2000:])
    warnings = [l for l in stderr.splitlines() if l.startswith(_MISMATCH)]
    env.assertEqual(len(warnings), 2 if _has_tls() else 1, message=stderr[-2000:])
    env.assertTrue(any("running with libevent 2.99.1-fake" in l for l in warnings), message=warnings)
    if _has_tls():
        env.assertTrue(any("running with OpenSSL 15.99.0-fake" in l for l in warnings), message=warnings)
    env.assertFalse(_MISMATCH in stdout, message=stdout[-2000:])
    env.assertEqual((info or {}).get("libevent runtime version"), "2.99.1-fake")
    env.assertNotEqual((info or {}).get("libevent build version"), "2.99.1-fake")
    if _has_tls():
        env.assertEqual((info or {}).get("OpenSSL runtime version"), "OpenSSL 15.99.0-fake")
        env.assertNotEqual((info or {}).get("OpenSSL build version"), "OpenSSL 15.99.0-fake")
    # --version stays quiet on stderr.
    env.assertEqual(version.returncode, 0)
    env.assertEqual(version.stderr, "")


def test_libevent_patch_difference_does_not_warn(env):
    with tempfile.TemporaryDirectory() as directory:
        child_env = _shim_env(env, directory)
        # Same series as any 2.1.x build; only a different patch level.
        real = _version_field(_version_line(), "libevent") or ""
        if not real.startswith("2.1."):
            print(f"SKIP: needs a libevent 2.1.x build, have {real}")
            env.skip()
        child_env["MEMTIER_TEST_LIBEVENT_VERSION"] = "2.1.99-fake:0x02016300"
        rc, stderr, _, _ = _run(env, directory, child_env=child_env)
    env.assertEqual(rc, 0, message=stderr[-2000:])
    env.assertFalse(_MISMATCH in stderr, message=stderr[-2000:])


def test_evthread_failure_is_fatal(env):
    with tempfile.TemporaryDirectory() as directory:
        child_env = _shim_env(env, directory)
        child_env["MEMTIER_TEST_FAIL_EVTHREAD"] = "1"
        rc, stderr, _, info = _run(env, directory, child_env=child_env)
        marker = _marker(child_env)
        # --help and --version do not need worker threads and still work
        # (--help exits 2 by design; compare with a run without the shim).
        options = {}
        for option in ("--version", "--help"):
            options[option] = (
                subprocess.run([MEMTIER_BINARY, option], capture_output=True, text=True, timeout=30, env=child_env),
                subprocess.run([MEMTIER_BINARY, option], capture_output=True, text=True, timeout=30))
        option_marker = _marker(child_env)
    env.assertEqual(marker, ["evthread_use_pthreads"])
    env.assertEqual(rc, 1, message=stderr[-2000:])
    env.assertContains("error: evthread_use_pthreads() failed", stderr)
    env.assertTrue(info is None, message=info)
    # A clean exit: nothing left for LeakSanitizer to report.
    env.assertFalse("LeakSanitizer" in stderr, message=stderr[-4000:])
    # The shim was loaded for both option runs too.
    env.assertEqual(option_marker, ["evthread_use_pthreads"] * 3)
    for option, (out, plain) in options.items():
        env.assertEqual(out.returncode, plain.returncode, message=option + ": " + out.stderr[-500:])
        env.assertEqual(out.stdout, plain.stdout, message=option)
        env.assertFalse("evthread_use_pthreads() failed" in out.stderr, message=option)


def test_bufferevent_creation_failure_fails_connection(env):
    with tempfile.TemporaryDirectory() as directory:
        child_env = _shim_env(env, directory)
        child_env["MEMTIER_TEST_FAIL_BUFFEREVENT"] = "1"
        rc, stderr, _, _ = _run(env, directory, child_env=child_env)
        marker = _marker(child_env)
    function = "bufferevent_openssl_socket_new" if env.useTLS else "bufferevent_socket_new"
    env.assertTrue(function in marker, message=marker)
    # An error exit, not a signal (the assert used to abort here).
    env.assertGreater(rc, 0, message=stderr[-2000:])
    env.assertContains("error: " + function + "() failed", stderr)
    # memtier closed the socket the failed call was given (libevent >= 2.1.9
    # and plain bufferevents leave it to the caller). A TLS bufferevent on a
    # real libevent <= 2.1.8 (Rocky Linux 8) may already have closed it, so
    # memtier leaves it open and says so.
    runtime = _version_field(_version_line(), "libevent") or ""
    numbers = [int(part) for part in runtime.split("-")[0].split(".")[:3] if part.isdigit()]
    if env.useTLS and len(numbers) == 3 and tuple(numbers) < (2, 1, 9):
        env.assertContains("left open on purpose: libevent " + runtime, stderr)
        env.assertTrue("socket open" in marker, message=marker)
    else:
        env.assertTrue("socket closed" in marker, message=marker)
    # Standalone aborts the run in prepare; cluster mode retries the
    # connection and ends the run as failed.
    env.assertTrue("failed to connect" in stderr or "the results of this run are not valid" in stderr,
                   message=stderr[-2000:])
    env.assertFalse("ERROR: AddressSanitizer" in stderr, message=stderr[-4000:])


def test_tls_bufferevent_failure_leaves_socket_on_libevent_2_1_8(env):
    """On libevent <= 2.1.8 the socket may already be closed by libevent, so
    memtier leaves it open and says so."""
    if not env.useTLS:
        print("SKIP: needs TLS")
        env.skip()
    with tempfile.TemporaryDirectory() as directory:
        child_env = _shim_env(env, directory)
        child_env["MEMTIER_TEST_FAIL_BUFFEREVENT"] = "1"
        child_env["MEMTIER_TEST_LIBEVENT_VERSION"] = "2.1.8-fake:0x02010800"
        rc, stderr, _, _ = _run(env, directory, child_env=child_env)
        marker = _marker(child_env)
    env.assertTrue("bufferevent_openssl_socket_new" in marker, message=marker)
    env.assertGreater(rc, 0, message=stderr[-2000:])
    env.assertContains("left open on purpose: libevent 2.1.8-fake", stderr)
    env.assertTrue("socket open" in marker, message=marker)
    env.assertFalse("ERROR: AddressSanitizer" in stderr, message=stderr[-4000:])


def test_crash_report_separates_build_and_runtime_versions(env):
    """With a faked runtime libevent/OpenSSL, the crash report's runtime
    fields show the fake and its build fields do not."""
    import re
    import signal
    import time
    with tempfile.TemporaryDirectory() as directory:
        child_env = _shim_env(env, directory)
        child_env["MEMTIER_TEST_LIBEVENT_VERSION"] = "2.99.1-fake:0x02630100"
        if _has_tls():
            child_env["MEMTIER_TEST_OPENSSL_VERSION"] = "OpenSSL 15.99.0-fake:0xf6300000"
        specs = {"name": env.testName, "args": []}
        addTLSArgs(specs, env)
        config = get_default_memtier_config(threads=1, clients=1, requests=None, test_time=30)
        add_required_env_arguments(specs, config, env, env.getMasterNodesList())
        run_config = RunConfig(directory, env.testName, config, {})
        ensure_clean_benchmark_folder(run_config.results_dir)
        benchmark = Benchmark.from_json(run_config, specs)
        log_path = os.path.join(directory, "crash.log")
        with open(log_path, "w") as log:
            proc = subprocess.Popen(benchmark.args, stdout=log, stderr=subprocess.STDOUT, env=child_env)
        try:
            time.sleep(2)
            env.assertTrue(proc.poll() is None, message="memtier_benchmark exited before the SEGV")
            if proc.poll() is None:
                os.kill(proc.pid, signal.SIGSEGV)
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        with open(log_path) as log:
            output = log.read()

    def field(name):
        match = re.search(r"# " + name + r":(.*)$", output, re.MULTILINE)
        return match.group(1).strip() if match else None

    env.assertEqual(field("libevent_version"), "2.99.1-fake", message=output[-3000:])
    env.assertTrue(field("libevent_build_version") not in (None, "2.99.1-fake"), message=output[-3000:])
    if _has_tls():
        env.assertEqual(field("openssl_runtime_version"), "OpenSSL 15.99.0-fake", message=output[-3000:])
        env.assertTrue(field("openssl_version") not in (None, "OpenSSL 15.99.0-fake"), message=output[-3000:])
