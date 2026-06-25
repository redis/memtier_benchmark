"""
Redaction tests for the Prometheus /metrics exporter (PLAN.md §6, F6 dedicated).

Hard rule (#440 redaction precedent, mirrored on test_tls_logging.py:102-150):
the /metrics body echoes ZERO connection/config identity.  A scrape must NOT
contain:

  * the target redis password (canary) -- never, in any cell;
  * the target server host or port as a metric value or label;
  * any filesystem path token (cert/key/CA paths or their basenames, the
    --data-import path, absolute paths in general);
  * a server/host/addr/target identity label (Prometheus `instance` covers
    identity -- the exporter must not duplicate it).

The body legitimately carries memtier_build_info{version=...,git_sha=...} and
the exporter's own metric names; nothing else identity-bearing.

This suite runs across the CI matrix.  Under TLS cells (TLS=1, after
./tests/gen-test-certs.sh) it additionally drives --tls/--cert/--key/--cacert
and asserts none of those absolute paths nor their basenames leak.  The cases
self-skip under OSS_CLUSTER only where they assert standalone-specific shape;
the core redaction assertions are cluster-agnostic.

Run with:
  . .venv-tests/bin/activate
  TEST=test_prometheus_redaction.py OSS_STANDALONE=1 \
    REDIS_SERVER=/usr/local/bin/redis-server \
    MEMTIER_BINARY=$PWD/memtier_benchmark ./tests/run_tests.sh
  # TLS flavor:
  ./tests/gen-test-certs.sh
  TLS=1 TEST=test_prometheus_redaction.py OSS_STANDALONE=1 \
    REDIS_SERVER=/usr/local/bin/redis-server \
    MEMTIER_BINARY=$PWD/memtier_benchmark ./tests/run_tests.sh
"""
import os
import re
import subprocess
import tempfile

from include import (
    MEMTIER_BINARY,
    TLS_CACERT,
    TLS_CERT,
    TLS_KEY,
    prom_scrape,
    wait_for_prometheus_url,
)


# A distinctive password canary set on the target before the run.  It is chosen
# so that an accidental substring match cannot be a coincidence.
_PASSWORD_CANARY = "pr0m-redaction-canary-SECRET-9f3a"


def _is_cluster(env):
    try:
        return env.isCluster()
    except Exception:
        return False


def _master(env):
    return env.getMasterNodesList()[0]


def _server_args(env):
    node = _master(env)
    port = node["port"]
    host = node.get("host", "127.0.0.1") or "127.0.0.1"
    args = ["--server", str(host), "--port", str(port)]
    if _is_cluster(env):
        args.append("--cluster-mode")
    return args, str(host), str(port)


def _tls_args(env):
    """Mirror include.addTLSArgs but as a flat list for a raw Popen.

    Returns (args, path_tokens) where path_tokens is every path string the run
    feeds memtier -- each must be absent (full and basename) from the body.
    """
    args = []
    paths = []
    if not env.useTLS:
        return args, paths
    args.append("--tls")
    args.append("--cert={}".format(TLS_CERT))
    paths.append(TLS_CERT)
    args.append("--cacert={}".format(TLS_CACERT))
    paths.append(TLS_CACERT)
    if TLS_KEY != "":
        args.append("--key={}".format(TLS_KEY))
        paths.append(TLS_KEY)
    else:
        args.append("--tls-skip-verify")
    # Always skip verify so a self-signed test CA does not abort the run.
    if "--tls-skip-verify" not in args:
        args.append("--tls-skip-verify")
    return args, [p for p in paths if p]


def _master_conns(env, password=None):
    """A fresh redis-py connection to EVERY master node, TLS-aware.

    Cluster envs have one master per shard; requirepass must be set/cleared on
    all of them or memtier (which authenticates to every shard) restart-loops
    against the shards that still reject AUTH.  When clearing, pass the canary
    as ``password`` so the client AUTHs before issuing CONFIG SET.
    """
    import redis as _redis

    tls_kwargs = {}
    if getattr(env, "useTLS", False):
        tls_kwargs = dict(
            ssl=True,
            ssl_certfile=TLS_CERT or None,
            ssl_keyfile=TLS_KEY or None,
            ssl_ca_certs=TLS_CACERT or None,
            ssl_cert_reqs="none",
        )
    conns = []
    for node in env.getMasterNodesList():
        conns.append(
            _redis.Redis(
                host=node.get("host") or "127.0.0.1",
                port=node["port"],
                password=password if password is not None else node.get("password"),
                socket_connect_timeout=5,
                **tls_kwargs
            )
        )
    return conns


def _set_password(env, password):
    """requirepass every master so the run must authenticate.

    Returns True only if it was set on all masters (the assertions that need it
    self-skip otherwise).
    """
    try:
        for conn in _master_conns(env):
            conn.execute_command("CONFIG", "SET", "requirepass", password)
        return True
    except Exception:
        # Best-effort rollback so a partial set does not wedge the env.
        _clear_password(env, password)
        return False


def _clear_password(env, password):
    try:
        for conn in _master_conns(env, password=password):
            conn.execute_command("CONFIG", "SET", "requirepass", "")
    except Exception:
        pass


def _popen(env, extra_args, results_dir, password=None):
    stdout_path = os.path.join(results_dir, "mb.stdout")
    stderr_path = os.path.join(results_dir, "mb.stderr")
    tls, _paths = _tls_args(env)
    srv, _host, _port = _server_args(env)
    auth = ["--authenticate={}".format(password)] if password else []
    args = [MEMTIER_BINARY] + srv + auth + tls + extra_args
    so = open(stdout_path, "w")
    se = open(stderr_path, "w")
    proc = subprocess.Popen(args, stdout=so, stderr=se, cwd=results_dir)
    proc._so = so
    proc._se = se
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


def _assert_redacted(env, body, host, port, paths, password=None):
    """Core redaction assertions over a scrape body."""
    # 1. No password canary, ever.
    if password:
        env.assertFalse(
            password in body,
            message="PASSWORD LEAK: canary present in /metrics body",
        )
    # 2. No server/host/addr/target identity label.
    env.assertFalse(
        re.search(r'\b(server|host|hostname|addr|address|target|peer)\s*=\s*"', body),
        message="connection-identity label leaked: {!r}".format(body[:400]),
    )
    # 3. The target host must not appear as a label value (loopback names are
    #    too generic to assert on, but a non-loopback host must be absent).
    if host and host not in ("127.0.0.1", "localhost", "::1"):
        env.assertFalse(
            host in body, message="target host {!r} leaked into body".format(host)
        )
    # 4. The target port must not appear as a label/metric value.  Guard against
    #    false positives from histogram bounds by requiring the port adjacent to
    #    a label-assign or as a bare metric value.
    if port:
        env.assertFalse(
            re.search(r'(=|:)\s*"?' + re.escape(port) + r'"?\s*[,}\n]', body)
            and re.search(r'(server|host|addr|port|target)[^\n]*' + re.escape(port), body),
            message="target port {!r} leaked into an identity context".format(port),
        )
    # 5. No filesystem path tokens.  No '/'-rooted path, no Windows-style path.
    #    The only legitimate text is metric names/help; none contain a slash
    #    outside the Content-Type (which is a header, not body).
    for line in body.splitlines():
        if line.startswith("#"):
            continue  # HELP/TYPE lines: free text, but still must not carry paths
        # A metric line never contains an absolute path.
        env.assertFalse(
            re.search(r'"[^"]*/[^"]*"', line),
            message="path-like label value leaked: {!r}".format(line),
        )
    # 6. TLS path tokens: neither the absolute path nor its basename appears.
    for p in paths:
        env.assertFalse(p in body, message="TLS path leaked: {!r}".format(p))
        base = os.path.basename(p)
        if base:
            env.assertFalse(
                base in body, message="TLS path basename leaked: {!r}".format(base)
            )


def test_R1_no_secret_in_metrics_body(env):
    """A full live scrape leaks no password, host:port, or path tokens."""
    rd = tempfile.mkdtemp(prefix="promredact_")
    have_pw = _set_password(env, _PASSWORD_CANARY)
    password = _PASSWORD_CANARY if have_pw else None
    srv, host, port = _server_args(env)
    _tls, paths = _tls_args(env)
    proc, out_path, err_path = _popen(
        env, ["--test-time=4", "--prometheus-port=0"], rd, password=password
    )
    failed = env.getNumberOfFailedAssertion()
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line")
        if url is None:
            return
        import time

        time.sleep(1.0)
        r = prom_scrape(url)
        env.assertEqual(r.status, 200, message="scrape status {}".format(r.status))
        body = r.body
        # Persist the scraped body as an artifact for post-mortem.
        with open(os.path.join(rd, "prom_scrape.txt"), "w") as fh:
            fh.write(body)
        _assert_redacted(env, body, host, port, paths, password=password)
        try:
            rc = proc.wait(timeout=20)
            env.assertEqual(rc, 0, message="memtier exit {}".format(rc))
        except subprocess.TimeoutExpired:
            env.assertTrue(False, message="memtier did not exit within 20s")
    finally:
        _kill(proc)
        if have_pw:
            _clear_password(env, _PASSWORD_CANARY)
        if env.getNumberOfFailedAssertion() > failed:
            try:
                with open(err_path) as fh:
                    env.debugPrint("mb.stderr:\n" + fh.read(), True)
            except Exception:
                pass


def test_R2_error_bodies_echo_nothing(env):
    """404 / non-GET bodies are fixed strings that echo no request detail."""
    import http.client
    from urllib.parse import urlparse

    rd = tempfile.mkdtemp(prefix="promredact_")
    srv, host, port = _server_args(env)
    proc, out_path, err_path = _popen(
        env, ["--test-time=5", "--prometheus-port=0"], rd
    )
    try:
        url = wait_for_prometheus_url(out_path, timeout=20)
        env.assertFalse(url is None, message="no announce line")
        if url is None:
            return
        u = urlparse(url)
        # A 404 on an attacker-controlled URI must not echo that URI.
        secret_uri = "/" + _PASSWORD_CANARY + "/leak"
        conn = http.client.HTTPConnection(u.hostname, u.port, timeout=5)
        conn.request("GET", secret_uri)
        resp = conn.getresponse()
        body = resp.read().decode("latin-1")
        env.assertEqual(resp.status, 404, message="status {}".format(resp.status))
        env.assertFalse(
            _PASSWORD_CANARY in body,
            message="404 body echoes the request URI: {!r}".format(body),
        )
        env.assertEqual(body, "not found\n", message="404 body not the fixed string: {!r}".format(body))
        conn.close()
    finally:
        _kill(proc)
