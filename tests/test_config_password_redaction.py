"""
--json-out-file and --show-config must not carry the server password (#731).

Both print the run configuration: "uri" and "authenticate" in the JSON
"configuration" object, and "uri = " / "authenticate = " on stderr with
--show-config. They used to echo the --uri and -a/--authenticate values
verbatim, so a JSON result file uploaded as a CI artifact carried the
password. The user name is kept and only the password is replaced by "***".

Run with:
  TEST=test_config_password_redaction.py OSS_STANDALONE=1 \
    MEMTIER_BINARY=$PWD/memtier_benchmark ./tests/run_tests.sh
"""
import json
import os
import socket
import subprocess
import tempfile
from contextlib import contextmanager

from include import MEMTIER_BINARY, TLS_CACERT, TLS_CERT, TLS_KEY, TLS_PROTOCOLS

# Dummy password, distinctive enough that a substring match is no accident.
_CANARY = "cfg-redaction-canary-731-dummy"
# Two halves of a dummy password that contains a raw '@'.
_CANARY_HEAD = "cfg731-head-dummy"
_CANARY_TAIL = "cfg731-tail-dummy"
_CANARY_TAIL2 = "cfg731-tail2-dummy"


def _is_cluster(env):
    try:
        return env.isCluster()
    except Exception:
        return False


def _conn(env, host, port, password):
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
    return _redis.Redis(host=host, port=port, password=password or None,
                        socket_connect_timeout=5, **tls_kwargs)


def _all_nodes(env):
    """(host, port, password) of every master and of its replicas.

    Replicas (--use-slaves) need requirepass/masterauth too, or memtier and
    replication would trip on the ones still in the old state.
    """
    nodes = []
    for m in env.getMasterNodesList():
        host, port, pw = m.get("host") or "127.0.0.1", m["port"], m.get("password")
        nodes.append((host, port, pw))
        info = _conn(env, host, port, pw).info("replication")
        for k, v in info.items():
            if k.startswith("slave") and isinstance(v, dict):
                nodes.append((v["ip"], int(v["port"]), pw))
    return nodes


@contextmanager
def _requirepass(env, password):
    """Set requirepass and masterauth on every node; restore only those changed."""
    changed = []
    try:
        for host, port, current in _all_nodes(env):
            conn = _conn(env, host, port, current)
            old = {k: conn.config_get(k).get(k, "") for k in ("requirepass", "masterauth")}
            conn.config_set("masterauth", password)
            changed.append((host, port, old))
            conn.config_set("requirepass", password)
        yield
    finally:
        for host, port, old in reversed(changed):
            try:
                conn = _conn(env, host, port, password)
                conn.config_set("requirepass", old["requirepass"])
                conn.config_set("masterauth", old["masterauth"])
            except Exception:
                # Connecting with the new password failed, so CONFIG SET
                # requirepass did not take on this node (masterauth, set
                # first, did): requirepass is still the old value, so only
                # masterauth needs restoring, over the old password.
                conn = _conn(env, host, port, old["requirepass"])
                conn.config_set("masterauth", old["masterauth"])


def _tls_args(env):
    if not getattr(env, "useTLS", False):
        return []
    args = ["--tls", "--tls-skip-verify", "--cert={}".format(TLS_CERT)]
    if TLS_KEY:
        args.append("--key={}".format(TLS_KEY))
    if TLS_PROTOCOLS:
        args.append("--tls-protocols={}".format(TLS_PROTOCOLS))
    return args


def _run(env, target_args, workdir, expect_ok=True):
    json_path = os.path.join(workdir, "out.json")
    args = (
        [MEMTIER_BINARY]
        + target_args
        + _tls_args(env)
        + (["--cluster-mode"] if _is_cluster(env) else [])
        + ["-n", "100", "-t", "1", "-c", "1", "--hide-histogram", "--show-config",
           "--json-out-file", json_path]
    )
    proc = subprocess.run(args, cwd=workdir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
    stdout = proc.stdout.decode("utf-8", "replace")
    stderr = proc.stderr.decode("utf-8", "replace")
    raw_json = ""
    if expect_ok:
        env.assertEqual(proc.returncode, 0, message="memtier failed:\n" + stderr[-2000:])
        with open(json_path) as f:
            raw_json = f.read()
    elif expect_ok is False:
        env.assertEqual(proc.returncode, 1, message="expected a URI error (rc 1):\n" + stderr[-2000:])
    # expect_ok=None: the exit code is not checked (only the printed config is).
    return raw_json, stdout, stderr


# A port in the range reserved for these tests, assumed closed; a host-less
# --uri whose fallback would otherwise hit localhost:6379 is pointed here.
_CLOSED_PORT = 19799


def _closed_port(env):
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.settimeout(1)
        if probe.connect_ex(("127.0.0.1", _CLOSED_PORT)) == 0:
            env.skip()  # something listens there; the expectations below would not hold
    finally:
        probe.close()
    return _CLOSED_PORT


def _show_config_value(stderr, key):
    prefix = key + " = "
    for line in stderr.splitlines():
        if line.startswith(prefix):
            return line[len(prefix):]
    return None


def _target(env):
    node = env.getMasterNodesList()[0]
    return node.get("host") or "127.0.0.1", node["port"]


def _check(env, password, target_args, want_uri, want_auth, secrets):
    """Run against a server whose password is `password` and check that none of
    `secrets` leaks while uri/authenticate hold the redacted forms."""
    with _requirepass(env, password):
        with tempfile.TemporaryDirectory() as workdir:
            raw_json, stdout, stderr = _run(env, target_args, workdir)
    for name, text in (("json", raw_json), ("stdout", stdout), ("stderr", stderr)):
        for secret in secrets:
            env.assertFalse(secret in text,
                            message="password part {!r} leaked in {} for {}".format(secret, name, target_args))
    cfg = json.loads(raw_json)["configuration"]
    env.assertEqual(cfg["uri"], want_uri)
    env.assertEqual(cfg["authenticate"], want_auth)
    env.assertEqual(_show_config_value(stderr, "uri"), want_uri)
    env.assertEqual(_show_config_value(stderr, "authenticate"), want_auth)
    return stderr


def test_config_password_redaction_uri_user_password(env):
    host, port = _target(env)
    _check(env, _CANARY, ["--uri", "redis://default:{}@{}:{}".format(_CANARY, host, port)],
           "redis://default:***@{}:{}".format(host, port), "default:***", [_CANARY])


def test_config_password_redaction_uri_password_only(env):
    host, port = _target(env)
    _check(env, _CANARY, ["--uri", "redis://{}@{}:{}/0".format(_CANARY, host, port)],
           "redis://***@{}:{}/0".format(host, port), "***", [_CANARY])


def test_config_password_redaction_authenticate_password(env):
    host, port = _target(env)
    _check(env, _CANARY, ["-s", host, "-p", str(port), "-a", _CANARY], "", "***", [_CANARY])


def test_config_password_redaction_authenticate_user_password(env):
    host, port = _target(env)
    _check(env, _CANARY, ["-s", host, "-p", str(port), "-a", "default:" + _CANARY], "", "default:***",
           [_CANARY])


def test_config_password_redaction_password_with_colon(env):
    # The user ends at the first ':'; the password "a:<canary>" keeps its ':'.
    host, port = _target(env)
    _check(env, "a:" + _CANARY, ["-s", host, "-p", str(port), "-a", "default:a:" + _CANARY], "", "default:***",
           [_CANARY])


def test_config_password_redaction_uri_password_with_slash(env):
    # A raw '/' in the password (with no '@' before it) works, as on master.
    host, port = _target(env)
    password = "a/" + _CANARY
    _check(env, password, ["--uri", "redis://default:{}@{}:{}".format(password, host, port)],
           "redis://default:***@{}:{}".format(host, port), "default:***", [_CANARY])


def test_config_password_redaction_uri_password_with_at(env):
    # The userinfo ends at the LAST '@', so a raw '@' in the password works
    # and the part after it is not taken for the host (and echoed).
    host, port = _target(env)
    password = _CANARY_HEAD + "@" + _CANARY_TAIL
    _check(env, password, ["--uri", "redis://default:{}@{}:{}".format(password, host, port)],
           "redis://default:***@{}:{}".format(host, port), "default:***", [_CANARY_HEAD, _CANARY_TAIL])


def test_config_password_with_at_bad_port_no_leak(env):
    """A URI error must not echo the part of the password after an '@'.

    Splitting at the first '@' took "<tail>:x@host:notaport" for host:port
    and printed "invalid port number 'x@host:notaport'"-style messages that
    carried the password tail.
    """
    host, _port = _target(env)
    uri = "redis://default:{}@{}:{}@{}:notaport".format(_CANARY_HEAD, _CANARY_TAIL, _CANARY_TAIL2, host)
    with tempfile.TemporaryDirectory() as workdir:
        _raw, stdout, stderr = _run(env, ["--uri", uri], workdir, expect_ok=False)
    env.assertContains("invalid port number 'notaport'", stderr)
    for secret in (_CANARY_HEAD, _CANARY_TAIL, _CANARY_TAIL2):
        env.assertFalse(secret in stdout + stderr, message="{!r} leaked: {}".format(secret, stderr[-500:]))


def test_config_uri_stray_at_in_path_rejected(env):
    """An '@' after the db path is not the end of the credentials.

    Taking it for one left an empty host, so the run fell back to the default
    server and benchmarked the wrong target. The authority ends at the '/'.
    """
    host, port = _target(env)
    with tempfile.TemporaryDirectory() as workdir:
        uri = "redis://default:{}@{}:{}/1@".format(_CANARY, host, port)
        _raw, stdout, stderr = _run(env, ["--uri", uri], workdir, expect_ok=False)
    env.assertContains("invalid database number '1@'", stderr)
    env.assertFalse(_CANARY in stdout + stderr)


def test_config_uri_at_in_password_and_path_keeps_host(env):
    """redis://u:p@w@host:port/x@y must not be read as host "y".

    The /x@wrong/2 form also catches a whole-string last-'@' split, which
    would connect to host "wrong", db 2.
    """
    host, port = _target(env)
    for path in ("/x@cfg731-wrong-host", "/x@cfg731-wrong-host/2"):
        uri = "redis://default:{}@{}@{}:{}{}".format(_CANARY_HEAD, _CANARY_TAIL, host, port, path)
        with tempfile.TemporaryDirectory() as workdir:
            _raw, stdout, stderr = _run(env, ["--uri", uri], workdir, expect_ok=False)
        env.assertContains("invalid database number '{}'".format(path[1:]), stderr)
        env.assertFalse("server = cfg731-wrong-host" in stderr)
        for secret in (_CANARY_HEAD, _CANARY_TAIL):
            env.assertFalse(secret in stdout + stderr, message="{!r} leaked: {}".format(secret, stderr[-500:]))


def test_config_uri_at_in_query_or_fragment_not_retargeted(env):
    """An '@' after '?' or '#' does not end the credentials.

    The authority ends at '?' and '#' too (RFC 3986). Otherwise
    redis://u:pw@host:port?x=@other:port connected to "other" and sent it
    AUTH u pw@host:port?x=. The URI must be rejected and nothing may reach
    the second host.
    """
    host, port = _target(env)
    decoy = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        decoy.bind(("127.0.0.1", 0))
        decoy.listen(8)
        decoy.setblocking(False)
        decoy_port = decoy.getsockname()[1]
        for sep in ("?x=", "#x"):
            uri = "redis://default:{}@{}:{}{}@127.0.0.1:{}".format(_CANARY, host, port, sep, decoy_port)
            with tempfile.TemporaryDirectory() as workdir:
                _raw, stdout, stderr = _run(env, ["--uri", uri], workdir, expect_ok=False)
            env.assertContains("invalid port number", stderr)
            env.assertFalse(_CANARY in stdout + stderr)
            try:
                conn, _addr = decoy.accept()
                conn.close()
                env.assertTrue(False, message="memtier connected to the second host for {!r}".format(sep))
            except BlockingIOError:
                pass
    finally:
        decoy.close()


# The default server is "localhost"; RLTest nodes listen on loopback, which
# _target() reports as "localhost" (or "127.0.0.1" as a fallback). Either way
# the default server reaches the test target.
_LOOPBACK = ("localhost", "127.0.0.1")


def _check_default_server_warning(env, stderr, port):
    env.assertContains("warning: --uri has no host; connecting to the default server localhost:{}.".format(port),
                       stderr)
    env.assertEqual(_show_config_value(stderr, "server"), "localhost")
    env.assertFalse("missing host in URI" in stderr)


def test_config_uri_credentials_without_host_warns_and_uses_default(env):
    """Credentials with an empty host and no --server/--unix-socket fall back
    to the default server (localhost), as on master, but now say so on stderr
    without the password. The run reaches the target, which is on loopback,
    and the credentials are redacted everywhere."""
    host, port = _target(env)
    env.assertContains(host, _LOOPBACK)  # never skip: the warning path needs coverage
    stderr = _check(env, _CANARY, ["--uri", "redis://default:{}@:{}".format(_CANARY, port)],
                    "redis://default:***@:{}".format(port), "default:***", [_CANARY])
    _check_default_server_warning(env, stderr, port)


def test_config_uri_password_with_slash_without_host_warns(env):
    """A raw '/' in the password with an empty host but a port after the '@'
    (redis://u:a/b@:port) is real credentials: it falls back and warns like
    the other host-less forms."""
    _host, port = _target(env)
    password = "a/" + _CANARY
    stderr = _check(env, password, ["--uri", "redis://default:{}@:{}".format(password, port)],
                    "redis://default:***@:{}".format(port), "default:***", [_CANARY])
    _check_default_server_warning(env, stderr, port)


def test_config_uri_password_with_slash_nothing_after_at_warns(env):
    """redis://u:a/b@ (nothing after the '@') falls back to the default server
    with the host-less warning, like every other host-less form, as on master.
    It has the same shape as a stray '@' after a path; telling them apart is
    #732's. -p points the default server at the target, so the run
    authenticates with the "a/<canary>" password and is redacted."""
    host, port = _target(env)
    env.assertContains(host, _LOOPBACK)
    password = "a/" + _CANARY
    stderr = _check(env, password, ["-p", str(port), "--uri", "redis://default:{}@".format(password)],
                    "redis://default:***@", "default:***", [_CANARY])
    _check_default_server_warning(env, stderr, port)


def test_config_uri_without_host_or_credentials_warns(env):
    """redis://:port (no '@') warns the same way as redis://u:pw@:port: the
    warning keys on the empty host, not on credentials."""
    if env.getMasterNodesList()[0].get("password"):
        env.skip()
    host, port = _target(env)
    env.assertContains(host, _LOOPBACK)
    with tempfile.TemporaryDirectory() as workdir:
        raw_json, _stdout, stderr = _run(env, ["--uri", "redis://:{}".format(port)], workdir)
    _check_default_server_warning(env, stderr, port)
    cfg = json.loads(raw_json)["configuration"]
    env.assertEqual(cfg["uri"], "redis://:{}".format(port))
    env.assertEqual(cfg["authenticate"], "")


def test_config_uri_without_host_uses_explicit_server(env):
    """-s HOST with a host-less --uri keeps working, as on master: the URI only
    carries the credentials and port, and the run goes to HOST."""
    host, port = _target(env)
    _check(env, _CANARY, ["-s", host, "--uri", "redis://default:{}@:{}".format(_CANARY, port)],
           "redis://default:***@:{}".format(port), "default:***", [_CANARY])


def test_config_uri_without_host_uses_explicit_unix_socket(env):
    """-S PATH with a host-less --uri passes the credentials over the unix
    socket, as on master. Nothing listens on PATH here, so the run must fail
    to connect to that socket (not be rejected as a URI error, and not go to
    the default server), with the credentials redacted."""
    with tempfile.TemporaryDirectory() as workdir:
        sock = os.path.join(workdir, "none.sock")
        args = [MEMTIER_BINARY, "-S", sock, "--uri", "redis://default:{}@".format(_CANARY),
                "-n", "1", "-t", "1", "-c", "1", "--hide-histogram", "--show-config"]
        proc = subprocess.run(args, cwd=workdir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
    out = proc.stdout.decode("utf-8", "replace") + proc.stderr.decode("utf-8", "replace")
    env.assertEqual(proc.returncode, 1, message=out[-2000:])
    env.assertFalse("missing host in URI" in out)
    env.assertContains("connect failed, error = No such file or directory", out)
    env.assertEqual(_show_config_value(out, "unix socket"), sock)
    env.assertEqual(_show_config_value(out, "uri"), "redis://default:***@")
    env.assertEqual(_show_config_value(out, "authenticate"), "default:***")
    env.assertFalse(_CANARY in out)


def test_config_uri_at_in_path_without_credentials_known_limit(env):
    """Known, pre-existing limit (same as master), pinned until #732.

    Without credentials, an '@' in the db path still ends a "userinfo":
    redis://host:port/1@other:port is read as credentials "host:port/1" for
    host "other", and redis://host:port/1@ as those credentials with no host,
    which falls back to the default server with the host-less warning.
    Treating a '/' before the first '@' as "no userinfo" would break a raw
    '/' in passwords, which master supports. The value is redacted like any
    credential. Only the printed configuration is checked; the connection
    goes to a closed port.
    """
    host, port = _target(env)
    closed = _closed_port(env)
    uri = "redis://{}:{}/1@127.0.0.1:{}".format(host, port, closed)
    with tempfile.TemporaryDirectory() as workdir:
        _raw, _stdout, stderr = _run(env, ["--uri", uri], workdir, expect_ok=None)
    env.assertEqual(_show_config_value(stderr, "server"), "127.0.0.1")
    env.assertEqual(_show_config_value(stderr, "port"), str(closed))
    env.assertEqual(_show_config_value(stderr, "authenticate"), "{}:***".format(host))
    env.assertEqual(_show_config_value(stderr, "uri"), "redis://{}:***@127.0.0.1:{}".format(host, closed))

    # -p keeps the default-server fallback off localhost:6379.
    with tempfile.TemporaryDirectory() as workdir:
        _raw, _stdout, stderr = _run(env, ["-p", str(closed), "--uri", "redis://{}:{}/1@".format(host, port)],
                                     workdir, expect_ok=None)
    _check_default_server_warning(env, stderr, closed)
    env.assertEqual(_show_config_value(stderr, "authenticate"), "{}:***".format(host))


def test_config_uri_without_credentials(env):
    """A credential-less --uri is printed unchanged, authenticate stays ""."""
    if env.getMasterNodesList()[0].get("password"):
        env.skip()
    host, port = _target(env)
    uri = "redis://{}:{}".format(host, port)
    with tempfile.TemporaryDirectory() as workdir:
        raw_json, _stdout, stderr = _run(env, ["--uri", uri], workdir)
    cfg = json.loads(raw_json)["configuration"]
    env.assertEqual(cfg["uri"], uri)
    env.assertEqual(cfg["authenticate"], "")
    env.assertEqual(_show_config_value(stderr, "uri"), uri)
    env.assertEqual(_show_config_value(stderr, "authenticate"), "")


def test_config_no_authenticate_is_empty(env):
    """Without credentials, "authenticate" stays "" (not "***")."""
    if env.getMasterNodesList()[0].get("password"):
        env.skip()
    host, port = _target(env)
    with tempfile.TemporaryDirectory() as workdir:
        raw_json, _stdout, stderr = _run(env, ["-s", host, "-p", str(port)], workdir)
    cfg = json.loads(raw_json)["configuration"]
    env.assertEqual(cfg["uri"], "")
    env.assertEqual(cfg["authenticate"], "")
    env.assertEqual(_show_config_value(stderr, "authenticate"), "")
