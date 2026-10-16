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
                # requirepass may not have been applied on this node yet.
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
    else:
        env.assertEqual(proc.returncode, 1, message="expected a URI error (rc 1):\n" + stderr[-2000:])
    return raw_json, stdout, stderr


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
    """redis://u:p@w@host:port/x@y must not be read as host "y"."""
    host, port = _target(env)
    uri = "redis://default:{}@{}@{}:{}/x@cfg731-wrong-host".format(_CANARY_HEAD, _CANARY_TAIL, host, port)
    with tempfile.TemporaryDirectory() as workdir:
        _raw, stdout, stderr = _run(env, ["--uri", uri], workdir, expect_ok=False)
    env.assertContains("invalid database number 'x@cfg731-wrong-host'", stderr)
    env.assertFalse("server = cfg731-wrong-host" in stderr)
    for secret in (_CANARY_HEAD, _CANARY_TAIL):
        env.assertFalse(secret in stdout + stderr, message="{!r} leaked: {}".format(secret, stderr[-500:]))


def test_config_uri_credentials_without_host_rejected(env):
    """Credentials with an empty host must not fall back to the default server."""
    _host, port = _target(env)
    with tempfile.TemporaryDirectory() as workdir:
        _raw, stdout, stderr = _run(env, ["--uri", "redis://default:{}@:{}".format(_CANARY, port)], workdir,
                                    expect_ok=False)
    env.assertContains("missing host after credentials in URI", stderr)
    env.assertFalse(_CANARY in stdout + stderr)


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
