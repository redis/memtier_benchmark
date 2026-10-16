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

from include import MEMTIER_BINARY, TLS_CACERT, TLS_CERT, TLS_KEY

# Dummy password, distinctive enough that a substring match is no accident.
_CANARY = "cfg-redaction-canary-731-dummy"


def _is_cluster(env):
    try:
        return env.isCluster()
    except Exception:
        return False


def _master_conns(env, password):
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
    return [
        _redis.Redis(
            host=node.get("host") or "127.0.0.1",
            port=node["port"],
            password=password,
            socket_connect_timeout=5,
            **tls_kwargs
        )
        for node in env.getMasterNodesList()
    ]


def _set_requirepass(env, current, new):
    for conn in _master_conns(env, current):
        conn.execute_command("CONFIG", "SET", "requirepass", new)


def _tls_args(env):
    if not getattr(env, "useTLS", False):
        return []
    args = ["--tls", "--tls-skip-verify", "--cert={}".format(TLS_CERT)]
    if TLS_KEY:
        args.append("--key={}".format(TLS_KEY))
    return args


def _run(env, target_args, workdir):
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
    env.assertEqual(proc.returncode, 0, message="memtier failed:\n" + stderr[-2000:])
    with open(json_path) as f:
        raw_json = f.read()
    return raw_json, stdout, stderr


def _show_config_value(stderr, key):
    prefix = key + " = "
    for line in stderr.splitlines():
        if line.startswith(prefix):
            return line[len(prefix):]
    return None


def test_config_password_redaction(env):
    node = env.getMasterNodesList()[0]
    host = node.get("host") or "127.0.0.1"
    port = node["port"]
    previous = node.get("password") or None

    # (memtier target args, expected "uri", expected "authenticate")
    cases = [
        (["--uri", "redis://default:{}@{}:{}".format(_CANARY, host, port)],
         "redis://default:***@{}:{}".format(host, port), "default:***"),
        (["--uri", "redis://{}@{}:{}/0".format(_CANARY, host, port)],
         "redis://***@{}:{}/0".format(host, port), "***"),
        (["-s", host, "-p", str(port), "-a", _CANARY], "", "***"),
        (["-s", host, "-p", str(port), "-a", "default:" + _CANARY], "", "default:***"),
    ]

    _set_requirepass(env, previous, _CANARY)
    try:
        for target_args, want_uri, want_auth in cases:
            with tempfile.TemporaryDirectory() as workdir:
                raw_json, stdout, stderr = _run(env, target_args, workdir)

            for name, text in (("json", raw_json), ("stdout", stdout), ("stderr", stderr)):
                env.assertFalse(_CANARY in text,
                                message="password leaked in {} for {}".format(name, target_args))

            cfg = json.loads(raw_json)["configuration"]
            env.assertEqual(cfg["uri"], want_uri)
            env.assertEqual(cfg["authenticate"], want_auth)
            env.assertEqual(_show_config_value(stderr, "uri"), want_uri)
            env.assertEqual(_show_config_value(stderr, "authenticate"), want_auth)
    finally:
        _set_requirepass(env, _CANARY, previous or "")


def test_config_no_authenticate_is_empty(env):
    """Without credentials, "authenticate" stays "" (not "***")."""
    node = env.getMasterNodesList()[0]
    if node.get("password"):
        env.skip()
    host = node.get("host") or "127.0.0.1"
    with tempfile.TemporaryDirectory() as workdir:
        raw_json, _stdout, stderr = _run(env, ["-s", host, "-p", str(node["port"])], workdir)
    cfg = json.loads(raw_json)["configuration"]
    env.assertEqual(cfg["uri"], "")
    env.assertEqual(cfg["authenticate"], "")
    env.assertEqual(_show_config_value(stderr, "authenticate"), "")
