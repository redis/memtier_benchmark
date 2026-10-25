"""The harness forwards the env's password to memtier and fails tests that forget it (#842)."""
from types import SimpleNamespace

from include import allow_noauth, env_password, memtier_auth_args, noauth_guard


class _Env:
    """Just enough of an RLTest Env for the auth helpers."""

    def __init__(self, password, shards=()):
        self.password = password
        self.envRunner = SimpleNamespace(shards=list(shards))
        self.failures = []

    def getMasterNodesList(self):
        return [{"host": "localhost", "port": 1, "unix_socket_path": None,
                 "password": self.password}]

    def assertEqual(self, first, second, depth=0, message=None):
        if first != second:
            self.failures.append(message)


class _Server:
    """A shard whose INFO errorstats reports `noauth` NOAUTH replies."""

    def __init__(self, noauth=0, replicas=()):
        self.noauth = noauth
        self.useSlaves = bool(replicas)
        self.slavePorts = list(range(len(replicas)))
        self.replicas = list(replicas)

    def _conn(self, server):
        stats = {"errorstat_NOAUTH": {"count": server.noauth}} if server.noauth else {}
        return SimpleNamespace(info=lambda section: stats, close=lambda: None)

    def getConnection(self):
        return self._conn(self)

    def getSlaveConnection(self, index):
        return self._conn(self.replicas[index])


_PW = "pw"
_AUTH = ["--authenticate", _PW]

# (description, env password, memtier args, expected extra args)
_AUTH_ARGS_CASES = [
    ("no password", None, [], []),
    ("empty password", "", [], []),
    ("password, no auth args", _PW, ["--ratio=1:0"], _AUTH),
    ("non-string args", _PW, ["-p", 1, "-t", 1], _AUTH),
    ("-a value", _PW, ["-a", "other"], []),
    ("-avalue", _PW, ["-aother"], []),
    ("--authenticate value", _PW, ["--authenticate", "other"], []),
    ("--authenticate=value", _PW, ["--authenticate=user:other"], []),
    ("--uri with userinfo", _PW, ["--uri", "redis://user:other@host:1"], []),
    ("--uri= with userinfo", _PW, ["--uri=rediss://:other@host:1/0"], []),
    ("-u with userinfo", _PW, ["-u", "redis://other@host:1"], []),
    ("--uri without userinfo", _PW, ["--uri=redis://host:1"], _AUTH),
    ("@ in the uri path only", _PW, ["--uri=redis://host:1/x@y"], _AUTH),
    ("last --uri wins (no userinfo)", _PW,
     ["--uri=redis://u:p@host:1", "--uri=redis://host:1"], _AUTH),
    ("last --uri wins (userinfo)", _PW,
     ["--uri=redis://host:1", "--uri=redis://u:p@host:1"], []),
    ("-P memcache_text", _PW, ["-P", "memcache_text"], []),
    ("--protocol=memcache_binary", _PW, ["--protocol=memcache_binary"], []),
    ("--protocol=memcache_meta", _PW, ["--protocol=memcache_meta"], []),
    ("--protocol=resp3", _PW, ["--protocol=resp3"], _AUTH),
    ("last --protocol wins (redis)", _PW,
     ["--protocol=memcache_text", "--protocol=redis"], _AUTH),
    ("last --protocol wins (memcache)", _PW,
     ["--protocol=redis", "-Pmemcache_text"], []),
]


def test_memtier_auth_args_table(env):
    for description, password, args, expected in _AUTH_ARGS_CASES:
        fake = _Env(password)
        env.assertEqual(memtier_auth_args(fake, args), expected, message=description)
        env.assertEqual(env_password(fake), password or None, message=description)


def _run_guarded(test, fake):
    guarded = noauth_guard(test)
    guarded(fake)
    return fake.failures


def test_noauth_guard_fails_new_noauth_on_any_server(env):
    replica = _Server()
    shards = [_Server(noauth=3), _Server(replicas=[replica])]

    def gets_noauth(_env):
        # Only replies during the test count, on every shard and replica.
        shards[0].noauth += 2
        replica.noauth += 1

    failures = _run_guarded(gets_noauth, _Env(_PW, shards))
    env.assertEqual(len(failures), 2, message=repr(failures))
    env.assertTrue(any("shard 1 master answered 2 request(s) -NOAUTH" in f for f in failures),
                   message=repr(failures))
    env.assertTrue(any("shard 2 replica 1 answered 1 request(s) -NOAUTH" in f for f in failures),
                   message=repr(failures))


def test_noauth_guard_passes_and_opts_out(env):
    def gets_noauth(fake):
        fake.envRunner.shards[0].noauth += 1

    # No NOAUTH during the test.
    env.assertEqual(_run_guarded(lambda fake: None, _Env(_PW, [_Server(noauth=5)])), [])
    # The env has no password: the guard does not look.
    env.assertEqual(_run_guarded(gets_noauth, _Env(None, [_Server()])), [])
    # A server restarted during the test counts from zero again.
    def restarts(fake):
        fake.envRunner.shards[0].noauth = 1
    env.assertEqual(len(_run_guarded(restarts, _Env(_PW, [_Server(noauth=5)]))), 1)
    # Opted out on purpose.
    marked = allow_noauth("sends no credentials on purpose")(gets_noauth)
    env.assertTrue(noauth_guard(marked) is marked)
    env.assertEqual(_run_guarded(marked, _Env(_PW, [_Server()])), [])
    # The check also runs when the test fails.
    def raises(fake):
        gets_noauth(fake)
        raise RuntimeError("test body failed")
    fake = _Env(_PW, [_Server()])
    try:
        noauth_guard(raises)(fake)
    except RuntimeError:
        pass
    env.assertEqual(len(fake.failures), 1)
    try:
        allow_noauth("")
        env.assertTrue(False, message="allow_noauth accepted an empty reason")
    except ValueError:
        pass
