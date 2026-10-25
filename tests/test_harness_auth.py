"""The harness forwards the env's password to memtier and fails tests that forget it (#842)."""
from types import SimpleNamespace

import redis
from RLTest import loader

from include import (allow_noauth, bounded_client, env_password, memtier_auth_args,
                     noauth_guard, shutdown_nosave)


class _Env:
    """Just enough of an RLTest Env for the auth helpers."""

    def __init__(self, password, shards=()):
        self.password = password
        self.envRunner = SimpleNamespace(shards=list(shards))
        self.failures = []
        self.printed = []

    def getMasterNodesList(self):
        return [{"host": "localhost", "port": 1, "unix_socket_path": None,
                 "password": self.password}]

    def assertEqual(self, first, second, depth=0, message=None):
        if first != second:
            self.failures.append(message)

    def assertTrue(self, value, depth=0, message=None):
        if not value:
            self.failures.append(message)

    def debugPrint(self, message, force=False):
        self.printed.append(message)


class _Server:
    """A shard whose INFO errorstats reports `noauth` NOAUTH replies."""

    def __init__(self, noauth=0, replicas=()):
        self.noauth = noauth
        self.up = True
        self.useSlaves = bool(replicas)
        self.slavePorts = list(range(len(replicas)))
        self.replicas = list(replicas)

    def _conn(self, server):
        if not server.up:
            raise redis.exceptions.ConnectionError("Connection refused")
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


def test_noauth_guard_never_passes_unchecked(env):
    # No server readable before the test: the guard fails rather than pass.
    down = _Server()
    down.up = False
    failures = _run_guarded(lambda fake: None, _Env(_PW, [down]))
    env.assertEqual(len(failures), 1, message=repr(failures))
    env.assertTrue("could not read INFO errorstats" in failures[0], message=repr(failures))
    env.assertTrue("Connection refused" in failures[0], message=repr(failures))

    # A server that stops during the test is logged, not failed.
    replica = _Server()
    fake = _Env(_PW, [_Server(replicas=[replica])])

    def stops_replica(_env):
        replica.up = False
    env.assertEqual(_run_guarded(stops_replica, fake), [])
    env.assertTrue(any("shard 1 replica 1 not checked" in line for line in fake.printed),
                   message=repr(fake.printed))

    # A restart resets the count to 0; no NOAUTH since then is no failure.
    def restarts_clean(fake):
        fake.envRunner.shards[0].noauth = 0
    env.assertEqual(_run_guarded(restarts_clean, _Env(_PW, [_Server(noauth=5)])), [])

    # Passing tests assert nothing, so a VERBOSE log has no NOAUTH lines.
    fake = _Env(_PW, [_Server(noauth=2)])
    env.assertEqual(_run_guarded(lambda fake: None, fake), [])
    env.assertEqual(fake.printed, [])


def test_noauth_guard_wraps_tests_whose_first_parameter_is_env(env):
    def with_default(fake, count=1):
        fake.envRunner.shards[0].noauth += count
    env.assertEqual(len(_run_guarded(with_default, _Env(_PW, [_Server()]))), 1)

    def no_env():
        pass
    env.assertTrue(noauth_guard(no_env) is no_env)


def test_guard_is_installed_in_loader(env):
    """RLTest's loader wraps every test it resolves (the guard is really on)."""
    env.assertTrue(getattr(loader.TestFunction, "_noauth_guard", False))
    test = loader.TestFunction(__file__, "test_guard_is_installed_in_loader", "test_harness_auth")
    test.initialize()
    env.assertTrue(getattr(test.target, "_noauth_guarded", False))


class _ShutdownConn:
    """A redis-py stand-in for shutdown_nosave."""

    def __init__(self, shutdown_error, pings_before_down=0, ping_error=None):
        self.shutdown_error = shutdown_error
        self.pings_left = pings_before_down
        self.ping_error = ping_error or redis.exceptions.ConnectionError("Connection refused")

    def execute_command(self, *args):
        if self.shutdown_error is not None:
            raise self.shutdown_error

    def ping(self):
        if self.pings_left is None or self.pings_left > 0:
            if self.pings_left:
                self.pings_left -= 1
            return True
        raise self.ping_error


def test_shutdown_nosave_reports_only_a_real_stop(env):
    closed = redis.exceptions.ConnectionError("Connection closed by server.")
    noauth = redis.exceptions.AuthenticationError("Authentication required.")
    # Refused with NOAUTH (a ConnectionError subclass): still running.
    env.assertFalse(shutdown_nosave(_ShutdownConn(noauth), timeout=0.3))
    # Any other error reply: still running.
    env.assertFalse(shutdown_nosave(_ShutdownConn(redis.exceptions.ResponseError("ERR")), timeout=0.3))
    # The connection closes and the server stops answering, at once or soon.
    env.assertTrue(shutdown_nosave(_ShutdownConn(closed), timeout=0.3))
    env.assertTrue(shutdown_nosave(_ShutdownConn(closed, pings_before_down=2), timeout=2))
    # The connection closes but the server keeps answering: not stopped.
    env.assertFalse(shutdown_nosave(_ShutdownConn(closed, pings_before_down=None), timeout=0.3))
    # Answering NOAUTH to PING means it is still running.
    env.assertFalse(shutdown_nosave(_ShutdownConn(closed, ping_error=noauth), timeout=0.3))


def test_bounded_client_turns_off_retries_on_open_connections(env):
    # No server needed: port 1 is never connected to.
    conn = redis.Redis(host="localhost", port=1)
    pool = conn.connection_pool
    # A connection the client already opened, back in the pool, with
    # redis-py's default retries.
    opened = pool.make_connection()
    pool._available_connections.append(opened)
    env.assertGreater(opened.retry.get_retries(), 0)
    bounded_client(conn, timeout=2.0)
    env.assertEqual(opened.retry.get_retries(), 0)
    env.assertEqual(pool.connection_kwargs["socket_timeout"], 2.0)
    # A connection made afterwards has retries off too.
    env.assertEqual(pool.make_connection().retry.get_retries(), 0)
