These are unchanged timeout reproducers from the scheduled MONITOR-input fuzz
run https://github.com/redis/memtier_benchmark/actions/runs/35558796567 (2026-09-21).
The driver runs them before its random mutations and requires actual RESP requests.

* `wait_forever.txt`: mutated WAIT 0 0 to WAIT 8 0, which can block indefinitely
  on a real standalone Redis server without replicas.
* `hello_protocol_switch.txt`: contains a replayed HELLO 3, which changes the
  server protocol while the benchmark's parser still expects RESP2.

The command sink acknowledges these requests without executing them. These are
regressions for isolation of the parser-fuzz target, not claims that arbitrary
protocol switching or unbounded server commands are supported by memtier.
