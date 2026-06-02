# What's Changed

## New Features
- **Cluster `--monitor-input` support**: Route-then-stage replay across shards with end-to-end backpressure (#395, #399).
- **Cluster `--multi-key-get`**: Slot-aware MGET routing with a shared cache rebuilt on every topology refresh (#397).
- **`--transaction` flag**: Pins a `--command` rotation to a single shard in `--cluster-mode`; works with `--pipeline > 1` (#390, #400).
- **`--retry-on-error` pipeline**: Adds `--max-retries`, `--retry-backoff-ms`, `--retry-backoff-factor`, `--retry-on=LIST`, `--max-retry-queue`, `--failed-keys-file` (#381).
- **`--realtime-latencies`**: Streaming HDR-histogram block emits per-second `cur (avg: …)` for ops/sec, bytes/sec, miss%, and configured percentiles (#379).
- **Per-key miss tracking for `--command`**: `--command-miss-tracking={auto,off}` plus `--miss-rate-threshold=PCT`; emits a `Per-Key Misses` JSON section (#374, #398).
- **`--connection-stage-timeout=SECS`**: Bounds the AUTH/HELLO/SELECT/CLUSTER SLOTS / first-steady-state phase; defaults to 30 s, `0` disables (#431).
- **TLS handshake logging**: New `TLS connection established: protocol X, cipher Y` stderr line and a top-level `TLS` object in `mb.json` (#403).

## Bug Fixes
- **Stack overflow in `--monitor-input` replay**: VLA-allocated split-token buffer overflowed for large lines; now heap-allocated (#405).
- **Stack smash in memcached text parser**: Unbounded `sscanf("%s %s …")` allowed a hostile server to overflow a 256-byte stack buffer; widths now bounded (#415).
- **OOB read on malformed `CLUSTER SLOTS`**: `handle_cluster_slots` now validates reply shape and clamps slot bounds; malformed shards are logged and skipped (#425).
- **SIGPIPE on TLS RST**: `signal(SIGPIPE, SIG_IGN)` installed process-wide so a peer RST mid-write no longer exits 141 (#383).
- **Crash handler hardening**: `sigaltstack` on main and each worker, pre-warmed backtrace symbols, `backtrace_symbols_fd`, and a 5 s watchdog (#412, #416).
- **Cluster reconnect SIGABRT**: `cluster_client::connect()` is now idempotent under `--cluster-mode --reconnect-on-error` (#378).
- **Reconnect duplicate-error thread kill**: Duplicate error callbacks while a reconnect is pending no longer break the event loop (#392).
- **Unbounded reconnect/retry backoff**: Exponential backoff is now capped at 60 s; previously `factor=2.0` + unlimited attempts could schedule reconnects ~34 years out (#433).
- **`--monitor-input` parsing**: Preserves embedded NUL bytes in the loader; handles CR-only line endings (#434).
- **RESP3 push and nil**: Accepts `>` push frames (drained out-of-band) and counts `_` (nil) as a miss in arbitrary-command tracking (#435).
- **CLI input validation**: Rejects at parse time inputs that 2.3 silently accepted and then crashed/hung on — `-c/-t/-n/--test-time` negatives (#436), `--run-count` overflow (#429), `--pipeline / --data-size / --data-size-list` (#428), `--command / --command-ratio` (#427), `--key-stddev / --key-median / G:G` degenerate ranges (#430). All return a clear error and `exit 2`.
- **Bound recursive RESP teardown**: Iterative `~mbulk_size_el` so deeply-nested replies can't stack-overflow on cleanup; `-Werror=vla` build guard (#415).
- **TLS path leakage in JSON**: `--cert/--key/--cacert` are now basename-only in `mb.json` (#440).
- **`m_reqs_generated` defensive clamp**: Underflow guard at three cluster_client subtraction sites (#437).
- **Hits/sec /0 guard**: `summarize()` rate computations no longer emit `+Inf`/`nan` on zero-duration runs (#436).

## Build & Configuration Improvements
- **OpenSSL 4.0 build**: Replaces removed `SSL_OP_NO_TLSv*` flags with `SSL_CTX_set_min/max_proto_version`; rejects non-contiguous `--tls-protocols` selection at parse time on 4.0+ (#432, #444).

## Developer Tooling & CI Improvements
- **libFuzzer harnesses**: Per-PR 60 s smoke fuzz across `split_command_to_args`, `monitor_input`, and `redis_protocol` (#421).
- **Adversarial RESP mock-server fixtures**: 12 deterministic payloads exercising the parser surface (#419).
- **Monitor-input black-box fuzzer**: Corpus + synthetic seeds, nightly + `run-fuzz` label trigger (#422).
- **Hypothesis CLI fuzzer**: Re-broadened strategy + new `cli-fuzz-nightly.yml` (#424, #445).
- **Differential vs `redis-benchmark` / `redis-cli`**: `run-differential` PR label, label/cron-triggered workflow (#420).
- **Nightly soak suite**: 7 scenarios (long-run memory, large payloads, high concurrency, connection churn, cluster turbulence, retry storm, slow network) with `run-soak` label (#423).
- **Sanitizer stress matrix**: STRESS=1 cells across ASAN/UBSan/TSAN (#418).
- **Workflow permissions hardening**: `permissions: contents: read` declared on every workflow (#380, #393).
- **PARALLELISM=2 on cluster cells**: Resolves Test cell 15-min timeout on cluster TLS (#402).

## New Contributors
- @arpitjain099 made their first contribution (#380, #393).

**Full Changelog**: https://github.com/redis/memtier_benchmark/compare/2.3.1...2.4.0
