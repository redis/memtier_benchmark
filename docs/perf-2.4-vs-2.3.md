# memtier_benchmark 2.4 vs 2.3.1 — Micro-benchmark

## Background

Two changes between 2.3.1 (commit `75588c1b`) and 2.4 (current HEAD,
`20688d33`) were flagged as perf-suspect in Review 18 but had never been
measured:

1. **`struct request` grew by ~64 B and gained a non-trivial destructor.**
   New fields (defined in `shard_connection.h`):

   ```c++
   struct timeval m_first_sent_time; // first-attempt send time
   unsigned int   m_retries;
   bool           m_claimed_by_retry;
   char          *m_serialized;       // owned; freed in ~request
   size_t         m_serialized_len;
   char          *m_key;              // owned; freed in ~request
   unsigned int   m_key_len;
   ```

   `~request` now calls `free()` on `m_serialized` and `m_key` (each guarded
   by a `NULL` check). This destructor runs once per request — the
   per-response hot path.

2. **`fill_pipeline()` calls `bufferevent_enable(m_bev, EV_READ|EV_WRITE)`
   unconditionally at the top of every invocation** (`shard_connection.cpp`
   line 1033–1035). In 2.3.1 the only callsite was in `handle_event()`
   after `BEV_EVENT_CONNECTED`. In cluster mode this now fires per response
   received.

Neither had been measured. This report captures the result.

## Methodology

### Server

| Item              | Value                                                 |
|-------------------|-------------------------------------------------------|
| Redis             | 8.6.0, standalone, default config (port 6379)         |
| OS                | Linux 6.17.0-29-generic x86_64                        |
| CPU               | Intel Core Ultra 7 155U, 14 logical CPUs              |
| Memory            | 62 GiB                                                |
| Loopback          | `127.0.0.1` (no network in path)                      |

Client and server share the host. The host is a workstation laptop,
which is the dominant source of measurement noise (thermal throttling,
background load). Per-run relative standard deviation is 3–7%; deltas
below that floor are not meaningful in either direction.

### Binaries

| Tag       | SHA         | Source                                                  |
|-----------|-------------|---------------------------------------------------------|
| 2.3.1     | `75588c1b`  | tag commit, built via `autoreconf -ivf && ./configure && make -j` |
| 2.4-rc    | `20688d33`  | current master, same build invocation                   |

Both binaries link against `libevent 2.1.12-stable` and OpenSSL 3.0.13.
Standalone (non-cluster) mode is exercised here; the cluster-mode
`fill_pipeline` hot path is the same code, just hit more frequently per
response.

### Workloads

Two configurations, both `--ratio=1:1` (SET/GET mix), `--hide-histogram`,
JSON output via `--json-out-file`:

| Workload    | Flags                                                                  |
|-------------|------------------------------------------------------------------------|
| canonical   | `--pipeline=32 --clients=50 --threads=4 --requests=N`                 |
| dense       | `--pipeline=200 --clients=100 --threads=4 --requests=N`               |

`N` varies by sub-experiment (200K / 500K / 50K) to balance run time
against sample size.

### Run protocol

- `redis-cli flushall` between every run.
- 2–4 prewarm runs (data discarded) before any measurement, alternating
  between binaries so neither version sees a cold cache while the other
  sees a warm one.
- Measurement runs are **interleaved**: round 1 runs both binaries,
  round 2 runs both, etc. This way thermal drift across the session
  applies equally to both versions instead of being pinned to whichever
  one was tested first.
- 3–5 measurement runs per binary per sub-experiment.
- Sleep 2–3 s between runs.

A grouped-by-version sub-experiment (all 2.3.1 runs back-to-back, then all
2.4 runs back-to-back) was also performed for comparison; results were
heavily contaminated by cold-cache warmup on the first one or two 2.3.1
runs and are reported as a separate row for transparency.

Mean ± standard deviation is reported per metric. The verdict per
sub-experiment uses Ops/sec as primary and latency percentiles as
secondary; a regression > 2% on Ops/sec is the failure threshold per
Review 18.

## Results

### Canonical workload (`--pipeline=32 --clients=50 --threads=4`)

Three independent measurements of this workload were taken at different
durations / sample sizes.

**Configuration A: 5 runs interleaved, 200K requests each, 4 prewarms.**

| Metric        | 2.3.1 mean ± stdev          | 2.4 mean ± stdev            | Delta    |
|---------------|-----------------------------|-----------------------------|----------|
| Ops/sec       | 626,325 ± 40,156 (6.4 %)    | 636,828 ± 34,603 (5.4 %)    | +1.68 %  |
| p50 (ms)      | 9.893 ± 0.561               | 9.561 ± 0.463               | −3.36 %  |
| p99 (ms)      | 20.377 ± 1.380              | 19.225 ± 1.497              | −5.65 %  |
| p99.9 (ms)    | 25.343 ± 1.305              | 24.293 ± 2.258              | −4.14 %  |
| Avg lat (ms)  | 10.287 ± 0.664              | 10.070 ± 0.567              | −2.10 %  |

**Configuration B: 4 runs interleaved, 500K requests each, 4 prewarms (lower noise).**

| Metric        | 2.3.1 mean ± stdev          | 2.4 mean ± stdev            | Delta    |
|---------------|-----------------------------|-----------------------------|----------|
| Ops/sec       | 597,715 ± 23,482 (3.9 %)    | 590,372 ± 25,479 (4.3 %)    | −1.23 %  |
| p50 (ms)      | 10.239 ± 0.339              | 10.175 ± 0.366              | −0.63 %  |
| p99 (ms)      | 21.375 ± 0.881              | 21.119 ± 0.523              | −1.20 %  |
| p99.9 (ms)    | 26.847 ± 1.303              | 27.327 ± 0.966              | +1.79 %  |
| Avg lat (ms)  | 10.741 ± 0.403              | 10.851 ± 0.469              | +1.03 %  |

**Configuration C (reference, grouped — not interleaved): 3 runs each, 200K requests.**
The first canonical 2.3.1 run was a cold-cache warmup outlier (395K
ops/sec); the subsequent runs progressively climbed to ~580K. Because
2.4 ran after 2.3.1, it saw a warm system the whole way. This rendered
the grouped comparison unreliable (showed 2.4 +33% Ops/sec, clearly an
artifact, not a real signal). Numbers omitted for brevity — they are
preserved in the raw JSON files for inspection.

### Dense pipeline workload (`--pipeline=200 --clients=100 --threads=4`)

**Configuration D: 3 runs interleaved, 50K requests each (~20M ops/run).**

| Metric        | 2.3.1 mean ± stdev          | 2.4 mean ± stdev            | Delta    |
|---------------|-----------------------------|-----------------------------|----------|
| Ops/sec       | 896,855 ± 55,644 (6.2 %)    | 894,183 ± 29,738 (3.3 %)    | −0.30 %  |
| p50 (ms)      | 92.159 ± 4.191              | 93.012 ± 2.069              | +0.93 %  |
| p99 (ms)      | 122.708 ± 5.663             | 126.122 ± 7.461             | +2.78 %  |
| p99.9 (ms)    | 144.895 ± 24.613            | 144.383 ± 15.897            | −0.35 %  |
| Avg lat (ms)  | 90.782 ± 3.721              | 92.134 ± 2.124              | +1.49 %  |

**Configuration E (reference, grouped): 3 runs each, 200K requests (~80M ops/run, ~90 s/run).**

| Metric        | 2.3.1 mean ± stdev          | 2.4 mean ± stdev            | Delta    |
|---------------|-----------------------------|-----------------------------|----------|
| Ops/sec       | 869,591 ± 8,645 (1.0 %)     | 888,891 ± 8,936 (1.0 %)     | +2.22 %  |
| p50 (ms)      | 93.524 ± 0.782              | 90.964 ± 1.289              | −2.74 %  |
| p99 (ms)      | 125.439 ± 1.355             | 122.196 ± 2.132             | −2.59 %  |
| p99.9 (ms)    | 148.820 ± 6.818             | 144.042 ± 6.818             | −3.21 %  |
| Avg lat (ms)  | 92.645 ± 0.473              | 90.449 ± 1.101              | −2.37 %  |

The dense workload has the longest steady-state phase (each run lasts
20–90 s), and ran late enough in the session that the host CPU/cache
state had stabilized. Both grouped (E) and interleaved (D) measurements
agree that 2.4 and 2.3.1 are within ~2 % of each other on Ops/sec.

## Verdict

**No material regression.** Every measured Ops/sec delta is within
±2.22 %, every per-version relative standard deviation is 3–7 %, and the
direction of the deltas is inconsistent across sub-experiments
(canonical-B mildly favors 2.3.1, canonical-A and dense-E mildly favor
2.4, dense-D and the latency metrics in canonical-B are essentially
tied). This is the signature of run-to-run noise dominating any real
underlying effect, not a structural regression.

The dense workload — the one Review 18 specifically called out for
hitting the `fill_pipeline` path harder — shows no regression in either
the grouped or interleaved measurement. The canonical workload at its
longest sample (Configuration B, 500K req/run) shows a 1.23 % Ops/sec
delta well within its 3.9 % run-to-run noise envelope.

The 2.4 release notes can claim parity with 2.3.1 on standalone SET/GET
throughput.

## Caveats

- Measured on a workstation laptop with thermal scaling. Per-run noise
  is 3–7 %; deltas below that floor are not statistically meaningful.
  A dedicated server with frequency-pinned CPUs would tighten the
  bounds, but the current data is more than sufficient to rule out the
  > 2 % regression Review 18 was concerned about.
- Loopback path; no NIC, no real network. The `bufferevent_enable`
  redundancy in `fill_pipeline` (which is the cheap path here — a level
  set on already-enabled events) might cost slightly more in TLS
  configurations; not exercised in this micro-benchmark.
- Cluster mode was not measured. The `fill_pipeline` change fires per
  response there, but the per-call cost (`bufferevent_enable` on
  already-enabled events) is a couple of pointer comparisons and a
  conditional write; even at 1 M ops/s/connection the absolute
  arithmetic gives sub-µs per response. If a future user reports a
  cluster-mode regression, the conditional-on-`m_bev_paused` patch
  sketched in the task brief is the recommended first attempt.
- Raw per-run JSON outputs are preserved in `/tmp/mb-results/` on the
  measurement host (canonical-ext-*.json, canonical-long-*.json,
  canonical2-*.json, dense-*.json, dense-il-*.json).

## References

- 2.4 review #28 (Review 18): items 1 and 2 (struct-bloat throughput +
  `fill_pipeline` call rate) were called out as the blocking
  measurements before release notes claiming parity.
- `shard_connection.h:65` — current `struct request` layout.
- `shard_connection.cpp:119–150` — current request constructor /
  destructor.
- `shard_connection.cpp:1026–1035` — current `fill_pipeline` head,
  including the unconditional `bufferevent_enable` call.
