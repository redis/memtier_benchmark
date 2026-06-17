# Real-Time Metrics Visualization

memtier_benchmark supports streaming real-time metrics to a StatsD-compatible server for live visualization during benchmark runs. This is particularly useful for:

- Observing performance during long-running tests
- Monitoring latency spikes during scaling events (e.g., Redis Enterprise slot migrations)
- Comparing multiple benchmark runs side-by-side
- Sharing live dashboards with team members

## Quick Start

### 1. Start the Monitoring Stack

A pre-configured Docker Compose setup is included with Graphite (StatsD receiver) and Grafana:

```bash
docker-compose -f docker-compose.statsd.yml up -d
```

This starts:
- **Graphite + StatsD** on `localhost:8125` (UDP) - receives metrics
- **Grafana** on `http://localhost:3000` - visualization (login: `admin` / `admin`)

### 2. Run a Benchmark with Metrics

```bash
./memtier_benchmark -s <redis-host> -p <redis-port> \
    --statsd-host=localhost \
    --test-time=60
```

### 3. View the Dashboard

Open http://localhost:3000 in your browser, log in with `admin`/`admin`, and navigate to the **Memtier Benchmark** dashboard. You'll see live metrics updating in real-time.

### 4. Stop the Monitoring Stack

```bash
docker-compose -f docker-compose.statsd.yml down
```

To also remove stored data:
```bash
docker-compose -f docker-compose.statsd.yml down -v
```

## Command-Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--statsd-host=HOST` | *(disabled)* | StatsD server hostname. Metrics are only sent when this is set. |
| `--statsd-port=PORT` | `8125` | StatsD server UDP port. |
| `--statsd-prefix=PREFIX` | `memtier` | Prefix for all metric names in Graphite. |
| `--statsd-run-label=LABEL` | `default` | Label to identify this benchmark run. Use different labels to compare runs. |
| `--graphite-port=PORT` | `8080` | Graphite HTTP port for event annotations. Use `80` when running inside the same Docker network. |

### Examples

Basic usage:
```bash
./memtier_benchmark -s redis.example.com --statsd-host=localhost --test-time=120
```

With a custom run label for comparison:
```bash
./memtier_benchmark -s redis.example.com --statsd-host=localhost \
    --statsd-run-label=baseline --test-time=60

# Later, run another test with a different label
./memtier_benchmark -s redis.example.com --statsd-host=localhost \
    --statsd-run-label=after-upgrade --test-time=60
```

Custom prefix (useful if sharing a Graphite instance):
```bash
./memtier_benchmark -s redis.example.com --statsd-host=metrics.internal \
    --statsd-prefix=team1.memtier --statsd-run-label=prod-test
```

## Metrics Reference

The following metrics are sent approximately every 1 second during the benchmark.

### Throughput

| Metric | StatsD type | Graphite path | Description |
|--------|-------------|---------------|-------------|
| `ops_sec` | gauge | `stats.gauges.<prefix>.<label>.ops_sec` | Instantaneous ops/sec over the last interval |
| `ops_sec_avg` | gauge | `stats.gauges.<prefix>.<label>.ops_sec_avg` | Running average ops/sec since benchmark start |
| `bytes_sec` | gauge | `stats.gauges.<prefix>.<label>.bytes_sec` | Instantaneous byte throughput over the last interval |
| `bytes_sec_avg` | gauge | `stats.gauges.<prefix>.<label>.bytes_sec_avg` | Running average byte throughput since benchmark start |

### Latency

| Metric | StatsD type | Graphite path | Description |
|--------|-------------|---------------|-------------|
| `latency_ms` | timing (ms) | `stats.timers.<prefix>.<label>.latency_ms.*` | Instantaneous average latency (ms) over the last interval |
| `latency_avg_ms` | timing (ms) | `stats.timers.<prefix>.<label>.latency_avg_ms.*` | Running average latency (ms) since benchmark start |
| `latency_p<N>` | gauge | `stats.gauges.<prefix>.<label>.latency_p<N>` | Instantaneous latency at percentile N (ms). One metric per percentile configured via `--print-percentiles`. Default: `latency_p50`, `latency_p99`, `latency_p99_9`. Decimal points are replaced with underscores (e.g. `latency_p99_9` for p99.9). |

> **Note:** `latency_ms` and `latency_avg_ms` are sent as StatsD timing metrics (`ms` type). StatsD
> processes them into derived stats (mean, upper, lower, etc.) that appear under
> `stats.timers.*` in Graphite — not under `stats.gauges.*` like the other metrics.
> The per-percentile `latency_p<N>` metrics are plain gauges and appear under `stats.gauges.*`.

### Connections and Errors

| Metric | StatsD type | Graphite path | Description |
|--------|-------------|---------------|-------------|
| `connections` | gauge | `stats.gauges.<prefix>.<label>.connections` | Active connection count (`--clients` × active thread count) |
| `connection_errors` | gauge | `stats.gauges.<prefix>.<label>.connection_errors` | Cumulative connection error count. **Only sent when the count is > 0** and not zeroed at run end — stale values may linger in Graphite after errors clear. Protocol-level command errors are not tracked here. |

### Progress

| Metric | StatsD type | Graphite path | Description |
|--------|-------------|---------------|-------------|
| `progress_pct` | gauge | `stats.gauges.<prefix>.<label>.progress_pct` | Benchmark completion percentage (0–100) |

### Events (Graphite annotations)

Two events are sent via HTTP POST to the Graphite events API (not StatsD UDP):

| Event | Tags | When |
|-------|------|------|
| `Benchmark Started` | `memtier,start` | Immediately before the benchmark loop begins |
| `Benchmark Completed` | `memtier,end` | Immediately after all threads finish |

These appear as vertical annotation lines on the Grafana dashboard.

### End-of-run zeroing

When the benchmark completes, the following gauges are explicitly zeroed so graphs
return to baseline rather than holding the last value:
`ops_sec`, `ops_sec_avg`, `bytes_sec`, `bytes_sec_avg`, `progress_pct`.

`connections`, `latency_ms`, `latency_avg_ms`, `latency_p<N>`, and `connection_errors`
are **not** zeroed at run end.

## Comparing Multiple Benchmark Runs

One of the most powerful features is the ability to **overlay multiple benchmark runs on the same graphs** for direct comparison. This is ideal for A/B testing, before/after comparisons, or evaluating different configurations.

### How It Works

1. **Run benchmarks with different labels:**
   ```bash
   # First run - baseline
   ./memtier_benchmark -s redis-server --statsd-host=localhost \
       --statsd-run-label=baseline --test-time=60

   # Second run - after changes
   ./memtier_benchmark -s redis-server --statsd-host=localhost \
       --statsd-run-label=after-tuning --test-time=60

   # Third run - different configuration
   ./memtier_benchmark -s redis-server --statsd-host=localhost \
       --statsd-run-label=high-concurrency -c 100 -t 8 --test-time=60
   ```

2. **Use the Run Label dropdown** at the top of the Grafana dashboard to:
   - **Select multiple labels** - Hold Ctrl/Cmd and click to select several runs
   - **Select "All"** - Overlay every run on the same graphs
   - **Deselect runs** - Click to toggle individual runs on/off

3. **Compare visually** - Each run appears as a separate line with its own color, making it easy to spot performance differences.

### Example Use Cases

| Scenario | Labels to Compare |
|----------|-------------------|
| Before/after Redis upgrade | `redis-6.2`, `redis-7.0` |
| Connection pool tuning | `pool-10`, `pool-50`, `pool-100` |
| Cluster scaling test | `3-shards`, `6-shards`, `12-shards` |
| Network latency impact | `same-az`, `cross-az`, `cross-region` |

## Grafana Dashboard

The included dashboard provides:

- **Operations per Second** - Current and average ops/sec over time
- **Latency** - Current and average latency in milliseconds
- **Throughput** - Current and average bytes/sec
- **Connections** - Active connection count
- **Progress** - Benchmark completion percentage
- **Connection Errors** - Error count indicator

All panels support multi-run overlay when multiple run labels are selected.

## Troubleshooting

### Verify StatsD is Receiving Metrics

Send a test metric manually:
```bash
echo "test.metric:100|g" | nc -u -w1 localhost 8125
```

Check Graphite's web UI at http://localhost:8080 to see if metrics appear.

### No Data in Grafana

1. Ensure the benchmark is running with `--statsd-host` set
2. Check that port 8125/UDP is accessible (not blocked by firewall)
3. Verify Grafana's Graphite datasource is configured (should be automatic with the Docker setup)
4. Try refreshing the dashboard or adjusting the time range to "Last 5 minutes"

### Metrics Delayed or Missing

- StatsD aggregates metrics every 1 second by default
- The dashboard refreshes every 1 second
- If running memtier from a container, use `--statsd-host=host.docker.internal` (macOS/Windows) or the host's IP

### Reset Stored Data

To clear all historical metrics and start fresh:
```bash
docker-compose -f docker-compose.statsd.yml down -v
docker-compose -f docker-compose.statsd.yml up -d
```

## Architecture

```
┌─────────────────────┐     UDP:8125      ┌─────────────────────┐
│ memtier_benchmark   │ ───────────────── │ Graphite + StatsD   │
│ --statsd-host=...   │                   │ (metrics storage)   │
└─────────────────────┘                   └──────────┬──────────┘
                                                     │
                                          ┌──────────▼──────────┐
                                          │      Grafana        │
                                          │   localhost:3000    │
                                          └─────────────────────┘
```

## Prometheus /metrics Exporter

In addition to the StatsD/Graphite push transport above, memtier_benchmark can
expose the same live metrics over a Prometheus `/metrics` HTTP endpoint (pull
model). The exporter is compiled in by default and enabled at runtime by
`--prometheus-port`.

```bash
# Fixed port (loopback by default):
./memtier_benchmark -s redis-server --test-time=60 --prometheus-port=8080
curl http://127.0.0.1:8080/metrics

# Ephemeral port — the bound URL is announced on stdout as a single line:
./memtier_benchmark -s redis-server --test-time=60 --prometheus-port=0
# Prometheus exporter listening on http://127.0.0.1:43217/metrics
```

### Command-Line Options (Prometheus)

| Option | Default | Description |
|--------|---------|-------------|
| `--prometheus-port=PORT` | *(disabled)* | TCP port for `/metrics`. `0` selects an ephemeral port. Setting it enables the exporter. |
| `--prometheus-bind-addr=ADDR` | `127.0.0.1` | Numeric IPv4/IPv6 address to bind to. Hostnames are not supported. A non-loopback address prints a one-shot warning. |
| `--prometheus-run-label=KEY=VALUE` | *(none)* | Constant label applied to every sample; repeatable, max 16. |
| `--prometheus-latency-buckets=LIST` | *(26 built-in)* | Comma-separated, strictly ascending latency bucket bounds in seconds. Bounds within ~1% of each other collapse to one HDR slot and are rejected. |

### Ephemeral-port contract

With `--prometheus-port=0` the kernel assigns a free port; memtier prints the
fully-resolved URL exactly once on **stdout**:

```
Prometheus exporter listening on http://127.0.0.1:43217/metrics
```

IPv6 binds are bracketed (`http://[::1]:43217/metrics`). Automation should parse
this line rather than guess a port. The endpoint serves `GET /metrics` only;
other methods return `501`, unknown paths return a fixed `404` body, and the
response carries `Content-Type: text/plain; version=0.0.4; charset=utf-8`.

### Metric Reference and StatsD Mapping

The exporter publishes the **same one-producer snapshot** as StatsD, reshaped to
Prometheus conventions (cumulative counters, native histogram). Notable
differences from the StatsD names above:

| Prometheus | Type | StatsD equivalent | Notes |
|---|---|---|---|
| `memtier_ops_total` | counter | `ops_sec` (rate) | Prometheus exports the cumulative total; rate is derived at query time. |
| `memtier_sent_bytes_total` / `memtier_received_bytes_total` | counter | `bytes_sec` | Cumulative bytes since process start. |
| `memtier_hits_total` / `memtier_misses_total` | counter | hits/misses | |
| `memtier_errors_total` | counter | — | Errors after retries are exhausted. |
| `memtier_connection_errors_total` | counter | `connection_errors` | Accumulated across runs (StatsD reports the raw per-run value). |
| `memtier_retry_attempts_total` / `memtier_retried_ops_total` | counter | — | Nonzero only with `--retry-on-error`. |
| `memtier_connections` / `memtier_threads` | gauge | `connections` | |
| `memtier_run` / `memtier_configured_runs` | gauge | — | Current run (1-based; 0 before the first run) and `--run-count`. |
| `memtier_config_test_time_seconds` | gauge | — | Configured `--test-time` (0 when bounded by `--requests`). |
| `memtier_latency_seconds` | histogram | `latency_ms` family | Seconds, not milliseconds; see the accuracy notes below. |
| `memtier_build_info{version,git_sha}` | gauge=1 | — | Build identity only — no config labels. |
| `memtier_start_time_seconds` | gauge | — | Process start, Unix seconds. |
| `memtier_exporter_renders_total` | counter | — | Number of times the body was rendered (scrapes that missed the 1 s render cache). |
| `memtier_exporter_snapshot_age_seconds` | gauge | — | Seconds since the benchmark loop last published a snapshot. |

Counters accumulate across runs and connection restarts, so a multi-run session
(`--run-count > 1`) produces monotonically non-decreasing series instead of the
per-run reset that a naive exporter would emit.

### Latency accuracy (error model)

`memtier_latency_seconds` is built from **1 Hz snapshots** of each connection's
in-progress second of HDR-recorded latencies. Treat it as an operational signal,
not the authoritative result:

- **Phase-dependent capture (~50% in steady state).** Only the portion of each
  connection's per-second histogram that is complete at snapshot time is folded
  in, so in steady state roughly half of all operations are represented.
  `memtier_latency_seconds_count` is therefore **not** comparable to
  `memtier_ops_total` and must not be used to compute a hit fraction.
- **Stall dedup + residuals.** A snapshot whose source histogram has not changed
  since the previous tick is skipped, so a stalled server (every in-flight op
  frozen) does **not** keep inflating `_count`; once traffic resumes the series
  grows again. A small residual at the tail of a run can be lost this way.
- **Run-tail loss.** The final fraction of a second at run end may not be
  captured by the last tick.
- **HDR quantization (≤1%).** Bucket placement carries up to 1% HDR
  quantization; the `le` bound rendered is the user-supplied (µs-quantized)
  bound, not the internal slot edge.

For exact percentiles and totals, use the **end-of-run output** (or the full
latency spectrum, see the README). The exporter is for live observation.

### Security / threat model

The `/metrics` body is designed to leak **zero** connection or configuration
identity. It contains no server address or port, no password or
`--authenticate` credentials, no request URI, and no filesystem paths (including
TLS cert/key/CA paths). Prometheus's own `instance` label covers target
identity, so the exporter deliberately omits any `server`/`host`/`target` label.
Error responses (`404`/`503`) are fixed strings that echo nothing from the
request. The exporter binds **loopback by default**; binding a non-loopback
address is allowed but warned, and exposing `/metrics` beyond a trusted network
is the operator's responsibility (there is no authentication on the endpoint).
HTTP hardening rejects non-GET methods, oversized headers, and unknown paths.

### Mode interactions

- **`--cluster-mode`** is supported. Counters are cluster-wide totals summed
  across all shard connections; the exporter binds its own TCP socket
  independently of the benchmark transport. (MOVED/ASK redirection counters are
  not yet exported.)
- **`--verify-only`** performs no benchmark run, so the exporter serves only the
  constructor's zero snapshot (`memtier_run 0`, all counters 0, with
  `memtier_exporter_snapshot_age_seconds` growing) for the whole verification
  window. `--verify-only` requires `--data-import` (it implies `--data-verify`).
- **`--unix-socket`** does not interact with the exporter: the benchmark talks to
  the target over the Unix socket while the exporter still binds its own TCP
  port.

## Using with External StatsD/Graphite

If you have an existing StatsD-compatible metrics infrastructure:

```bash
./memtier_benchmark -s redis-server \
    --statsd-host=statsd.your-company.com \
    --statsd-port=8125 \
    --statsd-prefix=benchmarks.memtier \
    --statsd-run-label=redis-perf-$(date +%Y%m%d-%H%M%S)
```

You can import the dashboard from `grafana/dashboards/memtier.json` into your Grafana instance. You may need to adjust the datasource UID to match your Graphite datasource.
