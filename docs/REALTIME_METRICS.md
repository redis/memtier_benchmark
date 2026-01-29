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

The following metrics are sent every ~200ms during the benchmark:

| Metric | Type | Description |
|--------|------|-------------|
| `ops_sec` | gauge | Current operations per second |
| `ops_sec_avg` | gauge | Running average ops/sec since start |
| `bytes_sec` | gauge | Current throughput in bytes/sec |
| `bytes_sec_avg` | gauge | Running average throughput |
| `latency_ms` | timing | Current average latency (ms) |
| `latency_avg_ms` | timing | Running average latency since start |
| `connections` | gauge | Number of active connections |
| `connection_errors` | gauge | Total connection errors |
| `progress_pct` | gauge | Benchmark progress percentage |

Metrics appear in Graphite under the path: `stats.gauges.<prefix>.<run_label>.<metric>`

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

