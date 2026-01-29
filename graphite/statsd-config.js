/*
 * StatsD configuration for memtier_benchmark
 * Flush interval set to 1 second for real-time visualization
 */
{
  graphitePort: 2003,
  graphiteHost: "127.0.0.1",
  port: 8125,
  mgmt_port: 8126,
  backends: ["./backends/graphite"],
  
  // Flush metrics every 1 second (default is 10 seconds)
  flushInterval: 1000,
  
  // Delete idle stats after 5 minutes
  deleteIdleStats: true,
  deleteGauges: true,
  deleteTimers: true,
  deleteCounters: true,
  deleteSets: true,
  
  // Percentiles for timer metrics
  percentThreshold: [50, 90, 95, 99, 99.9]
}

