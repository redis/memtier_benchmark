# What's Changed

## Bug Fixes
- **Recover client-bound throughput**: `bufferevent_enable` in `fill_pipeline` is now conditional on a new `m_bev_paused` flag instead of firing on every response. Recovers ~12% ops/sec when the client is the bottleneck; no change on server-bound workloads (#448).

**Full Changelog**: https://github.com/redis/memtier_benchmark/compare/2.4.0...2.4.1
