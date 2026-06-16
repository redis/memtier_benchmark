# What's Changed

## Performance
- **Restore HGETALL/HMGET throughput regressed by arbitrary-command miss-tracking** (RED-200840). Miss-tracking (default-on since 2.4.0) materialized every reply to attribute per-position hits/misses. EmptyCollection commands (HGETALL/SMEMBERS/LRANGE) now read emptiness from the declared top-level array length with zero materialization (#469); ArrayPerElementNulls commands (HMGET/MGET/ZMSCORE/GEOPOS) now record per-position hits via an alloc-free parser bitmap instead of building the reply tree (#472). Restores both HGETALL and HMGET to within ~2-3% of the pre-2.4 (2.2.1) baseline with miss-tracking left on; no change to reported hit/miss statistics.

**Full Changelog**: https://github.com/redis/memtier_benchmark/compare/2.4.1...2.4.2
