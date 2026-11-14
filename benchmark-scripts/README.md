# Memtier Benchmark Scripts Manual

This repository provides two simple bash scripts to benchmark **Redis** and **Memcached** using [`memtier_benchmark`](https://github.com/redis/memtier_benchmark).

- `bench_redis.sh` → Benchmark Redis
- `bench_memcached.sh` → Benchmark Memcached

---

## Prerequisites

Before using the scripts, make sure you have `memtier_benchmark` installed and run Redis or Memcached container.



### Install memtier_benchmark

**On Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install memtier-benchmark

### Make Scripts Executable

chmod +x scripts/bench_redis.sh
chmod +x scripts/bench_memcached.sh
