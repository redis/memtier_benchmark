#!/bin/bash
# Memcached benchmarking script with memtier_benchmark

HOST=${1:-127.0.0.1}
PORT=${2:-11211}
THREADS=${3:-8}
CLIENTS=${4:-50}
DATA_SIZE=${5:-128}
TEST_TIME=${6:-60}
RATIO=${7:-1:1}          # SET:GET
KEY_MAX=${8:-1000000}
PROTOCOL=${9:-memcache_text}   # or memcache_binary

echo "=== Memcached Benchmark ==="
echo "Host: $HOST:$PORT | Protocol: $PROTOCOL"
echo "Threads: $THREADS | Clients/thread: $CLIENTS"
echo "Data size: ${DATA_SIZE}B | Ratio SET:GET = $RATIO | Time: ${TEST_TIME}s"
echo "----------------------------------------"

# 1. Optional: populate keys (write-only)
# memtier_benchmark -s $HOST -p $PORT \
#   --protocol=$PROTOCOL \
#   -t $THREADS -c $CLIENTS \
#   -d $DATA_SIZE \
#   --key-maximum=$KEY_MAX \
#   --key-pattern=P:P \
#   --ratio=1:0 \
#   -n allkeys \
#   --hide-histogram

# 2. Main workload
sudo memtier_benchmark -s $HOST -p $PORT \
  --protocol=$PROTOCOL \
  -t $THREADS \
  -c $CLIENTS \
  -d $DATA_SIZE \
  --ratio=$RATIO \
  --test-time=$TEST_TIME \
  --key-maximum=$KEY_MAX \
  --key-pattern=R:R \
  --pipeline=1 \
  --hide-histogram \
  --out-file=memcached_bench_$(date +%Y%m%d_%H%M%S).txt

echo "Done. Results saved to memcached_bench_*.txt"
