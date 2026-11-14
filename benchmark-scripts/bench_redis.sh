#!/bin/bash
# Redis benchmarking script with memtier_benchmark

HOST=${1:-127.0.0.1}
PORT=${2:-6379}
PASSWORD=${3:-}          # leave empty if no auth
THREADS=${4:-8}
CLIENTS=${5:-50}
DATA_SIZE=${6:-128}
TEST_TIME=${7:-60}
RATIO=${8:-1:1}          # SET:GET
KEY_MAX=${9:-1000000}

AUTH_OPT=""
if [ -n "$PASSWORD" ]; then
  AUTH_OPT="-a $PASSWORD"
fi

echo "=== Redis Benchmark ==="
echo "Host: $HOST:$PORT | Threads: $THREADS | Clients/thread: $CLIENTS"
echo "Data size: ${DATA_SIZE}B | Ratio SET:GET = $RATIO | Time: ${TEST_TIME}s"
echo "----------------------------------------"

# 1. Optional: populate keys (write-only)
# Uncomment if you want a pre-filled dataset
# memtier_benchmark -s $HOST -p $PORT $AUTH_OPT \
#   --protocol=redis \
#   -t $THREADS -c $CLIENTS \
#   -d $DATA_SIZE \
#   --key-maximum=$KEY_MAX \
#   --key-pattern=P:P \
#   --ratio=1:0 \
#   -n allkeys \
#   --hide-histogram

# 2. Main mixed / custom workload
memtier_benchmark -s $HOST -p $PORT $AUTH_OPT \
  --protocol=redis \
  -t $THREADS \
  -c $CLIENTS \
  -d $DATA_SIZE \
  --ratio=$RATIO \
  --test-time=$TEST_TIME \
  --key-maximum=$KEY_MAX \
  --key-pattern=R:R \
  --pipeline=1 \
  --hide-histogram \
  --out-file=redis_bench_$(date +%Y%m%d_%H%M%S).txt

echo "Done. Results saved to redis_bench_*.txt"
