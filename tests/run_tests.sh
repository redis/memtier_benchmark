#!/bin/bash

[[ $VERBOSE == 1 ]] && set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/.. && pwd)

#----------------------------------------------------------------------------------------------

help() {
	cat <<-END
		Run flow tests.

		[ARGVARS...] run_tests.sh [--help|help]

		Argument variables:

		OSS_STANDALONE=0|1       General tests on standalone Redis (default)
		OSS_CLUSTER=0|1          General tests on Redis OSS Cluster
		OSS_CLUSTER_REPLICAS=0|1 When set to 1, run the OSS-CLUSTER suite with one
		                         replica per shard (passes --use-slaves to RLTest and
		                         sets --shards-count to SHARDS).  Used by the
		                         read-preference CI matrix cell.
		TLS=0|1                  Run tests with TLS enabled
		SHARDS=n                 Number of shards (default: 3)
		STRESS=0|1               Override default test set with tests/test_sanitizer_stress.py
		                         (large data, long monitor lines, reconnect churn, --debug paths).
		                         Used by the sanitizer workflows' STRESS matrix axis (issue #411).

		REDIS_SERVER=path   Location of redis-server
		VERBOSE=1           Print commands
		LOG_LEVEL=level     RLTest log level (default: debug)
		TEST_TIMEOUT=n      Test timeout in seconds (default: 300)
		PARALLELISM=n       Run n tests concurrently (RLTest --parallelism); each
		                    worker gets its own redis env on a distinct port range.
		                    Unset = serial. Used to keep the slow sanitizer cluster
		                    cells under the CI step timeout.
		RLTEST_VERBOSE=1    Enable RLTest verbose mode
		RLTEST_DEBUG=1      Enable RLTest debug print
		MEMTIER_FUZZ=1      Run the pytest-driven Hypothesis CLI fuzzer after the
		                    RLTest suites (see tests/cli_fuzz.py, issue #410).
		MEMTIER_FUZZ_SEED=n Hypothesis seed (default: 0).

	END
}

#----------------------------------------------------------------------------------------------

run_tests() {
	local title="$1"
	if [[ -n $title ]]; then
		printf "Running $title:\n\n"
	fi

	if [[ $VERBOSE == 1 ]]; then
		echo "RLTest configuration:"
		echo "$RLTEST_ARGS"
	fi

	cd $ROOT/tests

	local E=0
	{
		$OP python3 -m RLTest $RLTEST_ARGS
		((E |= $?))
	} || true

	return $E
}

#----------------------------------------------------------------------------------------------

[[ $1 == --help || $1 == help ]] && {
	help
	exit 0
}

#----------------------------------------------------------------------------------------------

OSS_STANDALONE=${OSS_STANDALONE:-1}
OSS_CLUSTER=${OSS_CLUSTER:-0}
OSS_CLUSTER_REPLICAS=${OSS_CLUSTER_REPLICAS:-0}
SHARDS=${SHARDS:-3}
TEST=${TEST:-""}
STRESS=${STRESS:-0}

# STRESS=1 overrides the default test set with the sanitizer stress suite
# (tests/test_sanitizer_stress.py), which exercises large data sizes, long
# monitor-input lines, huge key prefixes, reconnect churn and --debug
# codepaths under sanitizers. See GH issue #411.
if [[ $STRESS == 1 && -z $TEST ]]; then
	TEST="test_sanitizer_stress.py"
fi

TLS_KEY=$ROOT/tests/tls/redis.key
TLS_CERT=$ROOT/tests/tls/redis.crt
TLS_CACERT=$ROOT/tests/tls/ca.crt
REDIS_SERVER=${REDIS_SERVER:-redis-server}
MEMTIER_BINARY=$ROOT/memtier_benchmark

RLTEST_ARGS=" --cluster-start-timeout 180 --oss-redis-path $REDIS_SERVER --enable-debug-command --cluster_node_timeout 15000"
# RLTest's --test uses action='append' with no nargs, so a space-separated
# TEST="f1 f2 f3" must be emitted as multiple --test flags. Splitting on $TEST
# unquoted lets the shell word-split into individual tokens.
if [[ "$TEST" != "" ]]; then
	for t in $TEST; do
		RLTEST_ARGS+=" --test $t"
	done
fi
[[ -n "$PARALLELISM" ]] && RLTEST_ARGS+=" --parallelism $PARALLELISM"
[[ $VERBOSE == 1 ]] && RLTEST_ARGS+=" -v"
[[ $TLS == 1 ]] && RLTEST_ARGS+=" --tls-cert-file $TLS_CERT --tls-key-file $TLS_KEY --tls-ca-cert-file $TLS_CACERT --tls"

LOG_LEVEL=${LOG_LEVEL:-notice}
RLTEST_ARGS+=" --log-level $LOG_LEVEL"

if [[ $RLTEST_DEBUG == 1 ]]; then
	RLTEST_ARGS+=" -s --debug-print"
fi

cd $ROOT/tests

E=0
[[ $OSS_STANDALONE == 1 ]] && {
	(ROOT_FOLDER=$ROOT TLS_KEY=$TLS_KEY TLS_CERT=$TLS_CERT TLS_CACERT=$TLS_CACERT MEMTIER_BINARY=$MEMTIER_BINARY RLTEST_ARGS="${RLTEST_ARGS}" run_tests "tests on OSS standalone")
	((E |= $?))
} || true

[[ $OSS_CLUSTER == 1 ]] && {
	(ROOT_FOLDER=$ROOT TLS_KEY=$TLS_KEY TLS_CERT=$TLS_CERT TLS_CACERT=$TLS_CACERT MEMTIER_BINARY=$MEMTIER_BINARY RLTEST_ARGS="${RLTEST_ARGS} --env oss-cluster --shards-count $SHARDS" run_tests "tests on OSS cluster")
	((E |= $?))
} || true

# OSS_CLUSTER_REPLICAS=1: run the cluster suite with one replica per shard.
# RLTest's --use-slaves flag tells RLTest to start replica nodes alongside each
# master so that read-preference tests can route traffic to replicas.  The
# shard count is taken from the SHARDS variable (default: 3).
[[ $OSS_CLUSTER_REPLICAS == 1 ]] && {
	(ROOT_FOLDER=$ROOT TLS_KEY=$TLS_KEY TLS_CERT=$TLS_CERT TLS_CACERT=$TLS_CACERT MEMTIER_BINARY=$MEMTIER_BINARY RLTEST_ARGS="${RLTEST_ARGS} --env oss-cluster --shards-count $SHARDS --use-slaves" run_tests "tests on OSS cluster with replicas (read-preference)")
	((E |= $?))
} || true

# Optional pytest-driven Hypothesis CLI fuzzer (see tests/cli_fuzz.py
# and issue #410). Off by default so the standard suite stays fast; opt in
# with MEMTIER_FUZZ=1, which is also the gate that the test itself checks.
# Intended to run nightly under ASAN/UBSan builds.
[[ $MEMTIER_FUZZ == 1 ]] && {
	printf "\nRunning CLI hypothesis fuzzer (MEMTIER_FUZZ=1):\n\n"
	(cd $ROOT/tests && MEMTIER_BINARY=$MEMTIER_BINARY \
		python3 -m pytest -p no:asyncio cli_fuzz.py \
		--hypothesis-seed=${MEMTIER_FUZZ_SEED:-0} -v)
	((E |= $?))
} || true

exit $E
