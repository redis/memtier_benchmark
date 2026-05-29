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

		OSS_STANDALONE=0|1  General tests on standalone Redis (default)
		OSS_CLUSTER=0|1     General tests on Redis OSS Cluster
		TLS=0|1             Run tests with TLS enabled
		SHARDS=n            Number of shards (default: 3)

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
SHARDS=${SHARDS:-3}
TEST=${TEST:-""}

TLS_KEY=$ROOT/tests/tls/redis.key
TLS_CERT=$ROOT/tests/tls/redis.crt
TLS_CACERT=$ROOT/tests/tls/ca.crt
REDIS_SERVER=${REDIS_SERVER:-redis-server}
MEMTIER_BINARY=$ROOT/memtier_benchmark

RLTEST_ARGS=" --cluster-start-timeout 180 --oss-redis-path $REDIS_SERVER --enable-debug-command --cluster_node_timeout 15000"
[[ "$TEST" != "" ]] && RLTEST_ARGS+=" --test $TEST"
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

exit $E
