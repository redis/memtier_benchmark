#!/bin/sh
# Self-test for utils/apt_install.sh. It runs the helper against a stub
# apt-get and a temporary sources directory, so it needs no root and leaves
# the host's apt configuration alone.
set -u
helper="$(cd "$(dirname "$0")" && pwd)/apt_install.sh"
work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

# The stub logs each call and fails the first $STUB_UPDATE_FAILS updates and
# $STUB_INSTALL_FAILS installs, and every update from the
# $STUB_UPDATE_FAILS_FROM-th on (counting from 0, if set). STUB_MISSING=1
# makes install report an unknown package, STUB_MISSING=2 a package with no
# installation candidate.
cat > "$work/apt-get" <<'EOF'
#!/bin/sh
echo "$*" >> "$STUB_CALLS"
case " $* " in
*" update "*) var=STUB_UPDATE_FAILS ;;
*) var=STUB_INSTALL_FAILS ;;
esac
n=0
[ -f "$STUB_COUNTS" ] && n=$(grep -c "^$var$" "$STUB_COUNTS")
echo "$var" >> "$STUB_COUNTS"
if [ "$var" = STUB_INSTALL_FAILS ] && [ "${STUB_MISSING:-0}" = 1 ]; then
    echo "E: Unable to locate package no-such-package"
    exit 100
fi
if [ "$var" = STUB_INSTALL_FAILS ] && [ "${STUB_MISSING:-0}" = 2 ]; then
    echo "E: Package 'no-candidate' has no installation candidate"
    exit 100
fi
eval "fails=\${$var:-0}"
if [ "$var" = STUB_UPDATE_FAILS ] && [ -n "${STUB_UPDATE_FAILS_FROM:-}" ] && [ "$n" -ge "$STUB_UPDATE_FAILS_FROM" ]; then
    fails=$((n + 1))
fi
if [ "$n" -lt "$fails" ]; then
    echo "E: Failed to fetch https://example.invalid/InRelease  403  Forbidden"
    exit 100
fi
echo "stub apt-get $*: ok"
EOF
chmod +x "$work/apt-get"

failures=0
fail() {
    echo "FAIL: $case_name: $*" >&2
    failures=$((failures + 1))
}

# run CASE_NAME EXPECTED_STATUS [helper args...]; the stub's settings come
# from the environment.
run() {
    case_name=$1
    expected=$2
    shift 2
    rm -rf "$work/sources" "$work/calls" "$work/counts"
    mkdir "$work/sources"
    echo "deb https://packages.microsoft.com/ubuntu/24.04/prod noble main" > "$work/sources/microsoft-prod.list"
    echo "deb https://packages.redis.io/deb noble main" > "$work/sources/redis.list"
    if [ "${WITH_DEB822:-0}" = 1 ]; then
        echo "URIs: https://packages.microsoft.com/ubuntu/24.04/prod" > "$work/sources/microsoft-prod.sources"
    fi
    [ "${READONLY_SOURCES:-0}" = 1 ] && chmod a-w "$work/sources"
    : > "$work/calls"
    APT_GET="$work/apt-get" APT_SUDO= APT_SOURCES_DIR="$work/sources" APT_RETRY_DELAY=0 \
        STUB_CALLS="$work/calls" STUB_COUNTS="$work/counts" \
        "$helper" "$@" > "$work/out" 2>&1
    status=$?
    chmod u+w "$work/sources"
    [ "$status" -eq "$expected" ] || fail "exit status $status, expected $expected"
}

calls() { grep -c "$1" "$work/calls"; }

expect_disabled() {
    [ ! -e "$work/sources/microsoft-prod.list" ] && [ -e "$work/sources/microsoft-prod.list.disabled" ] ||
        fail "microsoft-prod.list was not disabled"
    [ -e "$work/sources/redis.list" ] || fail "redis.list was touched"
}

STUB_UPDATE_FAILS=2 STUB_INSTALL_FAILS=0 STUB_MISSING=0
export STUB_UPDATE_FAILS STUB_INSTALL_FAILS STUB_MISSING

run "update succeeds on the third attempt" 0 -y --no-install-recommends pkg-a pkg-b
expect_disabled
[ "$(calls '^-qq update$')" -eq 3 ] || fail "expected 3 update calls"
grep -qx 'install -y --no-install-recommends pkg-a pkg-b' "$work/calls" || fail "install arguments not passed through"
grep -q '403  Forbidden' "$work/out" || fail "apt output not shown"

STUB_UPDATE_FAILS=9
run "update keeps failing" 1 -y pkg-a
[ "$(calls '^-qq update$')" -eq 3 ] || fail "expected 3 update calls"
[ "$(calls '^install')" -eq 0 ] || fail "install ran after a failed update"
grep -q 'apt-get update failed after 3 attempts' "$work/out" || fail "no final error message"

STUB_UPDATE_FAILS=0 STUB_INSTALL_FAILS=1
run "install succeeds on retry, after a fresh update" 0 -y pkg-a
[ "$(calls '^install')" -eq 2 ] || fail "expected 2 install calls"
[ "$(calls '^-qq update$')" -eq 2 ] || fail "expected the retry to refresh the package lists"

STUB_INSTALL_FAILS=9
run "install keeps failing" 1 -y pkg-a
[ "$(calls '^install')" -eq 3 ] || fail "expected 3 install calls"
grep -q 'apt-get install failed after 3 attempts' "$work/out" || fail "no final error message"

STUB_INSTALL_FAILS=0 STUB_MISSING=1
run "unknown package fails without a retry" 1 -y no-such-package
[ "$(calls '^install')" -eq 1 ] || fail "expected 1 install call"
grep -q 'Unable to locate package no-such-package' "$work/out" || fail "apt error not shown"

STUB_MISSING=2
run "package with no installation candidate fails without a retry" 1 -y no-candidate
[ "$(calls '^install')" -eq 1 ] || fail "expected 1 install call"

STUB_MISSING=0 STUB_INSTALL_FAILS=1 STUB_UPDATE_FAILS_FROM=1
export STUB_UPDATE_FAILS_FROM
run "install retry stops when the refresh keeps failing" 1 -y pkg-a
[ "$(calls '^install')" -eq 1 ] || fail "install retried after a failed refresh"
[ "$(calls '^-qq update$')" -eq 4 ] || fail "expected 1 update, then 3 refresh attempts"
grep -q 'apt-get update failed after 3 attempts' "$work/out" || fail "no final error message"
unset STUB_UPDATE_FAILS_FROM
STUB_INSTALL_FAILS=0

WITH_DEB822=1
run "a deb822 microsoft-prod.sources is disabled too" 0 -y pkg-a
expect_disabled
[ ! -e "$work/sources/microsoft-prod.sources" ] && [ -e "$work/sources/microsoft-prod.sources.disabled" ] ||
    fail "microsoft-prod.sources was not disabled"
WITH_DEB822=0

# Root ignores the directory's permissions, so this case needs a normal user.
if [ "$(id -u)" -ne 0 ]; then
    READONLY_SOURCES=1
    run "a source that cannot be disabled fails before apt-get runs" 1 -y pkg-a
    [ ! -s "$work/calls" ] || fail "apt-get ran after the source could not be disabled"
    READONLY_SOURCES=0
fi

run "--no-update skips the update" 0 --no-update -y pkg-a
expect_disabled
[ "$(calls 'update')" -eq 0 ] || fail "update ran with --no-update"

run "no packages is a usage error" 2

if [ "$failures" -ne 0 ]; then
    echo "$failures check(s) failed" >&2
    exit 1
fi
echo "apt_install.sh self-test passed"
