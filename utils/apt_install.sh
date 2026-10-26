#!/bin/sh
# Install Debian/Ubuntu packages for a CI job:
#
#   utils/apt_install.sh [--no-update] [apt-get install options] package...
#
# 1. Disable the GitHub runner's packages.microsoft.com sources, which memtier
#    never installs from: when it answers 403, apt-get update exits 100 and
#    fails the job (#866). Other sources, such as packages.redis.io, stay.
# 2. Run apt-get -qq update (skipped with --no-update), then apt-get install
#    with the given arguments. Each step is tried 3 times, 10 s then 20 s
#    apart; a failed install refreshes the package lists before its retry.
#    An unknown package fails at once, without a retry.
#
# Runs apt-get through sudo unless already root. For tests, APT_GET,
# APT_SUDO, APT_SOURCES_DIR and APT_RETRY_DELAY override the apt-get
# command, the sudo prefix, the sources directory and the base delay.

apt_get=${APT_GET:-apt-get}
sources_dir=${APT_SOURCES_DIR:-/etc/apt/sources.list.d}
delay=${APT_RETRY_DELAY:-10}
attempts=3
if [ "${APT_SUDO+set}" = set ]; then
    sudo=$APT_SUDO
elif [ "$(id -u)" -eq 0 ]; then
    sudo=
else
    sudo=sudo
fi

update=1
if [ "$1" = --no-update ]; then
    update=0
    shift
fi
if [ $# -eq 0 ]; then
    echo "usage: $0 [--no-update] [apt-get install options] package..." >&2
    exit 2
fi

# packages-microsoft-prod.deb installs microsoft-prod.list on the Ubuntu
# 22.04 and 24.04 images. Disable every one-line (.list) or deb822 (.sources)
# file that names packages.microsoft.com, whatever its name, such as one an
# azure-cli install leaves. apt skips *.disabled files without a warning.
for f in "$sources_dir"/*.list "$sources_dir"/*.sources; do
    if [ -f "$f" ] && grep -q 'packages\.microsoft\.com' "$f"; then
        $sudo mv "$f" "$f.disabled" || exit 1
        echo "apt_install: disabled unused source $f" >&2
    fi
done

log=$(mktemp) || exit 1
trap 'rm -f "$log"' EXIT
exec 4>&1

# Run apt-get with the given arguments, streaming its output and keeping a
# copy in $log; return apt-get's exit status.
run_apt() {
    # $sudo and $apt_get may be empty or hold several words.
    # shellcheck disable=SC2086
    status=$({ { $sudo $apt_get "$@" 2>&1; echo $? >&3; } | tee "$log" >&4; } 3>&1)
    return "$status"
}

update_lists() {
    u=1
    while :; do
        run_apt -qq update && return 0
        [ "$u" -ge "$attempts" ] && break
        echo "apt-get update attempt $u/$attempts failed; retrying in $((u * delay)) s" >&2
        sleep $((u * delay))
        u=$((u + 1))
    done
    echo "apt_install: apt-get update failed after $attempts attempts" >&2
    return 1
}

if [ "$update" -eq 1 ]; then
    update_lists || exit 1
fi

i=1
while :; do
    run_apt install "$@" && exit 0
    if grep -Eq '^E: (Unable to locate package|Package .* has no installation candidate)' "$log"; then
        echo "apt_install: apt-get install failed: package not available" >&2
        exit 1
    fi
    [ "$i" -ge "$attempts" ] && break
    echo "apt-get install attempt $i/$attempts failed; retrying in $((i * delay)) s" >&2
    sleep $((i * delay))
    i=$((i + 1))
    update_lists || exit 1
done
echo "apt_install: apt-get install failed after $attempts attempts" >&2
exit 1
