#!/usr/bin/env bash
# Build a pinned libevent into a private prefix; never replace the system library.
#
#   [LIBEVENT_VERSION=2.1.13-stable] [LIBEVENT_FLAVOUR=release|sanitize] \
#       build-libevent.sh PREFIX [CMAKE_OPTIONS...]
#   LIBEVENT_VERSION=... build-libevent.sh --source-id
#
# Supported versions (each pinned to a release asset sha256, or a commit):
#   2.1.12-stable  2.1.13-stable  2.2.2-alpha  master (canary, pinned commit)
# LIBEVENT_FLAVOUR=sanitize instruments libevent with ASan+UBSan so that
# invalid accesses inside libevent itself are reported, not just memtier's.
# --source-id prints "<version>-<sha256-or-commit>" for cache keys.
# Needs cmake (>= 3.15 for 2.2), curl, tar, sha256sum or shasum, and git for master.
set -euo pipefail

usage() {
    sed -n '4,6p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
    exit 2
}

version="${LIBEVENT_VERSION:-2.1.13-stable}"
flavour="${LIBEVENT_FLAVOUR:-release}"
# Upstream libevent master; bump deliberately (the canary tracks this pin).
master_commit=d82464a277d0f42703702c4dfd9af6af38595a83
case "$version" in
    2.1.12-stable) source_id=92e6de1be9ec176428fd2367677e61ceffc2ee1cb119035037a27d346b0403bb ;;
    2.1.13-stable) source_id=f7e9383b8c0baa81b687e5b5eecc01beefaf1b19b64151d95ed61647fe7a315c ;;
    2.2.2-alpha) source_id=4ab1b369bcb5af0c5971b8ade4e95a2c1326f6d0dc1ba75d620bf0331c3184a8 ;;
    master) source_id=$master_commit ;;
    *) echo "Unsupported libevent version: $version (2.1.12-stable, 2.1.13-stable, 2.2.2-alpha, master)" >&2; exit 2 ;;
esac
case "$flavour" in
    release) sanitize_flags= ;;
    sanitize) sanitize_flags='-fsanitize=address,undefined -fno-omit-frame-pointer -g' ;;
    *) echo "Unsupported LIBEVENT_FLAVOUR: $flavour (release, sanitize)" >&2; exit 2 ;;
esac

[[ $# -ge 1 ]] || usage
if [[ "$1" == --source-id ]]; then
    printf '%s-%s\n' "$version" "$source_id"
    exit 0
fi
prefix="$1"
shift

# Refuse to install over anything: mixing two libevent versions in one prefix
# leaves stale libraries and pkg-config files that memtier may pick up.
if [[ -e "$prefix" || -L "$prefix" ]]; then
    if [[ ! -d "$prefix" ]]; then
        echo "Install prefix exists and is not a directory: $prefix" >&2
        exit 2
    fi
    if [[ -n "$(ls -A "$prefix")" ]]; then
        echo "Use an empty install prefix to avoid mixing libevent versions: $prefix" >&2
        exit 2
    fi
fi
mkdir -p "$prefix"
prefix="$(cd "$prefix" && pwd)"

build_root="$(mktemp -d "${TMPDIR:-/tmp}/memtier-libevent.XXXXXXXX")"
trap 'rm -rf "$build_root"' EXIT
source_dir="$build_root/src"
if [[ "$version" == master ]]; then
    # No release asset exists for master: fetch exactly the pinned commit.
    git init -q "$source_dir"
    git -C "$source_dir" fetch -q --depth 1 https://github.com/libevent/libevent.git "$master_commit"
    git -C "$source_dir" checkout -q FETCH_HEAD
    actual="$(git -C "$source_dir" rev-parse HEAD)"
    if [[ "$actual" != "$master_commit" ]]; then
        echo "libevent master checkout is $actual, expected $master_commit" >&2
        exit 1
    fi
else
    # Release assets only: GitHub's /archive/ tarballs are regenerated and
    # are not covered by these checksums.
    archive="$build_root/libevent-$version.tar.gz"
    curl -fsSL --retry 3 -o "$archive" \
        "https://github.com/libevent/libevent/releases/download/release-$version/libevent-$version.tar.gz"
    if command -v sha256sum >/dev/null 2>&1; then
        printf '%s  %s\n' "$source_id" "$archive" | sha256sum -c -
    else
        printf '%s  %s\n' "$source_id" "$archive" | shasum -a 256 -c -
    fi
    mkdir "$source_dir"
    tar -xzf "$archive" -C "$source_dir" --strip-components=1
fi

cmake_args=(
    -DCMAKE_BUILD_TYPE=Release
    # libevent 2.1's CMakeLists predates the minimum CMake 4 accepts.
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    -DEVENT__LIBRARY_TYPE=SHARED
    -DEVENT__DISABLE_OPENSSL=OFF
    -DEVENT__DISABLE_MBEDTLS=ON
    -DEVENT__DISABLE_TESTS=ON
    -DEVENT__DISABLE_SAMPLES=ON
    -DEVENT__DISABLE_BENCHMARK=ON
)
if [[ -n "$sanitize_flags" ]]; then
    cmake_args+=("-DCMAKE_C_FLAGS=$sanitize_flags" "-DCMAKE_SHARED_LINKER_FLAGS=-fsanitize=address,undefined")
fi
jobs="${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)}"
cmake -S "$source_dir" -B "$build_root/build" "${cmake_args[@]}" "$@" \
    -DCMAKE_INSTALL_PREFIX="$prefix" -DCMAKE_INSTALL_LIBDIR=lib
cmake --build "$build_root/build" --parallel "$jobs"
cmake --install "$build_root/build"
printf '\nInstalled libevent %s (%s, %s) in %s\n' "$version" "$source_id" "$flavour" "$prefix"
