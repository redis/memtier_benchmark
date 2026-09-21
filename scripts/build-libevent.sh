#!/usr/bin/env bash
# Install a pinned libevent in a private prefix; never replace the system library.
set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo 'Usage: [LIBEVENT_VERSION=2.2.2-alpha] build-libevent.sh PREFIX [CMAKE_OPTIONS...]' >&2
    exit 2
fi
prefix="$1"
shift
version="${LIBEVENT_VERSION:-2.1.13-stable}"
case "$version" in
    2.1.13-stable) checksum=f7e9383b8c0baa81b687e5b5eecc01beefaf1b19b64151d95ed61647fe7a315c ;;
    2.2.2-alpha) checksum=4ab1b369bcb5af0c5971b8ade4e95a2c1326f6d0dc1ba75d620bf0331c3184a8 ;;
    *) echo "Unsupported libevent version: $version" >&2; exit 2 ;;
esac
mkdir -p "$prefix"
prefix="$(cd "$prefix" && pwd)"
if [[ -n "$(ls -A "$prefix")" ]]; then
    echo "Use an empty install prefix to avoid mixing libevent versions: $prefix" >&2
    exit 2
fi
build_root="$(mktemp -d "${TMPDIR:-/tmp}/memtier-libevent.XXXXXXXX")"
trap 'rm -rf "$build_root"' EXIT
archive="$build_root/libevent-$version.tar.gz"
curl -fsSL --retry 3 -o "$archive" \
    "https://github.com/libevent/libevent/releases/download/release-$version/libevent-$version.tar.gz"
printf '%s  %s\n' "$checksum" "$archive" | shasum -a 256 -c -
tar -xzf "$archive" -C "$build_root"
cmake -S "$build_root/libevent-$version" -B "$build_root/build" \
    -DCMAKE_BUILD_TYPE=Release \
    -DEVENT__LIBRARY_TYPE=SHARED \
    -DEVENT__DISABLE_OPENSSL=OFF \
    -DEVENT__DISABLE_MBEDTLS=ON \
    -DEVENT__DISABLE_TESTS=ON \
    -DEVENT__DISABLE_SAMPLES=ON \
    -DEVENT__DISABLE_BENCHMARK=ON \
    "$@" \
    -DCMAKE_INSTALL_PREFIX="$prefix" \
    -DCMAKE_INSTALL_LIBDIR=lib
cmake --build "$build_root/build" --parallel "${JOBS:-2}"
cmake --install "$build_root/build"
printf '\nInstalled libevent %s in %s\n' "$version" "$prefix"
