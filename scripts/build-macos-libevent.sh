#!/usr/bin/env bash
# Build the CI libevent against the same OpenSSL installation as memtier.
set -euo pipefail

ssl_prefix="${1:?usage: build-macos-libevent.sh OPENSSL_PREFIX}"
build_root="${RUNNER_TEMP:?RUNNER_TEMP must name a private build directory}"
mkdir -p "$build_root"
archive="$build_root/libevent-2.1.13-stable.tar.gz"
curl -fsSL --retry 3 -o "$archive" \
  https://github.com/libevent/libevent/releases/download/release-2.1.13-stable/libevent-2.1.13-stable.tar.gz
echo "f7e9383b8c0baa81b687e5b5eecc01beefaf1b19b64151d95ed61647fe7a315c  $archive" | shasum -a 256 -c -
# Start clean: stale objects from a run against another OpenSSL would be
# reinstalled unchanged (#674).
rm -rf "$build_root/libevent-2.1.13-stable" "$build_root/libevent-memtier"
tar -xzf "$archive" -C "$build_root"
cd "$build_root/libevent-2.1.13-stable"
PKG_CONFIG_PATH="$ssl_prefix/lib/pkgconfig" \
  CPPFLAGS="-I$ssl_prefix/include" LDFLAGS="-L$ssl_prefix/lib" \
  ./configure --prefix="$build_root/libevent-memtier" --disable-debug-mode --disable-samples --disable-libevent-regress
make -j3
make install
