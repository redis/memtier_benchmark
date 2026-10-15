#!/usr/bin/env bash
# Build the CI libevent against the same OpenSSL installation as memtier.
set -euo pipefail

ssl_prefix="${1:?usage: build-macos-libevent.sh OPENSSL_PREFIX}"
build_root="${RUNNER_TEMP:?RUNNER_TEMP must name a private build directory}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIBEVENT_VERSION=2.1.13-stable PKG_CONFIG_PATH="$ssl_prefix/lib/pkgconfig" \
  bash "$script_dir/build-libevent.sh" "$build_root/libevent-memtier" \
    -DOPENSSL_ROOT_DIR="$ssl_prefix"
