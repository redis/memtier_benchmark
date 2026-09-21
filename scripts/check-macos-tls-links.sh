#!/usr/bin/env bash
# Reject foreign SSL/libevent installations, including another OpenSSL 3.x copy.
set -euo pipefail

ssl_prefix="${1:?usage: check-macos-tls-links.sh OPENSSL_PREFIX [LIBEVENT_PREFIX]}"
event_prefix="${2:-${RUNNER_TEMP:?}/libevent-memtier}/lib/"
binary_links="$(otool -L ./memtier_benchmark)"
printf '%s\n' "$binary_links"
event_libraries="$(printf '%s\n' "$binary_links" | awk 'NR > 1 && $1 ~ /\/libevent[^\/]*\.dylib$/ { print $1 }')"
test -n "$event_libraries"
printf '%s\n' ./memtier_benchmark "$event_libraries" |
  while IFS= read -r library; do
    otool -L "$library" | awk -v event_prefix="$event_prefix" -v ssl_prefix="$ssl_prefix/lib/" '
      NR > 1 && $1 ~ /\/libevent[^\/]*\.dylib$/ && index($1, event_prefix) != 1 {
        print "Unexpected libevent dependency: " $1 > "/dev/stderr"; bad = 1
      }
      NR > 1 && $1 ~ /\/lib(ssl|crypto)[^\/]*\.dylib$/ && index($1, ssl_prefix) != 1 {
        print "Unexpected OpenSSL dependency: " $1 > "/dev/stderr"; bad = 1
      }
      END { exit bad }
    ' || exit 1
  done
