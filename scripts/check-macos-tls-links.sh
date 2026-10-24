#!/usr/bin/env bash
# Reject foreign SSL/libevent installations, including another OpenSSL 3.x copy,
# and a libevent_openssl linked against a different OpenSSL than memtier (#674).
set -euo pipefail

ssl_prefix="${1:?usage: check-macos-tls-links.sh OPENSSL_PREFIX [LIBEVENT_PREFIX]}"
event_prefix="${2:-${RUNNER_TEMP:?}/libevent-memtier}/lib/"
binary_links="$(otool -L ./memtier_benchmark)"
event_libraries="$(printf '%s\n' "$binary_links" | awk 'NR > 1 && $1 ~ /\/libevent[^\/]*\.dylib$/ { print $1 }')"
if ! printf '%s\n' "$event_libraries" | grep -q '/libevent_openssl[^/]*\.dylib$'; then
  printf '%s\n' "$binary_links"
  echo "memtier_benchmark does not link libevent_openssl" >&2
  exit 1
fi
printf '%s\n' ./memtier_benchmark "$event_libraries" |
  while IFS= read -r library; do
    links="$(otool -L "$library")"
    printf '%s\n' "$links"
    printf '%s\n' "$links" | awk -v library="$library" -v event_prefix="$event_prefix" -v ssl_prefix="$ssl_prefix/lib/" '
      NR > 1 && $1 ~ /\/libevent[^\/]*\.dylib$/ && index($1, event_prefix) != 1 {
        print library ": unexpected libevent dependency " $1 " (expected under " event_prefix ")" > "/dev/stderr"; bad = 1
      }
      NR > 1 && $1 ~ /\/lib(ssl|crypto)[^\/]*\.dylib$/ {
        if (index($1, ssl_prefix) != 1) {
          print library ": unexpected OpenSSL dependency " $1 " (expected under " ssl_prefix ")" > "/dev/stderr"; bad = 1
        } else if ($1 ~ /\/libssl[^\/]*\.dylib$/) {
          has_ssl = 1
        }
      }
      END {
        if ((library == "./memtier_benchmark" || library ~ /\/libevent_openssl[^\/]*\.dylib$/) && !has_ssl) {
          print library ": does not link libssl from " ssl_prefix > "/dev/stderr"; bad = 1
        }
        exit bad
      }
    ' || exit 1
  done
