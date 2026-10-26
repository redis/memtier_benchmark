#!/usr/bin/env bash
# Prove which libevent a memtier_benchmark build actually runs with.
#
#   check-libevent-linkage.sh --prefix DIR --expect-version VERSION \
#       [--binary ./memtier_benchmark] [--tls yes|no] [--prometheus yes|no]
#
# Fails unless:
#   - `--version` reports libevent=VERSION (the runtime event_get_version()),
#     plus the requested openssl= and prometheus= fields;
#   - pkg-config in PREFIX reports that release (numeric part) for every component;
#   - every libevent library memtier loads, directly or through another
#     libevent library, resolves under PREFIX/lib (Linux: ldd);
#   - with TLS, the process loads exactly one libssl and one libcrypto, so
#     libevent_openssl and memtier share one OpenSSL.
# On macOS the library checks are delegated to check-macos-tls-links.sh.
set -euo pipefail

binary=./memtier_benchmark
prefix=
expected=
tls=yes
prometheus=yes
while [[ $# -gt 0 ]]; do
    case "$1" in
        --binary) binary="$2"; shift 2 ;;
        --prefix) prefix="$2"; shift 2 ;;
        --expect-version) expected="$2"; shift 2 ;;
        --tls) tls="$2"; shift 2 ;;
        --prometheus) prometheus="$2"; shift 2 ;;
        *) sed -n '4,5p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2; exit 2 ;;
    esac
done
if [[ -z "$prefix" || -z "$expected" ]] || [[ "$tls" != yes && "$tls" != no ]] ||
    [[ "$prometheus" != yes && "$prometheus" != no ]]; then
    sed -n '4,5p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
    exit 2
fi
prefix="$(cd "$prefix" && pwd)"
libdir="$prefix/lib"
status=0
fail() {
    echo "check-libevent-linkage: $*" >&2
    status=1
}

# 1. Runtime version and feature flags as memtier reports them.
version_line="$("$binary" --version | head -n 1)"
echo "$version_line"
runtime="$(printf '%s\n' "$version_line" | sed -n 's/.* libevent=\([^ ]*\).*/\1/p')"
[[ "$runtime" == "$expected" ]] || fail "runtime libevent is '$runtime', expected '$expected'"
if [[ "$tls" == yes ]]; then
    [[ "$version_line" == *" openssl="* ]] || fail "TLS build expected, --version has no openssl= field"
else
    [[ "$version_line" != *" openssl="* ]] || fail "--disable-tls build expected, --version has an openssl= field"
fi
[[ "$version_line" == *" prometheus=$prometheus"* ]] || fail "expected prometheus=$prometheus"

# 2. The headers/pkg-config the build used belong to the same release.
components=(libevent libevent_core libevent_extra libevent_pthreads)
[[ "$tls" == yes ]] && components+=(libevent_openssl)
for component in "${components[@]}"; do
    pc_version="$(PKG_CONFIG_PATH="$libdir/pkgconfig" PKG_CONFIG_LIBDIR="$libdir/pkgconfig" \
        pkg-config --modversion "$component" 2>/dev/null || true)"
    # pkg-config carries only the numeric part (2.2.2 for 2.2.2-alpha-dev).
    [[ "$pc_version" == "${runtime%%-*}" ]] ||
        fail "pkg-config $component is '${pc_version:-missing}', runtime is '$runtime'"
done

if [[ "$(uname -s)" == Darwin ]]; then
    if [[ "$tls" == yes ]]; then
        ssl_prefix="${OPENSSL_PREFIX:?set OPENSSL_PREFIX to the OpenSSL memtier was built with}"
        bash "$(dirname "${BASH_SOURCE[0]}")/check-macos-tls-links.sh" "$ssl_prefix" "$prefix" || status=1
    fi
    exit "$status"
fi

# 3. Every libevent object loaded at runtime comes from the private prefix.
resolved() { # ldd output -> "soname path" for libevent/libssl/libcrypto lines
    ldd "$1" | awk '$2 == "=>" && $1 ~ /^lib(event|ssl|crypto)/ { print $1, $3 }'
}
binary_links="$(resolved "$binary")"
printf '%s\n' "$binary_links"
loaded=()
while read -r soname path; do
    [[ "$soname" == libevent* ]] || continue
    loaded+=("$soname")
    [[ "$path" == "$libdir"/* ]] || fail "$soname resolves to '$path', not under $libdir"
done <<<"$binary_links"
direct="$(readelf -d "$binary" | sed -n 's/.*(NEEDED).*\[\(libevent[^]]*\)\].*/\1/p')"
[[ -n "$direct" ]] || fail "$binary has no direct libevent dependency"
required=(libevent_pthreads)
[[ "$tls" == yes ]] && required+=(libevent_openssl)
for component in "${required[@]}"; do
    if ! printf '%s\n' "${loaded[@]}" | grep -q "^${component}-"; then
        fail "$binary does not load $component"
    fi
done
if [[ "$tls" == no ]] && printf '%s\n' "$binary_links" | grep -q '^libssl'; then
    fail "--disable-tls build loads libssl"
fi
for library in "$libdir"/libevent*.so; do
    while read -r soname path; do
        # Without a RUNPATH a library alone may print "not found"; the binary's
        # ldd above (the full transitive set) decides what really loads.
        if [[ "$soname" == libevent* && "$path" == /* && "$path" != "$libdir"/* ]]; then
            fail "$(basename "$library") loads $soname from '$path'"
        fi
    done < <(resolved "$library")
done

# 4. libevent_openssl and memtier must share one OpenSSL. memtier's ldd set
# already contains what libevent_openssl needs, resolved the way the process
# resolves it (a private OpenSSL found through RUNPATH included), so it must
# hold exactly one libssl and one libcrypto; two sonames such as libssl.so.3
# and libssl.so.1.1 mean the two were built against different OpenSSLs.
if [[ "$tls" == yes ]]; then
    for component in libssl libcrypto; do
        found="$(printf '%s\n' "$binary_links" | awk -v c="$component" 'index($1, c ".") == 1 || index($1, c "-") == 1')"
        count="$(printf '%s' "$found" | grep -c . || true)"
        if [[ "$count" -ne 1 ]]; then
            fail "$binary loads $count $component libraries (expected 1): $(printf '%s' "$found" | tr '\n' ' ')"
        fi
    done
fi

if [[ "$status" -eq 0 ]]; then
    echo "libevent $runtime linkage OK (prefix $prefix, tls=$tls, prometheus=$prometheus)"
fi
exit "$status"
