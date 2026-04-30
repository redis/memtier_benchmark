#!/usr/bin/env bash
# Reproduce the RPM build + smoke test from .github/workflows/release-rpm.yml
# inside a single container. Useful for validating spec/workflow changes
# before pushing to CI.
#
# Usage:
#   scripts/test-rpm-locally.sh                  # default: el9 x86_64
#   scripts/test-rpm-locally.sh el8
#   scripts/test-rpm-locally.sh amzn2023
#   scripts/test-rpm-locally.sh el10 aarch64    # cross-arch (needs qemu-user-static)
#
# Requirements: docker or podman.

set -euo pipefail

DISTRO="${1:-el9}"
ARCH="${2:-x86_64}"

case "$DISTRO" in
  el8)       IMAGE="rockylinux:8" ;;
  el9)       IMAGE="rockylinux:9" ;;
  el10)      IMAGE="rockylinux/rockylinux:10" ;;
  amzn2023)  IMAGE="amazonlinux:2023" ;;
  *)
    echo "Unknown distro: $DISTRO (expected: el8, el9, el10, amzn2023)" >&2
    exit 2
    ;;
esac

if command -v docker >/dev/null 2>&1; then
  ENGINE=docker
elif command -v podman >/dev/null 2>&1; then
  ENGINE=podman
else
  echo "Need docker or podman in PATH" >&2
  exit 2
fi

PLATFORM_ARG=()
if [ "$ARCH" != "x86_64" ]; then
  case "$ARCH" in
    aarch64) PLATFORM_ARG=(--platform linux/arm64) ;;
    *) echo "Unknown arch: $ARCH" >&2; exit 2 ;;
  esac
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo ">>> Engine:   $ENGINE"
echo ">>> Distro:   $DISTRO ($IMAGE)"
echo ">>> Arch:     $ARCH"
echo ">>> Repo:     $REPO_ROOT"
echo

"$ENGINE" run --rm "${PLATFORM_ARG[@]}" \
  -v "$REPO_ROOT:/src:ro" \
  -e DISTRO="$DISTRO" \
  -e ARCH="$ARCH" \
  "$IMAGE" bash -euo pipefail -c '
set -x

# Install build deps (mirrors release-rpm.yml build-rpm step).
if command -v dnf >/dev/null 2>&1; then
  PKG=dnf
else
  PKG=yum
fi
$PKG install -y rpm-build rpmdevtools rsync gcc-c++ make autoconf automake \
  libtool libevent-devel openssl-devel zlib-devel pkgconfig

# Stage a writable copy of the source tree.
cp -r /src /work
cd /work

VERSION=$(awk -F"[(),]" "/AC_INIT/ {gsub(/ /, \"\", \$3); print \$3}" configure.ac)
echo "Building memtier-benchmark version: $VERSION"

rpmdev-setuptree

# Tarball matches what build-srpm produces in CI.
mkdir -p memtier-benchmark-$VERSION
rsync -a --exclude=".git" --exclude="memtier-benchmark-*" . memtier-benchmark-$VERSION/
tar czf ~/rpmbuild/SOURCES/memtier-benchmark-$VERSION.tar.gz memtier-benchmark-$VERSION

sed "s/^Version:.*/Version:        $VERSION/" rpm/memtier-benchmark.spec \
  > ~/rpmbuild/SPECS/memtier-benchmark.spec

# -ba builds both SRPM and binary RPM in one shot.
rpmbuild -ba ~/rpmbuild/SPECS/memtier-benchmark.spec

# Direct-install verification (mirrors smoke-test "Verify direct install").
RPM_FILE=$(find ~/rpmbuild/RPMS -name "*.rpm" -type f ! -name "*debug*" | head -1)
[ -n "$RPM_FILE" ] || { echo "No RPM produced"; exit 1; }
$PKG install -y "$RPM_FILE"

memtier_benchmark --version
memtier_benchmark --help > /dev/null
test -f /usr/share/man/man1/memtier_benchmark.1.gz \
  || test -f /usr/share/man/man1/memtier_benchmark.1
bash -n /usr/share/bash-completion/completions/memtier_benchmark

# Repo-install verification (mirrors smoke-test local-YUM-repo step).
$PKG install -y createrepo_c || $PKG install -y createrepo
$PKG remove -y memtier-benchmark

REPO_ROOT=/tmp/localrepo
mkdir -p "$REPO_ROOT/$DISTRO/$ARCH"
find ~/rpmbuild/RPMS -name "*.rpm" -type f ! -name "*debug*" \
  -exec cp {} "$REPO_ROOT/$DISTRO/$ARCH/" \;
if command -v createrepo_c >/dev/null 2>&1; then
  createrepo_c "$REPO_ROOT/$DISTRO/$ARCH"
else
  createrepo "$REPO_ROOT/$DISTRO/$ARCH"
fi

if [ "$DISTRO" = "amzn2023" ]; then
  BASEURL="file://$REPO_ROOT/amzn2023/\$basearch"
else
  BASEURL="file://$REPO_ROOT/el\$releasever/\$basearch"
fi

cat > /etc/yum.repos.d/local-test.repo <<EOF
[local-test]
name=Local Test
baseurl=$BASEURL
enabled=1
gpgcheck=0
EOF

$PKG install -y memtier-benchmark
memtier_benchmark --version

echo
echo "================================================"
echo "OK: $DISTRO $ARCH built, signed-not-verified, and repo-installed"
echo "================================================"
'
