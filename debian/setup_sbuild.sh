#!/bin/bash
if [ $# != 2 ]; then
    echo "Please use: setup_build.sh [dist] [arch]"
    exit 1
fi

dist="$1"
arch="$2"

# Mirrors are overridable so the same script works both on GitHub's Azure-hosted
# runners and on a developer box. ports.ubuntu.com (the armhf/arm64 archive) is
# NOT reachable from Azure runner egress, so CI overrides UBUNTU_PORTS_MIRROR with
# the Azure ports mirror; without that override every Ubuntu arm build fails at
# install-deps because libevent-dev:arm64 & friends can't be fetched. Note the
# ports archive lives under /ubuntu-ports (a bare host path 404s).
UBUNTU_ARCHIVE_MIRROR="${UBUNTU_ARCHIVE_MIRROR:-http://archive.ubuntu.com/ubuntu}"
UBUNTU_PORTS_MIRROR="${UBUNTU_PORTS_MIRROR:-http://ports.ubuntu.com/ubuntu-ports}"
DEBIAN_MIRROR="${DEBIAN_MIRROR:-http://deb.debian.org/debian}"

if ubuntu-distro-info --all | grep -Fqx "$dist"; then
    disttype="ubuntu"
else
    disttype="debian"
fi

# Determine base apt repository URL based on type of distribution.
case "$disttype" in
    ubuntu)
        url="$UBUNTU_ARCHIVE_MIRROR"
        ;;
    debian)
        url="$DEBIAN_MIRROR"
        ;;
    *)
        echo "Unknown distribution $disttype"
        exit 1
esac

sbuild-createchroot \
    --arch ${arch} --make-sbuild-tarball=/var/lib/sbuild/${dist}-${arch}.tar.gz \
    ${dist} `mktemp -d` ${url}

# Ubuntu splits amd64/i386 (archive.ubuntu.com) from armhf/arm64 (ports.ubuntu.com)
# across different hosts, so we rewrite /etc/apt/sources.list to point each arch at
# the right mirror; otherwise cross compilation can't resolve the foreign-arch -dev
# packages.
if [ "$disttype" = "ubuntu" ]; then
    cat <<__END__ | schroot -c source:${dist}-${arch}-sbuild -d / -- tee /etc/apt/sources.list
deb [arch=amd64,i386] ${UBUNTU_ARCHIVE_MIRROR} ${dist} main universe
deb [arch=amd64,i386] ${UBUNTU_ARCHIVE_MIRROR} ${dist}-updates main universe
deb [arch=armhf,arm64] ${UBUNTU_PORTS_MIRROR} ${dist} main universe
deb [arch=armhf,arm64] ${UBUNTU_PORTS_MIRROR} ${dist}-updates main universe
__END__
fi
