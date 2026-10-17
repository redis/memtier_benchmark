#!/bin/bash
if [ $# != 1 ]; then
    echo "usage: prepare_release.sh [release]"
    exit 1
fi
RELEASE="$1"

if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
    echo "Please commit or revert all local changes before preparing a release."
    exit 1
fi

set -e

# Update configure.ac version
sed -i 's/^\(AC_INIT(.*\),.*,\(.*)\)$/\1,'$RELEASE',\2/' configure.ac

# Build
autoreconf -ivf
./configure
make
make rebuild-man

# Commit
git add configure.ac memtier_benchmark.1
git commit -m "Release $RELEASE"
