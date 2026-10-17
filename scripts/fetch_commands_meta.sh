#!/usr/bin/env bash
# Copyright (C) 2011-2026 Redis Labs Ltd.
#
# fetch_commands_meta.sh REDIS_VERSION TARGET_DIR
#
# Fetches src/commands/*.json from redis/redis@<REDIS_VERSION> into TARGET_DIR.
# Records the resolved commit sha in TARGET_DIR/REDIS_TAG.txt.
#
# REDIS_VERSION may be a branch (e.g. "8.8"), a tag, or a full sha.

set -euo pipefail

REDIS_VERSION="${1:-8.8}"
TARGET_DIR="${2:-deps/commands_json}"
REPO="redis/redis"

echo "[fetch_commands_meta] redis ref: ${REDIS_VERSION}"
echo "[fetch_commands_meta] target:    ${TARGET_DIR}"

mkdir -p "${TARGET_DIR}"

# Resolve ref → commit sha. Try gh first (auth, rate limits), fall back to ls-remote.
SHA=""
if command -v gh >/dev/null 2>&1; then
    SHA="$(gh api "repos/${REPO}/commits/${REDIS_VERSION}" --jq '.sha' 2>/dev/null || true)"
fi
if [ -z "${SHA}" ]; then
    SHA="$(git ls-remote "https://github.com/${REPO}.git" "${REDIS_VERSION}" 2>/dev/null | head -1 | awk '{print $1}')"
fi
if [ -z "${SHA}" ]; then
    echo "[fetch_commands_meta] ERROR: could not resolve '${REDIS_VERSION}' to a sha" >&2
    exit 1
fi
echo "[fetch_commands_meta] resolved sha: ${SHA}"

TMPDIR="$(mktemp -d)"
trap 'rm -rf "${TMPDIR}"' EXIT

TARBALL="${TMPDIR}/redis.tar.gz"
URL="https://github.com/${REPO}/archive/${SHA}.tar.gz"
echo "[fetch_commands_meta] downloading: ${URL}"
curl -sSL --fail "${URL}" -o "${TARBALL}"

EXTRACT_DIR="${TMPDIR}/extract"
mkdir -p "${EXTRACT_DIR}"
# Extract only src/commands. The top-level directory inside the tarball is
# redis-<sha>/, so strip one component and select the sub-path.
tar -xzf "${TARBALL}" -C "${EXTRACT_DIR}" --strip-components=1 "redis-${SHA}/src/commands"

SRC_DIR="${EXTRACT_DIR}/src/commands"
if [ ! -d "${SRC_DIR}" ]; then
    echo "[fetch_commands_meta] ERROR: src/commands not found in tarball" >&2
    exit 1
fi

# Wipe existing JSONs so command renames/removals are reflected. Keep README/non-json files.
find "${TARGET_DIR}" -maxdepth 1 -name '*.json' -type f -delete

# Copy the fresh set.
cp "${SRC_DIR}"/*.json "${TARGET_DIR}/"
COUNT="$(find "${TARGET_DIR}" -maxdepth 1 -name '*.json' -type f | wc -l | tr -d ' ')"

# Record provenance for the codegen step and for PR auditability.
{
    echo "version=${REDIS_VERSION}"
    echo "sha=${SHA}"
    echo "fetched_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "files=${COUNT}"
} > "${TARGET_DIR}/REDIS_TAG.txt"

echo "[fetch_commands_meta] wrote ${COUNT} json files; pinned in ${TARGET_DIR}/REDIS_TAG.txt"
