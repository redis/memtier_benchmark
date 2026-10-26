#!/bin/sh
# Install the Python test dependencies, retrying a few times: RLTest is
# pinned to a git commit, and a transient clone failure would otherwise fail
# the job. pip's own --retries covers only HTTP downloads, not git.
attempts=3
i=1
while :; do
    pip install -r "$(dirname "$0")/../tests/test_requirements.txt" && exit 0
    [ "$i" -ge "$attempts" ] && break
    echo "pip install attempt $i/$attempts failed; retrying in $((i * 15)) s" >&2
    sleep $((i * 15))
    i=$((i + 1))
done
echo "pip install failed after $attempts attempts" >&2
exit 1
