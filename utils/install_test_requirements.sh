#!/bin/sh
# Install the Python test dependencies, retrying a few times: RLTest is
# pinned to a git commit, and a transient clone failure would otherwise fail
# the job. pip's own --retries covers only HTTP downloads, not git.
attempts=3
for i in $(seq 1 $attempts); do
    pip install -r "$(dirname "$0")/../tests/test_requirements.txt" && exit 0
    [ "$i" -lt "$attempts" ] && sleep $((i * 15))
done
echo "pip install failed after $attempts attempts" >&2
exit 1
