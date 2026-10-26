memtier_benchmark
=================
![GitHub](https://img.shields.io/github/license/redis/memtier_benchmark)
[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/redis/memtier_benchmark)](https://github.com/redis/memtier_benchmark/releases)
[![codecov](https://codecov.io/gh/redis/memtier_benchmark/branch/master/graph/badge.svg)](https://codecov.io/gh/redis/memtier_benchmark)


memtier_benchmark is a command line utility developed by [Redis](https://redis.io) (formerly Garantia Data Ltd.) for load generation and benchmarking NoSQL key-value databases. It offers the following:

* Support for both Redis and Memcache protocols (Memcache: text, binary, and meta; meta requires Memcached 1.6.10+)
* Multi-threaded multi-client execution
* Multiple configuration options, including:
    * Read:Write ratio
    * Random and sequential key name pattern policies
    * Random or ranged key expiration
    * Redis cluster
    * TLS support
    * ...and much more

Read more at:

* [A High Throughput Benchmarking Tool for Redis and Memcached](https://redis.io/blog/memtier_benchmark-a-high-throughput-benchmarking-tool-for-redis-memcached)
* [Pseudo-Random Data, Gaussian Access Pattern and Range Manipulation](https://redis.io/blog/new-in-memtier_benchmark-pseudo-random-data-gaussian-access-pattern-and-range-manipulation/)

## Getting Started

### Installing on Debian and Ubuntu

Pre-compiled binaries are available from the packages.redis.io Redis APT repository for
Debian 11 (bullseye), 12 (bookworm), and 13 (trixie), and for Ubuntu 20.04 (focal),
22.04 (jammy), 24.04 (noble), and 26.04 (resolute). To configure this repository, use the
following steps:

```
sudo apt install lsb-release curl gpg

curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list

sudo apt-get update
```

Once configured, to install memtier_benchmark use:

```
sudo apt-get install memtier-benchmark
```

### Installing on RHEL, Rocky Linux, and AlmaLinux

Pre-compiled binaries are available for RHEL 8/9/10 compatible distributions
from the packages.redis.io YUM repository. To configure this repository, use the following steps:

```
sudo curl -fsSL https://packages.redis.io/gpg -o /etc/pki/rpm-gpg/redis-archive-keyring.gpg

sudo tee /etc/yum.repos.d/redis.repo <<'EOF'
[redis]
name=Redis
baseurl=https://packages.redis.io/rpm/el$releasever/$basearch
enabled=1
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/redis-archive-keyring.gpg
EOF
```

Once configured, to install memtier_benchmark use:

```
sudo dnf install -y memtier-benchmark
```

### Installing on Amazon Linux 2023

Amazon Linux 2023 uses a separate repository path because `$releasever` on AL2023 is `2023`,
not an `el<n>` value:

```
sudo curl -fsSL https://packages.redis.io/gpg -o /etc/pki/rpm-gpg/redis-archive-keyring.gpg

sudo tee /etc/yum.repos.d/redis.repo <<'EOF'
[redis]
name=Redis
baseurl=https://packages.redis.io/rpm/amzn2023/$basearch
enabled=1
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/redis-archive-keyring.gpg
EOF
```

Once configured, to install memtier_benchmark use:

```
sudo dnf install -y memtier-benchmark
```

### Installing on MacOS

To install memtier_benchmark on MacOS, use Homebrew:

```
brew install memtier_benchmark
```

### Installing from source

For detailed instructions on building from source, running tests, and using sanitizers for development, see [DEVELOPMENT.md](DEVELOPMENT.md). To build natively on Windows with MSYS2 / MinGW-w64, see [Windows (MSYS2 / MinGW-w64)](DEVELOPMENT.md#windows-msys2--mingw-w64) in that document.

## Using Docker

Use available images on Docker Hub:

```
# latest stable release
$ docker run --rm redislabs/memtier_benchmark:latest --help

# master branch edge build
$ docker run --rm redislabs/memtier_benchmark:edge --help
```

Or, build locally:

```
$ docker build -t memtier_benchmark .
$ docker run --rm memtier_benchmark --help
```

### Using Docker Compose
```
$ docker-compose -f docker-compose.memcached.yml up --build
```
or
```
$ docker-compose -f docker-compose.redis.yml up --build
```

## Using memtier_benchmark

See the included manpage or run:

```
$ memtier_benchmark --help
```

for command line options.

Numeric options that take a count, size, timeout or port (for example `-n`, `-c`, `-t`, `--test-time`, `--pipeline`, `-d`, `--max-reconnect-attempts`, `--prometheus-port`) reject negative, empty, out-of-range and malformed values instead of silently wrapping them. Leading whitespace and a `+` are still accepted. Note that an empty value such as `--max-reconnect-attempts=` used to mean 0 (unlimited) and is now an error. The same applies to `--max-retries`, which takes -1 (unlimited) up to 2147483647: an empty `--max-retries=` used to mean 0 (retries disabled) and is now an error, and values above 2147483647 are rejected instead of wrapping (4294967295 used to become -1, unlimited). Empty values are also rejected for `--data-offset`, `--select-db`, `--thread-conn-start-min-jitter-micros`, `--thread-conn-start-max-jitter-micros`, `--scan-incremental-max-iterations`, `--replica-clients` and `--replicas-per-shard`, where they used to mean 0; pass `0` explicitly. `-0` (and `-00`) still parse as 0 on every option, since `strtoul()` never wrapped them; every other negative value is rejected. `--data-offset` above 536870911 is now rejected when the option is parsed, with "must be <= 536870911", instead of later with "data-offset too long". `--pipeline`, `-d` and `--reconnect-interval` now print "must be a positive integer" for `-1`, empty values and trailing junk (they used to print "must be greater than zero"), and `--rate-limiting` errors now name the option ("rate-limiting must be ..." instead of "rate must be ..."). `--multi-key-get` is now capped at 16777214, the largest value whose preallocated per-client key buffer (256 bytes per key) stays below 4 GiB (above it, keylist's old 32-bit size wrapped). It is a memory sanity limit: each client preallocates about 272 bytes per key, so size memory accordingly.

### Transaction rotations and planned reconnects

With `--transaction`, one weighted rotation of the `--command` entries is the
transaction unit. Keep each WATCH/MULTI/EXEC (or DISCARD) sequence within that
rotation. In standalone mode, `--reconnect-interval=N` waits until at least N
requests have been generated since the last planned reconnect, finishes the
current rotation and drains its replies before reconnecting. For example,
MULTI/SET/EXEC with interval 8 reconnects after 9 commands; the next interval
starts from that actual boundary. Command ratios and WATCH prefixes are included,
and pipelining remains supported. Without `--transaction`, the exact interval
behavior is unchanged. Cluster mode continues to reject `--reconnect-interval`.

Transaction command counts and latencies retain their acknowledgement semantics:
a successful SET inside MULTI records its QUEUED reply, not a later execution.
EXEC reports the transaction outcome. An unplanned disconnect, DISCARD or an
aborted EXEC can therefore leave acknowledged commands counted without executing
them. The lost-MULTI guard still skips the unsent remainder of an interrupted
block. This change prevents planned reconnects from deliberately discarding that
work; it does not convert statistics to executed-command counts. Request/time
limits still stop the run as before, including a request budget that ends inside
a rotation (the separate issue #647).

### Connecting with `--uri`

`--uri=redis://user:password@host:port/dbnum` (or `rediss://` for TLS) sets the server, port, credentials and database in one option, in the format `redis-cli -u` takes. As with `redis-cli -u`, the user name and password are percent-decoded, so characters that have a meaning in the URI can be written as `%40` (`@`), `%3A` (`:`, in the password), `%2F` (`/`), `%3F` (`?`), `%23` (`#`) and `%25` (`%`):

```
$ memtier_benchmark --uri 'redis://default:p%40ss%2Fword@127.0.0.1:6379/0'
```

Earlier releases sent `%XX` to the server literally, so a password that contains a valid `%XX` sequence must now be written with `%25` for its `%`. A `%` that is not followed by two hex digits (for example a password ending in `%`) is still taken literally, with a warning. `%00` is rejected, and so are a `%3A` and control characters in the user name, since `AUTH` credentials are split at the first `:` and the user name is printed by `--show-config`. A raw `/`, `?` or `#` in the credentials still works but prints a note, because it cannot be told apart from an `@` in the path; if the password is not recognised, percent-encode it. The host, port and database number are not decoded: to give an IPv6 zone id, write it with a raw `%` (for example `[fe80::1%eth0]`), since `%25` is not decoded in the host. `--show-config` and `--json-out-file` print the password as `***`.

### Using monitor input files

You can replay real command streams by pointing memtier_benchmark to a monitor log file with the `--monitor-input=/path/to/file` option. Special commands such as `__monitor_line1__` pick a specific entry from the file, while `__monitor_line@__` selects commands at runtime (optionally combined with `--monitor-pattern` and `--command-ratio`). For example, the following command replays the first command from the file on each request:

```
$ memtier_benchmark --monitor-input=monitor.txt --command=__monitor_line1__
```

Repeated `--command "__monitor_line@__"` entries keep their individual command
ratios and share one statistics slot per monitor command type. Even with
`--command-stats-breakdown=line`, these shared type rows and HDR files combine
requests from all random-monitor entries; they are not separate per-placeholder
distributions. Ordinary command entries retain their own statistics slots.

This lets you mix synthetic workloads with realistic captured traffic. Monitor input works in both single endpoint and cluster mode (`--cluster-mode`); in cluster mode each command is sent to the shard that owns its first argument's key. Monitor lines may use the `__key__` and `__data__` placeholders, and text around `__key__` (for example `user:__key__` or `{tag}__key__`) is kept. Each `__key__` argument gets its own generated key, so in cluster mode a `__monitor_line@__` line with more than one `__key__` argument must give them all the same `{hash tag}` (for example `"MSET" "{t}a:__key__" "v" "{t}b:__key__" "v"`), or memtier_benchmark refuses to start; without a tag the keys could land in different slots. The check looks only at `__key__` arguments: a literal key next to a generated one (for example `"MSET" "fixed" "v" "k:__key__" "v"`) is not detected, and Redis answers `CROSSSLOT` unless both share a hash tag. `--failed-keys-file` and `--retry-on-error` use the first argument as the request's key, the same one the command is routed by.

When the number of requested operations exceeds the number of commands in the monitor file, commands are reused:
- **Sequential mode** (`--monitor-pattern=S`): Commands wrap around and replay in order. For example, with 10K commands and 1M operations, the commands replay 100 times (0, 1, 2, ..., 9999, 0, 1, 2, ...).
- **Random mode** (`--monitor-pattern=R`, default): Each operation picks a random command from the file.

This allows you to benchmark with a representative sample of traffic patterns at any scale.

To generate monitor logs, you can use the Redis `MONITOR` command from `redis-cli`, which prints all commands received by the server. For example:

```
$ redis-cli MONITOR
OK
1460100081.165665 [0 127.0.0.1:51706] "set" "shipment:8000736522714:status" "sorting"
1460100083.053365 [0 127.0.0.1:51707] "get" "shipment:8000736522714:status"
```

You can pipe this output and filter specific patterns with tools such as `grep`, then save it to a file and use it as a `--monitor-input` source. For more details, see the official Redis documentation on [monitoring commands executed in Redis](https://redis.io/docs/latest/develop/tools/cli/#monitor-commands-executed-in-redis).

> **What's new in 2.4**
>
> **Binary payloads** — the monitor capture loader preserves binary payloads (including embedded NUL bytes) end-to-end into `monitor_command_list`. **However**, dispatch via `arbitrary_command(const char *)` currently re-truncates at the first NUL — full binary blob support through replay is tracked as #439.
>
> **CR-only line endings** (classic Mac `\r`, and some Windows export tools that emit bare `\r` without a following `\n`) parse correctly; previously the whole file would be read as one giant line and no commands were replayed.

### Command statistics breakdown

By default, when using arbitrary commands (`--command`), statistics are aggregated by command type (e.g., all SET commands are grouped together, all GET commands are grouped together). You can control this behavior with the `--command-stats-breakdown` option:

- `--command-stats-breakdown=command` (default): Aggregate statistics by command name (first word, e.g., SET, GET)
- `--command-stats-breakdown=line`: Show each command line separately

For example, if you run:

```
$ memtier_benchmark --command="SET foo __data__" --command="SET bar __data__" --command="GET foo"
```

(`SET foo __data__` has no `__key__`, so under `--data-size-pattern=S` its value always gets the minimum size: each `__data__` is sized by the most recent `__key__` before it in the command.)

With the default `command` breakdown, you'll see aggregated stats for "Sets" and "Gets". With `--command-stats-breakdown=line`, you'll see separate rows for each command line.

JSON object keys must be unique, so when several command lines share a name their per-line JSON entries are numbered in output-row order (`--command` order for user lines), e.g. `"Sets #1"`, `"Sets #2"`, and `"SET #1"`, `"SET #2"` for the latency histograms and `Per-Key Misses`. The per-command `--hdr-file-prefix` files use a shell-safe `_N` suffix instead (e.g. `<prefix>_GET_1_command_run_1.hgrm`, `<prefix>_GET_2_command_run_1.hgrm`), so they do not overwrite each other. Names that appear only once keep their plain key (`"Gets"`) and file name.

> **Note:** When using `--command-stats-breakdown=command`, the JSON output's Time-Serie percentiles (p50, p99, etc.) for aggregated command types are approximate. They reflect only one of the underlying commands rather than a true merge of all commands of that type. Totals, counts, bytes, and average latency are correctly aggregated.

### Cursor iteration

`--scan-incremental-iteration` follows the cursors returned by `SCAN`, `SSCAN`,
`HSCAN`, and `ZSCAN`, restarting when the cursor returns zero. Use one `--command`,
`--pipeline=1`, and a standalone Redis endpoint. For example:

```sh
memtier_benchmark --command="SSCAN myset 0 COUNT 100" --scan-incremental-iteration --pipeline=1
memtier_benchmark --command="HSCAN __key__ 0 MATCH field:* COUNT 100" --scan-incremental-iteration --pipeline=1
memtier_benchmark --command="ZSCAN myzset 0 COUNT 100" --scan-incremental-iteration --pipeline=1
```

Each client keeps its own cursor and generated arguments (including `__key__`
affixes) throughout a walk. A new walk selects new generated arguments.
For existing `SCAN` workloads, this changes generated `MATCH` arguments:
`SCAN 0 MATCH __data__` now reuses the initial pattern until the walk ends,
instead of regenerating it for each continuation.
Likewise, `SCAN 0 MATCH __key__` advances the key generator once per walk rather
than once per request, so the same request budget can visit fewer generated patterns.
`MATCH`, `COUNT`, and command-specific options such as `HSCAN NOVALUES` are
preserved; option availability depends on the Redis server version.
`--scan-incremental-max-iterations=N` caps the number of continuation requests
per walk (zero means unlimited). Statistics separate initial requests, such as
`HSCAN 0`, from continuations, such as `HSCAN <cursor>`. For compatibility, the
initial statistics label remains `0` even when the command specifies a nonzero
starting cursor; each new walk restarts at that supplied cursor.

### End of a `--test-time` run

When `--test-time` expires, memtier stops sending and waits for the replies to
requests already in flight. It waits at most 10 seconds (a fixed grace period).
If the server has stopped replying, each client still waiting after that is
ended: its unanswered requests are counted as errors, written to
`--failed-keys-file` (status `no-reply-after-test-time`) when set, and reported
as `"Unanswered requests"` in the JSON `Runtime` section. A warning is printed
to stderr and the run reports the `--test-time` window as usual, with exit
code 0. Commands that legitimately take longer than 10 seconds to reply after
the deadline (for example a slow `--command` Lua script) are reported the same
way.

With `--retry-on-error`, requests that get a retryable error after the
deadline are still retried, so a slot migration or a failover in progress can
complete. A client that is still retrying when the grace period is over (for
example against a server that rejects every write with `-OOM`, with the
default unlimited `--max-retries`) is ended the same way: its requests held
for retry are not resent, are counted as errors and written to
`--failed-keys-file` with status `run-ended-before-resend`, and stderr shows
`warning: N client(s) were still retrying M request(s) 10 seconds after the
test time ended`. They are not part of `"Unanswered requests"`, which only
counts requests that were waiting for a reply.

With a request-count workload (`--requests`), unlimited retries can run
indefinitely if the server keeps rejecting every attempt. A valid workload
error reply counts as connection progress, so `--connection-stage-timeout`
does not bound these retries. Use a finite `--max-retries` or `--test-time`
to bound such a workload; requests still count only on success or exhaustion.

These three replies are not workload errors but connection rejections:
`-ERR Protocol error:`, `-ERR max number of clients` and `-DENIED Redis is
running in protected mode`. Redis closes the connection after sending them. They
are not counted as ops and cannot complete `--requests` or `--test-time`, and
they do not count as connection progress. memtier handles them as a dropped
connection: without `--reconnect-on-error` the thread ends and the run fails
(exit code 1). With it, memtier reconnects (see Reconnecting after connection
errors).

With `--retry-on-error`, JSON `Totals` includes integer `Errors`, `Retry Attempts`
and `Retried Ops` counters, including zeros. `Errors` exposes logical failures
recorded by retry handling, including terminal errors, retry exhaustion and
abandoned replays; it is distinct from `Connection Errors`.
`Retry Attempts` counts retries queued for resend, which may later be canceled,
and `Retried Ops` counts requests queued for retry at least once. These counters
are totals for the run, without per-command attribution. They do not change
request completion or exit-code behavior.
A connection rejection (see above) is not a retried error reply. The request
it arrived for is held for replay on the reconnect, as on any dropped
connection, so it adds to neither `Retry Attempts` nor `Retried Ops`. It counts
in `Connection Errors`, and in `Errors` if it is abandoned, never resent.

### Repeated runs (`--run-count`)

With `--run-count N`, memtier reports the best and the worst run (by
Ops/sec), and an `AGGREGATED AVERAGE RESULTS (N runs)` section with per-run
values:

- The counts (`Count`, `Accumulated Latency`, `Connection Errors`,
  `Unanswered requests`, the retry-enabled `Errors` / `Retry Attempts` /
  `Retried Ops`, and the `Per-Key Misses` counts) are the mean over
  the runs, rounded to the nearest integer, so they keep the integer type of
  the other sections (29659 ops over 3 runs give `"Count": 9886`). A count
  that is not 0 over the series never rounds to 0: 1 connection error over 3
  runs gives 1, not 0, next to its non-zero rate. The `Totals` `Count` and
  `Accumulated Latency` are the sum of the rounded command rows, so `Totals`
  is exactly the sum of its rows; it can differ from the mean of the per-run
  `Totals` by about one unit per row. In the same way, the `Per-Key Misses`
  `Total Hits` / `Total Misses` are the sum of the rounded `key[i]` values.
- Ops/sec, KB/sec, and the default workload's Hits/sec and Misses/sec are
  the mean of the per-run values. The Hits/sec and Misses/sec of `--command`
  rows are time-weighted instead: the hits (or misses) of all runs divided
  by the duration of all runs.
- JSON `Latency` / `Average Latency` weight each completed operation equally.
  Totals, built-in commands and per-line command rows use total accumulated
  latency divided by total operations, before rounding the per-run counts.
  Runs with no samples do not lower this mean. Older versions averaged run
  means for these rows, producing incorrect results for unequal run counts.
  Grouped command rows retain their already sample-weighted merged HDR mean,
  in both individual and aggregate sections. Text averages and percentiles
  also use the merged HDR samples, so histogram bucket precision can produce
  small differences from the means calculated from accumulated latency.
- Percentiles come from the latency histograms of all the runs merged
  together, so the section's `Histogram log format` holds the samples of all
  the runs (N times its `Count`).
- `Runtime` `Total duration` is the mean run duration, `Start time` is the
  start of the first run, and `Finish time` is `Start time` plus
  `Total duration`.
- The `CPU` seconds are the mean over the runs that reported CPU usage (all
  of them on Linux).

The section name always shows `--run-count`, so the JSON key does not
change. If the series is cut short (see below), the means are over the runs
that happened, `"Runs completed"` in the JSON `run information` section.

### Interrupting a run

Ctrl+C (SIGINT) and SIGTERM (sent by `kill`, `timeout`, `docker stop`,
Kubernetes and most CI runners) stop a run gracefully: memtier stops the
threads, prints the results for the time it ran, and writes a complete
`--json-out-file` with `"Interrupted": "true"` in the `Runtime` section. With
`--run-count`, no further run starts; the summary covers the runs that
happened (`"Runs completed"` in the JSON `run information` section).

A second signal sent more than about 1 second after the first ends memtier
immediately, without results, as a way out of a stuck shutdown. Signals
within that second count as one: Ctrl+C under `timeout`, or a kill of the
whole process group, delivers the same signal twice.

Exit status:

- after a graceful interrupt (first SIGINT or SIGTERM): **0**. Check
  `"Interrupted": "true"` in the JSON to tell an interrupted run from a
  complete one. If the results cannot be written, the exit status is 1
  instead (see below). If an earlier `--run-count` run was aborted by
  `--connection-stage-timeout`, it is 2.
- after a second signal: memtier dies by that signal (128 + signal number in
  a shell: 130 for SIGINT, 143 for SIGTERM), and the JSON file is
  incomplete.
- with `--run-count`, the exit status covers every run, not only the last
  one: **2** if any run was aborted by `--connection-stage-timeout`,
  otherwise **1** if any run failed or the results could not be written,
  otherwise **0**. An aborted run still counts in `AGGREGATED AVERAGE
  RESULTS`, which is then marked `"Interrupted": "true"`. BEST and WORST
  rank only complete runs when any exist; otherwise they rank the available
  partial runs, retaining their `"Interrupted": "true"` flag. A run with a
  failed worker is also marked `"Interrupted": "true"`, including failures
  detected after a timed deadline. Ordinary command error replies do not
  mark an otherwise completed run interrupted. Per-run output
  and the aggregate average still include all runs that happened.

### Output write errors

If the results cannot be written to `--out-file`, `--json-out-file` or
stdout (for example on a full disk, an exceeded quota or an I/O error),
memtier prints `error: failed writing --out-file PATH:
REASON` (or `--json-out-file PATH`, or `results to stdout`) and exits with
status **1**, also after a graceful interrupt. A connection-stage abort still
exits with **2** when an output write fails too.

One exception: when stdout is a pipe or socket and its reader has gone
away (e.g. `memtier_benchmark ... | head`), the run is not failed, since the
reader chose to stop reading. This also applies to an `--out-file` that is
the same file as stdout, such as `/dev/stdout`. A write that fails while the
reader is still there (for example a full non-blocking pipe) still fails the
run. Any other `--out-file` (for example a FIFO whose reader exits) and
`--json-out-file` always fail the run when a write fails.

The same rule covers the other output files:

- `--failed-keys-file` is opened at startup, after option validation and
  before connecting. If it cannot be opened, or its CSV header cannot be
  written, memtier prints `error: cannot open --failed-keys-file PATH:
  REASON` (or `failed writing`) and exits with status **1** right away, as
  for `--out-file`. If a record cannot be written during the run, memtier
  prints `error: failed writing --failed-keys-file PATH: REASON; logging
  disabled.`, the benchmark goes on without the log, and memtier exits
  with **1** at the end. A `--failed-keys-file` on stdout whose reader
  went away (for example `/dev/stdout` and `| head`) only stops logging.
- The HDR histogram files (`--hdr-file-prefix`) and the `--client-stats`
  CSV files are only created when a run ends. At startup, before
  connecting, memtier checks that the directory of each prefix exists and
  is a writable directory (no file is created, and `--verify-only` skips
  the check), and otherwise prints `error: cannot write --hdr-file-prefix
  directory DIR: REASON` (or `--client-stats`) and exits with **1**. The
  check is best-effort: it uses `access()`, which tests the real user ID,
  so when running as root or on an NFS `root_squash` mount an unwritable
  directory can still only be caught when the run ends. A full disk is
  only seen when the run ends: if one of
  these files cannot be opened or written then, memtier prints `error:
  cannot open --hdr-file-prefix file PATH: REASON` or `error: failed
  writing --client-stats file PATH: REASON`, still writes the other files,
  and exits with **1**. For `--client-stats` only the first failing file of
  a run is named, followed by `error: N more --client-stats files could not
  be written`.

A stdout that was already closed when memtier started is handled separately,
see [Closed stdin, stdout or stderr](#closed-stdin-stdout-or-stderr).

### Fatal errors in JSON results

Once `--json-out-file` has opened, a fatal benchmark, connection-setup,
verification or other output failure adds a top-level `FATAL_ERROR` string.
Configuration and any results already written are retained. The string is a
short failure category; stderr keeps the detailed diagnostic, and the existing
exit status remains authoritative. This is one summary, not a complete list of
failures. Normal finalization prioritizes connection setup, worker, other output,
then verification failures; early process exits use a generic finalization
summary. Successful runs, recovered errors and graceful interrupts do not acquire
a fatal summary unless another failure occurred.

Validation failures before JSON initialization leave an existing JSON file
untouched. A failure writing the JSON file itself, a crash, or a forced process
termination cannot guarantee a complete artifact. JSON lifecycle notices are
printed only with `--debug`.

### Import-file validation

`--data-import` validates the file before connecting, including with
`--generate-keys` and `--verify-only`. A malformed item or a file with no
complete items exits **1**. Keys and values must be nonempty and no larger
than 512 MiB each; the declared key and value lengths must match the columns.
Previously a short key column could import uninitialized trailing bytes; it
now fails validation.

A truncated final item after complete items is ignored with a
`premature end of file` warning, so size-capped dumps can still be used.
The existing warnings for a missing final newline or closing quote remain.
With `--generate-keys`, validation adds one full sequential scan at startup
without storing the imported keys. In one local measurement, this added
about 3.9 seconds for a 488 MiB file containing two million items; the cost
depends on file size and storage speed.

### Data verification (`--data-verify`, `--verify-only`)

With `--data-import=FILE`, `--data-verify` reads imported keys back after the
benchmark and compares their values with the file. `--verify-only` runs only
that verification pass. The existing text and JSON summaries count matching
and failed keys.

If any checked key is missing, has the wrong value, or returns an error,
memtier prints `error: N of M keys failed verification` and exits with status
**1**. A pass that verifies no key (for example, because the server is
unreachable) prints `error: no keys were verified` and also exits with **1**.
A connection lost after some successful replies also exits with **1** if the
verification pass did not complete. Successful verification exits with **0**,
unless another run or output error occurred. A benchmark connection-stage abort
still takes precedence and exits with **2**. Import and verification remain
unsupported in cluster mode.
Imported keys that expire before verification count as missing; use
`--no-expiry` to ignore expiry times in the import file when that is intended.

### Closed stdin, stdout or stderr

If a benchmark run is started with stdin, stdout or stderr closed (for
example `memtier_benchmark ... >&-`), memtier reopens that descriptor on
`/dev/null` before it opens any file. So `--out-file`, `--json-out-file` and
the other files never share a descriptor with stdin, stdout or stderr. Then
two checks run at startup, before connecting:

- Right after option parsing, before any file is opened or read: a path
  option that names a closed descriptor would now silently use
  `/dev/null`. This covers the outputs `--out-file`, `--json-out-file` and
  `--failed-keys-file`, and the inputs `--data-import`, `--monitor-input`,
  `--cert`, `--key` and `--cacert`. The names matched are `/dev/stdout`,
  `/dev/fd/1`, `/proc/self/fd/1` or a symlink to one of them, and likewise
  for `/dev/stdin` and `/dev/stderr`. memtier prints `error: --out-file
  /dev/stdout resolves to stdout, which was closed at startup` and exits
  **1**. The message is not visible when stderr is the closed one.
- After all other option checks and the data import, just before the
  output files are opened: a closed stdout would lose the text results.
  Unless `--out-file` is given, memtier prints `error: stdout was closed at
  startup, so the results would not be written; use --out-file` and exits
  **1**. Since this check is late, a bad option still reports its own
  error. To keep only the JSON, pass `--out-file /dev/null`.

Otherwise:

- A closed stdin or stderr does not by itself fail the run. With stderr
  closed, the log lines are lost.
- Any spelling of `/dev/null` itself is accepted, and so is another
  process's descriptor such as `/proc/1/fd/1`.
- `--hdr-file-prefix` and `--client-stats` are prefixes: memtier always
  appends a suffix (`_FULL_RUN_1.txt`, `-1-0-0.csv`, ...), so they never
  name `/dev/stdout` or `/dev/fd/N` themselves. They are not checked. A
  file they cannot open or write fails the run, see
  [Output write errors](#output-write-errors).

## Crash Reporting

memtier_benchmark includes built-in crash handling that automatically generates detailed bug reports when the program crashes. If you encounter a crash, the tool will print a comprehensive report including:

* Stack traces for all threads
* System and build information
* Active client connection states
* Instructions for generating core dumps

For more information on crash handling, debugging, and how to report crashes, see the [Crash Handling and Debugging](DEVELOPMENT.md#crash-handling-and-debugging) section in DEVELOPMENT.md.

### Cluster mode

#### Connections

When using the cluster-mode option, each client opens one connection for each node.
So, when using a large number of threads and clients, the user must verify that he is not
limited by the maximum number of file descriptors.

#### Using sequential key pattern

When there is an asymmetry between the Redis nodes and user set
the --requests option, there may be gaps in the generated keys.

Also, the ratio and the key generator is per client (and not connection).
In this case, setting the ratio to 1:1 does not guarantee 100% hits because
the keys spread to different connections/nodes.

### Read Preference (cluster mode)

`--read-preference` selects which class of node receives read commands when
running against a Redis Cluster with replicas.  Four modes are available:

* `primary` (default) - reads land on the slot owner (primary). Legacy
  behaviour.
* `secondary` - all reads go to a replica; if no live replica owns the
  slot the `--read-preference-fallback` policy decides what happens.
* `secondaryPreferred` - prefer a replica; on absence of a live replica
  fall back silently to the primary.
* `nearest` - EWMA-based selection. Cold replicas are seeded round-robin
  until warm, then reads are routed to the lowest-EWMA endpoint among
  warm replicas (primary may participate once it has accumulated samples).

Replica setup rejection (AUTH, SELECT or HELLO) has a separate policy from
routing around an unavailable node. Under `primary`, a rejected replica is
closed and left closed because it is not needed. With `--reconnect-on-error`,
`secondaryPreferred` also closes such a replica without redialing it or
failing the run; reads can fall back to the primary. A replica that refuses
the connection (`-ERR max number of clients`, ...) before its first reply is
handled the same way, and with `--retry-on-error` the reads it held for resend
are handed to the primary. Without that flag,
`secondaryPreferred` keeps the existing setup-failure handling. `secondary`
and `nearest` do not use this optional-replica exception: although routing
may allow a primary fallback, an unrecovered setup rejection can still fail
the run. Ordinary connection losses continue to follow the reconnect policy.

When the target node class is unavailable, the fallback is controlled by
`--read-preference-fallback`:

* `error` (default) - return an error.
* `primary` - fall back silently to the primary.
* `queue` - reserved; currently treated as `error`. Request queuing is
  not yet implemented.

A partial `CLUSTER SLOTS` map is accepted. If a generated key has no slot
owner, writes and reads that permit primary routing are sent to an available
primary, preserving the key and letting Redis return its routing response.
`CLUSTERDOWN` replies count as workload errors and follow the usual retry
policy; they also trigger a topology refresh at most once per second per
client, including when a previously full map has become stale. Each client
maintains its own topology: for example, `-t 8 -c 50` permits up to 400 refreshes
per second during sustained `CLUSTERDOWN`, in addition to initial discovery.
Refreshes also occur when retries are disabled. Missing-owner requests use the
first available primary, so this error traffic can concentrate on one node.
The missing-owner warning prints once per process, including across `--run-count` runs.
MGET retains that primary as its producer until the next topology refresh.
If no primary is connected, connecting, or awaiting reconnect/refresh, missing-owner
requests fail the run explicitly. Transactions retain their key while reconnecting.
Monitor replay waits before selecting its next line while all primaries are in
reconnect backoff, including for owned keys. Already-selected monitor lines and
routed keys stay queued across redials to the same primary endpoint, including
repeated setup failures, without changing their generated-request counts. Replica
redials, including a primary demoted to replica, keep the existing policy of
clearing unsent queues and compensating their generated-request counts so other
shards can continue. Promotion also clears previously staged replica work before
changing its role, so strict-secondary reads cannot drain on the new primary.
This does not guarantee request identity across replica redials or promotion.
Strict `secondary` reads with `error` or `queue` fallback fail the run because no
authoritative replica can be chosen, including for a transient hole discovered
by a mid-run refresh.

Built-in read commands (GET, MGET) and arbitrary commands marked with
`--command-is-read` participate in read-preference routing.  Reads on
arbitrary keyless commands (e.g. `DBSIZE`, `CLUSTER NODES`) are routed
to the same node class.

`--read-server=HOST:PORT` lets you declare additional replica endpoints
for standalone (non-cluster) runs. The flag is parsed in all modes, but
read dispatch via the `--read-preference` policy currently fires only in
cluster mode.

`--replica-clients=N` and `--replicas-per-shard=K` are parsed but not yet
wired into the connection topology in this release; see the open
limitations in the PR.

Read-preference also extends the JSON output (`--json-out-file`) with two
new top-level blocks (emitted only when `--cluster-mode` is on AND
`--read-preference != primary`):

* `Read Routing` - `Ops from Primary`, `Ops from Replica`, and
  `Primary Fraction` for the built-in GET/MGET classes. Arbitrary
  per-command totals appear under `Arbitrary Read Routing`.
* `Endpoints` - one entry per distinct shard connection (address + role)
  with `conn_id`, `role`, `Ops`, `Avg Latency (us)`, and
  `Latency Samples`. `conn_id` is the shard_connection vector index
  (NOT a stable cluster shard identity).

### Testing with replicas

The `OSS-CLUSTER + replicas` CI cells start the cluster with RLTest's
`--use-slaves`. The RLTest fork pinned in `tests/test_requirements.txt` adds
each replica to the cluster with `CLUSTER MEET` and `CLUSTER REPLICATE`, so
replicas are cluster members that `CLUSTER SLOTS` reports and
memtier_benchmark can discover. On those cells, the test helper
`get_cluster_replica_connections()` fails the test when `CLUSTER NODES`
lists no replicas or cannot be read (#462). Cells without replicas skip the
replica tests.

### Reconnecting after connection errors

With `--reconnect-on-error`, a lost connection is reconnected with a delay
(`--reconnect-backoff-factor`) instead of ending its thread.
`--max-reconnect-attempts` bounds the consecutive failed attempts; when they
are exhausted, the thread ends and the run fails (exit code 1), whether or not
the `--test-time` deadline has passed. Every failed reconnect counts as a
connection error in the report, over TCP and over a unix socket alike. With
the default of 0 (unlimited), a server that stays down is retried until the
run's stop condition:

- `--test-time`: once the deadline has passed, memtier stops reconnecting and
  ends the run normally with its report. A pending reconnect is due no later
  than the deadline, whatever the backoff delay, and the report's duration and
  throughput cover `--test-time`, not the time spent waiting for a failed
  reconnect.
  - A required connection whose setup step (AUTH, SELECT, HELLO) was rejected
    since its last reply fails the run instead (exit code 1), as when setup
    keeps failing at the deadline. The optional-replica exceptions are
    described under Read Preference above. A connection refused with
    `-ERR Protocol error:`, `-ERR max number of clients` or `-DENIED Redis is
    running in protected mode` before its first reply counts as such a
    rejected setup; after a reply it is a dropped connection.
  - A connection that a live server drops around the deadline is still
    reconnected once, so requests held for replay are resent; if that
    reconnect fails or is dropped again, the run ends. So a peer that accepts
    connections and closes them after a delay can extend the run by up to
    about twice that delay past `--test-time`. In the report's duration, a
    client that sent requests ends at its own deadline (its start plus
    `--test-time`) or at its last reply, whichever is later: replies drained
    after the deadline count, and the idle time after the last one does not
    (a failed reconnect, Ctrl+C or the drain grace still end it at its
    deadline, #880).
    With `--retry-on-error`, a resent request answered after the reconnect
    is such a reply, so the end moves to it. A client whose reconnect setup
    (AUTH, say) stalls past its deadline can still send new requests once
    the setup completes, and their replies extend its end (#879).
  - In cluster mode this holds for the main connection only: another shard's
    connection is reconnected through a slot map refresh. Once the run is
    over, a refresh still redials it; if that dial fails, the connection is
    left closed and the requests it held count as errors.
  - A client that never got a reply before the server went away did no work:
    it stops at the deadline too, but fails the run (exit code 1), even if
    other clients on its thread completed requests. This includes a client
    prevented from starting by repeated TLS-handshake failures. One whose
    first connection only completes after the deadline (e.g. a server killing
    connections before their first reply) is timed from the start of the run,
    so it ends at its first reply instead of running a whole `--test-time`.
  - The deadline is measured from the start of the run, so it does not cover
    a server that is gone before any connection got a reply: that startup
    phase is bounded by `--connection-stage-timeout` (default 30 s), which
    fails the run with exit code 2.
- `--requests`: with unlimited attempts (the default of 0), the run waits for
  the server indefinitely, since the requests can only complete once it is
  back. memtier warns once, after 10 failed attempts, that the server is still
  unreachable; set `--max-reconnect-attempts` to give up instead.

### Using rate-limiting for informed benchmarking

When you impose a rate limit on your benchmark tests, you're essentially mimicking a controlled production environment. This setup is crucial for understanding how latency behaves under certain throughput constraints. Here's why benchmarking latency in a rate-limited scenario is important:


1. **Realistic Performance Metrics**: In real-world scenarios, systems often operate under various limitations. Understanding how these limitations affect latency gives you a more accurate picture of system performance, than simply running benchmarks at full stress level.

1. **Capacity Planning**: By observing latency at different rate limits, you can better plan for scaling your infrastructure. It helps in identifying at what point increased load leads to unacceptable latency, guiding decisions about when to scale up.

1. **Quality of Service (QoS) Guarantees**: For services that require a certain level of performance guarantee, knowing the latency at specific rate limits helps in setting realistic QoS benchmarks.

1. **Identifying Bottlenecks**: Rate-limited benchmarking can help in identifying bottlenecks in your system. If latency increases disproportionately with a small increase in rate limit, it may indicate a bottleneck that needs attention.

1. **Comparative Analysis**: It enables the comparison of different solutions, configurations or hardware in terms of how they handle latency under similar benchmark conditions.


#### Using rate-limiting in memtier

To use this feature, add the `--rate-limiting` parameter followed by the desired RPS per connection.


```
memtier_benchmark [other options] --rate-limiting=<RPS>
```

Note: When using rate-limiting together with cluster-mode option, the rate-limit is associated to the connection for each node.


#### Rate limited example: 100% writes, 1M Keys, 60 seconds benchmark at 10K RPS

```
memtier_benchmark --ratio=1:0 --test-time=60 --rate-limiting=100 -t 2 -c 50 --key-pattern=P:P --key-maximum 1000000
```

### Full latency spectrum analysis

For distributions that are non-normal, such as the latency, many “basic rules” of normally distributed statistics are violated.  Instead of computing just the mean, which tries to express the whole distribution in a single result, we can use a sampling of the distribution at intervals -- percentiles, which tell you how many requests actually would experience that delay. 


When used for normally distributed data, the samples are usually taken at regular intervals. However, since the data does not obey to a normal distribution it would be very expensive to keep equally spaced intervals of latency records while enabling large value ranges. We can apply algorithms that can calculate a good approximation of percentiles at minimal CPU and memory cost, such as [t-digest](https://github.com/tdunning/t-digest) or [HdrHistogram](https://github.com/HdrHistogram/HdrHistogram_c). On memtier_benchmark we’ve decided to use the  HdrHistogram due to its low memory footprint, high precision, zero allocation during the benchmark and constant access time.


By default Memtier will output the 50th, 99th, and 99.9th percentiles. They are the latency thresholds at which 50%, 99%, and 99.9% of commands are faster than that particular presented value. 
To output different percentiles, use `--print-percentiles` followed by a comma-separated list of finite values from 0 through 100 (for example, `--print-percentiles 90,99,99.9,99.99`). Non-finite and out-of-range values are rejected before connecting.

Rows with no latency samples display `---` for the average and percentiles in the
final text table. JSON keeps these latency fields and percentile keys with numeric
zero values; the encoded HDR histogram remains empty.

#### Saving the full latency spectrum
To save the full latencies you should use the --hdr-file-prefix option followed by the prefix name you wish the filenames to have. 
Each distinct command will be saved into two different files - one in .txt (textual format) and another in .hgrm (HistogramLogProcessor format).
The per-command files are named `<prefix>_<command>_command_run_<run>.txt` and `.hgrm`. In `<command>`, spaces, control characters and `< > : " / \ | ? *` are replaced with `_`, so names stay valid on Windows and in CI artifact uploads (e.g. `SCAN <cursor>` from `--scan-incremental-iteration` is written as `<prefix>_SCAN__cursor__command_run_1.hgrm`, and `GET (Line 1)` from `--command-stats-breakdown=line` as `<prefix>_GET_(Line_1)_command_run_1.hgrm`). Names that become identical after this replacement get the same `_N` suffix as duplicate names.
The JSON output also includes a `Compressed Histogram` string under
`Percentile Latencies` / `Histogram log format`, encoded as standard V2 compressed
HDR data in base64. If encoding fails, memtier reports the HDR error on stderr and
writes JSON `null` for that field. Consumers should check for `null` before
decoding; the other latency statistics remain available.
The textual format can be hard to analyze solely, but you can use an [online formatter](http://hdrhistogram.github.io/HdrHistogram/plotFiles.html) to generate visual histograms from it. The .hgrm format will be later added as input to Redislabs [mbdirector](https://github.com/redislabs/mbdirector) to enable visualization of time-domain results.

Sample Visual Feel of the full latency spectrum using an [online formatter](http://hdrhistogram.github.io/HdrHistogram/plotFiles.html):
![alt text][sample_visual_histogram]


[sample_visual_histogram]: ./docs/sample_visual_histogram.png "Sample Full Latency Spectrum Histogram"

### Real-Time Metrics Visualization

memtier_benchmark can stream live metrics to a StatsD server for real-time visualization in Grafana. This is useful for monitoring performance during long-running tests or observing behavior during scaling events.

#### Quick Start

```bash
# Start the included Graphite + Grafana stack
docker-compose -f docker-compose.statsd.yml up -d

# Run a benchmark with metrics streaming
./memtier_benchmark -s <redis-host> --statsd-host=localhost --test-time=60

# Open Grafana at http://localhost:3000 (admin/admin)
```

#### StatsD Options

| Option | Default | Description |
|--------|---------|-------------|
| `--statsd-host=HOST` | *(disabled)* | StatsD server hostname. Enables metrics when set. |
| `--statsd-port=PORT` | `8125` | StatsD server UDP port. |
| `--statsd-prefix=PREFIX` | `memtier` | Prefix for metric names. |
| `--statsd-run-label=LABEL` | `default` | Label to identify/compare benchmark runs. |

For detailed setup instructions, dashboard usage, and troubleshooting, see [Real-Time Metrics Guide](docs/REALTIME_METRICS.md).

#### Prometheus /metrics exporter

memtier_benchmark can also expose live metrics as a Prometheus `/metrics`
endpoint, scrapable by Prometheus or any OpenMetrics-compatible collector. The
exporter is built in by default (it requires libevent 2.1.1 or newer; see
[DEVELOPMENT.md](DEVELOPMENT.md) for the `--disable-prometheus` build flavor).
It is **off at runtime** until you pass `--prometheus-port`:

```bash
# Bind the exporter on a fixed port (loopback by default)
./memtier_benchmark -s <redis-host> --test-time=60 --prometheus-port=8080
# Scrape it:
curl http://127.0.0.1:8080/metrics

# Or let the OS pick an ephemeral port; the bound URL is announced on stdout:
./memtier_benchmark -s <redis-host> --test-time=60 --prometheus-port=0
# -> "Prometheus exporter listening on http://127.0.0.1:43217/metrics"
```

| Option | Default | Description |
|--------|---------|-------------|
| `--prometheus-port=PORT` | *(disabled)* | TCP port for the `/metrics` HTTP endpoint. `0` selects an ephemeral port (announced on stdout). Enables the exporter when set. |
| `--prometheus-bind-addr=ADDR` | `127.0.0.1` | Address to bind the exporter to. Loopback by default; a non-loopback address prints a one-shot warning. |
| `--prometheus-run-label=K=V` | *(none)* | Repeatable label applied to every exported sample, e.g. to compare runs. Invalid UTF-8 in a value is replaced with U+FFFD and a warning is printed. |
| `--prometheus-latency-buckets=L` | *(26 defaults)* | Comma-separated histogram bucket upper bounds in seconds. |

The exporter exposes cumulative counters (ops, bytes, hits/misses, errors,
connection errors, retries), gauges (connections, threads, current/configured
run), and a latency histogram (`memtier_latency_seconds`), plus build info. By
design it echoes **no** connection identity (no server address, password, URI,
or filesystem path) into the scrape body. The end-of-run output remains the
authoritative latency source; the histogram counts every operation but trails
`memtier_ops_total` by about one second of operations (longer while the server
is stalled or under a low `--rate-limiting` rate), and never exceeds it. See the
[Real-Time Metrics Guide](docs/REALTIME_METRICS.md) for the full metric
mapping, threat model, and operational notes.

### Unix socket listen queues

When a Unix socket listener's accept queue is full, memtier retries the pending
connect every 10 ms without blocking the worker event loop. This also applies to
TLS over Unix sockets. The existing `--connection-timeout` and
`--connection-stage-timeout` limits still apply. Setting both limits to zero
imposes no connection-specific time bound. Other connection errors retain the
normal reconnect policy. Once any connection reaches steady state, with
`--reconnect-on-error` and unlimited reconnect attempts, `--test-time` also ends
a backlog wait at the run deadline, as it does for other unsuccessful connects.
Before steady state, the connection and setup-stage limits bound startup. `--debug` logs the start of a backlog wait once
per connect attempt.

### Linux abstract Unix sockets

On Linux, `memtier_benchmark --unix-socket @NAME` connects to an abstract Unix
socket named `NAME`. No socket file or writable filesystem directory is needed.
The name is limited to 107 bytes, excluding `@`; longer names fail instead of
being silently truncated. The terminating NUL is not part of the abstract name.
Use `--unix-socket ./@NAME` for a relative filesystem socket beginning with `@`.
On other platforms, `@NAME` continues to be an ordinary filesystem pathname.
Abstract sockets do not have filesystem permission checks; access control must
be provided by the server (for example, Redis authentication).

### Progress display interval

Use `--progress-interval=5` to print progress every five seconds.
The default is one second; values must be positive whole seconds. This applies
to both the single-line display and `--realtime-latencies`. A final progress
update is printed on normal completion. Connection errors and other
diagnostics are not suppressed.

Only display rendering is throttled. Timeouts, interruption, StatsD/Prometheus
exports and CPU/latency sampling retain their existing cadence. Instantaneous
rates and latencies describe the most recent sample, not the whole display
interval. JSON configuration records the interval as `progress_interval`.

### Sliding-window tail latency

`--realtime-latencies` prints three latency rows per tick: `inst` (each
client's current second), `win<N>s` (every sample completed in the last N
seconds, set by `--latency-window=N`, default 60, 0 disables, max 3600) and
`overall` (the whole run). For example, the last 60 seconds' p99.9999:

```
memtier_benchmark ... --realtime-latencies --latency-window=60 --print-percentiles=50,99,99.9999
[RUN #1  50%  90s] latency [inst    ] n=31.2K    p50  0.111   p99  0.223   p99.9999  0.912*
[RUN #1  50%  90s] latency [win60s  ] n=1.87M    p50  0.119   p99  0.239   p99.9999  1.733
[RUN #1  50%  90s] latency [overall ] n=2.81M    p50  0.119   p99  0.239   p99.9999  2.104  ms
```

`n` is the row's sample count. A percentile p needs at least `100/(100-p)`
samples (100 for p99, 1,000,000 for p99.9999); with fewer, it falls on one of
the few largest samples and is not resolved, so the value is marked `*`. When
`--requests` or `--rate-limiting` bound the number of samples, memtier warns
before the run about percentiles the run or the window can never resolve. For
example, `--rate-limiting=100` on 160 connections gives at most about 976,000
samples per 60 second window (61 seconds of completed client seconds plus the
initial per-connection burst), not enough for p99.9999. The run-level warning
applies to the final results table too, so it is printed with or without
`--realtime-latencies`; it checks the total sample count, and per-command rows
have fewer samples. In cluster mode the limit applies to each endpoint
connection, so the check runs once the first topology is known, after the
threads start. If the run then opens more endpoint connections (for example
when a partial slot map heals), memtier prints the warnings again with the
raised bounds, or a note when a percentile may now be resolved.

The window and overall rows merge the per-second histograms each client has
completed, so every sample is counted exactly once. A client's second is
completed when it records its first sample of the next second, so these rows
normally lag the `inst` row by about a second, and samples of a stalled client
appear only once it completes another request. The window covers the last N
seconds of wall-clock time, even if the progress loop drifts. Each window
second keeps one histogram (about 21 KB).
