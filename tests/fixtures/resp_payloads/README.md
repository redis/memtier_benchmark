# RESP response-parser adversarial fixtures

This directory holds **defensive test fixtures** for the memtier_benchmark
client-side RESP response parser. Each `.bin` file is a deterministic byte
sequence designed to exercise one specific branch of the parser that an audit
of `protocol.cpp::parse_response`, `shard_connection::process_response`,
`cluster_client::handle_cluster_slots`, and `client::handle_response` flagged
as unaudited.

Framing: these are not attack tools. They are **regression payloads** that
keep the client honest about the contract "report and exit cleanly, never
crash" when a Redis-compatible endpoint (managed service, gateway proxy,
in-house RESP shim) emits a malformed frame. The same contract has applied
to the input/CLI side of memtier since PR #405; this catches up the
response-parsing side.

The fixtures are consumed by `tests/fixtures/mock_redis_resp_fuzzer.py`, a
small Python mock-redis server that lets the HELLO / AUTH / SELECT /
CLUSTER-SLOTS handshake pass with benign replies and then serves one
adversarial reply per other command in round-robin from this pool. The
RLTest harness `tests/test_resp_response_fuzzer.py` runs memtier against
that mock for ~5 s per fixture and asserts no fatal signal, no
`Aborted` / `Segmentation fault` / `AddressSanitizer` needle in stderr,
and no hang inside a 15 s wall-clock budget.

The `.bin` files are checked in so CI does not depend on Python being
available at fixture-build time. They are deterministic byte sequences;
to regenerate, see the generator script described in PR #409.

## Fixture -> targeted parser branch

| # | Fixture | Bytes | Targeted branch |
|---|---------|-------|-----------------|
| 1 | `bulk_huge_length.bin` | `$1000000000\r\nABCD` then EOF | `rs_read_bulk` length-vs-content desync (`protocol.cpp:642`). Parser must wait for `m_bulk_len + 2` bytes without wrapping the unsigned-long cast. |
| 2 | `bulk_neg_other.bin` | `$-2\r\n` | `m_bulk_len < 0` branch (`protocol.cpp:600`) with a non-`-1` negative length. Per RESP, only `$-1` is legal; the client treats any negative value as null-bulk but should not assert. |
| 3 | `bulk_int_overflow.bin` | `$99999999999999999999\r\n` | `strtol` ERANGE at `protocol.cpp:591`; `errno` is not checked, value clamps to `LONG_MAX`, downstream `+2` cast then needs a `MEMTIER_MAX_BULK_LEN` ceiling. |
| 4 | `mbulk_deep_nest.bin` | `*1\r\n` x10000 + `+OK\r\n` | Nested-array destructor recursion when `m_last_response` tears down its tree; also `m_total_bulks_count` accounting under deep nesting. |
| 5 | `mbulk_count_overflow.bin` | `*4294967295\r\n` | `m_total_bulks_count` is `unsigned int`; reaching `UINT_MAX` and then adding to it would wrap. |
| 6 | `integer_overflow.bin` | `:99999999999999999999\r\n` | RESP integer line, `strtoll` overflow. The line is consumed via `evbuffer_readln`; the question is whether any consumer of the integer value (e.g. as a count) handles ERANGE. |
| 7 | `resp3_verbatim_bad_prefix.bin` | `=15\r\nxxx:hello world\r\n` | RESP3 verbatim string with an invalid 3-char encoding prefix. The colon is at offset 3 but `xxx` is not a known encoding tag (`txt`, `mkd`, ...). |
| 8 | `resp3_map_odd.bin` | `%3\r\n+k\r\n+v\r\n+k\r\n` | RESP3 map count `3` is doubled to `6` expected items, but only `3` follow. Tests the `*=2` desync on odd map counts and the EOF-before-end behavior. |
| 9 | `resp3_push_unsolicited.bin` | `>2\r\n+pubsub\r\n+msg\r\n+OK\r\n` | RESP3 push `>` frames are not classified by `aggregate_type` / `blob_type` / `single_type`; `parse_response` currently returns `-1`. May have already advanced pipeline state, desyncing the per-connection loop. |
| 10 | `unsolicited_reply.bin` | `+OK\r\n+OK\r\n` (no command sent) | `pop_req()` on an empty `m_pipeline` is undefined behavior; this fixture forces the unsolicited-reply path. |
| 11 | `cluster_slots_malformed.bin` | `*1\r\n*1\r\n+notanint\r\n` | `cluster_client::handle_cluster_slots` blind-indexes `mbulks_elements[0..2]` and calls `as_bulk()` / `as_mbulk_size()` which `assert(0)` on type mismatch. A malformed CLUSTER SLOTS reply crashes the cluster client. |
| 12 | `truncated_frame_dribble.bin` | `*3\r\n$3\r\nfoo` then connection FIN | Incomplete frame followed by half-close. The parser should detect short-read and treat the connection as broken, not crash. |

## Pass criterion

For every fixture, memtier_benchmark must:

1. Exit on a non-fatal signal (no `SIGSEGV`, `SIGABRT`, `SIGBUS`, `SIGFPE`,
   `SIGILL`).
2. Not print any of `Aborted`, `Segmentation fault`, `AddressSanitizer`,
   `UndefinedBehaviorSanitizer`, `stack smashing detected`, `assertion
   failed` to stderr.
3. Either exit on its own or be terminated cleanly by the harness within a
   15 s budget per fixture (hangs / spin loops are failures).

A fixture that currently triggers (1) or (2) is a confirmed bug; the
harness records and reports it. The harness itself does not gate on
"clean exit code" because a malformed-protocol error path is allowed to
return non-zero - the contract is only "no crash, no hang."
