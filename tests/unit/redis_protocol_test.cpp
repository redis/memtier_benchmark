/*
 * Copyright (C) 2011-2026 Redis Labs Ltd.
 *
 * This file is part of memtier_benchmark.
 *
 * memtier_benchmark is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2.
 *
 * memtier_benchmark is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with memtier_benchmark.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "protocol.h"

void benchmark_log_file_line(int, const char *, unsigned int, const char *, ...) {}
void benchmark_log(int, const char *, ...) {}
bool is_redis_protocol(enum PROTOCOL_TYPE type)
{
    return type == PROTOCOL_REDIS_DEFAULT || type == PROTOCOL_RESP2 || type == PROTOCOL_RESP3;
}

static int failures = 0;
static void check(bool condition, const char *message)
{
    if (!condition) {
        fprintf(stderr, "FAIL: %s\n", message);
        ++failures;
    }
}

static void check_readonly(PROTOCOL_TYPE mode)
{
    abstract_protocol *protocol = protocol_factory(mode);
    evbuffer *input = evbuffer_new();
    evbuffer *output = evbuffer_new();
    if (!protocol || !input || !output) {
        fprintf(stderr, "FAIL: cannot allocate protocol fixture\n");
        exit(1);
    }
    protocol->set_buffers(input, output);
    check(protocol->configure_protocol(mode) >= 0, "configure requested Redis mode");
    evbuffer_drain(output, evbuffer_get_length(output));

    // Independent wire expectation includes both CRLF bytes and excludes NUL.
    const char expected[] = "*1\r\n$8\r\nREADONLY\r\n";
    for (int count = 1; count <= 2; ++count) {
        // This API returns evbuffer_add's status, not the emitted byte count.
        check(protocol->write_command_readonly() == 0, "READONLY returns success status");
        size_t length = evbuffer_get_length(output);
        check(length == static_cast<size_t>(count) * (sizeof(expected) - 1),
              "READONLY appends exactly one complete frame");
        const unsigned char *wire = evbuffer_pullup(output, -1);
        bool matches = wire && length == static_cast<size_t>(count) * (sizeof(expected) - 1);
        for (int i = 0; matches && i < count; ++i)
            matches = memcmp(wire + i * (sizeof(expected) - 1), expected, sizeof(expected) - 1) == 0;
        check(matches, "READONLY preserves exact bytes and consecutive frame boundaries");
    }
    delete protocol;
    evbuffer_free(input);
    evbuffer_free(output);
}

// Feeds `wire` to a fresh RESP3 keep_value parser and parses two replies,
// returning the parser so the caller can inspect the second reply.
static abstract_protocol *parse_two_resp3_kept_replies(const char *wire, evbuffer **input_out)
{
    abstract_protocol *protocol = protocol_factory(PROTOCOL_RESP3);
    evbuffer *input = evbuffer_new();
    evbuffer *output = evbuffer_new();
    if (!protocol || !input || !output) {
        fprintf(stderr, "FAIL: cannot allocate protocol fixture\n");
        exit(1);
    }
    protocol->set_buffers(input, output);
    protocol->configure_protocol(PROTOCOL_RESP3);
    protocol->set_keep_value(true);
    evbuffer_add(input, wire, strlen(wire));

    check(protocol->parse_response() == 1, "first kept reply with nested push completes");
    check(protocol->parse_response() == 1, "second kept reply completes");
    check(evbuffer_get_length(input) == 0, "both replies consume the whole input");
    evbuffer_free(output);
    *input_out = input;
    return protocol;
}

// Feeds reply1+reply2 to a fresh RESP3 parser and checks that reply 1 ends at
// its own last byte with the expected hits/error, and that reply 2 then parses.
static void check_reply_pair(const char *name, const char *reply1, const char *reply2, unsigned int hits, bool error,
                             bool keep)
{
    abstract_protocol *protocol = protocol_factory(PROTOCOL_RESP3);
    evbuffer *input = evbuffer_new();
    evbuffer *output = evbuffer_new();
    if (!protocol || !input || !output) {
        fprintf(stderr, "FAIL: cannot allocate protocol fixture\n");
        exit(1);
    }
    protocol->set_buffers(input, output);
    protocol->configure_protocol(PROTOCOL_RESP3);
    protocol->set_keep_value(keep);
    evbuffer_add(input, reply1, strlen(reply1));
    evbuffer_add(input, reply2, strlen(reply2));

    bool ok = protocol->parse_response() == 1;
    protocol_response *r = protocol->get_response();
    ok = ok && evbuffer_get_length(input) == strlen(reply2);
    ok = ok && r->get_total_len() == strlen(reply1);
    ok = ok && r->get_hits() == hits && r->is_error() == error;
    ok = ok && protocol->parse_response() == 1 && evbuffer_get_length(input) == 0;
    if (!ok) fprintf(stderr, "FAIL: %s (keep=%d)\n", name, keep);
    check(ok, "reply with nested push ends at its own last byte, next reply parses");
    delete protocol;
    evbuffer_free(input);
    evbuffer_free(output);
}

// Issue #847: a push frame nested in an aggregate (not valid RESP3, which only
// sends pushes at top level) fills one slot of the enclosing aggregate and is
// drained without touching that aggregate's counters, so reply/request
// pairing holds. Its contents are not counted as hits.
static void check_nested_push_framing()
{
    for (int keep = 0; keep <= 1; ++keep) {
        check_reply_pair("*2 +a +b", "*2\r\n+a\r\n+b\r\n", "+OK\r\n", 0, false, keep);
        check_reply_pair("*2 +a >0", "*2\r\n+a\r\n>0\r\n", "$2\r\nhi\r\n", 0, false, keep);
        check_reply_pair("*2 >0 +a", "*2\r\n>0\r\n+a\r\n", "+OK\r\n", 0, false, keep);
        check_reply_pair("*1 *2 >1 +x +a", "*1\r\n*2\r\n>1\r\n+x\r\n+a\r\n", "+OK\r\n", 0, false, keep);
        check_reply_pair("*2 >0 -ERR x", "*2\r\n>0\r\n-ERR x\r\n", "+OK\r\n", 0, true, keep);
        // Push inside a nested push is drained with it; only $2 ab is a hit.
        check_reply_pair("*2 >2 $1 x >0 $2 ab", "*2\r\n>2\r\n$1\r\nx\r\n>0\r\n$2\r\nab\r\n", "+OK\r\n", 1, false, keep);
        // Push as an attribute value: the attribute still prefixes +v.
        check_reply_pair("|1 +k >0 +v", "|1\r\n+k\r\n>0\r\n+v\r\n", "+OK\r\n", 0, false, keep);
    }

    // The kept tree gets a null element for the push slot, so later elements
    // land in the right aggregate: `*2 *2 >1 +x +a +b` keeps [[null, +a], +b]
    // and `*2 *2 +a >0 +b` keeps [[+a, null], +b].
    const char *kept[] = {"*2\r\n*2\r\n>1\r\n+x\r\n+a\r\n+b\r\n", "*2\r\n*2\r\n+a\r\n>0\r\n+b\r\n"};
    abstract_protocol *protocol;
    evbuffer *input;
    evbuffer *output = evbuffer_new();
    for (int null_at = 0; null_at < 2; null_at++) {
        protocol = protocol_factory(PROTOCOL_RESP3);
        input = evbuffer_new();
        protocol->set_buffers(input, output);
        protocol->configure_protocol(PROTOCOL_RESP3);
        protocol->set_keep_value(true);
        evbuffer_add(input, kept[null_at], strlen(kept[null_at]));
        check(protocol->parse_response() == 1, "kept reply with nested push completes");
        mbulk_size_el *root = protocol->get_response()->get_mbulk_value();
        bool shape = root && root->mbulks_elements.size() == 2 && root->mbulks_elements[0]->is_mbulk_size() &&
                     root->mbulks_elements[1]->is_bulk();
        if (shape) {
            mbulk_size_el *inner = root->mbulks_elements[0]->as_mbulk_size();
            int a_at = 1 - null_at;
            shape = inner->mbulks_elements.size() == 2 && inner->mbulks_elements[0]->is_bulk() &&
                    inner->mbulks_elements[1]->is_bulk() && inner->mbulks_elements[null_at]->as_bulk()->is_resp3_null &&
                    strcmp(inner->mbulks_elements[a_at]->as_bulk()->value, "+a") == 0 &&
                    strcmp(root->mbulks_elements[1]->as_bulk()->value, "+b") == 0;
        }
        check(shape, "nested push slot is kept as a null element");
        delete protocol;
        evbuffer_free(input);
    }

    // Alloc-free miss tracking counts the push slot as a miss and keeps the
    // top-level element positions: `*3 *2 >0 +a >0 +b` -> hit, miss, hit.
    protocol = protocol_factory(PROTOCOL_RESP3);
    input = evbuffer_new();
    protocol->set_buffers(input, output);
    protocol->configure_protocol(PROTOCOL_RESP3);
    protocol->set_track_elem_misses(true);
    const char tracked[] = "*3\r\n*2\r\n>0\r\n+a\r\n>0\r\n+b\r\n";
    evbuffer_add(input, tracked, strlen(tracked));
    check(protocol->parse_response() == 1, "tracked reply with nested push completes");
    const std::vector<uint8_t> &hits = protocol->get_response()->get_elem_hits();
    check(hits.size() == 3 && hits[0] == 1 && hits[1] == 0 && hits[2] == 1,
          "nested push slot is tracked as a miss at its own position");
    delete protocol;
    evbuffer_free(input);
    evbuffer_free(output);
}

// Issue #839: a reply that completes with the mbulk cursor still set must not
// let the next reply write through it once rs_initial frees the tree. A nested
// push used to cause that; it now fills one slot (#847), so these replies end
// with the cursor at NULL and check that the next kept reply parses cleanly.
static void check_nested_push_keep_value()
{
    evbuffer *input;
    abstract_protocol *protocol = parse_two_resp3_kept_replies("%1\r\n*2\r\n>0\r\n%0\r\n$2\r\nab\r\n+OK\r\n", &input);
    protocol_response *r = protocol->get_response();
    check(r->get_mbulk_value() == NULL, "status reply after nested push has no mbulk tree");
    check(r->get_status() && strcmp(r->get_status(), "+OK") == 0, "status reply after nested push is +OK");
    delete protocol;
    evbuffer_free(input);

    // Same, with a bulk second reply: it must be kept as the reply value, not
    // appended to the previous reply's freed aggregate.
    protocol = parse_two_resp3_kept_replies("%1\r\n*2\r\n>0\r\n%0\r\n$2\r\nab\r\n$2\r\nhi\r\n", &input);
    r = protocol->get_response();
    unsigned int len = 0;
    const char *value = r->get_value(&len);
    check(r->get_mbulk_value() == NULL, "bulk reply after nested push has no mbulk tree");
    check(value && len == 2 && memcmp(value, "hi", 2) == 0, "bulk reply after nested push keeps its value");
    delete protocol;
    evbuffer_free(input);

    // Same, with the first reply ending on a status rather than a bulk.
    protocol = parse_two_resp3_kept_replies("%1\r\n*2\r\n>0\r\n%0\r\n+s\r\n$2\r\nhi\r\n", &input);
    r = protocol->get_response();
    value = r->get_value(&len);
    check(value && len == 2 && memcmp(value, "hi", 2) == 0,
          "bulk reply after status-ended nested push keeps its value");
    delete protocol;
    evbuffer_free(input);

    // Same, with an aggregate second reply: it must start a new tree.
    protocol = parse_two_resp3_kept_replies("%1\r\n*2\r\n>0\r\n%0\r\n$2\r\nab\r\n*1\r\n$2\r\nhi\r\n", &input);
    r = protocol->get_response();
    mbulk_size_el *root = r->get_mbulk_value();
    check(root && root->upper_level == NULL && root->mbulks_elements.size() == 1,
          "aggregate reply after nested push starts a new tree");
    delete protocol;
    evbuffer_free(input);
}

int main()
{
    check_readonly(PROTOCOL_REDIS_DEFAULT);
    check_readonly(PROTOCOL_RESP2);
    check_readonly(PROTOCOL_RESP3);
    check_nested_push_keep_value();
    check_nested_push_framing();
    return failures ? 1 : 0;
}
