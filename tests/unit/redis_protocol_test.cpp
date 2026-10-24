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

// Issue #839: a RESP3 push frame nested in a kept aggregate drains the
// aggregate's remaining slots, so the reply completes with the mbulk cursor
// still pointing into the tree. The next reply frees that tree in rs_initial
// and must not write through the stale cursor (heap-use-after-free under ASan).
static void check_nested_push_keep_value()
{
    evbuffer *input;
    abstract_protocol *protocol =
        parse_two_resp3_kept_replies("%1\r\n*2\r\n>0\r\n%0\r\n+s\r\n$2\r\nab\r\n+OK\r\n", &input);
    protocol_response *r = protocol->get_response();
    check(r->get_mbulk_value() == NULL, "status reply after nested push has no mbulk tree");
    check(r->get_status() && strcmp(r->get_status(), "+OK") == 0, "status reply after nested push is +OK");
    delete protocol;
    evbuffer_free(input);

    // Same, with a bulk second reply: it must be kept as the reply value, not
    // appended to the previous reply's freed aggregate.
    protocol = parse_two_resp3_kept_replies("%1\r\n*2\r\n>0\r\n%0\r\n+s\r\n$2\r\nab\r\n$2\r\nhi\r\n", &input);
    r = protocol->get_response();
    unsigned int len = 0;
    const char *value = r->get_value(&len);
    check(r->get_mbulk_value() == NULL, "bulk reply after nested push has no mbulk tree");
    check(value && len == 2 && memcmp(value, "hi", 2) == 0, "bulk reply after nested push keeps its value");
    delete protocol;
    evbuffer_free(input);
}

int main()
{
    check_readonly(PROTOCOL_REDIS_DEFAULT);
    check_readonly(PROTOCOL_RESP2);
    check_readonly(PROTOCOL_RESP3);
    check_nested_push_keep_value();
    return failures ? 1 : 0;
}
