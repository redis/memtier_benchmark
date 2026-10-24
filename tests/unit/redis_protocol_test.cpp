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
#include <limits.h>
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

// Feed one reply to a fresh parser and return parse_response()'s result:
// 1 complete, 0 waiting for more bytes, -1 protocol error.
static int parse_reply(PROTOCOL_TYPE mode, const char *wire, bool track_misses = false)
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
    protocol->set_track_elem_misses(track_misses);
    evbuffer_add(input, wire, strlen(wire));
    int rc = protocol->parse_response();
    delete protocol;
    evbuffer_free(input);
    evbuffer_free(output);
    return rc;
}

static void check_length_limits()
{
    // Bulk lengths whose payload plus CRLF (len + 2) cannot be represented
    // are protocol errors; strtol clamps the out-of-range one to LONG_MAX.
    // long is 64-bit on LP64 and 32-bit on Windows; build the boundaries from LONG_MAX.
    char wire[64];
    snprintf(wire, sizeof(wire), "$%ld\r\n", LONG_MAX);
    check(parse_reply(PROTOCOL_RESP2, wire) == -1, "reject LONG_MAX bulk length");
    snprintf(wire, sizeof(wire), "$%ld\r\n", LONG_MAX - 1);
    check(parse_reply(PROTOCOL_RESP2, wire) == -1, "reject LONG_MAX - 1 bulk length");
    snprintf(wire, sizeof(wire), "$%ld\r\n", LONG_MAX - 2);
    check(parse_reply(PROTOCOL_RESP2, wire) == 0, "LONG_MAX - 2 bulk length waits for data");
    snprintf(wire, sizeof(wire), "!%ld\r\n", LONG_MAX);
    check(parse_reply(PROTOCOL_RESP3, wire) == -1, "reject LONG_MAX blob error length");
    snprintf(wire, sizeof(wire), "=%ld\r\n", LONG_MAX);
    check(parse_reply(PROTOCOL_RESP3, wire) == -1, "reject LONG_MAX verbatim length");
    snprintf(wire, sizeof(wire), "*1\r\n$%ld\r\n", LONG_MAX);
    check(parse_reply(PROTOCOL_RESP2, wire) == -1, "reject LONG_MAX nested bulk length");
    check(parse_reply(PROTOCOL_RESP2, "$99999999999999999999\r\n") == -1, "reject out-of-range bulk length");
    check(parse_reply(PROTOCOL_RESP2, "$3\r\nfoo\r\n") == 1, "regular bulk completes");
    check(parse_reply(PROTOCOL_RESP2, "$0\r\n\r\n") == 1, "empty bulk completes");
    check(parse_reply(PROTOCOL_RESP2, "$-1\r\n") == 1, "null bulk completes");

    // Aggregate counts are tracked as int; reject what cannot be represented.
    check(parse_reply(PROTOCOL_RESP2, "*2147483647\r\n") == 0, "INT_MAX array count waits for elements");
    check(parse_reply(PROTOCOL_RESP2, "*2147483648\r\n") == -1, "reject array count above INT_MAX");
    check(parse_reply(PROTOCOL_RESP2, "*99999999999999999999\r\n") == -1, "reject out-of-range array count");
    check(parse_reply(PROTOCOL_RESP2, "*-1\r\n") == 1, "null array completes");
    check(parse_reply(PROTOCOL_RESP3, "%1073741823\r\n") == 0, "largest representable map count waits");
    check(parse_reply(PROTOCOL_RESP3, "%1073741824\r\n") == -1, "reject map count whose doubling overflows");
    check(parse_reply(PROTOCOL_RESP3, "|1073741824\r\n") == -1, "reject attribute count whose doubling overflows");
    check(parse_reply(PROTOCOL_RESP3, "%1\r\n+k\r\n+v\r\n") == 1, "regular map completes");
    check(parse_reply(PROTOCOL_RESP2, "*2\r\n*1\r\n+a\r\n$1\r\nb\r\n", true) == 1, "nested array completes");

    // Nested counts add to the outstanding elements; with miss tracking the
    // nested bookkeeping is int arithmetic too.
    check(parse_reply(PROTOCOL_RESP2, "*2\r\n*2147483646\r\n*1\r\n", true) == 0,
          "nested counts up to INT_MAX outstanding wait");
    check(parse_reply(PROTOCOL_RESP2, "*2\r\n*2147483646\r\n*3\r\n", true) == -1,
          "reject nested counts above INT_MAX outstanding");
}

int main()
{
    check_readonly(PROTOCOL_REDIS_DEFAULT);
    check_readonly(PROTOCOL_RESP2);
    check_readonly(PROTOCOL_RESP3);
    check_length_limits();
    return failures ? 1 : 0;
}
