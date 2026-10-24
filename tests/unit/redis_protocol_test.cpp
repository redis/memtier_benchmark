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

int main()
{
    check_readonly(PROTOCOL_REDIS_DEFAULT);
    check_readonly(PROTOCOL_RESP2);
    check_readonly(PROTOCOL_RESP3);
    return failures ? 1 : 0;
}
