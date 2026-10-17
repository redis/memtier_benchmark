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

int main()
{
    // A new connection must not inherit either a partially read value or
    // accumulated bytes/hits from a reply whose MN never arrived.
    const char *partial[] = {"V", "VA 100\r\nx", "VA 1\r\nx\r\n", "EN\r\n", "HD\r\n"};
    for (int keep = 0; keep < 2; ++keep) {
        for (size_t i = 0; i < sizeof(partial) / sizeof(partial[0]); ++i) {
            abstract_protocol *protocol = protocol_factory(PROTOCOL_MEMCACHE_META);
            evbuffer *input = evbuffer_new();
            evbuffer *output = evbuffer_new();
            protocol->set_buffers(input, output);
            protocol->set_keep_value(keep != 0);
            evbuffer_add(input, partial[i], strlen(partial[i]));
            check(protocol->parse_response() == 0, "partial reply needs more bytes");
            evbuffer_drain(input, evbuffer_get_length(input));
            protocol->reset_state();
            evbuffer_add(input, "EN\r\nMN\r\n", 8);
            check(protocol->parse_response() == 1, "new connection parses a complete miss");
            check(protocol->get_response()->get_hits() == 0, "old hits were discarded");
            check(protocol->get_response()->get_total_len() == 8, "old response bytes were discarded");
            unsigned int length = 99;
            check(protocol->get_response()->get_value(&length) == NULL, "old value was discarded");
            delete protocol;
            evbuffer_free(input);
            evbuffer_free(output);
        }
    }
    return failures ? 1 : 0;
}
