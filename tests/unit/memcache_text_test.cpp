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

// memcache text replies, including lines shorter than the keywords the
// parser compares against (an ASan build reports any read past the line).

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

// Parse `wire` as one reply and return parse_response()'s result.
static int parse(const char *wire, unsigned int *hits = NULL)
{
    abstract_protocol *protocol = protocol_factory(PROTOCOL_MEMCACHE_TEXT);
    evbuffer *input = evbuffer_new();
    evbuffer *output = evbuffer_new();
    protocol->set_buffers(input, output);
    evbuffer_add(input, wire, strlen(wire));
    int ret = protocol->parse_response();
    if (hits != NULL && ret == 1) *hits = protocol->get_response()->get_hits();
    delete protocol;
    evbuffer_free(input);
    evbuffer_free(output);
    return ret;
}

int main(void)
{
    unsigned int hits = 99;
    check(parse("END\r\n", &hits) == 1, "a GET miss (END) completes");
    check(hits == 0, "a miss has no hit");
    check(parse("STORED\r\n") == 1, "a SET reply (STORED) completes");
    hits = 99;
    check(parse("VALUE k 0 1\r\nv\r\nEND\r\n", &hits) == 1, "a GET hit completes");
    check(hits == 1, "a hit is counted");
    // A multi-key GET: every VALUE line after the status line is freed (an
    // ASan build reports a leak otherwise), and so is an unknown later line.
    hits = 99;
    check(parse("VALUE a 0 1\r\nx\r\nVALUE b 0 1\r\ny\r\nVALUE c 0 1\r\nz\r\nEND\r\n", &hits) == 1,
          "a multi-key GET completes");
    check(hits == 3, "every hit is counted");
    check(parse("VALUE a 0 1\r\nx\r\nBOGUS\r\n") == -1, "an unknown line after a value is rejected");

    // Lines shorter than "VALUE", "STORED" or "END" are unknown replies.
    const char *short_lines[] = {"\r\n", "E\r\n", "EN\r\n", "VAL\r\n", "STORE\r\n"};
    for (const char *line : short_lines) {
        check(parse(line) == -1, line);
    }

    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("memcache_text_test: all checks passed\n");
    return 0;
}
