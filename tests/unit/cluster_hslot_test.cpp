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
#include "cluster_client.h"

void benchmark_log_file_line(int, const char *, unsigned int, const char *, ...) {}
void benchmark_log(int, const char *, ...) {}
void report_connection_stage_failure(const char *)
{
    abort();
}
void report_connection_stage_success()
{
    abort();
}
bool connection_stage_steady_state_reached()
{
    abort();
}
bool is_redis_protocol(enum PROTOCOL_TYPE type)
{
    return type == PROTOCOL_REDIS_DEFAULT || type == PROTOCOL_RESP2 || type == PROTOCOL_RESP3;
}

// Expected values are CLUSTER KEYSLOT replies from Redis 8.6.
static const struct
{
    const char *key;
    uint32_t slot;
} cases[] = {
    {"", 0},
    {"123456789", 12739},
    {"memtier-1", 3498},
    {"ask", 11420},
    {"{ask}-1", 11420},
    {"{ask}-memtier-1", 11420},
    {"{user1000}.following", 3443},
    {"{user1000}.followers", 3443},
    {"{}-x", 2326},          // empty tag: whole key
    {"{ask", 12534},         // no closing brace: whole key
    {"foo{}{bar}", 8363},    // first tag is empty: whole key
    {"foo{{bar}}zap", 4015}, // tag is "{bar"
    {"foo{bar}{zap}", 5061}, // only the first tag counts
    {"a{b}c{d}", 3300},
    {"}{a}", 15495},
};

int main()
{
    int failures = 0;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        uint32_t slot = calc_hslot_crc16_cluster(cases[i].key, strlen(cases[i].key));
        if (slot != cases[i].slot) {
            fprintf(stderr, "FAIL: slot of \"%s\" is %u, expected %u\n", cases[i].key, slot, cases[i].slot);
            failures++;
        }
    }
    if (failures) return 1;
    printf("cluster_hslot_test: PASS\n");
    return 0;
}
