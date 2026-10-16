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

#include <stdio.h>
#include <stdint.h>

#include "keylist_growth.h"

static int failures = 0;

#define CHECK_EQ(actual, expected)                                                                                     \
    do {                                                                                                               \
        size_t value = (actual);                                                                                       \
        size_t want = (expected);                                                                                      \
        if (value != want) {                                                                                           \
            fprintf(stderr, "FAIL [%s:%d] got %zu, expected %zu\n", __FILE__, __LINE__, value, want);                  \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

int main(void)
{
    // Already big enough, or doubled until it is.
    CHECK_EQ(keylist_grow_size(256, 100), 256);
    CHECK_EQ(keylist_grow_size(256, 256), 256);
    CHECK_EQ(keylist_grow_size(256, 257), 512);
    CHECK_EQ(keylist_grow_size(256, 1000), 1024);
    CHECK_EQ(keylist_grow_size(0, 3), 4);

    // An unsigned int size wrapped to 0 here and the growth loop spun
    // forever (--multi-key-get=8388607 with ~260-byte keys).
    const size_t two31 = (size_t) 1 << 31;
    size_t grown = keylist_grow_size(two31, two31 + 1);
    if (grown < two31 + 1) {
        fprintf(stderr, "FAIL [%s:%d] got %zu, expected >= %zu\n", __FILE__, __LINE__, grown, two31 + 1);
        failures++;
    }

    // Near SIZE_MAX the doubling would overflow size_t: needed is returned.
    CHECK_EQ(keylist_grow_size(SIZE_MAX / 2 + 1, SIZE_MAX / 2 + 2), SIZE_MAX / 2 + 2);
    CHECK_EQ(keylist_grow_size(SIZE_MAX / 2 + 1, SIZE_MAX), SIZE_MAX);

    if (failures) {
        fprintf(stderr, "%d check(s) failed\n", failures);
        return 1;
    }
    printf("keylist_growth_test: all checks passed\n");
    return 0;
}
