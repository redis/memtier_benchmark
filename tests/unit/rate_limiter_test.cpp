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

#include "rate_limiter.h"

static int failures = 0;

#define CHECK_EQ(actual, expected)                                                                                     \
    do {                                                                                                               \
        unsigned int value = (actual);                                                                                 \
        if (value != (expected)) {                                                                                     \
            fprintf(stderr, "FAIL [%s:%d] got %u, expected %u\n", __FILE__, __LINE__, value, (expected));              \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

int main(void)
{
    CHECK_EQ(calculate_rate_limit_phase(0, 3, 4), 0U);
    CHECK_EQ(calculate_rate_limit_phase(20000, 3, 0), 0U);

    CHECK_EQ(calculate_rate_limit_phase(20000, 0, 4), 0U);
    CHECK_EQ(calculate_rate_limit_phase(20000, 1, 4), 5000U);
    CHECK_EQ(calculate_rate_limit_phase(20000, 2, 4), 10000U);
    CHECK_EQ(calculate_rate_limit_phase(20000, 3, 4), 15000U);
    CHECK_EQ(calculate_rate_limit_phase(20000, 5, 4), 5000U);

    CHECK_EQ(calculate_rate_limit_phase(20000, 79, 80), 19750U);
    CHECK_EQ(calculate_rate_limit_phase(1000000, 3999999999ULL, 4000000000ULL), 999999U);

    if (failures != 0) {
        fprintf(stderr, "%d rate limiter test(s) failed\n", failures);
        return 1;
    }

    printf("rate limiter phase tests passed\n");
    return 0;
}
