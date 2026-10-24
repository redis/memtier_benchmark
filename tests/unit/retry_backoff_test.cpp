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

#include <float.h>
#include <math.h>
#include <stdio.h>

#include "retry_backoff.h"

static int failures = 0;

#define CHECK_MS(actual, expected)                                                                                     \
    do {                                                                                                               \
        double value = (actual);                                                                                       \
        if (!(fabs(value - (expected)) < 1e-9)) {                                                                      \
            fprintf(stderr, "FAIL [%s:%d] got %.17g, expected %.17g\n", __FILE__, __LINE__, value,                     \
                    (double) (expected));                                                                              \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

int main(void)
{
    // Factor 0 (the default) keeps the delay constant.
    CHECK_MS(retry_backoff_delay_ms(10, 0.0, 0), 10.0);
    CHECK_MS(retry_backoff_delay_ms(10, 0.0, 7), 10.0);
    // No base delay: immediate, whatever the factor (no 0 * inf).
    CHECK_MS(retry_backoff_delay_ms(0, 2.0, 0), 0.0);
    CHECK_MS(retry_backoff_delay_ms(0, DBL_MAX, 4000), 0.0);

    // The Nth resend waits base * factor^(N-1).
    CHECK_MS(retry_backoff_delay_ms(10, 2.0, 0), 10.0);
    CHECK_MS(retry_backoff_delay_ms(10, 2.0, 1), 20.0);
    CHECK_MS(retry_backoff_delay_ms(10, 2.0, 4), 160.0);
    CHECK_MS(retry_backoff_delay_ms(10, 1.0, 1000), 10.0);
    CHECK_MS(retry_backoff_delay_ms(1000, 0.5, 2), 250.0);

    // A factor below 1 shrinks the delay down to 100 ms, not to 0 (#712).
    CHECK_MS(retry_backoff_delay_ms(1000, 0.5, 3), 125.0);
    CHECK_MS(retry_backoff_delay_ms(1000, 0.5, 4), 100.0);
    CHECK_MS(retry_backoff_delay_ms(1000, 0.5, 4000000000U), 100.0);
    CHECK_MS(retry_backoff_delay_ms(100, 0.5, 1), 100.0);
    CHECK_MS(retry_backoff_delay_ms(100, 0.5, 20), 100.0);
    CHECK_MS(retry_backoff_delay_ms(120000, 0.5, 30), 100.0);
    // A base below 100 ms is its own floor: the delay never drops below it.
    CHECK_MS(retry_backoff_delay_ms(10, 0.5, 1), 10.0);
    CHECK_MS(retry_backoff_delay_ms(1, 0.5, 50), 1.0);
    // A tiny factor underflows, not to 0 or NaN but to the floor.
    CHECK_MS(retry_backoff_delay_ms(10, 1e-300, 10), 10.0);
    CHECK_MS(retry_backoff_delay_ms(1000, DBL_MIN, 4000000000U), 100.0);
    // No base delay stays immediate with a factor below 1 too.
    CHECK_MS(retry_backoff_delay_ms(0, 0.5, 30), 0.0);

    // Growth stops at 60 s, including on overflow to inf.
    CHECK_MS(retry_backoff_delay_ms(10, 2.0, 12), 40960.0);
    CHECK_MS(retry_backoff_delay_ms(10, 2.0, 13), 60000.0);
    CHECK_MS(retry_backoff_delay_ms(10, 2.0, 4000000000U), 60000.0);
    CHECK_MS(retry_backoff_delay_ms(10, DBL_MAX, 2), 60000.0);

    // A base above 60 s is used as is: the cap only limits the growth.
    CHECK_MS(retry_backoff_delay_ms(120000, 0.0, 3), 120000.0);
    CHECK_MS(retry_backoff_delay_ms(120000, 1.0, 0), 120000.0);
    CHECK_MS(retry_backoff_delay_ms(120000, 1.0, 3), 120000.0);
    CHECK_MS(retry_backoff_delay_ms(120000, 2.0, 3), 120000.0);
    CHECK_MS(retry_backoff_delay_ms(120000, 0.5, 1), 60000.0);

    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("retry_backoff_test: all checks passed\n");
    return 0;
}
