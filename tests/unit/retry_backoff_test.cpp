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
#include <limits.h>
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

    // CLUSTERDOWN topology refreshes back off from 1 s to 8 s (#834).
    {
        clusterdown_refresh_backoff b;
        struct timeval t = {1000, 0};
        CHECK_MS(b.gap_us(), 1000000.0);
        CHECK_MS(b.due(t), 1.0); // the first refresh is immediate
        // Gaps after each refresh of a run: 1 s, 2 s, 4 s, then 8 s.
        const long long gaps[] = {1000000, 2000000, 4000000, 8000000, 8000000, 8000000};
        for (unsigned int i = 0; i < sizeof(gaps) / sizeof(gaps[0]); i++) {
            b.refreshed(t);
            CHECK_MS(b.gap_us(), (double) gaps[i]);
            struct timeval early = {t.tv_sec + (time_t) (gaps[i] / 1000000) - 1, 999999};
            CHECK_MS(b.due(early), 0.0);
            t.tv_sec += (time_t) (gaps[i] / 1000000);
            CHECK_MS(b.due(t), 1.0);
        }
        b.streak = UINT_MAX - 1;
        b.refreshed(t);
        b.refreshed(t); // saturates instead of wrapping to an immediate refresh
        CHECK_MS(b.gap_us(), 8000000.0);

        // Another reply ends the run, but the 1 s floor stays.
        b.reset();
        CHECK_MS(b.gap_us(), 1000000.0);
        struct timeval soon = {t.tv_sec, 999999};
        CHECK_MS(b.due(soon), 0.0);
        struct timeval later = {t.tv_sec + 1, 0};
        CHECK_MS(b.due(later), 1.0);
        b.refreshed(later);
        CHECK_MS(b.gap_us(), 1000000.0);
        b.refreshed(later);
        CHECK_MS(b.gap_us(), 2000000.0);
    }
    {
        // One client answered every 10 ms during a 60 s outage refreshes at
        // 0, 1, 3, 7, 15, 23, 31, 39, 47 and 55 s: 10, not 61.
        clusterdown_refresh_backoff b;
        int refreshes = 0;
        for (long long us = 0; us <= 60000000LL; us += 10000) {
            struct timeval now = {(time_t) (100 + us / 1000000), (suseconds_t) (us % 1000000)};
            if (b.due(now)) {
                b.refreshed(now);
                refreshes++;
            }
        }
        CHECK_MS(refreshes, 10.0);
    }

    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("retry_backoff_test: all checks passed\n");
    return 0;
}
