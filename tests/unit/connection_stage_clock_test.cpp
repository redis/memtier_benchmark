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

// --connection-stage-timeout=N trips after N seconds, never after N - 1.

#include <stdio.h>

#include "connection_stage_clock.h"

static int failures = 0;

static void check(bool condition, const char *message)
{
    if (!condition) {
        fprintf(stderr, "FAIL: %s\n", message);
        failures++;
    }
}

static long long usec(double seconds)
{
    return (long long) (seconds * 1000000 + 0.5);
}

int main(void)
{
    unsigned int elapsed = 99;

    // Started late in a second: 1.02 s later the wall clock has moved two
    // whole seconds, but a 2 s timeout has not run out.
    check(!connection_stage_timed_out(usec(10.99), usec(12.01), 2, &elapsed), "1.02 s is not 2 s");
    check(elapsed == 99, "elapsed untouched when not timed out");
    check(!connection_stage_timed_out(usec(10.99), usec(12.989999), 2, NULL), "just under 2 s");
    check(connection_stage_timed_out(usec(10.99), usec(12.99), 2, &elapsed), "exactly 2 s");
    check(elapsed == 2, "2 whole seconds elapsed");
    check(connection_stage_timed_out(usec(10.0), usec(13.5), 2, &elapsed), "3.5 s");
    check(elapsed == 3, "3 whole seconds elapsed");

    // Not started.
    check(!connection_stage_timed_out(0, usec(100.0), 2, NULL), "no phase start");
    // Timeout 0 is handled by the caller (disabled); here it trips at once.
    check(connection_stage_timed_out(usec(10.5), usec(10.5), 0, &elapsed), "zero timeout");
    // The timeout is converted to microseconds in 64 bits: 5000 s is past
    // a 32-bit microsecond count.
    check(!connection_stage_timed_out(usec(100.0), usec(4999.0), 5000, NULL), "5000 s timeout not yet");
    check(connection_stage_timed_out(usec(100.0), usec(5100.0), 5000, &elapsed), "5000 s timeout");
    check(elapsed == 5000, "5000 whole seconds elapsed");
    // Far from the epoch the microsecond count still fits.
    check(!connection_stage_timed_out(usec(4102444800.9), usec(4102444801.95), 2, NULL), "year-2100 timestamps");

    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("connection_stage_clock_test: all checks passed\n");
    return 0;
}
