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

// Check crash_format_time() and crash_utc_offset() from crash_report.h
// against gmtime_r()/localtime_r() + strftime() (#681).

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "crash_report.h"

static int failures = 0;
static long skipped = 0;

// True when v is representable as time_t. A 32-bit time_t (i386, older
// armhf) only covers 1901-12-13 to 2038-01-19, so libc cannot be the
// reference outside that range; crash_format_time() itself uses long long.
static bool fits_time_t(long long v)
{
    return (long long) (time_t) v == v;
}

static void check_format(long long t, long offset)
{
    char got[64];
    char want[64];
    if (!fits_time_t(t + offset)) {
        skipped++;
        return;
    }
    crash_format_time(got, sizeof(got), t, offset);
    time_t shifted = (time_t) (t + offset);
    struct tm tm;
    if (gmtime_r(&shifted, &tm) == NULL || strftime(want, sizeof(want), "%d %b %Y %H:%M:%S", &tm) == 0) {
        fprintf(stderr, "FAIL [%s:%d] libc cannot format %lld\n", __FILE__, __LINE__, t + offset);
        failures++;
        return;
    }
    if (strcmp(got, want) != 0) {
        if (failures < 20)
            fprintf(stderr, "FAIL t=%lld offset=%ld: got \"%s\", expected \"%s\"\n", t, offset, got, want);
        failures++;
    }
}

// With TZ set, the offset at t must make crash_format_time() print what
// localtime_r() + strftime() print.
static void check_zone(const char *tz, long long t, long expected_offset)
{
    setenv("TZ", tz, 1);
    tzset();
    long offset = crash_utc_offset((time_t) t);
    if (offset != expected_offset) {
        fprintf(stderr, "FAIL TZ=%s t=%lld: offset %ld, expected %ld\n", tz, t, offset, expected_offset);
        failures++;
    }
    char got[64];
    char want[64];
    crash_format_time(got, sizeof(got), t, offset);
    time_t tt = (time_t) t;
    struct tm tm;
    if (localtime_r(&tt, &tm) == NULL || strftime(want, sizeof(want), "%d %b %Y %H:%M:%S", &tm) == 0) {
        fprintf(stderr, "FAIL TZ=%s t=%lld: libc cannot format\n", tz, t);
        failures++;
        return;
    }
    if (strcmp(got, want) != 0) {
        fprintf(stderr, "FAIL TZ=%s t=%lld: got \"%s\", expected \"%s\"\n", tz, t, got, want);
        failures++;
    }
}

int main(void)
{
    // strftime's %b must give the C locale's month names.
    setenv("LC_ALL", "C", 1);

    // Day boundaries around leap days (2100 is not a leap year), year ends
    // and the epoch.
    static const long long days[] = {
        0,      // 1970-01-01
        -1,     // 1969-12-31
        59,     // 1970-03-01
        10957,  // 2000-01-01
        11016,  // 2000-02-29
        11017,  // 2000-03-01
        19781,  // 2024-02-28
        19782,  // 2024-02-29
        19783,  // 2024-03-01
        47540,  // 2100-02-28
        47541,  // 2100-03-01
        157053, // 2399-12-31
        -25567, // 1900-01-01
    };
    for (size_t i = 0; i < sizeof(days) / sizeof(days[0]); i++) {
        check_format(days[i] * 86400, 0);
        check_format(days[i] * 86400 - 1, 0);
        check_format(days[i] * 86400 + 86399, 0);
    }

    // Every month and hour from 1900 to 2400, at odd seconds, and with the
    // offsets of real time zones.
    static const long offsets[] = {0, -43200, -36000, -16200, 3600, 19800, 20700, 50400};
    for (size_t i = 0; i < sizeof(offsets) / sizeof(offsets[0]); i++) {
        for (long long t = -2208988800LL; t < 13569465600LL; t += 7 * 86400 + 3671)
            check_format(t, offsets[i]);
    }

    // POSIX TZ strings, so the test does not depend on installed zone data.
    const long long jan = 1768478400; // 2026-01-15 12:00:00 UTC
    const long long jul = 1784116800; // 2026-07-15 12:00:00 UTC
    check_zone("UTC0", jan, 0);
    check_zone("<+0530>-5:30", jan, 19800);
    check_zone("<-0330>3:30", jan, -12600);
    check_zone("<+14>-14", jan, 50400);
    check_zone("<-12>12", jan, -43200);
    check_zone("EST5EDT,M3.2.0,M11.1.0", jan, -18000);
    check_zone("EST5EDT,M3.2.0,M11.1.0", jul, -14400);
    // Local date differs from the UTC date, on both sides of a year end.
    check_zone("<+14>-14", 1767182400, 50400); // 2025-12-31 12:00 UTC is 2026-01-01 local
    check_zone("<-12>12", 1767229200, -43200); // 2026-01-01 01:00 UTC is 2025-12-31 local

    if (failures != 0) {
        fprintf(stderr, "%d crash report test(s) failed\n", failures);
        return 1;
    }
    if (skipped != 0) printf("%ld cases outside the %zu-byte time_t range skipped\n", skipped, sizeof(time_t));
    printf("crash report tests passed\n");
    return 0;
}
