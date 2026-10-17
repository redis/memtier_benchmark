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

#ifndef MEMTIER_BENCHMARK_CRASH_REPORT_H
#define MEMTIER_BENCHMARK_CRASH_REPORT_H

// Async-signal-safe pieces of the crash report (#681). The crash handler
// cannot call localtime(): the first call loads the time zone (malloc,
// fopen) and every call takes glibc's time zone lock, so a crash that
// interrupted a thread holding the malloc or time zone lock deadlocked the
// handler before it printed anything, until alarm(5) killed the process
// silently. Instead the UTC offset is taken once at startup with
// crash_utc_offset(), and the handler formats the time by hand. A DST change
// during the run makes the crash timestamp off by the DST shift.

#include <errno.h>
#include <stddef.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

// A fixed-size line buffer the crash handler formats into and writes to
// stderr with write(2). Only async-signal-safe operations: no stdio, no
// malloc, no locale. Output that does not fit is truncated.
struct crash_line
{
    char buf[256];
    size_t len;
    crash_line() : len(0) {}
    void str(const char *s)
    {
        while (s != NULL && *s != '\0' && len < sizeof(buf))
            buf[len++] = *s++;
    }
    void num(unsigned long long v, unsigned int base, int min_digits)
    {
        char tmp[32];
        int n = 0;
        do {
            tmp[n++] = "0123456789abcdef"[v % base];
            v /= base;
        } while (v != 0 && n < (int) sizeof(tmp));
        while (n < min_digits && n < (int) sizeof(tmp))
            tmp[n++] = '0';
        while (n > 0 && len < sizeof(buf))
            buf[len++] = tmp[--n];
    }
    void num(long long v)
    {
        if (v < 0) {
            str("-");
            num(0ULL - (unsigned long long) v, 10, 1);
        } else {
            num((unsigned long long) v, 10, 1);
        }
    }
    void write_out(void)
    {
        size_t off = 0;
        while (off < len) {
            ssize_t n = write(STDERR_FILENO, buf + off, len - off);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) break;
            off += (size_t) n;
        }
        len = 0;
    }
};

// Local time minus UTC at `now`, in seconds. Not async-signal-safe; call it
// at startup. tm_gmtoff is not in POSIX, so derive the offset from the local
// and UTC broken-down forms of the same instant. Returns 0 on failure.
static inline long crash_utc_offset(time_t now)
{
    struct tm lt, gt;
    if (localtime_r(&now, &lt) == NULL || gmtime_r(&now, &gt) == NULL) return 0;
    long off = (lt.tm_hour - gt.tm_hour) * 3600L + (lt.tm_min - gt.tm_min) * 60L + (lt.tm_sec - gt.tm_sec);
    if (lt.tm_year != gt.tm_year) {
        off += (lt.tm_year > gt.tm_year ? 1 : -1) * 86400L;
    } else {
        off += (lt.tm_yday - gt.tm_yday) * 86400L;
    }
    return off;
}

// Format `now + utc_offset` as "%d %b %Y %H:%M:%S" (the format the report
// has always used) into out, with integer arithmetic only. The date math is
// Howard Hinnant's civil_from_days.
static inline void crash_format_time(char *out, size_t out_len, long long now, long utc_offset)
{
    static const char *const month_names[12] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                                "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    if (out_len == 0) return;
    long long t = now + utc_offset;
    long long days = t / 86400;
    long long secs = t % 86400;
    if (secs < 0) {
        secs += 86400;
        days -= 1;
    }
    long long z = days + 719468;
    long long era = (z >= 0 ? z : z - 146096) / 146097;
    long long doe = z - era * 146097;
    long long yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    long long doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    long long mp = (5 * doy + 2) / 153;
    long long day = doy - (153 * mp + 2) / 5 + 1;
    long long month = mp < 10 ? mp + 3 : mp - 9;
    long long year = yoe + era * 400 + (month <= 2 ? 1 : 0);

    crash_line l;
    l.num((unsigned long long) day, 10, 2);
    l.str(" ");
    l.str(month_names[month - 1]);
    l.str(" ");
    l.num(year);
    l.str(" ");
    l.num((unsigned long long) (secs / 3600), 10, 2);
    l.str(":");
    l.num((unsigned long long) (secs / 60 % 60), 10, 2);
    l.str(":");
    l.num((unsigned long long) (secs % 60), 10, 2);
    size_t n = l.len < out_len - 1 ? l.len : out_len - 1;
    memcpy(out, l.buf, n);
    out[n] = '\0';
}

#endif // MEMTIER_BENCHMARK_CRASH_REPORT_H
