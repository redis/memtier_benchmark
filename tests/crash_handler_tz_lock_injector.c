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

// Test-only LD_PRELOAD shim for issue #681. A crash that interrupts a thread
// holding glibc's time zone or malloc lock makes localtime() in the crash
// handler block forever. That race is too narrow to hit on demand, so this
// shim makes the time zone functions block whenever they are called on an
// alternate signal stack, which is where memtier's crash handler runs
// (SA_ONSTACK). Calls from normal code are forwarded to the real functions.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <signal.h>
#include <stddef.h>
#include <time.h>
#include <unistd.h>

static int on_signal_stack(void)
{
    stack_t ss;
    return sigaltstack(NULL, &ss) == 0 && (ss.ss_flags & SS_ONSTACK) != 0;
}

static void block_like_held_lock(void)
{
    for (;;) pause();
}

struct tm *localtime(const time_t *t)
{
    static struct tm *(*real)(const time_t *);
    if (on_signal_stack()) block_like_held_lock();
    if (!real) real = (struct tm * (*) (const time_t *) ) dlsym(RTLD_NEXT, "localtime");
    return real(t);
}

struct tm *localtime_r(const time_t *t, struct tm *result)
{
    static struct tm *(*real)(const time_t *, struct tm *);
    if (on_signal_stack()) block_like_held_lock();
    if (!real) real = (struct tm * (*) (const time_t *, struct tm *) ) dlsym(RTLD_NEXT, "localtime_r");
    return real(t, result);
}

size_t strftime(char *s, size_t max, const char *format, const struct tm *tm)
{
    static size_t (*real)(char *, size_t, const char *, const struct tm *);
    if (on_signal_stack()) block_like_held_lock();
    if (!real) real = (size_t(*)(char *, size_t, const char *, const struct tm *)) dlsym(RTLD_NEXT, "strftime");
    return real(s, max, format, tm);
}

void tzset(void)
{
    static void (*real)(void);
    if (on_signal_stack()) block_like_held_lock();
    if (!real) real = (void (*)(void)) dlsym(RTLD_NEXT, "tzset");
    real();
}
