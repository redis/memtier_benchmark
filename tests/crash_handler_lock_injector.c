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
//
// With MEMTIER_TEST_BLOCK_BACKTRACE=1, backtrace() blocks the same way, which
// stands for a hang in the best-effort part of the report and exercises the
// crash handler's SIGALRM watchdog.
//
// With MEMTIER_TEST_LOCK_STDERR=1, the crash handler's first alarm() call
// wakes a helper thread that takes stderr's stdio lock and never releases it,
// as if another thread had been interrupted inside fprintf(stderr). Then only
// output written with write(2) can appear.
//
// With MEMTIER_TEST_SHIM_MARKER=<path>, the shim appends "signal-stack" to that
// file when alarm() is called on a signal stack (the crash handler arms its
// watchdog), and "blocked <function>" when it blocks. The test reads it to
// check that the shim was loaded and the handler ran where the shim looks.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <execinfo.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/prctl.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <stddef.h>
#include <time.h>
#include <unistd.h>

static int on_signal_stack(void)
{
    stack_t ss;
    return sigaltstack(NULL, &ss) == 0 && (ss.ss_flags & SS_ONSTACK) != 0;
}

static int marker_fd = -1;

// Async-signal-safe: only write(2).
static void mark(const char *what)
{
    if (marker_fd < 0) return;
    char line[64];
    size_t n = 0;
    while (*what != '\0' && n < sizeof(line) - 1)
        line[n++] = *what++;
    line[n++] = '\n';
    ssize_t r = write(marker_fd, line, n);
    (void) r;
}

static void block_like_held_lock(const char *function)
{
    char what[48] = "blocked ";
    size_t n = strlen(what);
    while (*function != '\0' && n < sizeof(what) - 1)
        what[n++] = *function++;
    what[n] = '\0';
    mark(what);
    for (;;)
        pause();
}

// Read once at load time: getenv() is not meant for signal handlers.
static int block_backtrace;
static int lock_stderr;
static int wake_pipe[2] = {-1, -1};
static volatile sig_atomic_t stderr_locked;
// Resolved at load time: dlsym() is not async-signal-safe.
static unsigned int (*real_alarm)(unsigned int);

static void *stderr_locker(void *arg)
{
    (void) arg;
    // Named for debugging. It blocks every signal, so the test never picks it
    // as the worker to crash.
    prctl(PR_SET_NAME, "crash-shim");
    char c;
    while (read(wake_pipe[0], &c, 1) != 1) {}
    flockfile(stderr);
    stderr_locked = 1;
    for (;;)
        pause();
    return NULL;
}

__attribute__((constructor)) static void read_options(void)
{
    const char *v = getenv("MEMTIER_TEST_BLOCK_BACKTRACE");
    block_backtrace = v != NULL && strcmp(v, "1") == 0;
    real_alarm = (unsigned int (*)(unsigned int)) dlsym(RTLD_NEXT, "alarm");
    v = getenv("MEMTIER_TEST_SHIM_MARKER");
    if (v != NULL && *v != '\0') marker_fd = open(v, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    v = getenv("MEMTIER_TEST_LOCK_STDERR");
    lock_stderr = v != NULL && strcmp(v, "1") == 0;
    if (lock_stderr && pipe(wake_pipe) == 0) {
        // The helper blocks every signal, so none of memtier's signals
        // (crash, SIGALRM, SIGINT) can run on it.
        sigset_t all, old;
        sigfillset(&all);
        pthread_sigmask(SIG_SETMASK, &all, &old);
        pthread_t tid;
        if (pthread_create(&tid, NULL, stderr_locker, NULL) != 0) lock_stderr = 0;
        pthread_sigmask(SIG_SETMASK, &old, NULL);
    } else {
        lock_stderr = 0;
    }
}

struct tm *localtime(const time_t *t)
{
    static struct tm *(*real)(const time_t *);
    if (on_signal_stack()) block_like_held_lock("localtime");
    if (!real) real = (struct tm * (*) (const time_t *) ) dlsym(RTLD_NEXT, "localtime");
    return real(t);
}

struct tm *localtime_r(const time_t *t, struct tm *result)
{
    static struct tm *(*real)(const time_t *, struct tm *);
    if (on_signal_stack()) block_like_held_lock("localtime_r");
    if (!real) real = (struct tm * (*) (const time_t *, struct tm *) ) dlsym(RTLD_NEXT, "localtime_r");
    return real(t, result);
}

size_t strftime(char *s, size_t max, const char *format, const struct tm *tm)
{
    static size_t (*real)(char *, size_t, const char *, const struct tm *);
    if (on_signal_stack()) block_like_held_lock("strftime");
    if (!real) real = (size_t(*)(char *, size_t, const char *, const struct tm *)) dlsym(RTLD_NEXT, "strftime");
    return real(s, max, format, tm);
}

void tzset(void)
{
    static void (*real)(void);
    if (on_signal_stack()) block_like_held_lock("tzset");
    if (!real) real = (void (*)(void)) dlsym(RTLD_NEXT, "tzset");
    real();
}

int backtrace(void **buffer, int size)
{
    static int (*real)(void **, int);
    if (block_backtrace && on_signal_stack()) block_like_held_lock("backtrace");
    if (!real) real = (int (*)(void **, int)) dlsym(RTLD_NEXT, "backtrace");
    return real(buffer, size);
}

unsigned int alarm(unsigned int seconds)
{
    static volatile sig_atomic_t woken;
    // Arm the real alarm first, so the watchdog still ends the process if
    // the helper never gets the lock.
    unsigned int previous = real_alarm(seconds);
    if (on_signal_stack()) mark("signal-stack");
    if (lock_stderr && !woken && on_signal_stack()) {
        woken = 1;
        char c = 1;
        if (write(wake_pipe[1], &c, 1) == 1) {
            // Wait at most 2 s for the helper (clock_gettime is
            // async-signal-safe).
            struct timespec start, now;
            clock_gettime(CLOCK_MONOTONIC, &start);
            while (!stderr_locked) {
                clock_gettime(CLOCK_MONOTONIC, &now);
                if (now.tv_sec - start.tv_sec >= 2) {
                    mark("stderr-lock-timeout");
                    break;
                }
                sched_yield();
            }
            if (stderr_locked) mark("stderr-locked");
        }
    }
    return previous;
}
