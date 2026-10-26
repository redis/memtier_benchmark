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

// Test-only LD_PRELOAD shim for tests/test_library_provenance.py. It makes
// libevent/OpenSSL calls report what a broken or mismatched installation
// would; anything not enabled below is forwarded to the real library.
//
// MEMTIER_TEST_FAIL_EVTHREAD=1       evthread_use_pthreads() returns -1.
// MEMTIER_TEST_FAIL_BUFFEREVENT=1    bufferevent_socket_new() and
//     bufferevent_openssl_socket_new() return NULL. Like libevent >= 2.1.9,
//     the failed TLS call frees the SSL (BEV_OPT_CLOSE_ON_FREE) and leaves the
//     socket open.
// MEMTIER_TEST_LIBEVENT_VERSION=<text>:<number>   event_get_version() and
//     event_get_version_number() report another libevent, e.g.
//     "2.99.1-fake:0x02630100".
// MEMTIER_TEST_OPENSSL_VERSION=<text>:<number>    OpenSSL_version(OPENSSL_VERSION)
//     and OpenSSL_version_num() report another OpenSSL.
// MEMTIER_TEST_SHIM_MARKER=<path>    one line is appended per injected call.
//     At exit, if a bufferevent call was failed, "socket closed" or "socket
//     open" says what became of the socket the last failed call was given.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define BEV_OPT_CLOSE_ON_FREE_ (1 << 0)

static int marker_fd = -1;
static int fail_evthread;
static int fail_bufferevent;
static char libevent_text[128];
static unsigned int libevent_number;
static char openssl_text[128];
static unsigned long openssl_number;
// The socket handed to the last failed bufferevent call, and its identity, so
// a later descriptor that reuses the number is not mistaken for it.
static int failed_fd = -1;
static struct stat failed_fd_stat;

static void remember_failed_fd(int fd)
{
    if (fd >= 0 && fstat(fd, &failed_fd_stat) == 0) failed_fd = fd;
}

static void mark(const char *what)
{
    if (marker_fd < 0) return;
    size_t n = strlen(what);
    char line[128];
    if (n > sizeof(line) - 2) n = sizeof(line) - 2;
    memcpy(line, what, n);
    line[n++] = '\n';
    ssize_t r = write(marker_fd, line, n);
    (void) r;
}

// "<text>:<number>" -> text, number. Returns 0 when unset or malformed.
static int parse_version(const char *name, char *text, size_t size, unsigned long *number)
{
    const char *v = getenv(name);
    if (v == NULL) return 0;
    const char *colon = strrchr(v, ':');
    if (colon == NULL || (size_t) (colon - v) >= size) return 0;
    memcpy(text, v, colon - v);
    text[colon - v] = '\0';
    *number = strtoul(colon + 1, NULL, 0);
    return 1;
}

__attribute__((constructor)) static void read_options(void)
{
    const char *v = getenv("MEMTIER_TEST_SHIM_MARKER");
    if (v != NULL && *v != '\0') marker_fd = open(v, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    v = getenv("MEMTIER_TEST_FAIL_EVTHREAD");
    fail_evthread = v != NULL && strcmp(v, "1") == 0;
    v = getenv("MEMTIER_TEST_FAIL_BUFFEREVENT");
    fail_bufferevent = v != NULL && strcmp(v, "1") == 0;
    unsigned long n = 0;
    if (parse_version("MEMTIER_TEST_LIBEVENT_VERSION", libevent_text, sizeof(libevent_text), &n))
        libevent_number = (unsigned int) n;
    else
        libevent_text[0] = '\0';
    if (!parse_version("MEMTIER_TEST_OPENSSL_VERSION", openssl_text, sizeof(openssl_text), &openssl_number))
        openssl_text[0] = '\0';
}

__attribute__((destructor)) static void report_failed_fd(void)
{
    if (failed_fd < 0) return;
    struct stat st;
    int open_still = fstat(failed_fd, &st) == 0 && st.st_dev == failed_fd_stat.st_dev &&
                     st.st_ino == failed_fd_stat.st_ino;
    mark(open_still ? "socket open" : "socket closed");
}

int evthread_use_pthreads(void)
{
    if (fail_evthread) {
        mark("evthread_use_pthreads");
        return -1;
    }
    int (*real)(void) = (int (*)(void)) dlsym(RTLD_NEXT, "evthread_use_pthreads");
    return real ? real() : -1;
}

struct bufferevent *bufferevent_socket_new(void *base, int fd, int options)
{
    if (fail_bufferevent) {
        mark("bufferevent_socket_new");
        remember_failed_fd(fd);
        return NULL;
    }
    struct bufferevent *(*real)(void *, int, int) =
        (struct bufferevent * (*) (void *, int, int)) dlsym(RTLD_NEXT, "bufferevent_socket_new");
    return real ? real(base, fd, options) : NULL;
}

struct bufferevent *bufferevent_openssl_socket_new(void *base, int fd, void *ssl, int state, int options)
{
    if (fail_bufferevent) {
        mark("bufferevent_openssl_socket_new");
        remember_failed_fd(fd);
        if (options & BEV_OPT_CLOSE_ON_FREE_) {
            void (*ssl_free)(void *) = (void (*)(void *)) dlsym(RTLD_DEFAULT, "SSL_free");
            if (ssl_free) ssl_free(ssl);
        }
        return NULL;
    }
    struct bufferevent *(*real)(void *, int, void *, int, int) =
        (struct bufferevent * (*) (void *, int, void *, int, int)) dlsym(RTLD_NEXT, "bufferevent_openssl_socket_new");
    return real ? real(base, fd, ssl, state, options) : NULL;
}

const char *event_get_version(void)
{
    if (libevent_text[0] != '\0') return libevent_text;
    const char *(*real)(void) = (const char *(*) (void) ) dlsym(RTLD_NEXT, "event_get_version");
    return real ? real() : "";
}

unsigned int event_get_version_number(void)
{
    if (libevent_text[0] != '\0') return libevent_number;
    unsigned int (*real)(void) = (unsigned int (*)(void)) dlsym(RTLD_NEXT, "event_get_version_number");
    return real ? real() : 0;
}

const char *OpenSSL_version(int type)
{
    // 0 is OPENSSL_VERSION; the other types keep their real answers.
    if (type == 0 && openssl_text[0] != '\0') return openssl_text;
    const char *(*real)(int) = (const char *(*) (int) ) dlsym(RTLD_NEXT, "OpenSSL_version");
    return real ? real(type) : "";
}

unsigned long OpenSSL_version_num(void)
{
    if (openssl_text[0] != '\0') return openssl_number;
    unsigned long (*real)(void) = (unsigned long (*)(void)) dlsym(RTLD_NEXT, "OpenSSL_version_num");
    return real ? real() : 0;
}
