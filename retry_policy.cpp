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

#include "retry_policy.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// Built-in permanent-error prefixes. Matched after stripping a leading '-'.
// Anything not in this list is considered retryable by default.
static const char *kPermanentPrefixes[] = {
    "WRONGTYPE",
    "NOAUTH",
    "NOPERM",
    "NOSCRIPT",
    "NOREPLICAS",
    "ERR wrong number of arguments",
    "ERR syntax error",
    "ERR unknown command",
    "ERR Unknown subcommand",
    "CLIENT_ERROR", // memcached
};

static const char *strip_dash(const char *s)
{
    if (!s) return s;
    if (*s == '-') return s + 1;
    return s;
}

static bool starts_with_ci(const char *haystack, const char *needle)
{
    while (*needle) {
        if (!*haystack) return false;
        if (toupper((unsigned char) *haystack) != toupper((unsigned char) *needle)) return false;
        haystack++;
        needle++;
    }
    return true;
}

static bool matches_csv_prefix(const char *status, const char *filter_csv)
{
    // filter_csv is a comma-separated list of prefixes. Match case-insensitively.
    if (!filter_csv || !*filter_csv) return false;
    const char *p = filter_csv;
    while (*p) {
        // skip whitespace and leading commas
        while (*p == ',' || *p == ' ' || *p == '\t')
            p++;
        if (!*p) break;
        const char *start = p;
        while (*p && *p != ',')
            p++;
        size_t len = (size_t) (p - start);
        // trim trailing whitespace
        while (len > 0 && (start[len - 1] == ' ' || start[len - 1] == '\t'))
            len--;
        if (len > 0) {
            char tmp[64];
            if (len >= sizeof(tmp)) len = sizeof(tmp) - 1;
            memcpy(tmp, start, len);
            tmp[len] = '\0';
            if (starts_with_ci(status, tmp)) return true;
        }
    }
    return false;
}

bool is_retryable_error(const char *status, const char *filter_csv)
{
    if (!status || !*status) return false;
    const char *s = strip_dash(status);

    // Permanent set always wins.
    for (size_t i = 0; i < sizeof(kPermanentPrefixes) / sizeof(kPermanentPrefixes[0]); i++) {
        if (starts_with_ci(s, kPermanentPrefixes[i])) return false;
    }

    if (filter_csv && *filter_csv) {
        // Restrictive mode: only retry if prefix matches the filter.
        return matches_csv_prefix(s, filter_csv);
    }

    return true;
}

failed_keys_logger::failed_keys_logger() : m_fp(NULL), m_disabled(false), m_path(NULL)
{
    pthread_mutex_init(&m_mtx, NULL);
}

failed_keys_logger::~failed_keys_logger()
{
    close();
    pthread_mutex_destroy(&m_mtx);
    if (m_path) {
        free(m_path);
        m_path = NULL;
    }
}

bool failed_keys_logger::open(const char *path)
{
    if (!path || !*path) return false;
    pthread_mutex_lock(&m_mtx);
    if (m_fp != NULL) {
        // Already open.
        pthread_mutex_unlock(&m_mtx);
        return true;
    }
    m_fp = fopen(path, "a");
    if (!m_fp) {
        fprintf(stderr, "warning: failed-keys-file '%s' could not be opened (%s); logging disabled.\n", path,
                strerror(errno));
        m_disabled = true;
        pthread_mutex_unlock(&m_mtx);
        return false;
    }
    if (m_path) free(m_path);
    m_path = strdup(path);
    // CSV header. fopen with "a" doesn't truncate, so check size first.
    fseek(m_fp, 0, SEEK_END);
    long pos = ftell(m_fp);
    if (pos == 0) {
        fprintf(m_fp, "timestamp,command,key,status,retries\n");
    }
    fflush(m_fp);
    pthread_mutex_unlock(&m_mtx);
    return true;
}

void failed_keys_logger::close()
{
    pthread_mutex_lock(&m_mtx);
    if (m_fp) {
        fclose(m_fp);
        m_fp = NULL;
    }
    pthread_mutex_unlock(&m_mtx);
}

// Escape `in` for CSV: wrap in quotes, double inner quotes, hex-escape any
// non-printable byte. Writes into `out` (size out_sz) and returns the number
// of bytes written (excluding trailing NUL). Always NUL-terminates if out_sz > 0.
//
// Plain control-flow only (no C++11 lambdas) so older macOS Apple Clang
// images on CI compile cleanly.
static size_t csv_escape(const char *in, unsigned int in_len, char *out, size_t out_sz)
{
    if (out_sz == 0) return 0;
    if (out_sz < 3) {
        // Not enough room even for an empty quoted string: write what we can
        // and bail. out[0] = '\0' guarantees a valid C string.
        out[0] = '\0';
        return 0;
    }
    size_t o = 0;
    out[o++] = '"';
    for (unsigned int i = 0; i < in_len; i++) {
        unsigned char c = (unsigned char) in[i];
        if (c == '"') {
            // Need 2 bytes for "" + 1 for trailing quote + 1 for NUL.
            if (o + 3 >= out_sz) break;
            out[o++] = '"';
            out[o++] = '"';
        } else if (c >= 0x20 && c <= 0x7e) {
            if (o + 2 >= out_sz) break;
            out[o++] = (char) c;
        } else {
            // hex-escape: \xHH (4 bytes) + 1 trailing quote + 1 NUL = need 6 free.
            if (o + 5 >= out_sz) break;
            int n = snprintf(out + o, out_sz - o, "\\x%02x", c);
            if (n < 0 || (size_t) n >= out_sz - o) break;
            o += (size_t) n;
        }
    }
    if (o + 1 >= out_sz) {
        // Should not happen given the per-iteration checks, but defensively
        // bail to keep room for the trailing quote and NUL.
        o = out_sz - 2;
    }
    out[o++] = '"';
    out[o] = '\0';
    return o;
}

void failed_keys_logger::log_failure(const struct timeval &ts, const char *command, const char *key,
                                     unsigned int key_len, const char *status, unsigned int retries)
{
    pthread_mutex_lock(&m_mtx);
    if (!m_fp || m_disabled) {
        pthread_mutex_unlock(&m_mtx);
        return;
    }

    // Format timestamp as ISO-8601 with microsecond precision (UTC). Cast
    // tv_usec to unsigned to silence -Wformat-truncation (tv_usec is always
    // in [0, 999999]).
    char tsbuf[64];
    struct tm tmbuf;
    time_t sec = (time_t) ts.tv_sec;
    gmtime_r(&sec, &tmbuf);
    unsigned long usec = (unsigned long) ts.tv_usec;
    if (usec > 999999UL) usec = 999999UL;
    snprintf(tsbuf, sizeof(tsbuf), "%04d-%02d-%02dT%02d:%02d:%02d.%06luZ", tmbuf.tm_year + 1900, tmbuf.tm_mon + 1,
             tmbuf.tm_mday, tmbuf.tm_hour, tmbuf.tm_min, tmbuf.tm_sec, usec);

    char keybuf[1024];
    csv_escape(key ? key : "", key_len, keybuf, sizeof(keybuf));

    char statusbuf[512];
    csv_escape(status ? status : "", status ? (unsigned int) strlen(status) : 0, statusbuf, sizeof(statusbuf));

    int rc = fprintf(m_fp, "%s,%s,%s,%s,%u\n", tsbuf, command ? command : "UNKNOWN", keybuf, statusbuf, retries);
    if (rc < 0) {
        fprintf(stderr, "warning: failed-keys-file '%s' write failed (%s); logging disabled.\n", m_path ? m_path : "?",
                strerror(errno));
        m_disabled = true;
        fclose(m_fp);
        m_fp = NULL;
    } else {
        // Flush per-record so a crash doesn't lose the trailing window. Failed
        // keys are rare, so the perf cost is irrelevant.
        fflush(m_fp);
    }
    pthread_mutex_unlock(&m_mtx);
}

failed_keys_logger &global_failed_keys_logger()
{
    static failed_keys_logger inst;
    return inst;
}
