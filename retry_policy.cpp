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
#include "file_io.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <string>

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

failed_keys_logger::failed_keys_logger() : m_fp(NULL), m_disabled(false), m_err(0), m_path(NULL)
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
        m_err = errno;
        fprintf(stderr, "error: cannot open --failed-keys-file %s: %s\n", path, strerror(m_err));
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
    // A header that cannot be written (e.g. a full disk) fails like the open.
    int err = finish_output_file(m_fp, false);
    if (err != 0 && !stdout_reader_gone(m_fp, err)) {
        m_err = err;
        fprintf(stderr, "error: failed writing --failed-keys-file %s: %s\n", path, strerror(err));
    }
    if (err != 0) {
        m_disabled = true;
        fclose(m_fp);
        m_fp = NULL;
    }
    pthread_mutex_unlock(&m_mtx);
    return m_err == 0;
}

// Called with m_mtx held when a write (or the flush that performs it) failed,
// e.g. on a full disk: report it once, stop logging and keep the error for
// close(), so the run exits 1 like for --out-file. A stdout reader that went
// away (a path such as /dev/stdout and `| head`) only stops logging.
void failed_keys_logger::write_failed_locked(int err)
{
    if (!stdout_reader_gone(m_fp, err)) {
        fprintf(stderr, "error: failed writing --failed-keys-file %s: %s; logging disabled.\n", m_path ? m_path : "?",
                strerror(err));
        if (m_err == 0) m_err = err;
    }
    m_disabled = true;
    fclose(m_fp);
    m_fp = NULL;
}

int failed_keys_logger::close()
{
    pthread_mutex_lock(&m_mtx);
    if (m_fp) {
        int err = close_output_file(m_fp);
        m_fp = NULL;
        if (err != 0) {
            fprintf(stderr, "error: failed writing --failed-keys-file %s: %s\n", m_path ? m_path : "?", strerror(err));
            if (m_err == 0) m_err = err;
        }
    }
    int err = m_err;
    pthread_mutex_unlock(&m_mtx);
    return err;
}

// Escape `in` for CSV: wrap in quotes, double inner quotes, write a
// backslash as \\ and any non-printable byte as \xHH, so the escape is
// reversible ("\x01" text and a 0x01 byte log differently). The result is
// sized from the input, so a field is never truncated.
//
// Plain control-flow only (no C++11 lambdas) so older macOS Apple Clang
// images on CI compile cleanly.
static std::string csv_escape(const char *in, unsigned int in_len)
{
    std::string out;
    out.reserve(in_len + 2);
    out.push_back('"');
    for (unsigned int i = 0; i < in_len; i++) {
        unsigned char c = (unsigned char) in[i];
        if (c == '"') {
            out.append("\"\"");
        } else if (c == '\\') {
            out.append("\\\\");
        } else if (c >= 0x20 && c <= 0x7e) {
            out.push_back((char) c);
        } else {
            char hex[5];
            snprintf(hex, sizeof(hex), "\\x%02x", c);
            out.append(hex);
        }
    }
    out.push_back('"');
    return out;
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

    const std::string keyfield = csv_escape(key ? key : "", key ? key_len : 0);
    const std::string statusfield = csv_escape(status ? status : "", status ? (unsigned int) strlen(status) : 0);

    errno = 0;
    int rc = fprintf(m_fp, "%s,%s,%s,%s,%u\n", tsbuf, command ? command : "UNKNOWN", keyfield.c_str(),
                     statusfield.c_str(), retries);
    // Flush per-record so a crash doesn't lose the trailing window. Failed
    // keys are rare, so the perf cost is irrelevant. fprintf() only fills the
    // stdio buffer; a full disk shows up in the flush.
    int err = rc < 0 ? (errno != 0 ? errno : EIO) : finish_output_file(m_fp, false);
    if (err != 0) write_failed_locked(err);
    pthread_mutex_unlock(&m_mtx);
}

failed_keys_logger &global_failed_keys_logger()
{
    // Never destroyed. On the normal path main() joins the worker threads and
    // then closes it. The early exit() / exit(failure_exit_code()) paths (e.g.
    // a thread that fails to prepare, or a fatal error in a worker thread)
    // skip the join, so other worker threads may still log while exit() runs
    // the static destructors; a destroyed mutex would then be used. exit()
    // still flushes the stdio buffers.
    static failed_keys_logger *inst = new failed_keys_logger;
    return *inst;
}
