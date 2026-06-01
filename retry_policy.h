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

#ifndef MEMTIER_BENCHMARK_RETRY_POLICY_H
#define MEMTIER_BENCHMARK_RETRY_POLICY_H

#include <stdio.h>
#include <pthread.h>
#include <sys/time.h>

#include "memtier_benchmark.h"

// Classify a server error status string as either retryable or permanent.
// `status` is the raw error line from the server (e.g. "-WRONGTYPE Operation ..."
// or "WRONGTYPE Operation ...", depending on protocol). Leading '-' is tolerated.
//
// Built-in permanent set: WRONGTYPE, NOAUTH, NOPERM, NOSCRIPT, NOREPLICAS,
// "ERR wrong number of arguments", "ERR syntax error", "ERR unknown command",
// memcached CLIENT_ERROR. Anything else is retryable by default.
//
// When --retry-on=LIST is set, the filter is honored as a restrictive
// allowlist: only statuses whose prefix matches an entry in `filter_csv` are
// retryable; everything else is treated as permanent. The permanent set still
// short-circuits (a WRONGTYPE will never be retried even if listed).
//
// Returns true if the error should trigger a retry.
bool is_retryable_error(const char *status, const char *filter_csv);

// Thread-safe append logger for permanently-failed requests. A single shared
// file handle protected by a mutex. open()/close() are idempotent. Failures
// to write are logged once to stderr and silently swallowed thereafter so the
// benchmark never aborts because of a slow disk.
class failed_keys_logger
{
public:
    failed_keys_logger();
    ~failed_keys_logger();

    // Open the log file. Idempotent. Returns true on success; on failure logs
    // an error to stderr once and the instance becomes a no-op.
    bool open(const char *path);
    void close();

    // Write one record. Any of key/status may be NULL/empty.
    //   ts        timestamp of final failure
    //   command   short command name (e.g. "SET", "GET", "ARBITRARY")
    //   key       request key, if known (may contain non-printable bytes;
    //             they are hex-escaped in the output)
    //   key_len   length of `key`
    //   status    final error status from server, or "connection-dropped" for
    //             socket failures
    //   retries   number of attempts (0 = permanent on first try)
    void log_failure(const struct timeval &ts, const char *command, const char *key, unsigned int key_len,
                     const char *status, unsigned int retries);

private:
    FILE *m_fp;
    pthread_mutex_t m_mtx;
    bool m_disabled; // set true on first I/O failure to silence further attempts
    char *m_path;    // strdup'd, for diagnostic logging only
};

// Process-wide singleton. Lazy-initialized; safe to call from any thread.
failed_keys_logger &global_failed_keys_logger();

#endif // MEMTIER_BENCHMARK_RETRY_POLICY_H
