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

#ifndef MEMTIER_BENCHMARK_LIBRARY_VERSIONS_H
#define MEMTIER_BENCHMARK_LIBRARY_VERSIONS_H

// Build-time vs runtime library version checks. Each library keeps its ABI
// within a release series, so only a different series is reported: a patch
// level difference (e.g. built on libevent 2.1.13, run on 2.1.12) is normal.

// libevent version numbers are 0xMMmmpp00 (LIBEVENT_VERSION_NUMBER and
// event_get_version_number()); the series is major.minor.
static inline bool libevent_series_differs(unsigned long build, unsigned long runtime)
{
    return (build >> 16) != (runtime >> 16);
}

// Whether the caller still owns (and must close) the socket after a failed
// bufferevent_openssl_socket_new(..., BEV_OPT_CLOSE_ON_FREE). From 2.1.9-beta
// (upstream commit e86ccfe5, also 2.2) it frees the SSL itself and clears it
// before freeing the partial bufferevent, so the socket is never reached. In
// 2.1.8 and older that depends on where it failed: an allocation failure
// leaves the socket open (and leaks the SSL, which the caller can't know it
// still owns), but a failure to arm the handshake events frees the partial
// bufferevent with the SSL still attached, whose destructor closes both.
// When false the caller leaves the socket open rather than risk closing a
// descriptor another thread has just been given: one socket (and maybe one
// SSL) per failed attempt, which only happens when libevent cannot allocate.
// tests/library_failure_injector.c mimics the >= 2.1.9 contract; recheck it
// when the libevent floor or a new series changes.
static inline bool libevent_ssl_failure_socket_is_ours(unsigned long runtime)
{
    return runtime >= 0x02010900UL;
}

// OpenSSL version numbers are 0xMNN00PP0 from 3.0 on and 0xMNNFFPPS before
// that. From 3.0 on a major version only promises that a binary built with
// older headers runs on a newer library, so a library of another major
// version, or an older minor version than the headers, is reported. Before
// 3.0 compatibility holds within major.minor (1.0.x vs 1.1.x). LibreSSL
// reports a fixed 0x20000000.
static inline unsigned long openssl_version_series(unsigned long version)
{
    return version >= 0x30000000UL ? version >> 28 : version >> 20;
}

static inline bool openssl_series_differs(unsigned long build, unsigned long runtime)
{
    if (openssl_version_series(build) != openssl_version_series(runtime)) return true;
    // Same 3.x major: only a runtime minor older than the headers' differs.
    return build >= 0x30000000UL && (runtime >> 20) < (build >> 20);
}

#endif // MEMTIER_BENCHMARK_LIBRARY_VERSIONS_H
