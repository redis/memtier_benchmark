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

// OpenSSL version numbers are 0xMNN00PP0 from 3.0 on, compatible within a
// major version, and 0xMNNFFPPS before that, compatible within major.minor
// (1.0.x vs 1.1.x). LibreSSL reports a fixed 0x20000000.
static inline unsigned long openssl_version_series(unsigned long version)
{
    return version >= 0x30000000UL ? version >> 28 : version >> 20;
}

static inline bool openssl_series_differs(unsigned long build, unsigned long runtime)
{
    return openssl_version_series(build) != openssl_version_series(runtime);
}

#endif // MEMTIER_BENCHMARK_LIBRARY_VERSIONS_H
