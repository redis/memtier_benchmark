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

#ifndef MEMTIER_BENCHMARK_RETRY_BACKOFF_H
#define MEMTIER_BENCHMARK_RETRY_BACKOFF_H

#include <math.h>

// Growth of the --retry-backoff-factor delay stops at 60 s.
static const double MEMTIER_RETRY_BACKOFF_CAP_MS = 60000.0;

// Delay in ms before resending a request that was already resent `resends`
// times: base_ms * factor^resends. A factor of 0 keeps the delay at base_ms.
// The factor never takes the delay above 60 s, but a base_ms above 60 s is
// used as is, so the cap only limits the growth.
static inline double retry_backoff_delay_ms(unsigned int base_ms, double factor, unsigned int resends)
{
    double ms = (double) base_ms;
    if (ms > 0.0 && factor > 0.0 && resends > 0) {
        double limit = ms > MEMTIER_RETRY_BACKOFF_CAP_MS ? ms : MEMTIER_RETRY_BACKOFF_CAP_MS;
        ms *= pow(factor, (double) resends);
        // Also catches inf; base_ms > 0 and factor > 0 rule out NaN.
        if (!(ms <= limit)) ms = limit;
    }
    return ms;
}

#endif // MEMTIER_BENCHMARK_RETRY_BACKOFF_H
