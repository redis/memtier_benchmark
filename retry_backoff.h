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

#include <limits.h>
#include <math.h>
#include <sys/time.h>

// Growth of the --retry-backoff-factor delay stops at 60 s.
static const double MEMTIER_RETRY_BACKOFF_CAP_MS = 60000.0;

// A --retry-backoff-factor below 1 shrinks the delay on each resend; with no
// floor it reaches 0 ms after a few resends and, with unlimited --max-retries,
// a request the server keeps rejecting is resent in a tight loop (hundreds of
// thousands of resends per second). 100 ms, as for reconnects, bounds each
// request to ~10 resends per second. A base_ms below 100 ms is its own floor,
// so --retry-backoff-ms 0 still means no delay.
static const double MEMTIER_RETRY_BACKOFF_MIN_MS = 100.0;

// Delay in ms before resending a request that was already resent `resends`
// times: base_ms * factor^resends. A factor of 0 keeps the delay at base_ms.
// The factor never takes the delay above 60 s, but a base_ms above 60 s is
// used as is, so the cap only limits the growth. Nor does it take the delay
// below 100 ms, or below base_ms if that is smaller.
static inline double retry_backoff_delay_ms(unsigned int base_ms, double factor, unsigned int resends)
{
    double ms = (double) base_ms;
    if (ms > 0.0 && factor > 0.0 && resends > 0) {
        double limit = ms > MEMTIER_RETRY_BACKOFF_CAP_MS ? ms : MEMTIER_RETRY_BACKOFF_CAP_MS;
        double floor_ms = ms < MEMTIER_RETRY_BACKOFF_MIN_MS ? ms : MEMTIER_RETRY_BACKOFF_MIN_MS;
        ms *= pow(factor, (double) resends);
        // Also catches inf; base_ms > 0 and factor > 0 rule out NaN.
        if (!(ms <= limit))
            ms = limit;
        else if (ms < floor_ms)
            ms = floor_ms;
    }
    return ms;
}

// A -CLUSTERDOWN reply refreshes the topology with CLUSTER SLOTS. Each client
// keeps its own map, so a fixed 1 s interval meant -t 8 -c 50 sent up to 400
// refreshes per second for the whole outage (#834). Within a run of
// CLUSTERDOWN replies, the gap between refreshes doubles from 1 s up to 8 s.
// At 8 s, -t 8 -c 50 sends at most 50 refreshes per second, and a new map is
// seen at most 8 s after it is published, less than the cluster's own failure
// detection (cluster-node-timeout, 15 s by default) takes.
static const long long MEMTIER_CLUSTERDOWN_REFRESH_MIN_US = 1000000LL;
static const long long MEMTIER_CLUSTERDOWN_REFRESH_CAP_US = 8000000LL;

struct clusterdown_refresh_backoff
{
    struct timeval last; // last CLUSTERDOWN-triggered refresh; tv_sec 0 = never
    unsigned int streak; // refreshes since the last non-CLUSTERDOWN reply

    clusterdown_refresh_backoff() : streak(0) { last.tv_sec = last.tv_usec = 0; }

    // Required gap since `last`: 1 s for the first refreshes of a run, then
    // 2 s, 4 s and 8 s. It never drops below 1 s, even after a reset.
    long long gap_us() const
    {
        long long gap = MEMTIER_CLUSTERDOWN_REFRESH_MIN_US;
        for (unsigned int i = 1; i < streak && gap < MEMTIER_CLUSTERDOWN_REFRESH_CAP_US; i++)
            gap *= 2;
        return gap < MEMTIER_CLUSTERDOWN_REFRESH_CAP_US ? gap : MEMTIER_CLUSTERDOWN_REFRESH_CAP_US;
    }

    bool due(const struct timeval &now) const
    {
        if (last.tv_sec == 0) return true;
        long long since = (long long) (now.tv_sec - last.tv_sec) * 1000000LL + (now.tv_usec - last.tv_usec);
        return since >= gap_us();
    }

    void refreshed(const struct timeval &now)
    {
        last = now;
        if (streak < UINT_MAX) streak++;
    }

    // Any other reply shows the cluster serves requests again.
    void reset() { streak = 0; }
};

#endif // MEMTIER_BENCHMARK_RETRY_BACKOFF_H
