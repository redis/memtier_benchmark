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

#ifndef MEMTIER_CONNECTION_STAGE_CLOCK_H
#define MEMTIER_CONNECTION_STAGE_CLOCK_H

// Whether --connection-stage-timeout has run out for a phase that started at
// start_usec (wall-clock microseconds, 0 = not started), and how many whole
// seconds have elapsed. Comparing whole seconds instead would let a timeout
// of N seconds trip after just over N - 1 when the phase starts late in a
// second.
static inline bool connection_stage_timed_out(long long start_usec, long long now_usec, unsigned int timeout_secs,
                                              unsigned int *elapsed_secs)
{
    if (start_usec <= 0) return false;
    long long elapsed = now_usec - start_usec;
    if (elapsed < (long long) timeout_secs * 1000000) return false;
    if (elapsed_secs != NULL) *elapsed_secs = (unsigned int) (elapsed / 1000000);
    return true;
}

#endif // MEMTIER_CONNECTION_STAGE_CLOCK_H
