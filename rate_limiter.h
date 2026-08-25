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

#ifndef MEMTIER_BENCHMARK_RATE_LIMITER_H
#define MEMTIER_BENCHMARK_RATE_LIMITER_H

#include <stdint.h>

// Spread logical clients evenly over one refill interval. Multiplication is
// widened before division because both inputs originate in 32-bit CLI fields.
static inline unsigned int calculate_rate_limit_phase(unsigned int interval_microseconds,
                                                      unsigned long long client_index, unsigned long long total_clients)
{
    if (interval_microseconds == 0 || total_clients == 0) return 0;
    client_index %= total_clients;
    return static_cast<unsigned int>((static_cast<uint64_t>(interval_microseconds) * client_index) / total_clients);
}

#endif // MEMTIER_BENCHMARK_RATE_LIMITER_H
