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

#ifndef MEMTIER_BENCHMARK_KEYLIST_GROWTH_H
#define MEMTIER_BENCHMARK_KEYLIST_GROWTH_H

#include <stddef.h>
#include <stdint.h>

// New size for keylist's key buffer: cur doubled until it holds needed
// bytes.  If one more doubling would overflow size_t, needed itself is
// returned, so the loop always ends and the result is always >= needed.
static inline size_t keylist_grow_size(size_t cur, size_t needed)
{
    size_t size = cur ? cur : 1;
    while (size < needed) {
        if (size > SIZE_MAX / 2) return needed;
        size *= 2;
    }
    return size;
}

#endif // MEMTIER_BENCHMARK_KEYLIST_GROWTH_H
