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

#ifndef MEMTIER_LATENCY_HISTOGRAM_H
#define MEMTIER_LATENCY_HISTOGRAM_H

#include "deps/hdr_histogram/hdr_histogram.h"

// Preserve memtier's latency range: upstream's capped helpers allow zero and
// values below lowest_discernible_value. Keep this policy outside vendored code.
inline bool memtier_record_latency(struct hdr_histogram *h, int64_t value)
{
    return hdr_record_value_capped(h, value < h->lowest_discernible_value ? h->lowest_discernible_value : value);
}

// Atomic recording does not make concurrent histogram iteration/reset safe.
inline bool memtier_record_latency_atomic(struct hdr_histogram *h, int64_t value)
{
    return hdr_record_value_capped_atomic(h, value < h->lowest_discernible_value ? h->lowest_discernible_value : value);
}

#endif
