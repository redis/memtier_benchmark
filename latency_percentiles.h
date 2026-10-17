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

#ifndef MEMTIER_LATENCY_PERCENTILES_H
#define MEMTIER_LATENCY_PERCENTILES_H

#include <algorithm>
#include <cmath>
#include <numeric>
#include <vector>
#include "deps/hdr_histogram/hdr_histogram.h"

// Batch a configured percentile list while preserving the scalar query's
// ordering, p0 and empty-histogram behavior. Reuse one instance between ticks;
// calculate() does not allocate. The caller owns a stable histogram snapshot.
class latency_percentiles
{
    std::vector<double> m_sorted_quantiles;
    std::vector<size_t> m_order;
    std::vector<int64_t> m_sorted_values;
    std::vector<int64_t> m_values;
    bool m_finite;

public:
    explicit latency_percentiles(const std::vector<double> &quantiles) :
            m_sorted_quantiles(quantiles),
            m_order(quantiles.size()),
            m_sorted_values(quantiles.size()),
            m_values(quantiles.size()),
            m_finite(true)
    {
        std::iota(m_order.begin(), m_order.end(), 0);
        for (double p : quantiles)
            m_finite = m_finite && std::isfinite(p);
        // Keep legacy scalar handling of non-finite inputs; never sort NaNs.
        if (!m_finite) return;
        std::stable_sort(m_order.begin(), m_order.end(),
                         [&quantiles](size_t a, size_t b) { return quantiles[a] < quantiles[b]; });
        for (size_t i = 0; i < m_order.size(); ++i)
            m_sorted_quantiles[i] = quantiles[m_order[i]];
    }

    const std::vector<int64_t> &values() const { return m_values; }

    const std::vector<int64_t> &calculate(const hdr_histogram *histogram)
    {
        if (m_values.empty()) return m_values;
        // Upstream batch queries return rank placeholders on an empty histogram,
        // unlike scalar queries. Preserve existing live/final output semantics.
        if (!m_finite || hdr_total_count(histogram) == 0) {
            for (size_t i = 0; i < m_order.size(); ++i)
                m_values[m_order[i]] = hdr_value_at_percentile(histogram, m_sorted_quantiles[i]);
            return m_values;
        }
        hdr_value_at_percentiles(histogram, m_sorted_quantiles.data(), m_sorted_values.data(), m_sorted_values.size());
        for (size_t i = 0; i < m_order.size(); ++i) {
            // Batch returns the upper equivalent for p0; scalar returns lower.
            m_values[m_order[i]] = m_sorted_quantiles[i] == 0.0
                                       ? hdr_lowest_equivalent_value(histogram, m_sorted_values[i])
                                       : m_sorted_values[i];
        }
        return m_values;
    }
};

#endif
