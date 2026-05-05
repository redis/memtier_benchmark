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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>

#include "run_stats_types.h"
#include <limits>


one_sec_cmd_stats::one_sec_cmd_stats() :
        m_bytes_rx(0),
        m_bytes_tx(0),
        m_ops(0),
        m_hits(0),
        m_misses(0),
        m_moved(0),
        m_ask(0),
        m_total_latency(0),
        m_avg_latency(0.0),
        m_min_latency(std::numeric_limits<double>::max()),
        m_max_latency(std::numeric_limits<double>::lowest())
{
}


void one_sec_cmd_stats::reset()
{
    m_bytes_rx = 0;
    m_bytes_tx = 0;
    m_ops = 0;
    m_hits = 0;
    m_misses = 0;
    m_moved = 0;
    m_ask = 0;
    m_total_latency = 0;
    m_avg_latency = 0;
    m_max_latency = std::numeric_limits<double>::lowest();
    m_min_latency = std::numeric_limits<double>::max();
    summarized_quantile_values.clear();
}

void one_sec_cmd_stats::merge(const one_sec_cmd_stats &other)
{
    m_bytes_rx += other.m_bytes_rx;
    m_bytes_tx += other.m_bytes_tx;
    m_ops += other.m_ops;
    m_hits += other.m_hits;
    m_misses += other.m_misses;
    m_moved += other.m_moved;
    m_ask += other.m_ask;
    m_total_latency += other.m_total_latency;
    if (m_ops > 0) {
        m_avg_latency = (double) m_total_latency / (double) m_ops / (double) LATENCY_HDR_RESULTS_MULTIPLIER;
    }
    m_max_latency = other.m_max_latency > m_max_latency ? other.m_max_latency : m_max_latency;
    m_min_latency = other.m_min_latency < m_min_latency ? other.m_min_latency : m_min_latency;
}

void one_sec_cmd_stats::summarize_quantiles(safe_hdr_histogram histogram, std::vector<double> sorted_quantiles)
{
    std::vector<int64_t> values(sorted_quantiles.size());
    int result = hdr_value_at_percentiles(histogram, sorted_quantiles.data(), values.data(), sorted_quantiles.size());
    if (result != 0) {
        return;
    }
    for (std::size_t i = 0; i < sorted_quantiles.size(); i++) {
        summarized_quantile_values.push_back(values[i] / static_cast<double>(LATENCY_HDR_RESULTS_MULTIPLIER));
    }
}

void one_sec_cmd_stats::update_op(unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency)
{
    m_bytes_rx += bytes_rx;
    m_bytes_tx += bytes_tx;
    m_ops++;
    m_total_latency += latency;
    const double latency_millis = latency / (double) LATENCY_HDR_RESULTS_MULTIPLIER;
    m_avg_latency = (double) (m_total_latency) / (double) m_ops / (double) LATENCY_HDR_RESULTS_MULTIPLIER;
    m_max_latency = (m_max_latency < latency_millis) ? latency_millis : m_max_latency;
    m_min_latency = (m_min_latency > latency_millis) ? latency_millis : m_min_latency;
}

void one_sec_cmd_stats::update_op(unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency, unsigned int hits,
                                  unsigned int misses)
{
    update_op(bytes_rx, bytes_tx, latency);
    m_hits += hits;
    m_misses += misses;
}

void one_sec_cmd_stats::update_moved_op(unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency)
{
    update_op(bytes_rx, bytes_tx, latency);
    m_moved++;
}

void one_sec_cmd_stats::update_ask_op(unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency)
{
    update_op(bytes_rx, bytes_tx, latency);
    m_ask++;
}

void ar_one_sec_cmd_stats::setup(size_t n_arbitrary_commands)
{
    m_commands.resize(n_arbitrary_commands);
    reset();
}

void ar_one_sec_cmd_stats::reset()
{
    for (size_t i = 0; i < m_commands.size(); i++) {
        m_commands[i].reset();
    }
}

void ar_one_sec_cmd_stats::merge(const ar_one_sec_cmd_stats &other)
{
    for (size_t i = 0; i < m_commands.size(); i++) {
        m_commands[i].merge(other.m_commands[i]);
    }
}

unsigned long int ar_one_sec_cmd_stats::ops()
{
    unsigned long int total_ops = 0;
    for (size_t i = 0; i < m_commands.size(); i++) {
        total_ops += m_commands[i].m_ops;
    }

    return total_ops;
}


unsigned long int ar_one_sec_cmd_stats::bytes()
{
    unsigned long int total_bytes = 0;
    for (size_t i = 0; i < m_commands.size(); i++) {
        total_bytes += m_commands[i].m_bytes_rx;
        total_bytes += m_commands[i].m_bytes_tx;
    }

    return total_bytes;
}

unsigned long long int ar_one_sec_cmd_stats::total_latency()
{
    unsigned long long int latency = 0;
    for (size_t i = 0; i < m_commands.size(); i++) {
        latency += m_commands[i].m_total_latency;
    }

    return latency;
}

size_t ar_one_sec_cmd_stats::size() const
{
    return m_commands.size();
}

///////////////////////////////////////////////////////////////////////////


one_second_stats::one_second_stats(unsigned int second) :
        m_set_cmd(), m_get_cmd(), m_wait_cmd(), m_total_cmd(), m_ar_commands(), m_connection_errors(0)
{
    reset(second);
}

void one_second_stats::setup_arbitrary_commands(size_t n_arbitrary_commands)
{
    m_ar_commands.setup(n_arbitrary_commands);
}

void one_second_stats::reset(unsigned int second)
{
    m_second = second;
    m_get_cmd.reset();
    m_set_cmd.reset();
    m_wait_cmd.reset();
    m_total_cmd.reset();
    m_ar_commands.reset();
    m_connection_errors = 0;
}

void one_second_stats::merge(const one_second_stats &other)
{
    m_get_cmd.merge(other.m_get_cmd);
    m_set_cmd.merge(other.m_set_cmd);
    m_wait_cmd.merge(other.m_wait_cmd);
    m_total_cmd.merge(other.m_total_cmd);
    m_ar_commands.merge(other.m_ar_commands);
    m_connection_errors += other.m_connection_errors;
}

///////////////////////////////////////////////////////////////////////////

totals_cmd::totals_cmd() :
        m_ops_sec(0),
        m_bytes_sec(0),
        m_bytes_sec_rx(0),
        m_bytes_sec_tx(0),
        m_moved_sec(0),
        m_ask_sec(0),
        m_latency(0),
        m_total_latency(0),
        m_ops(0)
{
}

void totals_cmd::add(const totals_cmd &other)
{
    m_ops_sec += other.m_ops_sec;
    m_moved_sec += other.m_moved_sec;
    m_ask_sec += other.m_ask_sec;
    m_bytes_sec += other.m_bytes_sec;
    m_bytes_sec_rx += other.m_bytes_sec_rx;
    m_bytes_sec_tx += other.m_bytes_sec_tx;
    m_latency += other.m_latency;
    m_total_latency += other.m_total_latency;
    m_ops += other.m_ops;
}

void totals_cmd::aggregate_average(size_t stats_size)
{
    m_ops_sec /= stats_size;
    m_moved_sec /= stats_size;
    m_ask_sec /= stats_size;
    m_bytes_sec /= stats_size;
    m_bytes_sec_rx /= stats_size;
    m_bytes_sec_tx /= stats_size;
    m_latency /= stats_size;
}

void totals_cmd::summarize(const one_sec_cmd_stats &other, unsigned long test_duration_usec)
{
    m_ops = other.m_ops;
    m_total_latency = other.m_total_latency;

    m_ops_sec = (double) other.m_ops / test_duration_usec * 1000000;
    if (other.m_ops > 0) {
        m_latency = (double) (other.m_total_latency / other.m_ops) / 1000;
    } else {
        m_latency = 0;
    }
    m_bytes_sec = ((other.m_bytes_rx + other.m_bytes_tx) / 1024.0) / test_duration_usec * 1000000;
    m_bytes_sec_rx = (other.m_bytes_rx / 1024.0) / test_duration_usec * 1000000;
    m_bytes_sec_tx = (other.m_bytes_tx / 1024.0) / test_duration_usec * 1000000;
    m_moved_sec = (double) other.m_moved / test_duration_usec * 1000000;
    m_ask_sec = (double) other.m_ask / test_duration_usec * 1000000;
}

void ar_totals_cmd::setup(size_t n_arbitrary_commands)
{
    m_commands.resize(n_arbitrary_commands);
}

void ar_totals_cmd::add(const ar_totals_cmd &other)
{
    for (size_t i = 0; i < m_commands.size(); i++) {
        m_commands[i].add(other.m_commands[i]);
    }
}

void ar_totals_cmd::aggregate_average(size_t stats_size)
{
    for (size_t i = 0; i < m_commands.size(); i++) {
        m_commands[i].aggregate_average(stats_size);
    }
}

void ar_totals_cmd::summarize(const ar_one_sec_cmd_stats &other, unsigned long test_duration_usec)
{
    for (size_t i = 0; i < m_commands.size(); i++) {
        m_commands[i].summarize(other.at(i), test_duration_usec);
    }
}

size_t ar_totals_cmd::size() const
{
    return m_commands.size();
}

///////////////////////////////////////////////////////////////////////////

totals::totals() :
        m_set_cmd(),
        m_get_cmd(),
        m_wait_cmd(),
        m_total_cmd(),
        m_ar_commands(),
        m_ops_sec(0),
        m_bytes_sec(0),
        m_hits_sec(0),
        m_misses_sec(0),
        m_moved_sec(0),
        m_ask_sec(0),
        m_latency(0),
        m_total_latency(0),
        m_bytes_rx(0),
        m_bytes_tx(0),
        m_ops(0),
        m_connection_errors(0),
        m_connection_errors_sec(0)
{
}

void totals::setup_arbitrary_commands(size_t n_arbitrary_commands)
{
    m_ar_commands.setup(n_arbitrary_commands);
}

void totals::add(const totals &other)
{
    m_set_cmd.add(other.m_set_cmd);
    m_get_cmd.add(other.m_get_cmd);
    m_wait_cmd.add(other.m_wait_cmd);
    m_total_cmd.add(other.m_total_cmd);
    m_ar_commands.add(other.m_ar_commands);

    m_ops_sec += other.m_ops_sec;
    m_hits_sec += other.m_hits_sec;
    m_misses_sec += other.m_misses_sec;
    m_moved_sec += other.m_moved_sec;
    m_ask_sec += other.m_ask_sec;
    m_bytes_sec += other.m_bytes_sec;
    m_bytes_rx += other.m_bytes_rx;
    m_bytes_tx += other.m_bytes_tx;
    m_latency += other.m_latency;
    m_total_latency += other.m_total_latency;
    m_ops += other.m_ops;
    m_connection_errors += other.m_connection_errors;
    m_connection_errors_sec += other.m_connection_errors_sec;

    // aggregate latency data
    hdr_add(latency_histogram, other.latency_histogram);
}

void totals::update_op(unsigned long int bytes_rx, unsigned long int bytes_tx, unsigned int latency)
{
    m_bytes_rx += bytes_rx;
    m_bytes_tx += bytes_tx;
    m_ops++;
    m_latency += latency;
    m_total_latency += latency;
    hdr_record_value_capped(latency_histogram, latency);
}

void totals::update_connection_error()
{
    m_connection_errors++;
}
