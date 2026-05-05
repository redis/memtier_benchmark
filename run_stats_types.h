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

#ifndef MEMTIER_BENCHMARK_RUN_STATS_TYPES_H
#define MEMTIER_BENCHMARK_RUN_STATS_TYPES_H

#define LATENCY_HDR_MIN_VALUE 10
#define LATENCY_HDR_MAX_VALUE 6000000000##LL
#define LATENCY_HDR_SIGDIGTS 3
#define LATENCY_HDR_SEC_MIN_VALUE 10
#define LATENCY_HDR_SEC_MAX_VALUE 600000000##LL
#define LATENCY_HDR_SEC_SIGDIGTS 2
#define LATENCY_HDR_RESULTS_MULTIPLIER 1000
#define LATENCY_HDR_GRANULARITY 10

#include "deps/hdr_histogram/hdr_histogram.h"
#include "memtier_benchmark.h"

// A wrapper around hdr_histogram that handles allocation, deallocation
// and deep copy.
//
// TODO: In the future we may want to consider extending this wrapper
// further to expose all hdr functions as native methods and avoid the
// casting operator completely.
class safe_hdr_histogram
{
    hdr_histogram *m_hdr;

public:
    safe_hdr_histogram()
    {
        hdr_init(LATENCY_HDR_MIN_VALUE,     // Minimum value
                 LATENCY_HDR_SEC_MAX_VALUE, // Maximum value
                 LATENCY_HDR_SEC_SIGDIGTS,  // Number of significant figures
                 &m_hdr);
    }

    virtual ~safe_hdr_histogram() { hdr_close(m_hdr); }

    safe_hdr_histogram(const safe_hdr_histogram &other)
    {
        hdr_init(LATENCY_HDR_MIN_VALUE,     // Minimum value
                 LATENCY_HDR_SEC_MAX_VALUE, // Maximum value
                 LATENCY_HDR_SEC_SIGDIGTS,  // Number of significant figures
                 &m_hdr);
        hdr_add(m_hdr, other.m_hdr);
    }

    safe_hdr_histogram &operator=(const safe_hdr_histogram &other)
    {
        if (this != &other) {
            hdr_reset(m_hdr);
            hdr_add(m_hdr, other.m_hdr);
        }
        return *this;
    }

    // Cast safe_hdr_historgram to hdr_histogram to make it transparent
    operator hdr_histogram *() const { return m_hdr; }
};

class one_sec_cmd_stats
{
public:
    unsigned long int m_bytes_rx;
    unsigned long int m_bytes_tx;
    unsigned long int m_ops;
    unsigned int m_hits;
    unsigned int m_misses;
    unsigned int m_moved;
    unsigned int m_ask;
    unsigned long long int m_total_latency;
    std::vector<double> summarized_quantile_values;
    double m_avg_latency;
    double m_min_latency;
    double m_max_latency;
    one_sec_cmd_stats();
    void reset();
    void merge(const one_sec_cmd_stats &other);
    /**
     * Summarizes quantiles from the given histogram.
     *
     * @param histogram The histogram from which quantile values are extracted.
     * @param sorted_quantiles A sorted (ascending order) vector of quantiles for which values will be computed.
     *                         The caller must ensure the vector is sorted from smallest to largest.
     */
    void summarize_quantiles(safe_hdr_histogram histogram, std::vector<double> sorted_quantiles);
    void update_op(unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency);
    void update_op(unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency, unsigned int hits,
                   unsigned int misses);
    void update_moved_op(unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency);
    void update_ask_op(unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency);
};

class one_second_stats; // forward declaration

class ar_one_sec_cmd_stats
{
public:
    ar_one_sec_cmd_stats() { ; }
    void setup(size_t n_arbitrary_commands);
    void reset();
    void merge(const ar_one_sec_cmd_stats &other);
    unsigned long int ops();
    unsigned long int bytes();
    unsigned long long int total_latency();
    size_t size() const;
    one_sec_cmd_stats &at(std::size_t idx) { return m_commands.at(idx); }
    const one_sec_cmd_stats &at(std::size_t idx) const { return m_commands.at(idx); }

    // array subscript operator
    one_sec_cmd_stats &operator[](std::size_t idx) { return m_commands[idx]; }
    const one_sec_cmd_stats &operator[](std::size_t idx) const { return m_commands[idx]; }

    std::vector<one_sec_cmd_stats> m_commands;
};

class one_second_stats
{
public:
    unsigned int m_second; // from start of test
    one_sec_cmd_stats m_set_cmd;
    one_sec_cmd_stats m_get_cmd;
    one_sec_cmd_stats m_wait_cmd;
    one_sec_cmd_stats m_total_cmd;
    ar_one_sec_cmd_stats m_ar_commands;
    unsigned int m_connection_errors;
    one_second_stats(unsigned int second);
    void setup_arbitrary_commands(size_t n_arbitrary_commands);
    void reset(unsigned int second);
    void summarize();
    void merge(const one_second_stats &other);
};

class totals_cmd
{
public:
    double m_ops_sec;
    double m_bytes_sec;
    double m_bytes_sec_rx;
    double m_bytes_sec_tx;
    double m_moved_sec;
    double m_ask_sec;
    double m_latency;
    unsigned long long int m_total_latency;
    unsigned long int m_ops;
    totals_cmd();
    void add(const totals_cmd &other);
    void aggregate_average(size_t stats_size);
    void summarize(const one_sec_cmd_stats &other, unsigned long test_duration_usec);
};

class ar_totals_cmd
{
public:
    ar_totals_cmd() { ; }
    void setup(size_t n_arbitrary_commands);
    void add(const ar_totals_cmd &other);
    void aggregate_average(size_t stats_size);
    void summarize(const ar_one_sec_cmd_stats &other, unsigned long test_duration_usec);
    size_t size() const;

    totals_cmd &at(std::size_t idx) { return m_commands.at(idx); }
    const totals_cmd &at(std::size_t idx) const { return m_commands.at(idx); }

    // array subscript operator
    totals_cmd &operator[](std::size_t idx) { return m_commands[idx]; }
    const totals_cmd &operator[](std::size_t idx) const { return m_commands[idx]; }

    std::vector<totals_cmd> m_commands;
};

class totals
{
public:
    totals_cmd m_set_cmd;
    totals_cmd m_get_cmd;
    totals_cmd m_wait_cmd;
    totals_cmd m_total_cmd;
    ar_totals_cmd m_ar_commands;
    safe_hdr_histogram latency_histogram;
    double m_ops_sec;
    double m_bytes_sec;
    double m_bytes_sec_rx;
    double m_bytes_sec_tx;
    double m_hits_sec;
    double m_misses_sec;
    double m_moved_sec;
    double m_ask_sec;
    double m_latency;
    unsigned long long int m_total_latency;
    // number of bytes received
    unsigned long int m_bytes_rx;
    // number of bytes sent
    unsigned long int m_bytes_tx;
    unsigned long int m_ops;
    unsigned long int m_connection_errors;
    double m_connection_errors_sec;
    totals();
    void setup_arbitrary_commands(size_t n_arbitrary_commands);
    void add(const totals &other);
    void update_op(unsigned long int bytes_rx, unsigned long int bytes_tx, unsigned int latency);
    void update_connection_error();
};


#endif // MEMTIER_BENCHMARK_RUN_STATS_TYPES_H
