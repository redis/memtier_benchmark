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

#ifndef MEMTIER_BENCHMARK_RUN_STATS_H
#define MEMTIER_BENCHMARK_RUN_STATS_H

#include <stdlib.h>
#include <stdio.h>
#include <atomic>
#include <map>
#include <vector>
#include <string>
#include <pthread.h>

// Mutex wrapper with correct copy/move semantics: copies always initialize a
// fresh mutex rather than duplicating mutex state (which is undefined behavior).
// This allows run_stats to remain copyable without a hand-written copy constructor.
struct reinit_mutex_t
{
    mutable pthread_mutex_t mtx;
    reinit_mutex_t() { pthread_mutex_init(&mtx, NULL); }
    ~reinit_mutex_t() { pthread_mutex_destroy(&mtx); }
    reinit_mutex_t(const reinit_mutex_t &) { pthread_mutex_init(&mtx, NULL); }
    reinit_mutex_t &operator=(const reinit_mutex_t &) { return *this; }
};

#include "memtier_benchmark.h"
#include "run_stats_types.h"
#include "JSON_handler.h"
#include "deps/hdr_histogram/hdr_histogram.h"
#include "deps/hdr_histogram/hdr_histogram_log.h"


inline long long int ts_diff(struct timeval a, struct timeval b)
{
    unsigned long long aval = a.tv_sec * 1000000 + a.tv_usec;
    unsigned long long bval = b.tv_sec * 1000000 + b.tv_usec;

    return bval - aval;
}

enum tabel_el_type
{
    string_el,
    double_el
};

struct table_el
{
    tabel_el_type type;
    std::string format;
    std::string str_value;
    double double_value;

    table_el *init_str(std::string fmt, std::string val)
    {
        type = string_el;
        format = fmt;
        str_value = val;
        return this;
    }

    table_el *init_double(std::string fmt, double val)
    {
        type = double_el;
        format = fmt;
        double_value = val;
        return this;
    }
};

struct table_column
{
    table_column() {}
    table_column(unsigned int col_size) : column_size(col_size) {}

    unsigned int column_size;
    std::vector<table_el> elements;
};

class output_table
{
private:
    std::vector<table_column> columns;

public:
    void print_header(FILE *out, const char *header);
    void add_column(table_column &col);
    void print(FILE *out, const char *header);
};

// Per-endpoint snapshot (one entry per shard_connection at run end). Built
// in main thread after threads.join() but before the thread / client groups
// are destroyed, then merged into run_stats so it survives to JSON time.
struct endpoint_snapshot
{
    std::string addr;              // host:port
    std::string role;              // "primary" | "replica" | ""
    int conn_id;                   // shard_connection's vector index (NOT a stable
                                   // cluster shard identity); -1 when unknown
    unsigned long long routed_ops; // user-level requests sent on this conn
    double avg_latency_us;         // EWMA in microseconds (0.0 when no samples)
    unsigned int latency_samples;  // 0 when never warm
    // Per-endpoint MOVED/ASK and connection-error counters are NOT emitted
    // here; they live in the top-level cluster_summary aggregate.
    endpoint_snapshot() : conn_id(-1), routed_ops(0), avg_latency_us(0.0), latency_samples(0) {}
};

// Aggregated --read-preference routing counters (Ops from Primary / Ops from
// Replica). One entry per arbitrary-command index, plus a built-in GET slot.
// Merged across clients in main thread.
struct read_routing_summary
{
    unsigned long long ops_from_primary;
    unsigned long long ops_from_replica;
    read_routing_summary() : ops_from_primary(0), ops_from_replica(0) {}
};

// Structure to hold aggregated stats by command type
struct aggregated_command_type_stats
{
    std::string command_type;                        // e.g., "SET", "GET"
    totals_cmd stats;                                // aggregated totals
    safe_hdr_histogram latency_hist;                 // merged histogram
    std::vector<size_t> command_indices;             // indices of commands with this type
    std::vector<one_sec_cmd_stats> per_second_stats; // aggregated per-second stats for JSON time series
    unsigned long long total_hits;
    unsigned long long total_misses;
    aggregated_command_type_stats() : total_hits(0), total_misses(0) {}
};

class run_stats
{
protected:
    friend bool one_second_stats_predicate(const one_second_stats &a, const one_second_stats &b);

    benchmark_config *m_config;

    struct timeval m_start_time;
    struct timeval m_end_time;
    // Atomic flag set after m_start_time is fully written; guards cross-thread reads.
    // std::atomic is not copyable, but run_stats needs to be (stored in std::vector).
    // Copies occur only after threads join, so relaxed copy semantics are safe.
    struct copyable_atomic_bool
    {
        std::atomic<bool> flag;
        copyable_atomic_bool(bool v = false) : flag(v) {}
        copyable_atomic_bool(const copyable_atomic_bool &o) : flag(o.flag.load(std::memory_order_relaxed)) {}
        copyable_atomic_bool &operator=(const copyable_atomic_bool &o)
        {
            flag.store(o.flag.load(std::memory_order_relaxed), std::memory_order_relaxed);
            return *this;
        }
    } m_started;
    bool m_interrupted;

    totals m_totals;

    std::list<one_second_stats> m_stats;
    std::vector<double> quantiles_list;

    // current second stats ( appended to m_stats and reset every second )
    one_second_stats m_cur_stats;

    safe_hdr_histogram m_get_latency_histogram;
    safe_hdr_histogram m_set_latency_histogram;
    safe_hdr_histogram m_wait_latency_histogram;
    std::vector<safe_hdr_histogram> m_ar_commands_latency_histograms;
    safe_hdr_histogram m_totals_latency_histogram;

    // instantaneous command stats ( used in the per second latencies )
    safe_hdr_histogram inst_m_get_latency_histogram;
    safe_hdr_histogram inst_m_set_latency_histogram;
    safe_hdr_histogram inst_m_wait_latency_histogram;
    std::vector<safe_hdr_histogram> inst_m_ar_commands_latency_histograms;
    safe_hdr_histogram inst_m_totals_latency_histogram;

    // Protects inst_m_totals_latency_histogram against concurrent reset/aggregation reads.
    // Worker threads use hdr_record_value_capped_atomic() for lock-free writes;
    // this mutex serializes hdr_reset and hdr_add from the main thread.
    reinit_mutex_t m_inst_histogram_mutex;

    // Cumulative hits/misses bookkeeping for arbitrary commands. Indexed by
    // arbitrary command index. Per-key vectors are sized to the spec-resolved
    // key count for that command (or 1 when the command has no spec key
    // positions); empty otherwise.
    struct arbitrary_misses_total
    {
        unsigned long long total_hits;
        unsigned long long total_misses;
        std::vector<unsigned long long> per_key_hits;
        std::vector<unsigned long long> per_key_misses;
        arbitrary_misses_total() : total_hits(0), total_misses(0) {}
    };
    std::vector<arbitrary_misses_total> m_arbitrary_misses;

    void roll_cur_stats(struct timeval *ts);

public:
    run_stats(benchmark_config *config);
    void setup_arbitrary_commands(size_t n_arbitrary_commands);
    void set_start_time(struct timeval *start_time);
    void set_end_time(struct timeval *end_time);
    void set_interrupted(bool interrupted) { m_interrupted = interrupted; }
    bool get_interrupted() const { return m_interrupted; }

    void update_get_op(struct timeval *ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency,
                       unsigned int hits, unsigned int misses);
    void update_set_op(struct timeval *ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency);
    void update_connection_error(struct timeval *ts);

    // Retry/error counters. Bumped by client::handle_response on the retry
    // path. Lock-free relaxed bumps on a per-thread instance; aggregated when
    // run_stats are merged across threads.
    void inc_retry_attempt() { m_totals.m_retry_attempts++; }
    void inc_retried_op() { m_totals.m_retried_ops++; }
    void inc_error() { m_totals.m_errors++; }

    unsigned long int get_total_retry_attempts() const { return m_totals.m_retry_attempts; }
    unsigned long int get_total_retried_ops() const { return m_totals.m_retried_ops; }
    unsigned long int get_total_errors() const { return m_totals.m_errors; }

    void update_moved_get_op(struct timeval *ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency);
    void update_moved_set_op(struct timeval *ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency);
    void update_moved_arbitrary_op(struct timeval *ts, unsigned int bytes_rx, unsigned int bytes_tx,
                                   unsigned int latency, size_t arbitrary_index);

    void update_ask_get_op(struct timeval *ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency);
    void update_ask_set_op(struct timeval *ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency);
    void update_ask_arbitrary_op(struct timeval *ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency,
                                 size_t arbitrary_index);

    void update_wait_op(struct timeval *ts, unsigned int latency);
    void update_arbitrary_op(struct timeval *ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency,
                             size_t arbitrary_index);

    // Records hits/misses for a single arbitrary-command response. Aggregates
    // are mirrored into the per-second m_ar_commands slot (the existing 5-arg
    // update_op overload) so per-second JSON time series and the global
    // Hits/sec, Misses/sec totals stay in sync. The per_key_hit vector size
    // must equal num_key_buckets for that command (one entry per spec key
    // position; values are 1=hit, 0=miss). Safe to call with an empty vector
    // when shape doesn't carry per-position info.
    void update_arbitrary_op_misses(size_t arbitrary_index, unsigned int hits, unsigned int misses,
                                    const std::vector<bool> &per_key_hit);

    // ---------------------------------------------------------------------
    // Read-preference observability (Step 2f).
    // Snapshot the per-endpoint state at run end so the JSON dump can reflect
    // it after the worker threads are gone. Both fields are populated by
    // run_benchmark() right after the join loop; print_json() consumes them.
    // ---------------------------------------------------------------------
    std::vector<endpoint_snapshot> m_endpoint_snapshots;
    std::vector<read_routing_summary> m_arbitrary_read_routing; // per arbitrary-cmd index
    read_routing_summary m_get_read_routing;                    // built-in GET aggregate

    // Aggregator: fold the per-endpoint snapshot from one client's
    // shard_connections into m_endpoint_snapshots. Entries are coalesced by
    // (addr, role) so the JSON is at most O(distinct endpoints) regardless
    // of thread count. Latency is op-weighted average across threads.
    void absorb_endpoint(const endpoint_snapshot &snap);
    // Aggregator for read-routing counters.
    void absorb_arbitrary_routing(size_t arbitrary_index, unsigned long long primary, unsigned long long replica);
    void absorb_builtin_get_routing(unsigned long long primary, unsigned long long replica);

    void aggregate_average(const std::vector<run_stats> &all_stats);
    void summarize(totals &result) const;
    void summarize_current_second();
    void merge(const run_stats &other, int iteration);
    std::vector<one_sec_cmd_stats> get_one_sec_cmd_stats_get();
    std::vector<one_sec_cmd_stats> get_one_sec_cmd_stats_set();
    std::vector<one_sec_cmd_stats> get_one_sec_cmd_stats_wait();
    std::vector<one_sec_cmd_stats> get_one_sec_cmd_stats_totals();
    std::vector<one_sec_cmd_stats> get_one_sec_cmd_stats_arbitrary_command(unsigned int pos);
    std::vector<unsigned int> get_one_sec_cmd_stats_timestamp();
    void save_csv_one_sec(FILE *f, unsigned long int &total_get_ops, unsigned long int &total_set_ops,
                          unsigned long int &total_wait_ops);

    // Safely copy instantaneous total latency histogram into target under mutex.
    // Use this instead of a raw pointer getter to avoid data races with worker threads.
    void copy_inst_histogram(hdr_histogram *target) const;
    void save_csv_one_sec_cluster(FILE *f);
    void save_csv_set_get_commands(FILE *f, bool cluster_mode);
    void save_csv_arbitrary_commands_one_sec(FILE *f, arbitrary_command_list &command_list,
                                             std::vector<unsigned long int> &total_arbitrary_commands_ops);
    void save_csv_arbitrary_commands(FILE *f, arbitrary_command_list &command_list);
    bool save_hdr_percentiles_print_format(struct hdr_histogram *hdr, char *filename);
    bool save_hdr_log_format(struct hdr_histogram *hdr, char *filename, char *header);
    bool save_hdr_full_run(benchmark_config *config, int run_number);
    bool save_hdr_set_command(benchmark_config *config, int run_number);
    bool save_hdr_get_command(benchmark_config *config, int run_number);
    bool save_hdr_arbitrary_commands(benchmark_config *config, int run_number);

    bool save_csv(const char *filename, benchmark_config *config);
    void debug_dump(void);

    // function to handle the results output
    bool print_arbitrary_commands_results();

    // Build aggregated stats by command type (e.g., SET, GET) from per-command stats
    std::vector<aggregated_command_type_stats> build_aggregated_command_stats(arbitrary_command_list &command_list);

    void print_type_column(output_table &table, arbitrary_command_list &command_list,
                           const std::vector<aggregated_command_type_stats> *aggregated = nullptr);
    void print_ops_sec_column(output_table &table,
                              const std::vector<aggregated_command_type_stats> *aggregated = nullptr);
    void print_hits_sec_column(output_table &table,
                               const std::vector<aggregated_command_type_stats> *aggregated = nullptr);
    void print_missess_sec_column(output_table &table,
                                  const std::vector<aggregated_command_type_stats> *aggregated = nullptr);
    void print_moved_sec_column(output_table &table,
                                const std::vector<aggregated_command_type_stats> *aggregated = nullptr);
    void print_ask_sec_column(output_table &table,
                              const std::vector<aggregated_command_type_stats> *aggregated = nullptr);
    void print_avg_latency_column(output_table &table,
                                  const std::vector<aggregated_command_type_stats> *aggregated = nullptr);
    void print_quantile_latency_column(output_table &table, double quantile, char *label,
                                       const std::vector<aggregated_command_type_stats> *aggregated = nullptr);
    void print_kb_sec_column(output_table &table,
                             const std::vector<aggregated_command_type_stats> *aggregated = nullptr);
    void print_json(json_handler *jsonhandler, arbitrary_command_list &command_list, bool cluster_mode,
                    const std::vector<aggregated_command_type_stats> *aggregated = nullptr);
    void print_histogram(FILE *out, json_handler *jsonhandler, arbitrary_command_list &command_list,
                         const std::vector<aggregated_command_type_stats> *aggregated = nullptr);
    void print(FILE *file, benchmark_config *config, const char *header = NULL, json_handler *jsonhandler = NULL);

    unsigned int get_duration(void);
    unsigned long int get_duration_usec(void);
    unsigned long int get_total_bytes(void);
    unsigned long int get_total_ops(void);
    double get_total_latency(void);
    unsigned long int get_total_connection_errors(void);

    // Cumulative hits/misses on GET ops since the run started. Reads scalar
    // counters in m_totals incremented per-op by the owning worker. Same
    // benign-race pattern as get_total_ops()/get_total_bytes() — used only
    // for live progress display.
    unsigned long int get_total_hits(void);
    unsigned long int get_total_misses(void);

    // Returns true if set_start_time() was called, indicating the client
    // produced (or was ready to produce) meaningful stats data.
    bool has_started(void) const { return m_started.flag.load(std::memory_order_acquire); }
};

#endif // MEMTIER_BENCHMARK_RUN_STATS_H
