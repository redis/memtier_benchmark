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

#ifndef _MEMTIER_BENCHMARK_H
#define _MEMTIER_BENCHMARK_H

#include <atomic>
#include <vector>
#include <sys/time.h>
#include <pthread.h>
#include "config_types.h"

#ifdef USE_TLS
#include <openssl/ssl.h>
#endif

// Forward declaration
class statsd_client;

#define LOGLEVEL_ERROR 0
#define LOGLEVEL_DEBUG 1

#define benchmark_debug_log(...) benchmark_log_file_line(LOGLEVEL_DEBUG, __FILE__, __LINE__, __VA_ARGS__)

#define benchmark_error_log(...) benchmark_log(LOGLEVEL_ERROR, __VA_ARGS__)

enum key_pattern_index
{
    key_pattern_set = 0,
    key_pattern_delimiter = 1,
    key_pattern_get = 2
};

enum PROTOCOL_TYPE
{
    PROTOCOL_REDIS_DEFAULT,
    PROTOCOL_RESP2,
    PROTOCOL_RESP3,
    PROTOCOL_MEMCACHE_TEXT,
    PROTOCOL_MEMCACHE_BINARY,
};

// Shared MGET slot cache: built once (lazily, on first topology load) and
// read concurrently by all cluster_client threads.  m_mget_slot_keys is
// identical for every thread — only the per-slot round-robin cursors differ.
struct mget_slot_cache
{
    std::vector<std::vector<unsigned long long> > slot_keys; // [slot] → key indices; read-only after built
    std::atomic<bool> built;
    pthread_mutex_t mutex;

    mget_slot_cache() : built(false) { pthread_mutex_init(&mutex, NULL); }
    ~mget_slot_cache() { pthread_mutex_destroy(&mutex); }

private:
    mget_slot_cache(const mget_slot_cache &);
    mget_slot_cache &operator=(const mget_slot_cache &);
};

struct benchmark_config
{
    const char *server;
    unsigned short port;
    struct server_addr *server_addr;
    const char *unix_socket;
    int resolution;
    enum PROTOCOL_TYPE protocol;
    const char *out_file;
    const char *client_stats;
    unsigned int run_count;
    int debug;
    int show_config;
    int hide_histogram;
    config_quantiles print_percentiles;
    bool print_all_runs;
    bool realtime_latencies;
    int distinct_client_seed;
    int randomize;
    int next_client_idx;
    unsigned long long requests;
    unsigned int clients;
    unsigned int threads;
    unsigned int test_time;
    config_ratio ratio;
    unsigned int pipeline;
    unsigned int data_size;
    unsigned int data_offset;
    bool random_data;
    struct config_range data_size_range;
    config_weight_list data_size_list;
    const char *data_size_pattern;
    struct config_range expiry_range;
    const char *data_import;
    int data_verify;
    int verify_only;
    int generate_keys;
    const char *key_prefix;
    unsigned long long key_minimum;
    unsigned long long key_maximum;
    double key_stddev;
    double key_median;
    double key_zipf_exp;
    const char *key_pattern;
    unsigned int reconnect_interval;
    bool reconnect_on_error;
    unsigned int max_reconnect_attempts;
    double reconnect_backoff_factor;
    // Per-command retry (independent of reconnect_on_error):
    //   retry_on_error      master switch (default: off)
    //   max_retries         -1 = unlimited (default), 0 = disabled even with switch on, N>0 = bounded
    //   retry_backoff_ms    delay between retries, in milliseconds (default 0, immediate)
    //   retry_backoff_factor   exponential multiplier on retry_backoff_ms (default 0.0 = constant)
    //   retry_on_filter     NULL = built-in classifier (retry everything except permanent set);
    //                       non-NULL = comma-list of error-status prefixes to restrict retries to
    //   max_retry_queue     hard cap on per-connection retry queue (0 = pipeline * 4 default)
    bool retry_on_error;
    int max_retries;
    unsigned int retry_backoff_ms;
    double retry_backoff_factor;
    const char *retry_on_filter;
    unsigned int max_retry_queue;
    // When non-NULL, every request that ultimately fails (max_retries exhausted,
    // or permanent error like WRONGTYPE) is appended as a line of CSV to this
    // file. Off by default. Robust on errors: a failure to open or write is
    // logged once and the benchmark continues.
    const char *failed_keys_file;
    unsigned int connection_timeout;
    unsigned int thread_conn_start_min_jitter_micros;
    unsigned int thread_conn_start_max_jitter_micros;
    int multi_key_get;
    struct mget_slot_cache *mget_cache; // NULL unless cluster_mode && multi_key_get > 0
    const char *authenticate;
    int select_db;
    const char *uri;
    bool no_expiry;
    bool resolve_on_connect;
    // WAIT related
    config_ratio wait_ratio;
    config_range num_slaves;
    config_range wait_timeout;
    // JSON additions
    const char *json_out_file;
    bool cluster_mode;
    // When set together with --cluster-mode, every full rotation of --command
    // entries (one logical transactional unit, e.g. WATCH/MULTI/.../EXEC) is
    // pinned to a single shard connection so that keyless commands stay on
    // the same connection as the keyed ones.
    bool transaction;
    struct arbitrary_command_list *arbitrary_commands;
    const char *monitor_input;
    struct monitor_command_list *monitor_commands;
    char monitor_pattern;
    bool command_stats_by_type; // true = aggregate by command type (default), false = per command line
    bool command_miss_tracking; // true = auto (track misses for known shapes), false = off
    double miss_rate_threshold; // warn when miss rate exceeds this fraction (default 0.01 = 1%)
    const char *hdr_prefix;
    unsigned int request_rate;
    unsigned int request_per_interval;
    unsigned int request_interval_microsecond;
    // Client staircase ramp-up
    unsigned int clients_start;
    unsigned int clients_step;
    unsigned int step_duration;
    struct timeval benchmark_start_time;
    // StatsD metrics export
    const char *statsd_host;
    unsigned short statsd_port;
    const char *statsd_prefix;
    const char *statsd_run_label;
    unsigned short graphite_port;
    statsd_client *statsd;
    // SCAN incremental cursor iteration
    bool scan_incremental_iteration;
    unsigned int scan_incremental_max_iterations;
    arbitrary_command *scan_continuation_command;
#ifdef USE_TLS
    bool tls;
    const char *tls_cert;
    const char *tls_key;
    const char *tls_cacert;
    bool tls_skip_verify;
    const char *tls_sni;
    int tls_protocols;
    SSL_CTX *openssl_ctx;
    // Negotiated TLS protocol/cipher, captured once on the first completed
    // handshake (static OpenSSL strings; NULL until then). Written under a
    // call_once on a worker thread, read on the main thread post-join.
    const char *tls_negotiated_version;
    const char *tls_negotiated_cipher;
#endif
};


extern void benchmark_log_file_line(int level, const char *filename, unsigned int line, const char *fmt, ...);
extern void benchmark_log(int level, const char *fmt, ...);
bool is_redis_protocol(enum PROTOCOL_TYPE type);

#endif /* _MEMTIER_BENCHMARK_H */
