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

#include <vector>
#include "config_types.h"

#ifdef USE_TLS
#include <openssl/ssl.h>
#endif

// Forward declaration
class statsd_client;

#define LOGLEVEL_ERROR 0
#define LOGLEVEL_DEBUG 1

#define benchmark_debug_log(...) \
    benchmark_log_file_line(LOGLEVEL_DEBUG, __FILE__, __LINE__, __VA_ARGS__)

#define benchmark_error_log(...) \
    benchmark_log(LOGLEVEL_ERROR, __VA_ARGS__)

enum key_pattern_index {
    key_pattern_set       = 0,
    key_pattern_delimiter = 1,
    key_pattern_get       = 2
};

enum PROTOCOL_TYPE {
    PROTOCOL_REDIS_DEFAULT,
    PROTOCOL_RESP2,
    PROTOCOL_RESP3,
    PROTOCOL_MEMCACHE_TEXT,
    PROTOCOL_MEMCACHE_BINARY,
};

struct benchmark_config {
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
    unsigned int connection_timeout;
    unsigned int thread_conn_start_min_jitter_micros;
    unsigned int thread_conn_start_max_jitter_micros;
    int multi_key_get;
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
    struct arbitrary_command_list* arbitrary_commands;
    const char *hdr_prefix;
    unsigned int request_rate;
    unsigned int request_per_interval;
    unsigned int request_interval_microsecond;
    // StatsD metrics export
    const char *statsd_host;
    unsigned short statsd_port;
    const char *statsd_prefix;
    const char *statsd_run_label;
    statsd_client *statsd;
#ifdef USE_TLS
    bool tls;
    const char *tls_cert;
    const char *tls_key;
    const char *tls_cacert;
    bool tls_skip_verify;
    const char *tls_sni;
    int tls_protocols;
    SSL_CTX *openssl_ctx;
#endif
};


extern void benchmark_log_file_line(int level, const char *filename, unsigned int line, const char *fmt, ...);
extern void benchmark_log(int level, const char *fmt, ...);
bool is_redis_protocol(enum PROTOCOL_TYPE type);

#endif /* _MEMTIER_BENCHMARK_H */
