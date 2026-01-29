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

// Define _XOPEN_SOURCE before including system headers for ucontext.h on macOS
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 600
#endif

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "version.h"

#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>  // For strcasecmp() on POSIX systems
#include <stdarg.h>
#include <limits.h>
#include <getopt.h>
#include <assert.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <signal.h>
#include <execinfo.h>
#include <ucontext.h>
#include <time.h>
#include <sys/utsname.h>
#include <dirent.h>
#include <event2/event.h>

#ifdef USE_TLS
#include <openssl/crypto.h>
#include <openssl/conf.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/rand.h>

#define REDIS_TLS_PROTO_TLSv1       (1<<0)
#define REDIS_TLS_PROTO_TLSv1_1     (1<<1)
#define REDIS_TLS_PROTO_TLSv1_2     (1<<2)
#define REDIS_TLS_PROTO_TLSv1_3     (1<<3)

/* Use safe defaults */
#ifdef TLS1_3_VERSION
#define REDIS_TLS_PROTO_DEFAULT     (REDIS_TLS_PROTO_TLSv1_2|REDIS_TLS_PROTO_TLSv1_3)
#else
#define REDIS_TLS_PROTO_DEFAULT     (REDIS_TLS_PROTO_TLSv1_2)
#endif

#endif

#include <cstring>
#include <stdexcept>
#include <atomic>

#include "client.h"
#include "JSON_handler.h"
#include "obj_gen.h"
#include "memtier_benchmark.h"
#include "statsd.h"


static int log_level = 0;

// Global flag for signal handling
static volatile sig_atomic_t g_interrupted = 0;

// Forward declarations
struct cg_thread;
static void print_client_list(FILE* fp, int pid, const char* timestr);
static void print_all_threads_stack_trace(FILE* fp, int pid, const char* timestr);

// Global pointer to threads for crash handler access
static std::vector<cg_thread*>* g_threads = NULL;

// Signal handler for Ctrl+C
static void sigint_handler(int signum)
{
    (void)signum;  // unused parameter
    g_interrupted = 1;
}

// Crash handler - prints stack trace and other debugging information
static void crash_handler(int sig, siginfo_t *info, void *secret)
{
    (void)secret;  // unused parameter
    struct tm *tm;
    time_t now;
    char timestr[64];

    // Get current time
    now = time(NULL);
    tm = localtime(&now);
    strftime(timestr, sizeof(timestr), "%d %b %Y %H:%M:%S", tm);

    // Print crash header
    fprintf(stderr, "\n\n=== MEMTIER_BENCHMARK BUG REPORT START: Cut & paste starting from here ===\n");
    fprintf(stderr, "[%d] %s # memtier_benchmark crashed by signal: %d\n", getpid(), timestr, sig);

    // Print signal information
    const char *signal_name = "UNKNOWN";
    switch(sig) {
        case SIGSEGV: signal_name = "SIGSEGV"; break;
        case SIGBUS: signal_name = "SIGBUS"; break;
        case SIGFPE: signal_name = "SIGFPE"; break;
        case SIGILL: signal_name = "SIGILL"; break;
        case SIGABRT: signal_name = "SIGABRT"; break;
    }
    fprintf(stderr, "[%d] %s # Crashed running signal <%s>\n", getpid(), timestr, signal_name);

    if (info) {
        fprintf(stderr, "[%d] %s # Signal code: %d\n", getpid(), timestr, info->si_code);
        fprintf(stderr, "[%d] %s # Fault address: %p\n", getpid(), timestr, info->si_addr);
    }

    // Print stack trace for all threads
    print_all_threads_stack_trace(stderr, getpid(), timestr);

    // Print system information
    fprintf(stderr, "\n[%d] %s # --- INFO OUTPUT\n", getpid(), timestr);

    struct utsname name;
    if (uname(&name) == 0) {
        fprintf(stderr, "[%d] %s # os:%s %s %s\n", getpid(), timestr, name.sysname, name.release, name.machine);
    }

    fprintf(stderr, "[%d] %s # memtier_version:%s\n", getpid(), timestr, PACKAGE_VERSION);
    fprintf(stderr, "[%d] %s # memtier_git_sha1:%s\n", getpid(), timestr, MEMTIER_GIT_SHA1);
    fprintf(stderr, "[%d] %s # memtier_git_dirty:%s\n", getpid(), timestr, MEMTIER_GIT_DIRTY);

#if defined(__x86_64__) || defined(_M_X64)
    fprintf(stderr, "[%d] %s # arch_bits:64\n", getpid(), timestr);
#elif defined(__i386__) || defined(_M_IX86)
    fprintf(stderr, "[%d] %s # arch_bits:32\n", getpid(), timestr);
#elif defined(__aarch64__)
    fprintf(stderr, "[%d] %s # arch_bits:64\n", getpid(), timestr);
#elif defined(__arm__)
    fprintf(stderr, "[%d] %s # arch_bits:32\n", getpid(), timestr);
#else
    fprintf(stderr, "[%d] %s # arch_bits:unknown\n", getpid(), timestr);
#endif

#ifdef __GNUC__
    fprintf(stderr, "[%d] %s # gcc_version:%d.%d.%d\n", getpid(), timestr,
            __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__);
#endif

    fprintf(stderr, "[%d] %s # libevent_version:%s\n", getpid(), timestr, event_get_version());

#ifdef USE_TLS
    fprintf(stderr, "[%d] %s # openssl_version:%s\n", getpid(), timestr, OPENSSL_VERSION_TEXT);
#endif

    // Print client connection information
    print_client_list(stderr, getpid(), timestr);

    fprintf(stderr, "[%d] %s # For more information, please check the core dump if available.\n", getpid(), timestr);
    fprintf(stderr, "[%d] %s # To enable core dumps: ulimit -c unlimited\n", getpid(), timestr);
    fprintf(stderr, "[%d] %s # Core pattern: /proc/sys/kernel/core_pattern\n", getpid(), timestr);

    fprintf(stderr, "\n=== MEMTIER_BENCHMARK BUG REPORT END. Make sure to include from START to END. ===\n\n");
    fprintf(stderr, "       Please report this bug by opening an issue on github.com/RedisLabs/memtier_benchmark\n\n");

    // Remove the handler and re-raise the signal to generate core dump
    struct sigaction act;
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
    act.sa_handler = SIG_DFL;
    sigaction(sig, &act, NULL);
    raise(sig);
}

// Setup crash handlers
static void setup_crash_handlers(void)
{
    struct sigaction act;

    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND | SA_SIGINFO;
    act.sa_sigaction = crash_handler;

    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGBUS, &act, NULL);
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGILL, &act, NULL);
    sigaction(SIGABRT, &act, NULL);
}

void benchmark_log_file_line(int level, const char *filename, unsigned int line, const char *fmt, ...)
{
    if (level > log_level)
        return;

    va_list args;
    char fmtbuf[1024];

    snprintf(fmtbuf, sizeof(fmtbuf)-1, "%s:%u: ", filename, line);
    strcat(fmtbuf, fmt);

    va_start(args, fmt);
    vfprintf(stderr, fmtbuf, args);
    va_end(args);
}

void benchmark_log(int level, const char *fmt, ...)
{
    if (level > log_level)
        return;

    va_list args;

    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
}

bool is_redis_protocol(enum PROTOCOL_TYPE type) {
    return (type == PROTOCOL_REDIS_DEFAULT || type == PROTOCOL_RESP2 || type == PROTOCOL_RESP3);
}

static const char * get_protocol_name(enum PROTOCOL_TYPE type) {
    if (type == PROTOCOL_REDIS_DEFAULT) return "redis";
    else if (type == PROTOCOL_RESP2) return "resp2";
    else if (type == PROTOCOL_RESP3) return "resp3";
    else if (type == PROTOCOL_MEMCACHE_TEXT) return "memcache_text";
    else if (type == PROTOCOL_MEMCACHE_BINARY) return "memcache_binary";
    else return "none";
}

static void config_print(FILE *file, struct benchmark_config *cfg)
{
    char tmpbuf[512];

    fprintf(file,
        "server = %s\n"
        "port = %u\n"
        "uri = %s\n"
        "unix socket = %s\n"
        "address family = %s\n"
        "protocol = %s\n"
#ifdef USE_TLS
        "tls = %s\n"
        "cert = %s\n"
        "key = %s\n"
        "cacert = %s\n"
        "tls_skip_verify = %s\n"
        "sni = %s\n"
#endif
        "out_file = %s\n"
        "client_stats = %s\n"
        "run_count = %u\n"
        "debug = %u\n"
        "requests = %llu\n"
        "rate_limit = %u\n"
        "clients = %u\n"
        "threads = %u\n"
        "test_time = %u\n"
        "ratio = %u:%u\n"
        "pipeline = %u\n"
        "data_size = %u\n"
        "data_offset = %u\n"
        "random_data = %s\n"
        "data_size_range = %u-%u\n"
        "data_size_list = %s\n"
        "data_size_pattern = %s\n"
        "expiry_range = %u-%u\n"
        "data_import = %s\n"
        "data_verify = %s\n"
        "verify_only = %s\n"
        "generate_keys = %s\n"
        "key_prefix = %s\n"
        "key_minimum = %llu\n"
        "key_maximum = %llu\n"
        "key_pattern = %s\n"
        "key_stddev = %f\n"
        "key_median = %f\n"
        "reconnect_interval = %u\n"
        "connection_timeout = %u\n"
        "thread_conn_start_min_jitter_micros = %u\n"
        "thread_conn_start_max_jitter_micros = %u\n"
        "multi_key_get = %u\n"
        "authenticate = %s\n"
        "select-db = %d\n"
        "no-expiry = %s\n"
        "wait-ratio = %u:%u\n"
        "num-slaves = %u-%u\n"
        "wait-timeout = %u-%u\n"
        "json-out-file = %s\n"
        "print-all-runs = %s\n",
        cfg->server,
        cfg->port,
        cfg->uri ? cfg->uri : "",
        cfg->unix_socket,
        cfg->resolution == AF_UNSPEC ? "Unspecified" : cfg->resolution == AF_INET ? "AF_INET" : "AF_INET6",
        get_protocol_name(cfg->protocol),
#ifdef USE_TLS
        cfg->tls ? "yes" : "no",
        cfg->tls_cert,
        cfg->tls_key,
        cfg->tls_cacert,
        cfg->tls_skip_verify ? "yes" : "no",
        cfg->tls_sni,
#endif
        cfg->out_file,
        cfg->client_stats,
        cfg->run_count,
        cfg->debug,
        cfg->requests,
        cfg->request_rate,
        cfg->clients,
        cfg->threads,
        cfg->test_time,
        cfg->ratio.a, cfg->ratio.b,
        cfg->pipeline,
        cfg->data_size,
        cfg->data_offset,
        cfg->random_data ? "yes" : "no",
        cfg->data_size_range.min, cfg->data_size_range.max,
        cfg->data_size_list.print(tmpbuf, sizeof(tmpbuf)-1),
        cfg->data_size_pattern,
        cfg->expiry_range.min, cfg->expiry_range.max,
        cfg->data_import,
        cfg->data_verify ? "yes" : "no",
        cfg->verify_only ? "yes" : "no",
        cfg->generate_keys ? "yes" : "no",
        cfg->key_prefix,
        cfg->key_minimum,
        cfg->key_maximum,
        cfg->key_pattern,
        cfg->key_stddev,
        cfg->key_median,
        cfg->reconnect_interval,
        cfg->connection_timeout,
        cfg->thread_conn_start_min_jitter_micros,
        cfg->thread_conn_start_max_jitter_micros,
        cfg->multi_key_get,
        cfg->authenticate ? cfg->authenticate : "",
        cfg->select_db,
        cfg->no_expiry ? "yes" : "no",
        cfg->wait_ratio.a, cfg->wait_ratio.b,
        cfg->num_slaves.min, cfg->num_slaves.max,
        cfg->wait_timeout.min, cfg->wait_timeout.max,
        cfg->json_out_file,
        cfg->print_all_runs ? "yes" : "no");
}

static void config_print_to_json(json_handler * jsonhandler, struct benchmark_config *cfg)
{
    char tmpbuf[512];

    jsonhandler->open_nesting("configuration");

    jsonhandler->write_obj("server"            ,"\"%s\"",      	cfg->server);
    jsonhandler->write_obj("port"              ,"%u",          	cfg->port);
    jsonhandler->write_obj("uri"               ,"\"%s\"",      	cfg->uri ? cfg->uri : "");
    jsonhandler->write_obj("unix socket"       ,"\"%s\"",      	cfg->unix_socket);
    jsonhandler->write_obj("address family"    ,"\"%s\"",      	cfg->resolution == AF_UNSPEC ? "Unspecified" : cfg->resolution == AF_INET ? "AF_INET" : "AF_INET6");
    jsonhandler->write_obj("protocol"          ,"\"%s\"",      	get_protocol_name(cfg->protocol));
    jsonhandler->write_obj("out_file"          ,"\"%s\"",      	cfg->out_file);
#ifdef USE_TLS
    jsonhandler->write_obj("tls"               ,"\"%s\"",      	cfg->tls ? "true" : "false");
    jsonhandler->write_obj("cert"              ,"\"%s\"",      	cfg->tls_cert);
    jsonhandler->write_obj("key"               ,"\"%s\"",      	cfg->tls_key);
    jsonhandler->write_obj("cacert"            ,"\"%s\"",      	cfg->tls_cacert);
    jsonhandler->write_obj("tls_skip_verify"   ,"\"%s\"",      	cfg->tls_skip_verify ? "true" : "false");
    jsonhandler->write_obj("sni"               ,"\"%s\"",       cfg->tls_sni);
#endif
    jsonhandler->write_obj("client_stats"      ,"\"%s\"",      	cfg->client_stats);
    jsonhandler->write_obj("run_count"         ,"%u",          	cfg->run_count);
    jsonhandler->write_obj("debug"             ,"%u",          	cfg->debug);
    jsonhandler->write_obj("requests"          ,"%llu",        	cfg->requests);
    jsonhandler->write_obj("rate_limit"        ,"%u",         	cfg->request_rate);
    jsonhandler->write_obj("clients"           ,"%u",          	cfg->clients);
    jsonhandler->write_obj("threads"           ,"%u",          	cfg->threads);
    jsonhandler->write_obj("test_time"         ,"%u",          	cfg->test_time);
    jsonhandler->write_obj("ratio"             ,"\"%u:%u\"",   	cfg->ratio.a, cfg->ratio.b);
    jsonhandler->write_obj("pipeline"          ,"%u",          	cfg->pipeline);
    jsonhandler->write_obj("data_size"         ,"%u",          	cfg->data_size);
    jsonhandler->write_obj("data_offset"       ,"%u",          	cfg->data_offset);
    jsonhandler->write_obj("random_data"       ,"\"%s\"",      	cfg->random_data ? "true" : "false");
    jsonhandler->write_obj("data_size_range"   ,"\"%u:%u\"",	cfg->data_size_range.min, cfg->data_size_range.max);
    jsonhandler->write_obj("data_size_list"    ,"\"%s\"",   	cfg->data_size_list.print(tmpbuf, sizeof(tmpbuf)-1));
    jsonhandler->write_obj("data_size_pattern" ,"\"%s\"", 		cfg->data_size_pattern);
    jsonhandler->write_obj("expiry_range"      ,"\"%u:%u\"",   	cfg->expiry_range.min, cfg->expiry_range.max);
    jsonhandler->write_obj("data_import"       ,"\"%s\"",       cfg->data_import);
    jsonhandler->write_obj("data_verify"       ,"\"%s\"",       cfg->data_verify ? "true" : "false");
    jsonhandler->write_obj("verify_only"       ,"\"%s\"",       cfg->verify_only ? "true" : "false");
    jsonhandler->write_obj("generate_keys"     ,"\"%s\"",     	cfg->generate_keys ? "true" : "false");
    jsonhandler->write_obj("key_prefix"        ,"\"%s\"",       cfg->key_prefix);
    jsonhandler->write_obj("key_minimum"       ,"%11u",        	cfg->key_minimum);
    jsonhandler->write_obj("key_maximum"       ,"%11u",        	cfg->key_maximum);
    jsonhandler->write_obj("key_pattern"       ,"\"%s\"",       cfg->key_pattern);
    jsonhandler->write_obj("key_stddev"        ,"%f",           cfg->key_stddev);
    jsonhandler->write_obj("key_median"        ,"%f",           cfg->key_median);
    jsonhandler->write_obj("key_zipf_exp"      ,"%f",           cfg->key_zipf_exp);
    jsonhandler->write_obj("reconnect_interval","%u",    		cfg->reconnect_interval);
    jsonhandler->write_obj("connection_timeout","%u",    		cfg->connection_timeout);
    jsonhandler->write_obj("thread_conn_start_min_jitter_micros","%u", cfg->thread_conn_start_min_jitter_micros);
    jsonhandler->write_obj("thread_conn_start_max_jitter_micros","%u", cfg->thread_conn_start_max_jitter_micros);
    jsonhandler->write_obj("multi_key_get"     ,"%u",         	cfg->multi_key_get);
    jsonhandler->write_obj("authenticate"      ,"\"%s\"",      	cfg->authenticate ? cfg->authenticate : "");
    jsonhandler->write_obj("select-db"         ,"%d",           cfg->select_db);
    jsonhandler->write_obj("no-expiry"         ,"\"%s\"",       cfg->no_expiry ? "true" : "false");
    jsonhandler->write_obj("wait-ratio"        ,"\"%u:%u\"",    cfg->wait_ratio.a, cfg->wait_ratio.b);
    jsonhandler->write_obj("num-slaves"        ,"\"%u:%u\"",    cfg->num_slaves.min, cfg->num_slaves.max);
    jsonhandler->write_obj("wait-timeout"      ,"\"%u-%u\"",   	cfg->wait_timeout.min, cfg->wait_timeout.max);
    jsonhandler->write_obj("print-all-runs"   ,"\"%s\"",       cfg->print_all_runs ? "true" : "false");

    jsonhandler->close_nesting();
}

// Parse URI and populate config fields
// Returns 0 on success, -1 on error
static int parse_uri(const char *uri, struct benchmark_config *cfg)
{
    if (!uri || strlen(uri) == 0) {
        fprintf(stderr, "error: empty URI provided.\n");
        return -1;
    }

    // Make a copy to work with
    char *uri_copy = strdup(uri);
    if (!uri_copy) {
        fprintf(stderr, "error: memory allocation failed.\n");
        return -1;
    }

    char *ptr = uri_copy;

    // Parse scheme
    char *scheme_end = strstr(ptr, "://");
    if (!scheme_end) {
        fprintf(stderr, "error: invalid URI format, missing scheme.\n");
        free(uri_copy);
        return -1;
    }

    *scheme_end = '\0';
    if (strcmp(ptr, "redis") == 0) {
        // Regular Redis connection
    } else if (strcmp(ptr, "rediss") == 0) {
#ifdef USE_TLS
        cfg->tls = true;
#else
        fprintf(stderr, "error: TLS not supported in this build.\n");
        free(uri_copy);
        return -1;
#endif
    } else {
        fprintf(stderr, "error: unsupported URI scheme '%s'. Use 'redis' or 'rediss'.\n", ptr);
        free(uri_copy);
        return -1;
    }

    ptr = scheme_end + 3; // Skip "://"

    // Parse user:password@host:port/db
    char *auth_end = strchr(ptr, '@');
    char *host_start = ptr;

    if (auth_end) {
        // Authentication present
        *auth_end = '\0';
        char *colon = strchr(ptr, ':');
        if (colon) {
            // user:password format
            *colon = '\0';
            char *user = ptr;
            char *password = colon + 1;

            // Combine as user:password for authenticate field
            int auth_len = strlen(user) + strlen(password) + 2;
            char *auth_str = (char*)malloc(auth_len);
            if (!auth_str) {
                fprintf(stderr, "error: memory allocation failed.\n");
                free(uri_copy);
                return -1;
            }
            snprintf(auth_str, auth_len, "%s:%s", user, password);
            cfg->authenticate = auth_str;
        } else {
            // Just password (default user)
            cfg->authenticate = strdup(ptr);
        }
        host_start = auth_end + 1;
    }

    // Parse host:port/db
    char *db_start = strchr(host_start, '/');
    if (db_start) {
        *db_start = '\0';
        db_start++;
        if (strlen(db_start) > 0) {
            char *endptr;
            int db = (int)strtol(db_start, &endptr, 10);
            if (*endptr != '\0' || db < 0) {
                fprintf(stderr, "error: invalid database number '%s'.\n", db_start);
                free(uri_copy);
                return -1;
            }
            cfg->select_db = db;
        }
    }

    // Parse host:port
    char *port_start = strchr(host_start, ':');
    if (port_start) {
        *port_start = '\0';
        port_start++;
        char *endptr;
        unsigned long port = strtoul(port_start, &endptr, 10);
        if (*endptr != '\0' || port == 0 || port > 65535) {
            fprintf(stderr, "error: invalid port number '%s'.\n", port_start);
            free(uri_copy);
            return -1;
        }
        cfg->port = (unsigned short)port;
    }

    // Set host
    if (strlen(host_start) > 0) {
        cfg->server = strdup(host_start);
    }

    free(uri_copy);
    return 0;
}

static void config_init_defaults(struct benchmark_config *cfg)
{
    if (!cfg->server && !cfg->unix_socket)
        cfg->server = "localhost";
    if (!cfg->port && !cfg->unix_socket)
        cfg->port = 6379;
    if (!cfg->resolution)
        cfg->resolution = AF_UNSPEC;
    if (!cfg->run_count)
        cfg->run_count = 1;
    if (!cfg->clients)
        cfg->clients = 50;
    if (!cfg->threads)
        cfg->threads = 4;
    if (!cfg->ratio.is_defined())
        cfg->ratio = config_ratio("1:10");
    if (!cfg->pipeline)
        cfg->pipeline = 1;
    if (!cfg->data_size && !cfg->data_size_list.is_defined() && !cfg->data_size_range.is_defined() && !cfg->data_import)
        cfg->data_size = 32;
    if (cfg->generate_keys || !cfg->data_import) {
        if (!cfg->key_prefix)
            cfg->key_prefix = "memtier-";
        if (!cfg->key_maximum)
            cfg->key_maximum = 10000000;
    }
    if (!cfg->key_pattern)
        cfg->key_pattern = "R:R";
    if (!cfg->data_size_pattern)
        cfg->data_size_pattern = "R";
    if (cfg->requests == (unsigned long long)-1) {
        cfg->requests = cfg->key_maximum - cfg->key_minimum;
        if (strcmp(cfg->key_pattern, "P:P")==0)
            cfg->requests = cfg->requests / (cfg->clients * cfg->threads) + 1;
        printf("setting requests to %llu\n", cfg->requests);
    }
    if (!cfg->requests && !cfg->test_time)
        cfg->requests = 10000;
    if (!cfg->hdr_prefix)
        cfg->hdr_prefix = "";
    if (!cfg->print_percentiles.is_defined())
        cfg->print_percentiles = config_quantiles("50,99,99.9");

    // StatsD defaults - port only matters if host is set
    if (!cfg->statsd_port)
        cfg->statsd_port = 8125;
    if (!cfg->statsd_prefix)
        cfg->statsd_prefix = "memtier";
    if (!cfg->statsd_run_label)
        cfg->statsd_run_label = "default";

#ifdef USE_TLS
    if (!cfg->tls_protocols)
        cfg->tls_protocols = REDIS_TLS_PROTO_DEFAULT;
#endif
}

static int generate_random_seed()
{
    int R = 0;
    FILE* f = fopen("/dev/random", "r");
    if (f)
    {
        size_t ignore = fread(&R, sizeof(R), 1, f);
        (void)ignore; // Suppress unused variable warning
        fclose(f);
    }

    return (int)time(NULL)^getpid()^R;
}

static bool verify_cluster_option(struct benchmark_config *cfg) {
    if (cfg->reconnect_interval) {
        fprintf(stderr, "error: cluster mode dose not support reconnect-interval option.\n");
        return false;
    } else if (cfg->multi_key_get) {
        fprintf(stderr, "error: cluster mode dose not support multi-key-get option.\n");
        return false;
    } else if (cfg->wait_ratio.is_defined()) {
        fprintf(stderr, "error: cluster mode dose not support wait-ratio option.\n");
        return false;
    } else if (!is_redis_protocol(cfg->protocol)) {
        fprintf(stderr, "error: cluster mode supported only in redis protocol.\n");
        return false;
    } else if (cfg->unix_socket) {
        fprintf(stderr, "error: cluster mode dose not support unix-socket option.\n");
        return false;
    }

    return true;
}

static bool verify_arbitrary_command_option(struct benchmark_config *cfg) {
    if (cfg->key_pattern) {
        fprintf(stderr, "error: when using arbitrary command, key pattern is configured with --command-key-pattern option.\n");
        return false;
    } else if (cfg->ratio.is_defined()) {
        fprintf(stderr, "error: when using arbitrary command, ratio is configured with --command-ratio option.\n");
        return false;
    }

    // verify that when using Parallel key pattern, it's configured to all commands
    size_t parallel_count = 0;
    for (size_t i = 0; i<cfg->arbitrary_commands->size(); i++) {
        arbitrary_command& cmd =  cfg->arbitrary_commands->at(i);
        if (cmd.key_pattern == 'P') {
            parallel_count++;
        }
    }

    if (parallel_count > 0 && parallel_count != cfg->arbitrary_commands->size()) {
        fprintf(stderr, "error: parallel key-pattern must be configured to all commands.\n");
        return false;
    }

    return true;
}

static int config_parse_args(int argc, char *argv[], struct benchmark_config *cfg)
{
    enum extended_options {
        o_test_time = 128,
        o_ratio,
        o_pipeline,
        o_data_size_range,
        o_data_size_list,
        o_data_size_pattern,
        o_data_offset,
        o_expiry_range,
        o_data_import,
        o_data_verify,
        o_verify_only,
        o_key_prefix,
        o_key_minimum,
        o_key_maximum,
        o_key_pattern,
        o_key_stddev,
        o_key_median,
        o_key_zipf_exp,
        o_show_config,
        o_hide_histogram,
        o_print_percentiles,
        o_print_all_runs,
        o_distinct_client_seed,
        o_randomize,
        o_client_stats,
        o_reconnect_interval,
        o_reconnect_on_error,
        o_max_reconnect_attempts,
        o_reconnect_backoff_factor,
        o_connection_timeout,
        o_thread_conn_start_min_jitter_micros,
        o_thread_conn_start_max_jitter_micros,
        o_generate_keys,
        o_multi_key_get,
        o_select_db,
        o_no_expiry,
        o_wait_ratio,
        o_num_slaves,
        o_wait_timeout,
        o_json_out_file,
        o_cluster_mode,
        o_command,
        o_command_key_pattern,
        o_command_ratio,
        o_tls,
        o_tls_cert,
        o_tls_key,
        o_tls_cacert,
        o_tls_skip_verify,
        o_tls_sni,
        o_tls_protocols,
        o_hdr_file_prefix,
        o_rate_limiting,
        o_uri,
        o_statsd_host,
        o_statsd_port,
        o_statsd_prefix,
        o_statsd_run_label,
        o_help
    };

    static struct option long_options[] = {
        { "server",                     1, 0, 's' },
        { "host",                       1, 0, 'h' },
        { "port",                       1, 0, 'p' },
        { "unix-socket",                1, 0, 'S' },
        { "ipv4",                       0, 0, '4' },
        { "ipv6",                       0, 0, '6' },
        { "protocol",                   1, 0, 'P' },
#ifdef USE_TLS
        { "tls",                        0, 0, o_tls },
        { "cert",                       1, 0, o_tls_cert },
        { "key",                        1, 0, o_tls_key },
        { "cacert",                     1, 0, o_tls_cacert },
        { "tls-skip-verify",            0, 0, o_tls_skip_verify },
        { "sni",                        1, 0, o_tls_sni },
        { "tls-protocols",              1, 0, o_tls_protocols },
#endif
        { "out-file",                   1, 0, 'o' },
        { "hdr-file-prefix",            1, 0, o_hdr_file_prefix },
        { "client-stats",               1, 0, o_client_stats },
        { "run-count",                  1, 0, 'x' },
        { "debug",                      0, 0, 'D' },
        { "show-config",                0, 0, o_show_config },
        { "hide-histogram",             0, 0, o_hide_histogram },
        { "print-percentiles",          1, 0, o_print_percentiles },
        { "print-all-runs",            0, 0, o_print_all_runs },
        { "distinct-client-seed",       0, 0, o_distinct_client_seed },
        { "randomize",                  0, 0, o_randomize },
        { "requests",                   1, 0, 'n' },
        { "clients",                    1, 0, 'c' },
        { "threads",                    1, 0, 't' },
        { "test-time",                  1, 0, o_test_time },
        { "ratio",                      1, 0, o_ratio },
        { "pipeline",                   1, 0, o_pipeline },
        { "data-size",                  1, 0, 'd' },
        { "data-offset",                1, 0, o_data_offset },
        { "random-data",                0, 0, 'R' },
        { "data-size-range",            1, 0, o_data_size_range },
        { "data-size-list",             1, 0, o_data_size_list },
        { "data-size-pattern",          1, 0, o_data_size_pattern },
        { "expiry-range",               1, 0, o_expiry_range },
        { "data-import",                1, 0, o_data_import },
        { "data-verify",                0, 0, o_data_verify },
        { "verify-only",                0, 0, o_verify_only },
        { "generate-keys",              0, 0, o_generate_keys },
        { "key-prefix",                 1, 0, o_key_prefix },
        { "key-minimum",                1, 0, o_key_minimum },
        { "key-maximum",                1, 0, o_key_maximum },
        { "key-pattern",                1, 0, o_key_pattern },
        { "key-stddev",                 1, 0, o_key_stddev },
        { "key-median",                 1, 0, o_key_median },
        { "key-zipf-exp",               1, 0, o_key_zipf_exp},
        { "reconnect-interval",         1, 0, o_reconnect_interval },
        { "reconnect-on-error",         0, 0, o_reconnect_on_error },
        { "max-reconnect-attempts",     1, 0, o_max_reconnect_attempts },
        { "reconnect-backoff-factor",   1, 0, o_reconnect_backoff_factor },
        { "connection-timeout",         1, 0, o_connection_timeout },
        { "thread-conn-start-min-jitter-micros", 1, 0, o_thread_conn_start_min_jitter_micros },
        { "thread-conn-start-max-jitter-micros", 1, 0, o_thread_conn_start_max_jitter_micros },
        { "multi-key-get",              1, 0, o_multi_key_get },
        { "authenticate",               1, 0, 'a' },
        { "select-db",                  1, 0, o_select_db },
        { "no-expiry",                  0, 0, o_no_expiry },
        { "wait-ratio",                 1, 0, o_wait_ratio },
        { "num-slaves",                 1, 0, o_num_slaves },
        { "wait-timeout",               1, 0, o_wait_timeout },
        { "json-out-file",              1, 0, o_json_out_file },
        { "cluster-mode",               0, 0, o_cluster_mode },
        { "help",                       0, 0, o_help },
        { "version",                    0, 0, 'v' },
        { "command",                    1, 0, o_command },
        { "command-key-pattern",        1, 0, o_command_key_pattern },
        { "command-ratio",              1, 0, o_command_ratio },
        { "rate-limiting",              1, 0, o_rate_limiting },
        { "uri",                        1, 0, o_uri },
        { "statsd-host",                1, 0, o_statsd_host },
        { "statsd-port",                1, 0, o_statsd_port },
        { "statsd-prefix",              1, 0, o_statsd_prefix },
        { "statsd-run-label",           1, 0, o_statsd_run_label },
        { NULL,                         0, 0, 0 }
    };

    int option_index;
    int c;
    char *endptr;
    while ((c = getopt_long(argc, argv,
                "vs:S:p:P:o:x:DRn:c:t:d:a:h:46u:", long_options, &option_index)) != -1)
    {
        switch (c) {
                case o_help:
                    return -1;
                    break;
                case 'v':
                    {
                        // Print version information similar to Redis format
                        // First line: memtier_benchmark v=... sha=... bits=... libevent=... openssl=...
                        printf("memtier_benchmark v=%s sha=%s:%s", PACKAGE_VERSION, MEMTIER_GIT_SHA1, MEMTIER_GIT_DIRTY);

                        // Print architecture bits
#if defined(__x86_64__) || defined(_M_X64) || defined(__aarch64__)
                        printf(" bits=64");
#elif defined(__i386__) || defined(_M_IX86) || defined(__arm__)
                        printf(" bits=32");
#else
                        printf(" bits=unknown");
#endif

                        // Print libevent version
                        printf(" libevent=%s", event_get_version());

                        // Print OpenSSL version if TLS is enabled
#ifdef USE_TLS
                        printf(" openssl=%s", OPENSSL_VERSION_TEXT);
#endif

                        printf("\n");

                        // Copyright and license info
                        printf("Copyright (C) 2011-2026 Redis Ltd.\n");
                        printf("This is free software.  You may redistribute copies of it under the terms of\n");
                        printf("the GNU General Public License <http://www.gnu.org/licenses/gpl.html>.\n");
                        printf("There is NO WARRANTY, to the extent permitted by law.\n");
                    }
                    exit(0);
                case 's':
                case 'h':
                    cfg->server = optarg;
                    break;
                case 'S':
                    cfg->unix_socket = optarg;
                    break;
                case 'p':
                    endptr = NULL;
                    cfg->port = (unsigned short) strtoul(optarg, &endptr, 10);
                    if (!cfg->port || cfg->port > 65535 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: port must be a number in the range [1-65535].\n");
                        return -1;
                    }
                    break;
                case '4':
                    cfg->resolution = AF_INET;
                    break;
                case '6':
                    cfg->resolution = AF_INET6;
                    break;
                case 'P':
                    if (strcmp(optarg, "redis") == 0) {
                        cfg->protocol = PROTOCOL_REDIS_DEFAULT;
                    } else if (strcmp(optarg, "resp2") == 0) {
                        cfg->protocol = PROTOCOL_RESP2;
                    } else if (strcmp(optarg, "resp3") == 0) {
                        cfg->protocol = PROTOCOL_RESP3;
                    } else if (strcmp(optarg, "memcache_text") == 0) {
                        cfg->protocol = PROTOCOL_MEMCACHE_TEXT;
                    } else if (strcmp(optarg, "memcache_binary") == 0) {
                        cfg->protocol = PROTOCOL_MEMCACHE_BINARY;
                    } else {
                        fprintf(stderr, "error: supported protocols are 'memcache_text', 'memcache_binary', 'redis', 'resp2' and resp3'.\n");
                        return -1;
                    }
                    break;
                case 'o':
                    cfg->out_file = optarg;
                    break;
                case o_hdr_file_prefix:
                    cfg->hdr_prefix = optarg;
                    break;
                case o_client_stats:
                    cfg->client_stats = optarg;
                    break;
                case 'x':
                    endptr = NULL;
                    cfg->run_count = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!cfg->run_count || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: run count must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case 'D':
                    cfg->debug++;
                    break;
                case o_show_config:
                    cfg->show_config++;
                    break;
                case o_hide_histogram:
                    cfg->hide_histogram++;
                    break;
                case o_print_percentiles:
                    cfg->print_percentiles = config_quantiles(optarg);
                    if (!cfg->print_percentiles.is_defined()) {
                        fprintf(stderr, "error: quantiles must be expressed as [0.0-100.0],[0.0-100.0](,...) .\n");
                        return -1;
                    }
                    break;
                case o_print_all_runs:
                    cfg->print_all_runs = true;
                    break;
                case o_distinct_client_seed:
                    cfg->distinct_client_seed++;
                    break;
                case o_randomize:
                    srandom(generate_random_seed());
                    cfg->randomize = random();
                    break;
                case 'n':
                    endptr = NULL;
                    if (strcmp(optarg, "allkeys")==0)
                        cfg->requests = -1;
                    else {
                        cfg->requests = (unsigned long long) strtoull(optarg, &endptr, 10);
                        if (!cfg->requests || !endptr || *endptr != '\0') {
                            fprintf(stderr, "error: requests must be greater than zero.\n");
                            return -1;
                        }
                        if (cfg->test_time) {
                            fprintf(stderr, "error: --test-time and --requests are mutually exclusive.\n");
                            return -1;
                        }
                    }
                    break;
                case 'c':
                    endptr = NULL;
                    cfg->clients = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!cfg->clients || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: clients must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case 't':
                    endptr = NULL;
                    cfg->threads = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!cfg->threads || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: threads must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case o_test_time:
                    endptr = NULL;
                    cfg->test_time = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!cfg->test_time || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: test time must be greater than zero.\n");
                        return -1;
                    }
                    if (cfg->requests) {
                        fprintf(stderr, "error: --test-time and --requests are mutually exclusive.\n");
                        return -1;
                    }
                    break;
                case o_ratio:
                    cfg->ratio = config_ratio(optarg);
                    if (!cfg->ratio.is_defined()) {
                        fprintf(stderr, "error: ratio must be expressed as [0-n]:[0-n].\n");
                        return -1;
                    }
                    break;
                case o_pipeline:
                    endptr = NULL;
                    cfg->pipeline = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!cfg->pipeline || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: pipeline must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case 'd':
                    endptr = NULL;
                    cfg->data_size = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!cfg->data_size || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: data-size must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case 'R':
                    cfg->random_data = true;
                    break;
                case o_data_offset:
                    endptr = NULL;
                    cfg->data_offset = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!endptr || *endptr != '\0') {
                        fprintf(stderr, "error: data-offset must be greater than or equal to zero.\n");
                        return -1;
                    }
                    break;
                case o_data_size_range:
                    cfg->data_size_range = config_range(optarg);
                    if (!cfg->data_size_range.is_defined() || cfg->data_size_range.min < 1) {
                        fprintf(stderr, "error: data-size-range must be expressed as [1-n]-[1-n].\n");
                        return -1;
                    }
                    break;
                case o_data_size_list:
                    cfg->data_size_list = config_weight_list(optarg);
                    if (!cfg->data_size_list.is_defined()) {
                        fprintf(stderr, "error: data-size-list must be expressed as [size1:weight1],...[sizeN:weightN].\n");
                        return -1;
                    }
                    break;
                case o_expiry_range:
                    cfg->expiry_range = config_range(optarg);
                    if (!cfg->expiry_range.is_defined()) {
                        fprintf(stderr, "error: expiry-range must be expressed as [0-n]-[1-n].\n");
                        return -1;
                    }
                    break;
                case o_data_size_pattern:
                    cfg->data_size_pattern = optarg;
                    if (strlen(cfg->data_size_pattern) != 1 ||
                        (cfg->data_size_pattern[0] != 'R' && cfg->data_size_pattern[0] != 'S')) {
                            fprintf(stderr, "error: data-size-pattern must be either R or S.\n");
                            return -1;
                    }
                    break;
                case o_data_import:
                    cfg->data_import = optarg;
                    break;
                case o_data_verify:
                    cfg->data_verify = 1;
                    break;
                case o_verify_only:
                    cfg->verify_only = 1;
                    cfg->data_verify = 1;   // Implied
                    break;
                case o_key_prefix:
                    cfg->key_prefix = optarg;
                    break;
                case o_key_minimum:
                    endptr = NULL;
                    cfg->key_minimum = strtoull(optarg, &endptr, 10);
                    if (cfg->key_minimum < 1 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: key-minimum must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case o_key_maximum:
                    endptr = NULL;
                    cfg->key_maximum = strtoull(optarg, &endptr, 10);
                    if (cfg->key_maximum< 1 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: key-maximum must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case o_key_stddev:
                    endptr = NULL;
                    cfg->key_stddev = strtod(optarg, &endptr);
                    if (cfg->key_stddev<= 0 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: key-stddev must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case o_key_median:
                    endptr = NULL;
                    cfg->key_median = strtod(optarg, &endptr);
                    if (cfg->key_median<= 0 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: key-median must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case o_key_zipf_exp:
                    endptr = NULL;
                    cfg->key_zipf_exp = strtod(optarg, &endptr);
                    if (cfg->key_zipf_exp <= 0 || cfg->key_zipf_exp >= 5 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: key-zipf-exp must be within interval (0, 5).\n");
                        return -1;
                    }
                    break;
                case o_key_pattern:
                    cfg->key_pattern = optarg;

                    if (strlen(cfg->key_pattern) != 3 || cfg->key_pattern[key_pattern_delimiter] != ':' ||
                        (cfg->key_pattern[key_pattern_set] != 'R' &&
                         cfg->key_pattern[key_pattern_set] != 'S' &&
                         cfg->key_pattern[key_pattern_set] != 'G' &&
                         cfg->key_pattern[key_pattern_set] != 'Z' &&
                         cfg->key_pattern[key_pattern_set] != 'P') ||
                        (cfg->key_pattern[key_pattern_get] != 'R' &&
                         cfg->key_pattern[key_pattern_get] != 'S' &&
                         cfg->key_pattern[key_pattern_get] != 'G' &&
                         cfg->key_pattern[key_pattern_get] != 'Z' &&
                         cfg->key_pattern[key_pattern_get] != 'P')) {
                        fprintf(stderr, "error: key-pattern must be in the format of [S/R/G/P/Z]:[S/R/G/P/Z].\n");
                        return -1;
                    }

                    if ((cfg->key_pattern[key_pattern_set] == 'P' ||
                         cfg->key_pattern[key_pattern_get] == 'P') &&
                        (cfg->key_pattern[key_pattern_set] != cfg->key_pattern[key_pattern_get])) {

                        fprintf(stderr, "error: parallel key-pattern must be configured for both SET and GET commands.\n");
                        return -1;
                    }
                    break;
                case o_reconnect_interval:
                    endptr = NULL;
                    cfg->reconnect_interval = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!cfg->reconnect_interval || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: reconnect-interval must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case o_reconnect_on_error:
                    cfg->reconnect_on_error = true;
                    break;
                case o_max_reconnect_attempts:
                    endptr = NULL;
                    cfg->max_reconnect_attempts = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!endptr || *endptr != '\0') {
                        fprintf(stderr, "error: max-reconnect-attempts must be a valid number.\n");
                        return -1;
                    }
                    break;
                case o_reconnect_backoff_factor:
                    endptr = NULL;
                    cfg->reconnect_backoff_factor = strtod(optarg, &endptr);
                    if (cfg->reconnect_backoff_factor <= 0.0 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: reconnect-backoff-factor must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case o_connection_timeout:
                    endptr = NULL;
                    cfg->connection_timeout = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!endptr || *endptr != '\0') {
                        fprintf(stderr, "error: connection-timeout must be a valid number.\n");
                        return -1;
                    }
                    break;
                case o_thread_conn_start_min_jitter_micros:
                    endptr = NULL;
                    cfg->thread_conn_start_min_jitter_micros = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!endptr || *endptr != '\0') {
                        fprintf(stderr, "error: thread-conn-start-min-jitter-micros must be a valid number.\n");
                        return -1;
                    }
                    break;
                case o_thread_conn_start_max_jitter_micros:
                    endptr = NULL;
                    cfg->thread_conn_start_max_jitter_micros = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!endptr || *endptr != '\0') {
                        fprintf(stderr, "error: thread-conn-start-max-jitter-micros must be a valid number.\n");
                        return -1;
                    }
                    break;
                case o_generate_keys:
                    cfg->generate_keys = 1;
                    break;
                case o_multi_key_get:
                    endptr = NULL;
                    cfg->multi_key_get = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (cfg->multi_key_get <= 0 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: multi-key-get must be greater than zero.\n");
                        return -1;
                    }
                    break;
                case 'a':
                    cfg->authenticate = optarg;
                    break;
                case 'u':
                case o_uri:
                    cfg->uri = optarg;
                    break;
                case o_select_db:
                    cfg->select_db = (int) strtoul(optarg, &endptr, 10);
                    if (cfg->select_db < 0 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: select-db must be greater or equal zero.\n");
                        return -1;
                    }
                    break;
                case o_no_expiry:
                    cfg->no_expiry = true;
                    break;
                case o_wait_ratio:
                    cfg->wait_ratio = config_ratio(optarg);
                    if (!cfg->wait_ratio.is_defined()) {
                        fprintf(stderr, "error: wait-ratio must be expressed as [0-n]:[0-n].\n");
                        return -1;
                    }
                    break;
                case o_num_slaves:
                    cfg->num_slaves = config_range(optarg);
                    if (!cfg->num_slaves.is_defined()) {
                        fprintf(stderr, "error: num-slaves must be expressed as [0-n]-[1-n].\n");
                        return -1;
                    }
                    break;
                case o_wait_timeout:
                    cfg->wait_timeout = config_range(optarg);
                    if (!cfg->wait_timeout.is_defined()) {
                        fprintf(stderr, "error: wait-timeout must be expressed as [0-n]-[1-n].\n");
                        return -1;
                    }
                    break;
                case o_json_out_file:
                    cfg->json_out_file = optarg;
                    break;
                case o_cluster_mode:
                    cfg->cluster_mode = true;
                    break;
                case o_command: {
                    // add new arbitrary command
                    arbitrary_command cmd(optarg);

                    if (cmd.split_command_to_args()) {
                        cfg->arbitrary_commands->add_command(cmd);
                    } else {
                        fprintf(stderr, "error: failed to parse arbitrary command.\n");
                        return -1;
                    }
                    break;
                }
                case o_command_key_pattern: {
                    if (cfg->arbitrary_commands->size() == 0) {
                        fprintf(stderr, "error: no arbitrary command found.\n");
                        return -1;
                    }

                    // command configuration always applied on last configured command
                    arbitrary_command& cmd = cfg->arbitrary_commands->get_last_command();
                    if (!cmd.set_key_pattern(optarg)) {
                        fprintf(stderr, "error: key-pattern for command %s must be in the format of [S/R/Z/G/P].\n", cmd.command_name.c_str());
                        return -1;
                    }
                    break;
                }
                case o_command_ratio: {
                    if (cfg->arbitrary_commands->size() == 0) {
                        fprintf(stderr, "error: no arbitrary command found.\n");
                        return -1;
                    }

                    // command configuration always applied on last configured command
                    arbitrary_command& cmd = cfg->arbitrary_commands->get_last_command();
                    if (!cmd.set_ratio(optarg)) {
                        fprintf(stderr, "error: failed to set ratio for command %s.\n", cmd.command_name.c_str());
                        return -1;
                    }
                    break;
                }
                case o_rate_limiting: {
                    endptr = NULL;
                    cfg->request_rate = (unsigned int) strtoul(optarg, &endptr, 10);
                    if (!cfg->request_rate || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: rate must be greater than zero.\n");
                        return -1;
                    }
                    break;
                }
#ifdef USE_TLS
                case o_tls:
                    cfg->tls = true;
                    break;
                case o_tls_cert:
                    cfg->tls_cert = optarg;
                    break;
                case o_tls_key:
                    cfg->tls_key = optarg;
                    break;
                case o_tls_cacert:
                    cfg->tls_cacert = optarg;
                    break;
                case o_tls_skip_verify:
                    cfg->tls_skip_verify = true;
                    break;
                case o_tls_sni:
                    cfg->tls_sni = optarg;
                    break;
                case o_tls_protocols:
                {
                    const char* tls_delimiter = ",";
                    char* tls_token = std::strtok(optarg, tls_delimiter);
                    while (tls_token != NULL) {
                        if (!strcasecmp(tls_token, "tlsv1"))
                            cfg->tls_protocols |= REDIS_TLS_PROTO_TLSv1;
                        else if (!strcasecmp(tls_token, "tlsv1.1"))
                            cfg->tls_protocols |= REDIS_TLS_PROTO_TLSv1_1;
                        else if (!strcasecmp(tls_token, "tlsv1.2"))
                            cfg->tls_protocols |= REDIS_TLS_PROTO_TLSv1_2;
                        else if (!strcasecmp(tls_token, "tlsv1.3")) {
    #ifdef TLS1_3_VERSION
                            cfg->tls_protocols |= REDIS_TLS_PROTO_TLSv1_3;
    #else
                            fprintf(stderr, "TLSv1.3 is specified in tls-protocols but not supported by OpenSSL.");
                            return -1;
    #endif
                        } else {
                            fprintf(stderr, "Invalid tls-protocols specified %s. "
                                    "Use a combination of 'TLSv1', 'TLSv1.1', 'TLSv1.2' and 'TLSv1.3'.", tls_token);
                            return -1;
                            break;
                        }
                        tls_token = std::strtok(NULL, tls_delimiter);
                    }
                    break;
                }
#endif
                case o_statsd_host:
                    cfg->statsd_host = optarg;
                    break;
                case o_statsd_port:
                    endptr = NULL;
                    cfg->statsd_port = (unsigned short) strtoul(optarg, &endptr, 10);
                    if (cfg->statsd_port == 0 || !endptr || *endptr != '\0') {
                        fprintf(stderr, "error: statsd-port must be a valid port number.\n");
                        return -1;
                    }
                    break;
                case o_statsd_prefix:
                    cfg->statsd_prefix = optarg;
                    break;
                case o_statsd_run_label:
                    cfg->statsd_run_label = optarg;
                    break;
            default:
                    return -1;
                    break;
        }
    }

    if ((cfg->cluster_mode && !verify_cluster_option(cfg)) ||
        (cfg->arbitrary_commands->is_defined() && !verify_arbitrary_command_option(cfg))) {
        return -1;
    }

    return 0;
}

void usage() {
    fprintf(stdout, "Usage: memtier_benchmark [options]\n"
            "A memcache/redis NoSQL traffic generator and performance benchmarking tool.\n"
            "\n"
            "Connection and General Options:\n"
            "  -h, --host=ADDR                Server address (default: localhost)\n"
            "  -s, --server=ADDR              Same as --host\n"
            "  -p, --port=PORT                Server port (default: 6379)\n"
            "  -S, --unix-socket=SOCKET       UNIX Domain socket name (default: none)\n"
            "  -4, --ipv4                     Force IPv4 address resolution.\n"
            "  -6  --ipv6                     Force IPv6 address resolution.\n"
            "  -P, --protocol=PROTOCOL        Protocol to use (default: redis).\n"
            "                                 other supported protocols are resp2, resp3, memcache_text and memcache_binary.\n"
            "                                 when using one of resp2 or resp3 the redis protocol version will be set via HELLO command.\n"
            "  -a, --authenticate=CREDENTIALS Authenticate using specified credentials.\n"
            "                                 A simple password is used for memcache_text\n"
            "                                 and Redis <= 5.x. <USER>:<PASSWORD> can be\n"
            "                                 specified for memcache_binary or Redis 6.x\n"
            "                                 or newer with ACL user support.\n"
            "  -u, --uri=URI                  Server URI on format redis://user:password@host:port/dbnum\n"
            "                                 User, password and dbnum are optional. For authentication\n"
            "                                 without a username, use username 'default'. For TLS, use\n"
            "                                 the scheme 'rediss'.\n"
#ifdef USE_TLS
            "      --tls                      Enable SSL/TLS transport security\n"
            "      --cert=FILE                Use specified client certificate for TLS\n"
            "      --key=FILE                 Use specified private key for TLS\n"
            "      --cacert=FILE              Use specified CA certs bundle for TLS\n"
            "      --tls-skip-verify          Skip verification of server certificate\n"
            "      --tls-protocols            Specify the tls protocol version to use, comma delemited. Use a combination of 'TLSv1', 'TLSv1.1', 'TLSv1.2' and 'TLSv1.3'.\n"
            "      --sni=STRING               Add an SNI header\n"
#endif
            "  -x, --run-count=NUMBER         Number of full-test iterations to perform\n"
            "  -D, --debug                    Print debug output\n"
            "      --client-stats=FILE        Produce per-client stats file\n"
            "  -o, --out-file=FILE            Name of output file (default: stdout)\n"
            "      --json-out-file=FILE       Name of JSON output file, if not set, will not print to json\n"
            "      --hdr-file-prefix=FILE     Prefix of HDR Latency Histogram output files, if not set, will not save latency histogram files\n"
            "      --show-config              Print detailed configuration before running\n"
            "      --hide-histogram           Don't print detailed latency histogram\n"
            "      --print-percentiles        Specify which percentiles info to print on the results table (by default prints percentiles: 50,99,99.9)\n"
            "      --print-all-runs           When performing multiple test iterations, print and save results for all iterations\n"
            "      --cluster-mode             Run client in cluster mode\n"
            "      --statsd-host=HOST         StatsD server hostname to send real-time metrics (default: none, disabled)\n"
            "      --statsd-port=PORT         StatsD server UDP port (default: 8125)\n"
            "      --statsd-prefix=PREFIX     Prefix for StatsD metric names (default: memtier)\n"
            "      --statsd-run-label=LABEL   Label for this benchmark run, used to distinguish runs in dashboards (default: default)\n"
            "  -h, --help                     Display this help\n"
            "  -v, --version                  Display version information\n"
            "\n"
            "Test Options:\n"
            "  -n, --requests=NUMBER          Number of total requests per client (default: 10000)\n"
            "                                 use 'allkeys' to run on the entire key-range\n"
            "      --rate-limiting=NUMBER     The max number of requests to make per second from an individual connection (default is unlimited rate).\n"
            "                                 If you use --rate-limiting and a very large rate is entered which cannot be met, memtier will do as many requests as possible per second.\n"
            "  -c, --clients=NUMBER           Number of clients per thread (default: 50)\n"
            "  -t, --threads=NUMBER           Number of threads (default: 4)\n"
            "      --test-time=SECS           Number of seconds to run the test\n"
            "      --ratio=RATIO              Set:Get ratio (default: 1:10)\n"
            "      --pipeline=NUMBER          Number of concurrent pipelined requests (default: 1)\n"
            "      --reconnect-interval=NUM   Number of requests after which re-connection is performed\n"
            "      --reconnect-on-error       Enable automatic reconnection on connection errors (default: disabled)\n"
            "      --max-reconnect-attempts=NUM Maximum number of reconnection attempts (default: 0, unlimited)\n"
            "      --reconnect-backoff-factor=NUM Backoff factor for reconnection delays (default: 0, no backoff)\n"
            "      --connection-timeout=SECS  Connection timeout in seconds, 0 to disable (default: 0)\n"
            "      --thread-conn-start-min-jitter-micros=NUM Minimum jitter in microseconds between connection creation (default: 0)\n"
            "      --thread-conn-start-max-jitter-micros=NUM Maximum jitter in microseconds between connection creation (default: 0)\n"
            "      --multi-key-get=NUM        Enable multi-key get commands, up to NUM keys (default: 0)\n"
            "      --select-db=DB             DB number to select, when testing a redis server\n"
            "      --distinct-client-seed     Use a different random seed for each client\n"
            "      --randomize                random seed based on timestamp (default is constant value)\n"
            "\n"
            "Arbitrary command:\n"
            "      --command=COMMAND          Specify a command to send in quotes.\n"
            "                                 Each command that you specify is run with its ratio and key-pattern options.\n"
            "                                 For example: --command=\"set __key__ 5\" --command-ratio=2 --command-key-pattern=G\n"
            "                                 To use a generated key or object, enter:\n"
            "                                   __key__: Use key generated from Key Options.\n"
            "                                   __data__: Use data generated from Object Options.\n"
            "      --command-ratio            The number of times the command is sent in sequence.(default: 1)\n"
            "      --command-key-pattern      Key pattern for the command (default: R):\n"
            "                                 G for Gaussian distribution.\n"
            "                                 R for uniform Random.\n"
            "                                 Z for zipf distribution (will limit keys to positive).\n"
            "                                 S for Sequential.\n"
            "                                 P for Parallel (Sequential were each client has a subset of the key-range).\n"
            "\n"
            "Object Options:\n"
            "  -d  --data-size=SIZE           Object data size in bytes (default: 32)\n"
            "      --data-offset=OFFSET       Actual size of value will be data-size + data-offset\n"
            "                                 Will use SETRANGE / GETRANGE (default: 0)\n"
            "  -R  --random-data              Indicate that data should be randomized\n"
            "      --data-size-range=RANGE    Use random-sized items in the specified range (min-max)\n"
            "      --data-size-list=LIST      Use sizes from weight list (size1:weight1,..sizeN:weightN)\n"
            "      --data-size-pattern=R|S    Use together with data-size-range\n"
            "                                 when set to R, a random size from the defined data sizes will be used,\n"
            "                                 when set to S, the defined data sizes will be evenly distributed across\n"
            "                                 the key range, see --key-maximum (default R)\n"
            "      --expiry-range=RANGE       Use random expiry values from the specified range\n"
            "\n"
            "Imported Data Options:\n"
            "      --data-import=FILE         Read object data from file\n"
            "      --data-verify              Enable data verification when test is complete\n"
            "      --verify-only              Only perform --data-verify, without any other test\n"
            "      --generate-keys            Generate keys for imported objects\n"
            "      --no-expiry                Ignore expiry information in imported data\n"
            "\n"
            "Key Options:\n"
            "      --key-prefix=PREFIX        Prefix for keys (default: \"memtier-\")\n"
            "      --key-minimum=NUMBER       Key ID minimum value (default: 0)\n"
            "      --key-maximum=NUMBER       Key ID maximum value (default: 10000000)\n"
            "      --key-pattern=PATTERN      Set:Get pattern (default: R:R)\n"
            "                                 G for Gaussian distribution.\n"
            "                                 R for uniform Random.\n"
            "                                 Z for zipf distribution (will limit keys to positive).\n"
            "                                 S for Sequential.\n"
            "                                 P for Parallel (Sequential were each client has a subset of the key-range).\n"
            "      --key-stddev               The standard deviation used in the Gaussian distribution\n"
            "                                 (default is key range / 6)\n"
            "      --key-median               The median point used in the Gaussian distribution\n"
            "                                 (default is the center of the key range)\n"
            "      --key-zipf-exp             The exponent used in the zipf distribution, limit to (0, 5)\n"
            "                                 Higher exponents result in higher concentration in top keys\n"
            "                                 (default is 1, though any number >2 seems insane)\n"
            "\n"
            "WAIT Options:\n"
            "      --wait-ratio=RATIO         Set:Wait ratio (default is no WAIT commands - 1:0)\n"
            "      --num-slaves=RANGE         WAIT for a random number of slaves in the specified range\n"
            "      --wait-timeout=RANGE       WAIT for a random number of milliseconds in the specified range (normal \n"
            "                                 distribution with the center in the middle of the range)"
            "\n"
            );

    exit(2);
}

static void* cg_thread_start(void *t);

struct cg_thread {
    unsigned int m_thread_id;
    benchmark_config* m_config;
    object_generator* m_obj_gen;
    client_group* m_cg;
    abstract_protocol* m_protocol;
    pthread_t m_thread;
    std::atomic<bool> m_finished;  // Atomic to prevent data race between worker thread write and main thread read
    bool m_restart_requested;
    unsigned int m_restart_count;

    cg_thread(unsigned int id, benchmark_config* config, object_generator* obj_gen) :
        m_thread_id(id), m_config(config), m_obj_gen(obj_gen), m_cg(NULL), m_protocol(NULL),
        m_finished(false), m_restart_requested(false), m_restart_count(0)
    {
        m_protocol = protocol_factory(m_config->protocol);
        assert(m_protocol != NULL);

        m_cg = new client_group(m_config, m_protocol, m_obj_gen);
    }

    ~cg_thread()
    {
        if (m_cg != NULL) {
            delete m_cg;
        }
        if (m_protocol != NULL) {
            delete m_protocol;
        }
    }

    int prepare(void)
    {
        if (m_cg->create_clients(m_config->clients) < (int) m_config->clients)
            return -1;
        return m_cg->prepare();
    }

    int start(void)
    {
        return pthread_create(&m_thread, NULL, cg_thread_start, (void *)this);
    }

    void join(void)
    {
        int* retval;
        int ret;

        ret = pthread_join(m_thread, (void **)&retval);
        assert(ret == 0);
    }

    int restart(void)
    {
        // Clean up existing client group
        if (m_cg != NULL) {
            delete m_cg;
        }

        // Create new client group
        m_cg = new client_group(m_config, m_protocol, m_obj_gen);

        // Prepare new clients
        if (m_cg->create_clients(m_config->clients) < (int) m_config->clients)
            return -1;
        if (m_cg->prepare() < 0)
            return -1;

        // Reset state
        m_finished = false;
        m_restart_requested = false;
        m_restart_count++;

        // Start new thread
        return pthread_create(&m_thread, NULL, cg_thread_start, (void *)this);
    }

};

static void* cg_thread_start(void *t)
{
    cg_thread* thread = (cg_thread*) t;

    try {
        thread->m_cg->run();

        // Check if we should restart due to connection failures
        // If the thread finished but still has time left and connection errors, request restart
        if (thread->m_cg->get_total_connection_errors() > 0) {
            benchmark_error_log("Thread %u finished due to connection failures, requesting restart.\n", thread->m_thread_id);
            thread->m_restart_requested = true;
        }

        thread->m_finished = true;
    } catch (const std::exception& e) {
        benchmark_error_log("Thread %u caught exception: %s\n", thread->m_thread_id, e.what());
        thread->m_finished = true;
        thread->m_restart_requested = true;
    } catch (...) {
        benchmark_error_log("Thread %u caught unknown exception\n", thread->m_thread_id);
        thread->m_finished = true;
        thread->m_restart_requested = true;
    }

    return t;
}

void size_to_str(unsigned long int size, char *buf, int buf_len)
{
    if (size >= 1024*1024*1024) {
        snprintf(buf, buf_len, "%.2fGB",
            (float) size / (1024*1024*1024));
    } else if (size >= 1024*1024) {
        snprintf(buf, buf_len, "%.2fMB",
            (float) size / (1024*1024));
    } else {
        snprintf(buf, buf_len, "%.2fKB",
            (float) size / 1024);
    }
}

// Print client list for crash handler
static void print_client_list(FILE* fp, int pid, const char* timestr)
{
    if (g_threads != NULL) {
        fprintf(fp, "\n[%d] %s # --- CLIENT LIST OUTPUT\n", pid, timestr);

        for (size_t t = 0; t < g_threads->size(); t++) {
            cg_thread* thread = (*g_threads)[t];
            if (thread && thread->m_cg) {
                std::vector<client*>& clients = thread->m_cg->get_clients();

                for (size_t c = 0; c < clients.size(); c++) {
                    client* cl = clients[c];
                    if (cl) {
                        std::vector<shard_connection*>& connections = cl->get_connections();

                        for (size_t conn_idx = 0; conn_idx < connections.size(); conn_idx++) {
                            shard_connection* conn = connections[conn_idx];
                            if (conn) {
                                const char* state_str = "unknown";
                                switch (conn->get_connection_state()) {
                                    case conn_disconnected: state_str = "disconnected"; break;
                                    case conn_in_progress: state_str = "connecting"; break;
                                    case conn_connected: state_str = "connected"; break;
                                }

                                int local_port = conn->get_local_port();
                                const char* last_cmd = conn->get_last_request_type();

                                fprintf(fp, "[%d] %s # thread=%zu client=%zu conn=%zu addr=%s:%s local_port=%d state=%s pending=%d last_cmd=%s\n",
                                        pid, timestr, t, c, conn_idx,
                                        conn->get_address() ? conn->get_address() : "unknown",
                                        conn->get_port() ? conn->get_port() : "unknown",
                                        local_port,
                                        state_str,
                                        conn->get_pending_resp(),
                                        last_cmd);
                            }
                        }
                    }
                }
            }
        }
    }
}

// Helper function to print stack trace for all threads
static void print_all_threads_stack_trace(FILE* fp, int pid, const char* timestr)
{
    fprintf(fp, "\n[%d] %s # --- STACK TRACE (all threads)\n", pid, timestr);

    // Get the current (crashing) thread ID
    pthread_t current_thread = pthread_self();

    // Print main/crashing thread first
    fprintf(fp, "[%d] %s # Thread %lu (current/crashing thread):\n", pid, timestr, (unsigned long)current_thread);

    void *trace[100];
    int trace_size = backtrace(trace, 100);
    char **messages = backtrace_symbols(trace, trace_size);
    for (int i = 1; i < trace_size; i++) {
        fprintf(fp, "[%d] %s #   %s\n", pid, timestr, messages[i]);
    }
    free(messages);

    // Now print stack traces for worker threads if available
    if (g_threads != NULL) {
        for (size_t t = 0; t < g_threads->size(); t++) {
            cg_thread* thread = (*g_threads)[t];
            if (thread && thread->m_thread) {
                pthread_t tid = thread->m_thread;

                // Skip if this is the current thread (already printed)
                if (pthread_equal(tid, current_thread)) {
                    continue;
                }

                fprintf(fp, "[%d] %s # Thread %lu (worker thread %zu):\n", pid, timestr, (unsigned long)tid, t);
                fprintf(fp, "[%d] %s #   (Note: Stack trace for non-crashing threads not available on this platform)\n", pid, timestr);
            }
        }
    }
}

run_stats run_benchmark(int run_id, benchmark_config* cfg, object_generator* obj_gen)
{
    fprintf(stderr, "[RUN #%u] Preparing benchmark client...\n", run_id);

    // prepare threads data
    std::vector<cg_thread*> threads;
    g_threads = &threads;  // Set global pointer for crash handler

    for (unsigned int i = 0; i < cfg->threads; i++) {
        cg_thread* t = new cg_thread(i, cfg, obj_gen);
        assert(t != NULL);

        if (t->prepare() < 0) {
            benchmark_error_log("error: failed to prepare thread %u for test.\n", i);
            exit(1);
        }
        threads.push_back(t);
    }

    // launch threads
    fprintf(stderr, "[RUN #%u] Launching threads now...\n", run_id);
    for (std::vector<cg_thread*>::iterator i = threads.begin(); i != threads.end(); i++) {
        (*i)->start();
    }

    // Send "run started" annotation to Graphite
    if (cfg->statsd != NULL && cfg->statsd->is_enabled()) {
        char event_data[256];
        snprintf(event_data, sizeof(event_data) - 1,
            "threads=%u connections=%u run=%u",
            cfg->threads, cfg->clients, run_id);
        cfg->statsd->event("Benchmark Started", event_data, "memtier,start");
    }

    unsigned long int prev_ops = 0;
    unsigned long int prev_bytes = 0;
    unsigned long int prev_duration = 0;
    double prev_latency = 0, cur_latency = 0;
    unsigned long int cur_ops_sec = 0;
    unsigned long int cur_bytes_sec = 0;

    // provide some feedback...
    // NOTE: Reading stats from worker threads without synchronization is a benign race.
    // These stats are only for progress display and are approximate. Final results are
    // collected after pthread_join() when all threads have finished (race-free).
    unsigned int active_threads = 0;
    do {
        active_threads = 0;
        sleep(1);

        // Check for Ctrl+C interrupt
        if (g_interrupted) {
            // Calculate elapsed time before interrupting
            unsigned long int elapsed_duration = 0;
            unsigned int thread_counter = 0;
            for (std::vector<cg_thread*>::iterator i = threads.begin(); i != threads.end(); i++) {
                thread_counter++;
                float factor = ((float)(thread_counter - 1) / thread_counter);
                elapsed_duration = factor * elapsed_duration + (float)(*i)->m_cg->get_duration_usec() / thread_counter;
            }
            fprintf(stderr, "\n[RUN #%u] Interrupted by user (Ctrl+C) after %.1f secs, stopping threads...\n",
                    run_id, (float)elapsed_duration / 1000000);
            // Interrupt all threads (marks clients as interrupted, breaks event loops, and finalizes stats)
            for (std::vector<cg_thread*>::iterator i = threads.begin(); i != threads.end(); i++) {
                (*i)->m_cg->interrupt();
            }
            break;
        }

        unsigned long int total_ops = 0;
        unsigned long int total_bytes = 0;
        unsigned long int duration = 0;
        unsigned int thread_counter = 0;
        unsigned long int total_latency = 0;
        unsigned long int total_connection_errors = 0;

        for (std::vector<cg_thread*>::iterator i = threads.begin(); i != threads.end(); i++) {
            // Check if thread needs restart
            if ((*i)->m_finished && (*i)->m_restart_requested && (*i)->m_restart_count < 5) {
                benchmark_error_log("Restarting thread %u (restart #%u)...\n",
                                  (*i)->m_thread_id, (*i)->m_restart_count + 1);

                // Join the failed thread first
                (*i)->join();

                // Attempt to restart
                if ((*i)->restart() == 0) {
                    benchmark_error_log("Thread %u restarted successfully.\n", (*i)->m_thread_id);
                } else {
                    benchmark_error_log("Failed to restart thread %u.\n", (*i)->m_thread_id);
                }
            }

            if (!(*i)->m_finished)
                active_threads++;

            total_ops += (*i)->m_cg->get_total_ops();
            total_bytes += (*i)->m_cg->get_total_bytes();
            total_latency += (*i)->m_cg->get_total_latency();
            total_connection_errors += (*i)->m_cg->get_total_connection_errors();
            thread_counter++;
            float factor = ((float)(thread_counter - 1) / thread_counter);
            duration =  factor * duration +  (float)(*i)->m_cg->get_duration_usec() / thread_counter ;
        }

        unsigned long int cur_ops = total_ops-prev_ops;
        unsigned long int cur_bytes = total_bytes-prev_bytes;
        unsigned long int cur_duration = duration-prev_duration;
        double cur_total_latency = total_latency-prev_latency;
        prev_ops = total_ops;
        prev_bytes = total_bytes;
        prev_latency = total_latency;
        prev_duration = duration;

        unsigned long int ops_sec = 0;
        unsigned long int bytes_sec = 0;
        double avg_latency = 0;
        if (duration > 1) {
            ops_sec = (long)( (double)total_ops / duration * 1000000);
            bytes_sec = (long)( (double)total_bytes / duration * 1000000);
            avg_latency = ((double) total_latency / 1000 / total_ops) ;
        }
        if (cur_duration > 1 && active_threads == cfg->threads) {
            cur_ops_sec = (long)( (double)cur_ops / cur_duration * 1000000);
            cur_bytes_sec = (long)( (double)cur_bytes / cur_duration * 1000000);
            cur_latency = ((double) cur_total_latency / 1000 / cur_ops) ;
        }

        char bytes_str[40], cur_bytes_str[40];
        size_to_str(bytes_sec, bytes_str, sizeof(bytes_str)-1);
        size_to_str(cur_bytes_sec, cur_bytes_str, sizeof(cur_bytes_str)-1);

        double progress = 0;
        if(cfg->requests)
            progress = 100.0 * total_ops / ((double)cfg->requests*cfg->clients*cfg->threads);
        else
            progress = 100.0 * (duration / 1000000.0)/cfg->test_time;

        // Only show connection errors if there are any (backwards compatible output)
        if (total_connection_errors > 0) {
            fprintf(stderr, "[RUN #%u %.0f%%, %3u secs] %2u threads %2u conns %lu conn errors: %11lu ops, %7lu (avg: %7lu) ops/sec, %s/sec (avg: %s/sec), %5.2f (avg: %5.2f) msec latency\r",
                run_id, progress, (unsigned int) (duration / 1000000), active_threads, cfg->clients, total_connection_errors, total_ops, cur_ops_sec, ops_sec, cur_bytes_str, bytes_str, cur_latency, avg_latency);
        } else {
            fprintf(stderr, "[RUN #%u %.0f%%, %3u secs] %2u threads %2u conns: %11lu ops, %7lu (avg: %7lu) ops/sec, %s/sec (avg: %s/sec), %5.2f (avg: %5.2f) msec latency\r",
                run_id, progress, (unsigned int) (duration / 1000000), active_threads, cfg->clients, total_ops, cur_ops_sec, ops_sec, cur_bytes_str, bytes_str, cur_latency, avg_latency);
        }

        // Send metrics to StatsD if configured
        if (cfg->statsd != NULL && cfg->statsd->is_enabled()) {
            cfg->statsd->gauge("ops_sec", (long)cur_ops_sec);
            cfg->statsd->gauge("ops_sec_avg", (long)ops_sec);
            cfg->statsd->gauge("bytes_sec", (long)cur_bytes_sec);
            cfg->statsd->gauge("bytes_sec_avg", (long)bytes_sec);
            cfg->statsd->timing("latency_ms", cur_latency);
            cfg->statsd->timing("latency_avg_ms", avg_latency);
            cfg->statsd->gauge("connections", (long)(cfg->clients * active_threads));
            cfg->statsd->gauge("progress_pct", progress);
            if (total_connection_errors > 0) {
                cfg->statsd->gauge("connection_errors", (long)total_connection_errors);
            }

            // Calculate and send percentile metrics from instantaneous histograms
            // Allocate a temporary histogram to aggregate all threads' instantaneous histograms
            hdr_histogram* temp_histogram = NULL;
            if (hdr_init(LATENCY_HDR_MIN_VALUE, LATENCY_HDR_SEC_MAX_VALUE, LATENCY_HDR_SEC_SIGDIGTS, &temp_histogram) == 0) {
                // Aggregate instantaneous histograms from all threads
                for (std::vector<cg_thread*>::iterator i = threads.begin(); i != threads.end(); i++) {
                    if (!(*i)->m_finished) {
                        (*i)->m_cg->aggregate_inst_histogram(temp_histogram);
                    }
                }

                // Only calculate percentiles if we have samples
                if (hdr_total_count(temp_histogram) > 0) {
                    // Get the configured percentiles from config
                    const std::vector<float>& quantiles = cfg->print_percentiles.quantile_list;

                    // Calculate and send each configured percentile
                    for (std::size_t i = 0; i < quantiles.size(); i++) {
                        double percentile = quantiles[i];
                        int64_t value = hdr_value_at_percentile(temp_histogram, percentile);
                        double value_ms = value / (double)LATENCY_HDR_RESULTS_MULTIPLIER;

                        // Format the metric name (e.g., "latency_p50", "latency_p99", "latency_p99_9")
                        char metric_name[32];
                        if (percentile == (int)percentile) {
                            snprintf(metric_name, sizeof(metric_name), "latency_p%d", (int)percentile);
                        } else {
                            // Replace decimal point with underscore for metric name
                            snprintf(metric_name, sizeof(metric_name), "latency_p%.1f", percentile);
                            // Replace '.' with '_' in the metric name
                            for (char* p = metric_name; *p; p++) {
                                if (*p == '.') *p = '_';
                            }
                        }

                        cfg->statsd->gauge(metric_name, value_ms);
                    }
                }

                // Clean up the temporary histogram
                hdr_close(temp_histogram);
            }
        }
    } while (active_threads > 0);

    // Send "run completed" annotation and zero out gauges so graphs drop to 0
    if (cfg->statsd != NULL && cfg->statsd->is_enabled()) {
        char event_data[64];
        snprintf(event_data, sizeof(event_data) - 1, "run=%u", run_id);
        cfg->statsd->event("Benchmark Completed", event_data, "memtier,end");

        // Zero out gauges so the graph shows the run has ended
        cfg->statsd->gauge("ops_sec", (long)0);
        cfg->statsd->gauge("ops_sec_avg", (long)0);
        cfg->statsd->gauge("bytes_sec", (long)0);
        cfg->statsd->gauge("bytes_sec_avg", (long)0);
        cfg->statsd->gauge("progress_pct", (long)0);
    }

    fprintf(stderr, "\n\n");

    // join all threads back and unify stats
    run_stats stats(cfg);

    for (std::vector<cg_thread*>::iterator i = threads.begin(); i != threads.end(); i++) {
        (*i)->join();
        (*i)->m_cg->merge_run_stats(&stats);
    }

    // Do we need to produce client stats?
    if (cfg->client_stats != NULL) {
        unsigned int cg_id = 0;
        fprintf(stderr, "[RUN %u] Writing client stats files...\n", run_id);
        for (std::vector<cg_thread*>::iterator i = threads.begin(); i != threads.end(); i++) {
            char prefix[PATH_MAX];

            snprintf(prefix, sizeof(prefix)-1, "%s-%u-%u", cfg->client_stats, run_id, cg_id++);
            (*i)->m_cg->write_client_stats(prefix);
        }
    }

    // clean up all client_groups.  the main value of this is to be able to
    // properly look for leaks...
    while (threads.size() > 0) {
        cg_thread* t = *threads.begin();
        threads.erase(threads.begin());
        delete t;
    }

    g_threads = NULL;  // Clear global pointer

    return stats;
}

#ifdef USE_TLS

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
static pthread_mutex_t *__openssl_locks;

static void __openssl_locking_callback(int mode, int type, const char *file, int line)
{
    if (mode & CRYPTO_LOCK) {
        pthread_mutex_lock(&(__openssl_locks[type]));
    } else {
        pthread_mutex_unlock(&(__openssl_locks[type]));
    }
}

static unsigned long __openssl_thread_id(void)
{
    unsigned long id;

    id = (unsigned long) pthread_self();
    return id;
}
#pragma GCC diagnostic pop

static void init_openssl_threads(void)
{
    int i;

    __openssl_locks = (pthread_mutex_t *) malloc(CRYPTO_num_locks() * sizeof(pthread_mutex_t));
    assert(__openssl_locks != NULL);

    for (i = 0; i < CRYPTO_num_locks(); i++) {
        pthread_mutex_init(&(__openssl_locks[i]), NULL);
    }

    CRYPTO_set_id_callback(__openssl_thread_id);
    CRYPTO_set_locking_callback(__openssl_locking_callback);
}

static void cleanup_openssl_threads(void)
{
    int i;

    CRYPTO_set_locking_callback(NULL);
    for (i = 0; i < CRYPTO_num_locks(); i++) {
        pthread_mutex_destroy(&(__openssl_locks[i]));
    }
    OPENSSL_free(__openssl_locks);
}

static void init_openssl(void)
{
    SSL_library_init();
    SSL_load_error_strings();
    if (!RAND_poll()) {
        fprintf(stderr, "Failed to initialize OpenSSL random entropy.\n");
        exit(1);
    }

    //Enable memtier benchmark to load an OpenSSL config file.
    #if OPENSSL_VERSION_NUMBER < 0x10100000L
    OPENSSL_config(NULL);
    #else
    OPENSSL_init_crypto(OPENSSL_INIT_LOAD_CONFIG, NULL);
    #endif


    init_openssl_threads();
}

static void cleanup_openssl(void)
{
    cleanup_openssl_threads();
}

#endif

int main(int argc, char *argv[])
{
    // Install signal handler for Ctrl+C
    signal(SIGINT, sigint_handler);

    // Install crash handlers for debugging
    setup_crash_handlers();

    // Enable core dumps
    struct rlimit core_limit;
    core_limit.rlim_cur = RLIM_INFINITY;
    core_limit.rlim_max = RLIM_INFINITY;
    if (setrlimit(RLIMIT_CORE, &core_limit) != 0) {
        fprintf(stderr, "warning: failed to set core dump limit: %s\n", strerror(errno));
        fprintf(stderr, "warning: core dumps may not be generated on crash\n");
    }

    benchmark_config cfg = benchmark_config();
    cfg.arbitrary_commands = new arbitrary_command_list();

    if (config_parse_args(argc, argv, &cfg) < 0) {
        usage();
    }

    // Process URI if provided
    if (cfg.uri) {
        // Check for conflicts with individual connection parameters
        if (cfg.server && strcmp(cfg.server, "localhost") != 0) {
            fprintf(stderr, "warning: both URI and --host/--server specified, URI takes precedence.\n");
        }
        if (cfg.port && cfg.port != 6379) {
            fprintf(stderr, "warning: both URI and --port specified, URI takes precedence.\n");
        }
        if (cfg.authenticate) {
            fprintf(stderr, "warning: both URI and --authenticate specified, URI takes precedence.\n");
        }
        if (cfg.select_db) {
            fprintf(stderr, "warning: both URI and --select-db specified, URI takes precedence.\n");
        }
#ifdef USE_TLS
        if (cfg.tls) {
            fprintf(stderr, "warning: both URI and --tls specified, URI takes precedence.\n");
        }
#endif

        if (parse_uri(cfg.uri, &cfg) < 0) {
            exit(1);
        }

        // Validate cluster mode constraints
        if (cfg.cluster_mode && cfg.select_db > 0) {
            fprintf(stderr, "error: database selection not supported in cluster mode. Redis Cluster only supports database 0.\n");
            exit(1);
        }
    }

    config_init_defaults(&cfg);

    // Validate jitter parameters
    if (cfg.thread_conn_start_min_jitter_micros > cfg.thread_conn_start_max_jitter_micros) {
        fprintf(stderr, "error: thread-conn-start-min-jitter-micros (%u) cannot be greater than thread-conn-start-max-jitter-micros (%u).\n",
                cfg.thread_conn_start_min_jitter_micros, cfg.thread_conn_start_max_jitter_micros);
        exit(1);
    }

    log_level = cfg.debug;

    // Initialize StatsD client if configured
    cfg.statsd = NULL;
    if (cfg.statsd_host != NULL) {
        cfg.statsd = new statsd_client();
        if (!cfg.statsd->init(cfg.statsd_host, cfg.statsd_port, cfg.statsd_prefix, cfg.statsd_run_label)) {
            fprintf(stderr, "warning: failed to initialize StatsD client, metrics will not be sent\n");
            delete cfg.statsd;
            cfg.statsd = NULL;
        }
    }

    if (cfg.show_config) {
        fprintf(stderr, "============== Configuration values: ==============\n");
        config_print(stdout, &cfg);
        fprintf(stderr, "===================================================\n");
    }

    // if user configure arbitrary commands, format and prepare it
    for (unsigned int i=0; i<cfg.arbitrary_commands->size(); i++) {
        abstract_protocol* tmp_protocol = protocol_factory(cfg.protocol);
        assert(tmp_protocol != NULL);

        if (!tmp_protocol->format_arbitrary_command(cfg.arbitrary_commands->at(i))) {
            exit(1);
        }

        // Cluster mode supports only a single key commands
        if (cfg.cluster_mode && cfg.arbitrary_commands->at(i).keys_count > 1) {
            benchmark_error_log("error: Cluster mode supports only a single key commands\n");
            exit(1);
        }
        delete tmp_protocol;
    }

    // if user configured rate limiting, do some calculations
    if (cfg.request_rate) {
        /* Our event resolution is (at least) 50 events per second (event every >= 20 ml).
         * When we calculate the number of request per interval, we are taking
         * the upper bound and adjust the interval accordingly to get more accuracy */
        cfg.request_per_interval = (cfg.request_rate + 50 - 1) / 50;
        unsigned int events_per_second = cfg.request_rate / cfg.request_per_interval;
        cfg.request_interval_microsecond = 1000000 / events_per_second;
        benchmark_debug_log("Rate limiting configured to send %u requests per %u millisecond\n", cfg.request_per_interval, cfg.request_interval_microsecond / 1000);
    }

#ifdef USE_TLS
    // Initialize OpenSSL only if we're really going to use it.
    if (cfg.tls) {
        init_openssl();

        cfg.openssl_ctx = SSL_CTX_new(SSLv23_client_method());
        SSL_CTX_set_options(cfg.openssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);

        if (!(cfg.tls_protocols & REDIS_TLS_PROTO_TLSv1))
            SSL_CTX_set_options(cfg.openssl_ctx, SSL_OP_NO_TLSv1);
        if (!(cfg.tls_protocols & REDIS_TLS_PROTO_TLSv1_1))
            SSL_CTX_set_options(cfg.openssl_ctx, SSL_OP_NO_TLSv1_1);
        if (!(cfg.tls_protocols & REDIS_TLS_PROTO_TLSv1_2))
            SSL_CTX_set_options(cfg.openssl_ctx, SSL_OP_NO_TLSv1_2);
// TLS 1.3 is only available as from version 1.1.1.
#if OPENSSL_VERSION_NUMBER >= 0x10101000L
        if (!(cfg.tls_protocols & REDIS_TLS_PROTO_TLSv1_3))
            SSL_CTX_set_options(cfg.openssl_ctx, SSL_OP_NO_TLSv1_3);
#endif

        if (cfg.tls_cert) {
            if (!SSL_CTX_use_certificate_chain_file(cfg.openssl_ctx, cfg.tls_cert)) {
                ERR_print_errors_fp(stderr);
                fprintf(stderr, "Error: Failed to load certificate file.\n");
                exit(1);
            }
            if (!SSL_CTX_use_PrivateKey_file(cfg.openssl_ctx,
                        cfg.tls_key ? cfg.tls_key : cfg.tls_cert,
                        SSL_FILETYPE_PEM)) {
                ERR_print_errors_fp(stderr);
                fprintf(stderr, "Error: Failed to load private key file.\n");
                exit(1);
            }
        }
        if (cfg.tls_cacert) {
            if (!SSL_CTX_load_verify_locations(cfg.openssl_ctx, cfg.tls_cacert,
                        NULL)) {
                ERR_print_errors_fp(stderr);
                fprintf(stderr, "Error: Failed to load CA certificate file.\n");
                exit(1);
            }
        }
        SSL_CTX_set_verify(cfg.openssl_ctx,
                cfg.tls_skip_verify ? SSL_VERIFY_NONE : SSL_VERIFY_PEER,
                NULL);
    }
#endif

    // JSON file initiation
    json_handler *jsonhandler = NULL;
    if (cfg.json_out_file != NULL){
        jsonhandler = new json_handler((const char *)cfg.json_out_file);
        // We allways print the configuration to the JSON file
        config_print_to_json(jsonhandler,&cfg);
    }

    struct rlimit rlim;
    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        benchmark_error_log("error: getrlimit failed: %s\n", strerror(errno));
        exit(1);
    }

    if (cfg.unix_socket != NULL &&
        (cfg.server != NULL || cfg.port > 0)) {
        benchmark_error_log("error: UNIX domain socket and TCP cannot be used together.\n");
        exit(1);
    }

    if (cfg.server != NULL && cfg.port > 0) {
        try {
            cfg.server_addr = new server_addr(cfg.server, cfg.port, cfg.resolution);
        } catch (std::runtime_error& e) {
            benchmark_error_log("%s:%u: error: %s\n",
                    cfg.server, cfg.port, e.what());
            exit(1);
        }
    }

    unsigned int fds_needed = (cfg.threads * cfg.clients) + (cfg.threads * 10) + 10;
    if (fds_needed > rlim.rlim_cur) {
        if (fds_needed > rlim.rlim_max && getuid() != 0) {
            benchmark_error_log("error: running the tool with this number of connections requires 'root' privilegs.\n");
            exit(1);
        }
        rlim.rlim_cur = rlim.rlim_max = fds_needed;

        if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
            benchmark_error_log("error: setrlimit failed: %s\n", strerror(errno));
            exit(1);
        }
    }

    // create and configure object generator
    object_generator* obj_gen = NULL;
    imported_keylist* keylist = NULL;
    if (!cfg.data_import) {
        if (cfg.data_verify) {
            fprintf(stderr, "error: use data-verify only with data-import\n");
            exit(1);
        }
        if (cfg.no_expiry) {
            fprintf(stderr, "error: use no-expiry only with data-import\n");
            exit(1);
        }

        if (cfg.arbitrary_commands->is_defined()) {
            obj_gen = new object_generator(cfg.arbitrary_commands->size());
        } else {
            obj_gen = new object_generator();
        }
        assert(obj_gen != NULL);
    } else {
        // oss cluster API can't be enabled
        if (cfg.cluster_mode) {
            fprintf(stderr, "error: Cluster mode cannot be specified when importing.\n");
            exit(1);
        }
        // check paramters
        if (cfg.data_size ||
            cfg.data_size_list.is_defined() ||
            cfg.data_size_range.is_defined()) {
            fprintf(stderr, "error: data size cannot be specified when importing.\n");
            exit(1);
        }

        if (cfg.random_data) {
            fprintf(stderr, "error: random-data cannot be specified when importing.\n");
            exit(1);
        }

        if (!cfg.generate_keys &&
            (cfg.key_maximum || cfg.key_minimum || cfg.key_prefix)) {
                fprintf(stderr, "error: use key-minimum, key-maximum and key-prefix only with generate-keys.\n");
                exit(1);
        }

        if (!cfg.generate_keys) {
            // read keys
            fprintf(stderr, "Reading keys from %s...", cfg.data_import);
            keylist = new imported_keylist(cfg.data_import);
            assert(keylist != NULL);

            if (!keylist->read_keys()) {
                fprintf(stderr, "\nerror: failed to read keys.\n");
                exit(1);
            } else {
                fprintf(stderr, " %u keys read.\n", keylist->size());
            }
        }

        obj_gen = new import_object_generator(cfg.data_import, keylist, cfg.no_expiry);
        assert(obj_gen != NULL);

        if (dynamic_cast<import_object_generator*>(obj_gen)->open_file() != true) {
            fprintf(stderr, "error: %s: failed to open.\n", cfg.data_import);
            exit(1);
        }
    }

    if (cfg.authenticate) {
        if (cfg.protocol == PROTOCOL_MEMCACHE_TEXT) {
                fprintf(stderr, "error: authenticate can only be used with redis or memcache_binary.\n");
                usage();
        }
        if (cfg.protocol == PROTOCOL_MEMCACHE_BINARY &&
            strchr(cfg.authenticate, ':') == NULL) {
                fprintf(stderr, "error: binary_memcache credentials must be in the form of USER:PASSWORD.\n");
                usage();
        }
    }
    if (!cfg.data_import) {
        obj_gen->set_random_data(cfg.random_data);
    }

    if (cfg.select_db > 0 && !is_redis_protocol(cfg.protocol)) {
        fprintf(stderr, "error: select-db can only be used with redis protocol.\n");
        usage();
    }
    if (cfg.data_offset > 0) {
        if (cfg.data_offset > (1<<29)-1) {
            fprintf(stderr, "error: data-offset too long\n");
            usage();
        }
        if (cfg.expiry_range.min || cfg.expiry_range.max || !is_redis_protocol(cfg.protocol)) {
            fprintf(stderr, "error: data-offset can only be used with redis protocol, and cannot be used with expiry\n");
            usage();
        }
    }
    if (cfg.data_size) {
        if (cfg.data_size_list.is_defined() || cfg.data_size_range.is_defined()) {
            fprintf(stderr, "error: data-size cannot be used with data-size-list or data-size-range.\n");
            usage();
        }
        obj_gen->set_data_size_fixed(cfg.data_size);
    } else if (cfg.data_size_list.is_defined()) {
        if (cfg.data_size_range.is_defined()) {
            fprintf(stderr, "error: data-size-list cannot be used with data-size-range.\n");
            usage();
        }
        obj_gen->set_data_size_list(&cfg.data_size_list);
    } else if (cfg.data_size_range.is_defined()) {
        obj_gen->set_data_size_range(cfg.data_size_range.min, cfg.data_size_range.max);
        obj_gen->set_data_size_pattern(cfg.data_size_pattern);
    } else if (!cfg.data_import) {
        fprintf(stderr, "error: data-size, data-size-list or data-size-range must be specified.\n");
        usage();
    }

    if (!cfg.data_import || cfg.generate_keys) {
        obj_gen->set_key_prefix(cfg.key_prefix);
        obj_gen->set_key_range(cfg.key_minimum, cfg.key_maximum);
    }
    if (cfg.key_stddev>0 || cfg.key_median>0) {
        if (cfg.key_pattern[key_pattern_set]!='G' && cfg.key_pattern[key_pattern_get]!='G') {
            fprintf(stderr, "error: key-stddev and key-median are only allowed together with key-pattern set to G.\n");
            usage();
        }
        if (cfg.key_median!=0 && (cfg.key_median<cfg.key_minimum || cfg.key_median>cfg.key_maximum)) {
            fprintf(stderr, "error: key-median must be between key-minimum and key-maximum.\n");
            usage();
        }
        obj_gen->set_key_distribution(cfg.key_stddev, cfg.key_median);
    }
    obj_gen->set_expiry_range(cfg.expiry_range.min, cfg.expiry_range.max);

    // Check if Zipfian distribution is needed for global key patterns or arbitrary commands
    bool needs_zipfian = (cfg.key_pattern[key_pattern_set] == 'Z' || cfg.key_pattern[key_pattern_get] == 'Z');

    // Also check if any arbitrary command uses Zipfian distribution
    if (!needs_zipfian && cfg.arbitrary_commands->is_defined()) {
        for (size_t i = 0; i < cfg.arbitrary_commands->size(); i++) {
            if (cfg.arbitrary_commands->at(i).key_pattern == 'Z') {
                needs_zipfian = true;
                break;
            }
        }
    }

    if (needs_zipfian) {
        if (cfg.key_zipf_exp == 0.0) {
            // user can't specify 0.0, so 0.0 means unset
            cfg.key_zipf_exp = 1.0;
        }
        obj_gen->set_key_zipf_distribution(cfg.key_zipf_exp);
    }

    // Prepare output file
    FILE *outfile;
    if (cfg.out_file != NULL) {
        fprintf(stderr, "Writing results to %s...\n", cfg.out_file);
        outfile = fopen(cfg.out_file, "w");
        if (!outfile) {
            perror(cfg.out_file);
        }
    } else {
        fprintf(stderr, "Writing results to stdout\n");
        outfile = stdout;
    }

    if (!cfg.verify_only) {
        std::vector<run_stats> all_stats;
        all_stats.reserve(cfg.run_count);

        for (unsigned int run_id = 1; run_id <= cfg.run_count; run_id++) {
            if (run_id > 1)
                sleep(1);   // let connections settle

            run_stats stats = run_benchmark(run_id, &cfg, obj_gen);
            all_stats.push_back(stats);
            stats.save_hdr_full_run( &cfg,run_id );
            stats.save_hdr_get_command( &cfg,run_id );
            stats.save_hdr_set_command( &cfg,run_id );
            stats.save_hdr_arbitrary_commands( &cfg,run_id );
        }
        //
        // Print some run information
        fprintf(outfile,
               "%-9u Threads\n"
               "%-9u Connections per thread\n"
               "%-9llu %s\n",
               cfg.threads, cfg.clients,
               (unsigned long long)(cfg.requests > 0 ? cfg.requests : cfg.test_time),
               cfg.requests > 0 ? "Requests per client"  : "Seconds");

        if (jsonhandler != NULL){
            jsonhandler->open_nesting("run information");
            jsonhandler->write_obj("Threads","%u",cfg.threads);
            jsonhandler->write_obj("Connections per thread","%u",cfg.clients);
            jsonhandler->write_obj(cfg.requests > 0 ? "Requests per client"  : "Seconds","%llu",
                                   cfg.requests > 0 ? cfg.requests : (unsigned long long)cfg.test_time);
            jsonhandler->write_obj("Format version","%d",2);
            jsonhandler->close_nesting();
        }

        // If more than 1 run was used, compute best, worst and average
        // Furthermore, if print_all_runs is enabled we save separate histograms per run
        if (cfg.run_count > 1) {
            // User wants to see a separate histogram per run
            if (cfg.print_all_runs) {
                for (auto i = 0U; i < all_stats.size(); i++) {
                    auto run_title = std::string("RUN #") + std::to_string(i + 1) + " RESULTS";
                    all_stats[i].print(outfile, &cfg, run_title.c_str(), jsonhandler);
                }
            }
            // User wants the best and worst
            unsigned int min_ops_sec = (unsigned int) -1;
            unsigned int max_ops_sec = 0;
            run_stats* worst = NULL;
            run_stats* best = NULL;
            for (std::vector<run_stats>::iterator i = all_stats.begin(); i != all_stats.end(); i++) {
                unsigned long usecs = i->get_duration_usec();
                unsigned int ops_sec = (int)(((double)i->get_total_ops() / (usecs > 0 ? usecs : 1)) * 1000000);
                if (ops_sec < min_ops_sec || worst == NULL) {
                    min_ops_sec = ops_sec;
                    worst = &(*i);
                }
                if (ops_sec > max_ops_sec || best == NULL) {
                    max_ops_sec = ops_sec;
                    best = &(*i);
                }
            }
            // Best results:
            best->print(outfile, &cfg, "BEST RUN RESULTS", jsonhandler);
            // worst results:
            worst->print(outfile, &cfg, "WORST RUN RESULTS", jsonhandler);
            // average results:
            run_stats average(&cfg);
            average.aggregate_average(all_stats);
            char average_header[50];
            snprintf(average_header, sizeof(average_header) , "AGGREGATED AVERAGE RESULTS (%u runs)", cfg.run_count);
            average.print(outfile, &cfg, average_header, jsonhandler);
        } else {
            all_stats.begin()->print(outfile, &cfg, "ALL STATS", jsonhandler);
        }
    }

    // If needed, data verification is done now...
    if (cfg.data_verify) {
        struct event_base *verify_event_base = event_base_new();
        abstract_protocol *verify_protocol = protocol_factory(cfg.protocol);
        verify_client *client = new verify_client(verify_event_base, &cfg, verify_protocol, obj_gen);

        fprintf(outfile, "\n\nPerforming data verification...\n");

        // Run client in verification mode
        client->prepare();
        event_base_dispatch(verify_event_base);

        fprintf(outfile, "Data verification completed:\n"
                        "%-10llu keys verified successfuly.\n"
                        "%-10llu keys failed.\n",
                        client->get_verified_keys(),
                        client->get_errors());

        if (jsonhandler != NULL){
            jsonhandler->open_nesting("client verifications results");
            jsonhandler->write_obj("keys verified successfuly", "%-10llu",  client->get_verified_keys());
            jsonhandler->write_obj("keys failed", "%-10llu",  client->get_errors());
            jsonhandler->close_nesting();
        }

        // Clean up...
        delete client;
        delete verify_protocol;
        event_base_free(verify_event_base);
    }

    if (outfile != stdout) {
        fclose(outfile);
    }

    if (cfg.server_addr) {
        delete cfg.server_addr;
        cfg.server_addr = NULL;
    }

    if (jsonhandler != NULL) {
        // Log message for saving JSON file
        fprintf(stderr, "Saving JSON output file: %s\n", cfg.json_out_file);

        // closing the JSON
        delete jsonhandler;
    }

    delete obj_gen;
    if (keylist != NULL)
        delete keylist;

    if (cfg.arbitrary_commands != NULL) {
        delete cfg.arbitrary_commands;
    }

    // Clean up dynamically allocated strings from URI parsing
    if (cfg.uri) {
        if (cfg.server) {
            free((void*)cfg.server);
        }
        if (cfg.authenticate) {
            free((void*)cfg.authenticate);
        }
    }

    // Clean up StatsD client
    if (cfg.statsd != NULL) {
        cfg.statsd->close();
        delete cfg.statsd;
        cfg.statsd = NULL;
    }

#ifdef USE_TLS
    if(cfg.tls) {
        if (cfg.openssl_ctx) {
            SSL_CTX_free(cfg.openssl_ctx);
            cfg.openssl_ctx = NULL;
        }

        cleanup_openssl();
    }
#endif
}
