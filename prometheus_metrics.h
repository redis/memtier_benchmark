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

/*
 * prometheus_metrics.{h,cpp} is the PURE, config-free, libevent-free metrics
 * layer of the Prometheus exporter (PLAN.md v5 sections 3.1, 3.3, 3.5, 3.7,
 * 4, 5; Decisions #52 purity contract). It contains the counter model, the
 * metrics_snapshot POD, the monotonic accumulator, the text exposition
 * renderer (HDR-to-buckets walk + le formatting), and the bucket-list parser.
 * It MUST NOT include config.h, version.h, or any libevent header, and must
 * never reference the build-version/git-sha config macros. build_info values
 * arrive as plain-string ctor arguments supplied by prometheus_exporter.cpp.
 */

#ifndef _PROMETHEUS_METRICS_H
#define _PROMETHEUS_METRICS_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <string>
#include <vector>
#include <utility>

struct hdr_histogram;

/*
 * Mirror of the run_stats SEC HDR parameters (run_stats_types.h:25-27,
 * Decisions #22). prometheus_exporter.cpp static_asserts these against
 * LATENCY_HDR_SEC_MIN_VALUE / LATENCY_HDR_SEC_MAX_VALUE / LATENCY_HDR_SEC_SIGDIGTS;
 * this header cannot include run_stats_types.h (it pulls memtier_benchmark.h).
 */
#define PROM_HDR_MIN_VALUE 10
#define PROM_HDR_MAX_VALUE 600000000LL
#define PROM_HDR_SIGDIGITS 2

enum mt_counter
{
    MT_OPS = 0,
    MT_BYTES_TX,
    MT_BYTES_RX,
    MT_HITS,
    MT_MISSES,
    MT_ERRORS,
    MT_CONN_ERRORS,
    MT_RETRY_ATTEMPTS,
    MT_RETRIED_OPS,
    MT_NUM_COUNTERS
};

struct counter_set
{
    uint64_t v[MT_NUM_COUNTERS];
    counter_set() { memset(v, 0, sizeof(v)); }
};

/*
 * Fully trivial standard-layout struct (no ctor): zero with memset at every
 * fill site. No is_trivially_copyable static_assert (unreliable pre-GCC5);
 * U8's memcmp covers it.
 */
struct metrics_snapshot
{
    uint64_t counters[MT_NUM_COUNTERS]; // monotonic_accumulator::fill(); never decrease
    uint32_t run_id, run_count;         // memtier_run / memtier_configured_runs (0 = ctor snap)
    uint32_t active_threads;            // memtier_threads
    uint32_t connections;               // display_clients * active_threads
    int32_t test_time;                  // memtier_config_test_time_seconds; stamped by publish() from options
    uint64_t seq;                       // stamped by publish(); staleness assert only, never rendered
    struct timespec published_at;       // CLOCK_MONOTONIC, stamped by publish() -> snapshot_age
    // statsd-only (one producer, two transports; NOT rendered in v1):
    double progress_pct;
    long cur_ops_sec, avg_ops_sec, cur_bytes_sec, avg_bytes_sec;
    double cur_latency_ms, avg_latency_ms;
    uint64_t run_connection_errors; // RAW per-run for statsd incl. its >0 send condition
};

/*
 * Monotonic accumulator (main-thread-only; no lock — Decisions #4). Folds
 * per-thread last-seen counters plus a destroyed-group base into a
 * non-decreasing total. See PLAN.md section 3.5.
 */
class monotonic_accumulator
{
    counter_set m_base;              // folded totals of destroyed client_groups
    std::vector<counter_set> m_last; // last-seen live value per cg_thread index

public:
    void init(size_t n_threads) { m_last.assign(n_threads, counter_set()); }

    // 1 Hz benign-race sample. Decrease branch = SAFETY NET only (real basis
    // changes go through fold_final); clamps anomalous low racy reads.
    void observe_live(size_t t, const counter_set &cur)
    {
        for (int m = 0; m < MT_NUM_COUNTERS; m++) {
            if (cur.v[m] < m_last[t].v[m]) m_base.v[m] += m_last[t].v[m];
            m_last[t].v[m] = cur.v[m];
        }
    }

    // RACE-FREE post-join totals of a group about to be destroyed.
    void fold_final(size_t t, const counter_set &fin)
    {
        for (int m = 0; m < MT_NUM_COUNTERS; m++) {
            m_base.v[m] += (fin.v[m] >= m_last[t].v[m]) ? fin.v[m] : m_last[t].v[m]; // max clamp
            m_last[t].v[m] = 0;                                                      // successor group starts clean
        }
    }

    void fill(uint64_t out[MT_NUM_COUNTERS]) const; // out = base + sum(last)
};

namespace prom
{

extern const char *const CONTENT_TYPE;

// USECS_PER_SEC: the single µs->s conversion constant (one million). Its
// numeric literal appears exactly once in prometheus_metrics.cpp (the
// definition); every conversion site uses USECS_PER_SEC (S10 grep audit).
extern const double USECS_PER_SEC;

enum metric_type
{
    MT_TYPE_COUNTER,
    MT_TYPE_GAUGE,
    MT_TYPE_HISTOGRAM
};

struct metric_def
{
    const char *name;
    metric_type type;
    const char *help;
};

// The metric inventory (single source of truth, PLAN.md section 4). Returned
// by value-pointer; count via metric_defs_count(). U7 iterates these.
const metric_def *metric_defs();
size_t metric_defs_count();

// le format (Decisions #21/#45): "%.12g" of (double)bound_us / USECS_PER_SEC.
// Division-form losslessness invariant (U8):
//   strtod(format_le(us)) == (double)us / USECS_PER_SEC exactly.
std::string format_le(int64_t bound_us);

// Label-value / HELP escaping (PLAN.md section 4).
std::string escape_label_value(const std::string &s);
std::string escape_help(const std::string &s);

// validate_prom_label_name return codes (PLAN.md section 5).
enum label_name_result
{
    LABEL_NAME_OK = 0,
    LABEL_NAME_TOO_LONG,        // > 128 bytes
    LABEL_NAME_INVALID,         // charset [a-zA-Z_][a-zA-Z0-9_]*
    LABEL_NAME_RESERVED_PREFIX, // begins with __
    LABEL_NAME_RESERVED         // le / quantile / version / git_sha
};
label_name_result validate_prom_label_name(const std::string &name);

/*
 * parse_latency_buckets: comma-separated, strictly ascending bucket bounds in
 * seconds, 1-64 tokens, each within [1e-6, 86400]. On success `out` holds the
 * parsed seconds and the return value is true; on rejection returns false and
 * `err` carries the canonical E11 string. `warn` is set (W2 / W2b, first
 * out-of-range offender only) when an in-range list contains a warn-zone bound.
 * See PLAN.md section 5.
 */
bool parse_latency_buckets(const char *s, std::vector<double> &out, std::string &err, std::string &warn);

/*
 * text_renderer: the classic-text exposition renderer (PLAN.md sections 3.3,
 * 4). Construction is the ONLY place this layer opens an hdr_histogram (one
 * throwaway, for slot-edge quantization of the bucket bounds). render()
 * performs zero allocation in steady state (buffer reuse) and never opens or
 * closes a histogram.
 */
class text_renderer
{
public:
    typedef std::pair<std::string, std::string> label;

    // build_version / build_git_sha: plain strings (Decisions #52) for the
    // memtier_build_info{version,git_sha} sample. run_labels: user run labels in
    // flag order. bucket_bounds_sec: finite bucket bounds in seconds (empty =>
    // the default 26-bound list). The bounds MUST already be validated by
    // parse_latency_buckets (or be the defaults).
    text_renderer(const std::string &build_version, const std::string &build_git_sha,
                  const std::vector<label> &run_labels, const std::vector<double> &bucket_bounds_sec);

    // render the full exposition body into `out` (cleared, capacity preserved).
    // start_time_seconds: unix start time. hist: the latency histogram to walk.
    // snapshot_age_seconds / renders_total: the two exporter self-metrics.
    void render(std::string &out, const metrics_snapshot &snap, const struct hdr_histogram *hist,
                double start_time_seconds, double snapshot_age_seconds, uint64_t renders_total) const;

    // The default 26-bound bucket list, in seconds (PLAN.md section 4).
    static const std::vector<double> &default_buckets();

    // Exposed for tests: the µs-quantized slot-edge bounds.
    const std::vector<int64_t> &edge_us() const { return m_edge_us; }
    const std::vector<int64_t> &bounds_us() const { return m_bounds_us; }

private:
    void build_prefixes(const std::string &build_version, const std::string &build_git_sha,
                        const std::vector<label> &run_labels);

    std::vector<int64_t> m_bounds_us; // parse-time µs-quantized user bounds (rendered le)
    std::vector<int64_t> m_edge_us;   // inclusive upper edge of each bound's HDR slot (compare key)

    std::string m_label_block;                // "{k=\"v\",...}" for scalar metrics ("" if no run labels)
    std::string m_build_info_block;           // "{version=...,git_sha=...,<run labels>}"
    std::string m_run_label_inner;            // "k=\"v\",..." (no braces) for bucket lines, or ""
    std::vector<std::string> m_bucket_prefix; // per-le "memtier_latency_seconds_bucket{...le=\"X\"} "
};

} // namespace prom

#endif // _PROMETHEUS_METRICS_H
