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
 * PURITY CONTRACT (Decisions #52): no config.h, no version.h, no libevent
 * header; the build-version/git-sha config macros never appear, not even in
 * comments. build_info values arrive as text_renderer ctor string arguments.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <ctype.h>
#include <stdarg.h>
#include <stdint.h>
#include <errno.h>

#include "prometheus_metrics.h"
#include "deps/hdr_histogram/hdr_histogram.h"

void monotonic_accumulator::fill(uint64_t out[MT_NUM_COUNTERS]) const
{
    for (int m = 0; m < MT_NUM_COUNTERS; m++) {
        uint64_t total = m_base.v[m];
        for (size_t t = 0; t < m_last.size(); t++)
            total += m_last[t].v[m];
        out[m] = total;
    }
}

namespace prom
{

const char *const CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

// The single µs->s constant (one million). Its numeric literal appears
// exactly here; every conversion site uses USECS_PER_SEC (S10 grep audit).
const double USECS_PER_SEC = 1e6;

// ---------------------------------------------------------------------------
// Metric inventory (single source of truth; PLAN.md section 4 / Decisions).
// Exposition order == table order; U7 iterates this against the rendered body.
// ---------------------------------------------------------------------------
static const metric_def kMetricDefs[] = {
    {"memtier_build_info", MT_TYPE_GAUGE, "Build information for this memtier_benchmark binary; value is always 1."},
    {"memtier_start_time_seconds", MT_TYPE_GAUGE,
     "Unix time, in seconds, at which this memtier_benchmark process started."},
    {"memtier_ops_total", MT_TYPE_COUNTER,
     "Total operations completed since process start, accumulated across runs and connection restarts."},
    {"memtier_sent_bytes_total", MT_TYPE_COUNTER, "Total bytes written to the server since process start."},
    {"memtier_received_bytes_total", MT_TYPE_COUNTER, "Total bytes read from the server since process start."},
    {"memtier_hits_total", MT_TYPE_COUNTER,
     "Total read operations that found the key; tracked for GET and miss-tracked arbitrary commands only."},
    {"memtier_misses_total", MT_TYPE_COUNTER,
     "Total read operations that did not find the key; tracked for GET and miss-tracked arbitrary commands only."},
    {"memtier_errors_total", MT_TYPE_COUNTER,
     "Total commands that received an error reply after retries were exhausted; connection errors are counted "
     "separately."},
    {"memtier_connection_errors_total", MT_TYPE_COUNTER,
     "Total connection errors since process start, accumulated across runs (the StatsD transport reports the raw "
     "per-run value instead)."},
    {"memtier_retry_attempts_total", MT_TYPE_COUNTER,
     "Total command resend attempts; nonzero only with --retry-on-error."},
    {"memtier_retried_ops_total", MT_TYPE_COUNTER,
     "Total operations that eventually succeeded after at least one retry; nonzero only with --retry-on-error."},
    {"memtier_connections", MT_TYPE_GAUGE, "Client connections currently open."},
    {"memtier_threads", MT_TYPE_GAUGE, "Worker threads currently active."},
    {"memtier_run", MT_TYPE_GAUGE, "Current run number, 1-based; 0 before the first run starts."},
    {"memtier_configured_runs", MT_TYPE_GAUGE, "Configured number of runs (--run-count)."},
    {"memtier_config_test_time_seconds", MT_TYPE_GAUGE,
     "Configured --test-time in seconds; 0 when running by --requests."},
    {"memtier_latency_seconds", MT_TYPE_HISTOGRAM,
     "Client-observed request latency in seconds, accumulated from 1 Hz snapshots of each connection's in-progress "
     "second; a phase-dependent fraction of operations (about half in steady state) is sampled, so _count is not "
     "comparable to memtier_ops_total; bucket placement carries up to 1% HDR quantization; authoritative latency is "
     "the end-of-run output."},
    {"memtier_exporter_renders_total", MT_TYPE_COUNTER,
     "Number of times the /metrics body was rendered (scrapes that missed the 1-second render cache)."},
    {"memtier_exporter_snapshot_age_seconds", MT_TYPE_GAUGE,
     "Seconds since the exporter last received a snapshot from the benchmark loop, computed at render time."},
};

const metric_def *metric_defs()
{
    return kMetricDefs;
}

size_t metric_defs_count()
{
    return sizeof(kMetricDefs) / sizeof(kMetricDefs[0]);
}

// Maps mt_counter enum to its exposition name (counter rows above).
static const char *counter_metric_name(int m)
{
    switch (m) {
    case MT_OPS:
        return "memtier_ops_total";
    case MT_BYTES_TX:
        return "memtier_sent_bytes_total";
    case MT_BYTES_RX:
        return "memtier_received_bytes_total";
    case MT_HITS:
        return "memtier_hits_total";
    case MT_MISSES:
        return "memtier_misses_total";
    case MT_ERRORS:
        return "memtier_errors_total";
    case MT_CONN_ERRORS:
        return "memtier_connection_errors_total";
    case MT_RETRY_ATTEMPTS:
        return "memtier_retry_attempts_total";
    case MT_RETRIED_OPS:
        return "memtier_retried_ops_total";
    default:
        return "";
    }
}

// ---------------------------------------------------------------------------
// le formatting and escaping
// ---------------------------------------------------------------------------
std::string format_le(int64_t bound_us)
{
    char buf[64];
    snprintf(buf, sizeof(buf), "%.12g", (double) bound_us / USECS_PER_SEC);
    return std::string(buf);
}

std::string escape_label_value(const std::string &s)
{
    std::string out;
    out.reserve(s.size() + 8);
    for (size_t i = 0; i < s.size(); i++) {
        char c = s[i];
        switch (c) {
        case '\\':
            out += "\\\\";
            break;
        case '"':
            out += "\\\"";
            break;
        case '\n':
            out += "\\n";
            break;
        default:
            out += c;
        }
    }
    return out;
}

std::string escape_help(const std::string &s)
{
    std::string out;
    out.reserve(s.size() + 8);
    for (size_t i = 0; i < s.size(); i++) {
        char c = s[i];
        switch (c) {
        case '\\':
            out += "\\\\";
            break;
        case '\n':
            out += "\\n";
            break;
        default:
            out += c;
        }
    }
    return out;
}

// ---------------------------------------------------------------------------
// validate_prom_label_name (PLAN.md section 5; no <regex>, ctype loop)
// ---------------------------------------------------------------------------
label_name_result validate_prom_label_name(const std::string &name)
{
    if (name.size() > 128) return LABEL_NAME_TOO_LONG;
    if (name.empty()) return LABEL_NAME_INVALID;
    char c0 = name[0];
    if (!(isalpha((unsigned char) c0) || c0 == '_')) return LABEL_NAME_INVALID;
    for (size_t i = 1; i < name.size(); i++) {
        char c = name[i];
        if (!(isalnum((unsigned char) c) || c == '_')) return LABEL_NAME_INVALID;
    }
    if (name.size() >= 2 && name[0] == '_' && name[1] == '_') return LABEL_NAME_RESERVED_PREFIX;
    if (name == "le" || name == "quantile" || name == "version" || name == "git_sha") return LABEL_NAME_RESERVED;
    return LABEL_NAME_OK;
}

// ---------------------------------------------------------------------------
// parse_latency_buckets (PLAN.md section 5)
// ---------------------------------------------------------------------------
static const char *const kBucketErr =
    "error: --prometheus-latency-buckets must be 1-64 comma-separated, strictly ascending bucket bounds in "
    "seconds, each within [1e-06, 86400] (e.g. 0.001,0.005,0.05); +Inf is implicit and must not be listed.";

bool parse_latency_buckets(const char *s, std::vector<double> &out, std::string &err, std::string &warn)
{
    out.clear();
    err.clear();
    warn.clear();
    if (s == NULL || s[0] == '\0') {
        err = kBucketErr;
        return false;
    }

    std::vector<double> secs;
    // comma-split, no empty tokens, strtod fully consuming.
    const char *p = s;
    while (true) {
        const char *comma = strchr(p, ',');
        size_t len = comma ? (size_t) (comma - p) : strlen(p);
        if (len == 0) { // empty token
            err = kBucketErr;
            return false;
        }
        std::string tok(p, len);
        // trim is intentionally NOT performed: leading/trailing space makes the
        // token not fully consumed below -> reject (matches statsd-style strict parse).
        char *endp = NULL;
        errno = 0;
        double v = strtod(tok.c_str(), &endp);
        if (endp != tok.c_str() + tok.size() || endp == tok.c_str()) {
            err = kBucketErr;
            return false;
        }
        if (!isfinite(v) || v <= 0.0 || v < 1e-6 || v > 86400.0) {
            err = kBucketErr; // hard cap 86400 s BEFORE quantization (Decisions #45)
            return false;
        }
        secs.push_back(v);
        if (!comma) break;
        p = comma + 1;
    }

    if (secs.empty() || secs.size() > 64) {
        err = kBucketErr;
        return false;
    }

    // Quantize to µs and check strict ascent on the µs grid.
    std::vector<int64_t> us(secs.size());
    for (size_t i = 0; i < secs.size(); i++) {
        us[i] = (int64_t) llround(secs[i] * USECS_PER_SEC);
        if (i > 0 && us[i] <= us[i - 1]) {
            err = kBucketErr; // descending OR adjacent-equal µs
            return false;
        }
    }
    // also reject a non-ascending seconds list whose µs collapsed (covered above)
    // and a plain descending seconds list:
    for (size_t i = 1; i < secs.size(); i++) {
        if (secs[i] <= secs[i - 1]) {
            err = kBucketErr;
            return false;
        }
    }

    // Slot-collision check (Decisions #27): adjacent equal slot edges -> reject.
    {
        hdr_histogram *q = NULL;
        if (hdr_init(PROM_HDR_MIN_VALUE, PROM_HDR_MAX_VALUE, PROM_HDR_SIGDIGITS, &q) != 0) {
            err = kBucketErr;
            return false;
        }
        std::vector<int64_t> edge(us.size());
        for (size_t i = 0; i < us.size(); i++)
            edge[i] = hdr_next_non_equivalent_value(q, us[i]) - 1;
        hdr_close(q);
        for (size_t i = 1; i < edge.size(); i++) {
            if (edge[i] <= edge[i - 1]) {
                err = kBucketErr;
                return false;
            }
        }
    }

    // Warn-accept zones, decided on quantized µs; first out-of-range offender
    // only, ONE warning. W2 (floor) takes precedence over W2b (cap) when both
    // occur, because the first offender (lowest index) wins.
    for (size_t i = 0; i < us.size(); i++) {
        if (us[i] < 10) {
            char buf[256];
            snprintf(buf, sizeof(buf),
                     "warning: --prometheus-latency-buckets: bound %g is below the 1e-05 s recordable latency "
                     "floor; samples below the floor are clamped up to it at recording time, so this bucket may "
                     "stay empty.",
                     secs[i]);
            warn = buf;
            break;
        }
        if (us[i] > 600000000LL) {
            char buf[256];
            snprintf(buf, sizeof(buf),
                     "warning: --prometheus-latency-buckets: bound %g is above the 600 s recordable latency cap; "
                     "samples above the cap are clamped down to it at recording time, so this bucket will always "
                     "contain every sample.",
                     secs[i]);
            warn = buf;
            break;
        }
    }

    out = secs;
    return true;
}

// ---------------------------------------------------------------------------
// Default buckets (26 finite bounds, seconds; PLAN.md section 4)
// ---------------------------------------------------------------------------
const std::vector<double> &text_renderer::default_buckets()
{
    static const double kVals[] = {0.0001, 0.00025, 0.0005, 0.00075, 0.001, 0.0015, 0.002, 0.003, 0.004,
                                   0.005,  0.0075,  0.01,   0.015,   0.02,  0.03,   0.05,  0.075, 0.1,
                                   0.25,   0.5,     1,      2.5,     5,     10,     30,    60};
    static const std::vector<double> v(kVals, kVals + sizeof(kVals) / sizeof(kVals[0]));
    return v;
}

// ---------------------------------------------------------------------------
// append_fmt: 512-byte stack scratch + vsnprintf, no per-render allocation.
// ---------------------------------------------------------------------------
static void append_fmt(std::string &out, const char *fmt, ...)
{
    char buf[512];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    if (n < 0) return;
    if ((size_t) n < sizeof(buf)) {
        out.append(buf, (size_t) n);
    } else {
        // rare overflow path: grow a heap buffer (only happens with very long
        // label blocks; still no growth in the steady-state numeric path).
        std::vector<char> big((size_t) n + 1);
        va_start(ap, fmt);
        vsnprintf(&big[0], big.size(), fmt, ap);
        va_end(ap);
        out.append(&big[0], (size_t) n);
    }
}

// ---------------------------------------------------------------------------
// text_renderer
// ---------------------------------------------------------------------------
text_renderer::text_renderer(const std::string &build_version, const std::string &build_git_sha,
                             const std::vector<label> &run_labels, const std::vector<double> &bucket_bounds_sec)
{
    const std::vector<double> &bounds = bucket_bounds_sec.empty() ? default_buckets() : bucket_bounds_sec;

    m_bounds_us.resize(bounds.size());
    for (size_t j = 0; j < bounds.size(); j++)
        m_bounds_us[j] = (int64_t) llround(bounds[j] * USECS_PER_SEC);

    // The ONLY hdr_init/hdr_close in prometheus_metrics.cpp: slot-edge
    // quantization of the bucket bounds (Decisions #21).
    m_edge_us.resize(m_bounds_us.size());
    {
        hdr_histogram *q = NULL;
        if (hdr_init(PROM_HDR_MIN_VALUE, PROM_HDR_MAX_VALUE, PROM_HDR_SIGDIGITS, &q) != 0) abort();
        for (size_t j = 0; j < m_bounds_us.size(); j++)
            m_edge_us[j] = hdr_next_non_equivalent_value(q, m_bounds_us[j]) - 1;
        hdr_close(q);
    }

    build_prefixes(build_version, build_git_sha, run_labels);
}

void text_renderer::build_prefixes(const std::string &build_version, const std::string &build_git_sha,
                                   const std::vector<label> &run_labels)
{
    // m_run_label_inner: "k=\"v\",..." (no braces) for reuse on bucket lines.
    std::string inner;
    for (size_t i = 0; i < run_labels.size(); i++) {
        inner += run_labels[i].first;
        inner += "=\"";
        inner += escape_label_value(run_labels[i].second);
        inner += "\"";
        if (i + 1 < run_labels.size()) inner += ",";
    }
    m_run_label_inner = inner;

    // m_label_block: scalar metrics carry the run-label block (or "" if none).
    if (inner.empty())
        m_label_block.clear();
    else
        m_label_block = "{" + inner + "}";

    // m_build_info_block: version,git_sha reserved last after run labels.
    {
        std::string b = "{";
        if (!inner.empty()) {
            b += inner;
            b += ",";
        }
        b += "version=\"";
        b += escape_label_value(build_version);
        b += "\",git_sha=\"";
        b += escape_label_value(build_git_sha);
        b += "\"}";
        m_build_info_block = b;
    }

    // m_bucket_prefix[j]: "memtier_latency_seconds_bucket{<run labels,>le=\"X\"} "
    m_bucket_prefix.resize(m_bounds_us.size());
    for (size_t j = 0; j < m_bounds_us.size(); j++) {
        std::string pfx = "memtier_latency_seconds_bucket{";
        if (!inner.empty()) {
            pfx += inner;
            pfx += ",";
        }
        pfx += "le=\"";
        pfx += format_le(m_bounds_us[j]);
        pfx += "\"} ";
        m_bucket_prefix[j] = pfx;
    }
}

// help/type blocks for one family.
static void emit_help_type(std::string &out, const metric_def &d)
{
    const char *type_str = (d.type == MT_TYPE_COUNTER)     ? "counter"
                           : (d.type == MT_TYPE_HISTOGRAM) ? "histogram"
                                                           : "gauge";
    append_fmt(out, "# HELP %s %s\n", d.name, escape_help(d.help).c_str());
    append_fmt(out, "# TYPE %s %s\n", d.name, type_str);
}

void text_renderer::render(std::string &out, const metrics_snapshot &snap, const struct hdr_histogram *hist,
                           double start_time_seconds, double snapshot_age_seconds, uint64_t renders_total) const
{
    out.clear(); // preserves capacity

    const metric_def *defs = kMetricDefs;
    size_t ndefs = metric_defs_count();
    size_t di = 0;

    // 1. memtier_build_info{version,git_sha} = 1
    emit_help_type(out, defs[di++]);
    append_fmt(out, "memtier_build_info%s 1\n", m_build_info_block.c_str());

    // 2. memtier_start_time_seconds
    emit_help_type(out, defs[di++]);
    append_fmt(out, "memtier_start_time_seconds%s %.3f\n", m_label_block.c_str(), start_time_seconds);

    // 3-11. counters, in mt_counter order.
    for (int m = 0; m < MT_NUM_COUNTERS; m++) {
        emit_help_type(out, defs[di++]);
        append_fmt(out, "%s%s %llu\n", counter_metric_name(m), m_label_block.c_str(),
                   (unsigned long long) snap.counters[m]);
    }

    // 12. memtier_connections (gauge)
    emit_help_type(out, defs[di++]);
    append_fmt(out, "memtier_connections%s %.6g\n", m_label_block.c_str(), (double) snap.connections);

    // 13. memtier_threads (gauge)
    emit_help_type(out, defs[di++]);
    append_fmt(out, "memtier_threads%s %.6g\n", m_label_block.c_str(), (double) snap.active_threads);

    // 14. memtier_run (gauge)
    emit_help_type(out, defs[di++]);
    append_fmt(out, "memtier_run%s %.6g\n", m_label_block.c_str(), (double) snap.run_id);

    // 15. memtier_configured_runs (gauge)
    emit_help_type(out, defs[di++]);
    append_fmt(out, "memtier_configured_runs%s %.6g\n", m_label_block.c_str(), (double) snap.run_count);

    // 16. memtier_config_test_time_seconds (gauge): the configured --test-time,
    // carried on the snapshot (stamped by the exporter's publish() from
    // options.test_time). 0 when running by --requests.
    emit_help_type(out, defs[di++]);
    append_fmt(out, "memtier_config_test_time_seconds%s %.6g\n", m_label_block.c_str(), (double) snap.test_time);

    // 17. memtier_latency_seconds (histogram): cumulative bucket walk.
    emit_help_type(out, defs[di++]);
    {
        size_t n = m_edge_us.size();
        std::vector<uint64_t> cum_at(n, 0);
        uint64_t cum = 0;
        double sum_us = 0.0;
        if (hist != NULL) {
            hdr_iter it;
            hdr_iter_recorded_init(&it, hist);
            size_t j = 0;
            while (hdr_iter_next(&it)) {
                const int64_t v = it.highest_equivalent_value;
                while (j < n && m_edge_us[j] < v)
                    cum_at[j++] = cum;
                cum += (uint64_t) it.count;
                sum_us += (double) it.count * (double) it.median_equivalent_value;
            }
            while (j < n)
                cum_at[j++] = cum;
        }
        for (size_t j = 0; j < n; j++) {
            out += m_bucket_prefix[j];
            append_fmt(out, "%llu\n", (unsigned long long) cum_at[j]);
        }
        uint64_t total = (hist != NULL) ? (uint64_t) hdr_total_count(hist) : 0;
        // +Inf bucket == _count
        if (!m_run_label_inner.empty())
            append_fmt(out, "memtier_latency_seconds_bucket{%s,le=\"+Inf\"} %llu\n", m_run_label_inner.c_str(),
                       (unsigned long long) total);
        else
            append_fmt(out, "memtier_latency_seconds_bucket{le=\"+Inf\"} %llu\n", (unsigned long long) total);
        append_fmt(out, "memtier_latency_seconds_count%s %llu\n", m_label_block.c_str(), (unsigned long long) total);
        append_fmt(out, "memtier_latency_seconds_sum%s %.9g\n", m_label_block.c_str(), sum_us / USECS_PER_SEC);
    }

    // 18. memtier_exporter_renders_total (counter)
    emit_help_type(out, defs[di++]);
    append_fmt(out, "memtier_exporter_renders_total%s %llu\n", m_label_block.c_str(),
               (unsigned long long) renders_total);

    // 19. memtier_exporter_snapshot_age_seconds (gauge)
    emit_help_type(out, defs[di++]);
    append_fmt(out, "memtier_exporter_snapshot_age_seconds%s %.3f\n", m_label_block.c_str(), snapshot_age_seconds);

    (void) ndefs;
    (void) snap;
}

} // namespace prom
