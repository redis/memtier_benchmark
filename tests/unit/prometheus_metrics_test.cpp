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
 * Unit tests U1-U9 for the pure metrics layer (PLAN.md section 10).
 * Assert-style main, no gtest. PURITY: no config.h / version.h / libevent.
 * Links prometheus_metrics.cpp + hdr_histogram (a leaf dep). U2 ships a clamp
 * replica pinned to run_stats.cpp:46-47 instead of linking run_stats.cpp.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdint.h>
#include <string>
#include <vector>
#include <map>

#include "prometheus_metrics.h"
#include "deps/hdr_histogram/hdr_histogram.h"

static int g_failures = 0;
static int g_checks = 0;

#define CHECK(cond, msg)                                                                                               \
    do {                                                                                                               \
        g_checks++;                                                                                                    \
        if (!(cond)) {                                                                                                 \
            fprintf(stderr, "FAIL [%s:%d] %s\n", __FILE__, __LINE__, msg);                                             \
            g_failures++;                                                                                              \
        }                                                                                                              \
    } while (0)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// U2 ground-truth replica of the file-static hdr_record_value_capped_atomic
// clamp (run_stats.cpp:46-47). The clamp (never skip) IS the ground truth.
// The S10 drift grep byte-pins those two upstream lines.
static bool test_record_capped(struct hdr_histogram *h, int64_t value)
{
    int64_t capped = (value > h->highest_trackable_value) ? h->highest_trackable_value : value;
    capped = (capped < h->lowest_trackable_value) ? h->lowest_trackable_value : capped;
    return hdr_record_value(h, capped);
}

static struct hdr_histogram *new_hist()
{
    struct hdr_histogram *h = NULL;
    int rc = hdr_init(PROM_HDR_MIN_VALUE, PROM_HDR_MAX_VALUE, PROM_HDR_SIGDIGITS, &h);
    if (rc != 0 || h == NULL) {
        fprintf(stderr, "FATAL: hdr_init failed\n");
        exit(2);
    }
    return h;
}

// Extract the numeric value of "<line_prefix> <value>\n" from a rendered body.
// line_prefix is matched at line start. Returns true if found.
static bool find_line_value(const std::string &body, const std::string &line_prefix, std::string &val_out)
{
    size_t pos = 0;
    while (pos < body.size()) {
        size_t eol = body.find('\n', pos);
        if (eol == std::string::npos) eol = body.size();
        std::string line = body.substr(pos, eol - pos);
        if (line.compare(0, line_prefix.size(), line_prefix) == 0 && line.size() > line_prefix.size() &&
            line[line_prefix.size()] == ' ') {
            val_out = line.substr(line_prefix.size() + 1);
            return true;
        }
        pos = eol + 1;
    }
    return false;
}

// Count occurrences of a substring.
static int count_occurrences(const std::string &hay, const std::string &needle)
{
    int n = 0;
    size_t pos = 0;
    while ((pos = hay.find(needle, pos)) != std::string::npos) {
        n++;
        pos += needle.size();
    }
    return n;
}

static prom::text_renderer make_default_renderer()
{
    std::vector<prom::text_renderer::label> labels;
    std::vector<double> empty;
    return prom::text_renderer("1.4.0", "deadbeef", labels, empty);
}

static void zero_snapshot(metrics_snapshot &s)
{
    memset(&s, 0, sizeof(s));
}

// ---------------------------------------------------------------------------
// U1 — slot-edge bucket placement, µs->s, ms canary
// ---------------------------------------------------------------------------
static void U1()
{
    prom::text_renderer r = make_default_renderer();
    struct hdr_histogram *h = new_hist();
    test_record_capped(h, 1000000); // 1 000 000 µs = 1 s

    metrics_snapshot s;
    zero_snapshot(s);
    std::string body;
    r.render(body, s, h, 0.0, 0.0, 0);

    std::string v;
    // le="0.5" -> 0
    CHECK(find_line_value(body, "memtier_latency_seconds_bucket{le=\"0.5\"}", v) && atoi(v.c_str()) == 0,
          "U1: le=0.5 must be 0 for a 1s sample");
    // le="1" -> 1 (exact-bound inclusivity via slot-edge quantization)
    CHECK(find_line_value(body, "memtier_latency_seconds_bucket{le=\"1\"}", v) && atoi(v.c_str()) == 1,
          "U1: le=1 must be 1 for a 1s sample (slot-edge inclusivity)");
    // _sum ~ 1.0 s (ms canary: /1000 -> 0.001, /1e9 -> tiny; both trip)
    CHECK(find_line_value(body, "memtier_latency_seconds_sum", v), "U1: _sum line present");
    double sum = atof(v.c_str());
    CHECK(sum >= 0.99 && sum <= 1.01, "U1: _sum within [0.99,1.01] (ms/ns canary)");
    // +Inf == _count == 1
    CHECK(find_line_value(body, "memtier_latency_seconds_bucket{le=\"+Inf\"}", v) && atoi(v.c_str()) == 1,
          "U1: +Inf == 1");
    CHECK(find_line_value(body, "memtier_latency_seconds_count", v) && atoi(v.c_str()) == 1, "U1: _count == 1");

    hdr_close(h);
}

// U1b — exactly 100 µs lands at le="0.0001"
static void U1b()
{
    prom::text_renderer r = make_default_renderer();
    struct hdr_histogram *h = new_hist();
    test_record_capped(h, 100); // 100 µs = 0.0001 s

    metrics_snapshot s;
    zero_snapshot(s);
    std::string body;
    r.render(body, s, h, 0.0, 0.0, 0);

    std::string v;
    CHECK(find_line_value(body, "memtier_latency_seconds_bucket{le=\"0.0001\"}", v) && atoi(v.c_str()) == 1,
          "U1b: 100us must land at le=0.0001");
    hdr_close(h);
}

// ---------------------------------------------------------------------------
// U2 — brute-force walk equivalence vs ground truth (clamp replica)
// ---------------------------------------------------------------------------
static void U2()
{
    prom::text_renderer r = make_default_renderer();
    const std::vector<int64_t> &edge = r.edge_us();
    struct hdr_histogram *h = new_hist();

    // 100k log-uniform samples in [10 µs, 700e6 µs] via the replica.
    const int N = 100000;
    std::vector<int64_t> samples(N);
    double lo = log((double) 10);
    double hi = log((double) 700000000LL);
    unsigned int seed = 12345;
    for (int i = 0; i < N; i++) {
        double u = (double) rand_r(&seed) / (double) RAND_MAX;
        int64_t val = (int64_t) exp(lo + u * (hi - lo));
        samples[i] = val;
        test_record_capped(h, val);
    }

    // Ground truth per bound j = count of samples whose slot edge <= edge_us[j].
    struct hdr_histogram *q = new_hist();
    std::vector<uint64_t> truth(edge.size(), 0);
    for (int i = 0; i < N; i++) {
        // clamp into recordable range exactly as the replica did.
        int64_t cval = samples[i];
        if (cval > q->highest_trackable_value) cval = q->highest_trackable_value;
        if (cval < q->lowest_trackable_value) cval = q->lowest_trackable_value;
        int64_t se = hdr_next_non_equivalent_value(q, cval) - 1;
        for (size_t j = 0; j < edge.size(); j++)
            if (se <= edge[j]) truth[j]++;
    }
    hdr_close(q);

    // Render and compare every bucket.
    metrics_snapshot s;
    zero_snapshot(s);
    std::string body;
    r.render(body, s, h, 0.0, 0.0, 0);

    const std::vector<double> &bounds = prom::text_renderer::default_buckets();
    for (size_t j = 0; j < bounds.size(); j++) {
        std::string le = prom::format_le(r.bounds_us()[j]);
        std::string prefix = "memtier_latency_seconds_bucket{le=\"" + le + "\"}";
        std::string v;
        bool ok = find_line_value(body, prefix, v);
        char msg[128];
        snprintf(msg, sizeof(msg), "U2: bucket %s present", le.c_str());
        CHECK(ok, msg);
        if (ok) {
            uint64_t got = strtoull(v.c_str(), NULL, 10);
            snprintf(msg, sizeof(msg), "U2: bucket %s walk==truth (got %llu want %llu)", le.c_str(),
                     (unsigned long long) got, (unsigned long long) truth[j]);
            CHECK(got == truth[j], msg);
        }
    }
    hdr_close(h);

    // capped 700 s -> le="60"=0, +Inf == _count
    struct hdr_histogram *h2 = new_hist();
    test_record_capped(h2, 700000000LL); // 700 s in µs -> clamped to 600 s
    metrics_snapshot s2;
    zero_snapshot(s2);
    std::string body2;
    r.render(body2, s2, h2, 0.0, 0.0, 0);
    std::string v2;
    CHECK(find_line_value(body2, "memtier_latency_seconds_bucket{le=\"60\"}", v2) && atoi(v2.c_str()) == 0,
          "U2: capped 700s -> le=60 == 0");
    CHECK(find_line_value(body2, "memtier_latency_seconds_bucket{le=\"+Inf\"}", v2) && atoi(v2.c_str()) == 1,
          "U2: capped 700s -> +Inf == 1");
    CHECK(find_line_value(body2, "memtier_latency_seconds_count", v2) && atoi(v2.c_str()) == 1,
          "U2: capped 700s -> _count == 1");
    hdr_close(h2);
}

// ---------------------------------------------------------------------------
// U3 — empty histogram: full render, all families, zeros, ends \n
// ---------------------------------------------------------------------------
static void U3()
{
    prom::text_renderer r = make_default_renderer();
    struct hdr_histogram *h = new_hist();
    metrics_snapshot s;
    zero_snapshot(s);
    std::string body;
    r.render(body, s, h, 0.0, 0.0, 0);

    // every metric family present.
    const prom::metric_def *defs = prom::metric_defs();
    for (size_t i = 0; i < prom::metric_defs_count(); i++) {
        std::string help = "# HELP " + std::string(defs[i].name) + " ";
        std::string type = "# TYPE " + std::string(defs[i].name) + " ";
        char msg[160];
        snprintf(msg, sizeof(msg), "U3: family %s HELP present", defs[i].name);
        CHECK(body.find(help) != std::string::npos, msg);
        snprintf(msg, sizeof(msg), "U3: family %s TYPE present", defs[i].name);
        CHECK(body.find(type) != std::string::npos, msg);
    }
    std::string v;
    CHECK(find_line_value(body, "memtier_latency_seconds_count", v) && atoi(v.c_str()) == 0, "U3: _count 0");
    CHECK(find_line_value(body, "memtier_latency_seconds_sum", v) && atof(v.c_str()) == 0.0, "U3: _sum 0");
    CHECK(!body.empty() && body[body.size() - 1] == '\n', "U3: body ends with \\n");
    CHECK(body.find("\n\n") == std::string::npos, "U3: no blank lines");
    hdr_close(h);
}

// ---------------------------------------------------------------------------
// U4 — escaping + validate_prom_label_name + parse_latency_buckets matrix
// ---------------------------------------------------------------------------
static void U4()
{
    using namespace prom;

    // escaping
    CHECK(escape_label_value("a\"b\\c") == "a\\\"b\\\\c", "U4: escape_label_value backslash+quote");
    CHECK(escape_label_value("a\nb") == "a\\nb", "U4: escape_label_value newline");
    CHECK(escape_help("a\\b\nc") == "a\\\\b\\nc", "U4: escape_help backslash+newline");
    CHECK(escape_help("a\"b") == "a\"b", "U4: escape_help leaves quotes");

    // validate_prom_label_name accepts
    CHECK(validate_prom_label_name("phase") == LABEL_NAME_OK, "U4: accept phase");
    CHECK(validate_prom_label_name("_x9") == LABEL_NAME_OK, "U4: accept _x9");
    // rejects
    CHECK(validate_prom_label_name("1bad") == LABEL_NAME_INVALID, "U4: reject 1bad");
    CHECK(validate_prom_label_name("k-ey") == LABEL_NAME_INVALID, "U4: reject k-ey");
    CHECK(validate_prom_label_name("") == LABEL_NAME_INVALID, "U4: reject empty");
    CHECK(validate_prom_label_name("le") == LABEL_NAME_RESERVED, "U4: reject le");
    CHECK(validate_prom_label_name("quantile") == LABEL_NAME_RESERVED, "U4: reject quantile");
    CHECK(validate_prom_label_name("version") == LABEL_NAME_RESERVED, "U4: reject version");
    CHECK(validate_prom_label_name("git_sha") == LABEL_NAME_RESERVED, "U4: reject git_sha");
    CHECK(validate_prom_label_name("__x") == LABEL_NAME_RESERVED_PREFIX, "U4: reject __x");
    CHECK(validate_prom_label_name(std::string(129, 'a')) == LABEL_NAME_TOO_LONG, "U4: reject 129-byte");

    std::vector<double> out;
    std::string err, warn;

    // buckets — rejects
    CHECK(!parse_latency_buckets("", out, err, warn), "U4: reject empty string");
    CHECK(!parse_latency_buckets(",", out, err, warn), "U4: reject empty token");
    CHECK(!parse_latency_buckets("abc", out, err, warn), "U4: reject junk");
    CHECK(!parse_latency_buckets("inf", out, err, warn), "U4: reject inf");
    CHECK(!parse_latency_buckets("nan", out, err, warn), "U4: reject nan");
    CHECK(!parse_latency_buckets("1,+Inf", out, err, warn), "U4: reject +Inf token");
    CHECK(!parse_latency_buckets("0", out, err, warn), "U4: reject 0");
    CHECK(!parse_latency_buckets("-1", out, err, warn), "U4: reject negative");
    CHECK(!parse_latency_buckets("0.005,0.001", out, err, warn), "U4: reject descending");
    CHECK(!parse_latency_buckets("1e-7", out, err, warn), "U4: reject below 1e-6");
    CHECK(!parse_latency_buckets("86400.000001", out, err, warn), "U4: reject over-cap");
    CHECK(!parse_latency_buckets("1e13", out, err, warn), "U4: reject 1e13 (llround overflow canary)");
    CHECK(!parse_latency_buckets("1e300", out, err, warn), "U4: reject 1e300");
    CHECK(!parse_latency_buckets("0.001,0.0010005", out, err, warn), "U4: reject same-µs");
    CHECK(!parse_latency_buckets("0.001,0.001001", out, err, warn), "U4: reject same-HDR-slot");
    CHECK(!parse_latency_buckets("86300,86400", out, err, warn), "U4: reject top-of-range same-slot");
    // 65 entries
    {
        std::string big;
        for (int i = 1; i <= 65; i++) {
            char b[32];
            snprintf(b, sizeof(b), "%s%g", (i > 1 ? "," : ""), 0.0001 * i);
            big += b;
        }
        CHECK(!parse_latency_buckets(big.c_str(), out, err, warn), "U4: reject 65 entries");
    }
    // every reject sets the canonical E11 string
    CHECK(err.find("strictly ascending bucket bounds in seconds") != std::string::npos,
          "U4: reject uses canonical E11 string");

    // accepts
    CHECK(parse_latency_buckets("0.00201,0.00202", out, err, warn) && warn.empty(),
          "U4: accept adjacent slots no warn");
    CHECK(parse_latency_buckets("0.5", out, err, warn) && out.size() == 1, "U4: accept single 0.5");
    CHECK(parse_latency_buckets("86400", out, err, warn), "U4: accept 86400 (the cap)");
    CHECK(!warn.empty() && warn.find("will always contain every sample") != std::string::npos, "U4: 86400 warns W2b");

    // warn-accepts
    CHECK(parse_latency_buckets("0.000005", out, err, warn) &&
              warn.find("below the 1e-05 s recordable latency floor") != std::string::npos,
          "U4: 0.000005 warns W2 floor");
    CHECK(parse_latency_buckets("700", out, err, warn) &&
              warn.find("will always contain every sample") != std::string::npos,
          "U4: 700 warns W2b cap");
    // first offender wins: floor string
    CHECK(parse_latency_buckets("0.000005,700", out, err, warn) &&
              warn.find("below the 1e-05 s recordable latency floor") != std::string::npos,
          "U4: 0.000005,700 -> only W2 floor (first offender)");
}

// ---------------------------------------------------------------------------
// U5 — monotonic accumulator
// ---------------------------------------------------------------------------
static counter_set cs1(uint64_t ops)
{
    counter_set c;
    c.v[MT_OPS] = ops;
    return c;
}

static uint64_t fill_ops(const monotonic_accumulator &a)
{
    uint64_t out[MT_NUM_COUNTERS];
    a.fill(out);
    return out[MT_OPS];
}

static void U5()
{
    // (a) safety net + monotone fill
    {
        monotonic_accumulator a;
        a.init(2);
        uint64_t prev = 0;
        a.observe_live(0, cs1(10));
        a.observe_live(1, cs1(7)); // 17
        CHECK(fill_ops(a) == 17 && fill_ops(a) >= prev, "U5a: 10,7 -> 17");
        prev = fill_ops(a);
        a.observe_live(0, cs1(20));
        a.observe_live(1, cs1(9)); // 29
        CHECK(fill_ops(a) == 29 && fill_ops(a) >= prev, "U5a: 20,9 -> 29");
        prev = fill_ops(a);
        a.observe_live(0, cs1(5)); // clamp: base+=20, last0=5 -> 5+9+20=34
        CHECK(fill_ops(a) == 34 && fill_ops(a) >= prev, "U5a: clamp -> 34");
        prev = fill_ops(a);
        a.observe_live(1, cs1(3)); // clamp: base+=9, last1=3 -> 5+3+(20+9)=37? expect 47
        // base after: prior base = 20 (from ops 0->5) + 9 (from ops 9->3) = 29.
        // last0=5,last1=3 => 29+5+3 = 37. Plan pins (15,3)->47, see note below.
        a.observe_live(0, cs1(15)); // last0=15 -> 29+15+3 = 47
        CHECK(fill_ops(a) == 47 && fill_ops(a) >= prev, "U5a: 15,3 -> 47");
    }
    // (b) restart fold
    {
        monotonic_accumulator a;
        a.init(1);
        a.observe_live(0, cs1(100));
        CHECK(fill_ops(a) == 100, "U5b: observe 100");
        a.fold_final(0, cs1(120)); // base=120, last0=0
        CHECK(fill_ops(a) == 120, "U5b: fold_final 120");
        a.observe_live(0, cs1(5)); // last0=5 -> 125
        CHECK(fill_ops(a) == 125, "U5b: observe 5 -> 125");
        a.fold_final(0, cs1(30)); // base += max(30,5)=30 -> 150, last0=0
        CHECK(fill_ops(a) == 150, "U5b: fold_final 30 -> 150");
    }
    // (c) run-end clamp-up + last zeroed
    {
        monotonic_accumulator a;
        a.init(2);
        a.observe_live(0, cs1(50));
        a.observe_live(1, cs1(40)); // 90
        CHECK(fill_ops(a) == 90, "U5c: observe 50,40 -> 90");
        a.fold_final(0, cs1(45)); // clamp up to 50: base += 50
        a.fold_final(1, cs1(60)); // base += 60
        CHECK(fill_ops(a) == 110, "U5c: fold clamps -> 110");
    }
    // (d) failed-restart idempotence: re-fold zeros -> unchanged
    {
        monotonic_accumulator a;
        a.init(1);
        a.observe_live(0, cs1(100));
        a.fold_final(0, cs1(120)); // 120, last0=0
        CHECK(fill_ops(a) == 120, "U5d: 120");
        a.fold_final(0, counter_set()); // zeros: base += max(0,0)=0
        CHECK(fill_ops(a) == 120, "U5d: idempotent re-fold -> 120");
    }
}

// ---------------------------------------------------------------------------
// U6 — capacity + data() pointer stable from render #1 over 1000 renders
// ---------------------------------------------------------------------------
static void U6()
{
    // maximal config: 16 run labels (long values), 64 buckets.
    std::vector<prom::text_renderer::label> labels;
    for (int i = 0; i < 16; i++) {
        char k[32];
        snprintf(k, sizeof(k), "label_%02d", i);
        labels.push_back(std::make_pair(std::string(k), std::string(384, 'x')));
    }
    std::vector<double> bounds;
    for (int i = 1; i <= 64; i++)
        bounds.push_back(0.0001 * i + 0.0000001 * i); // strictly ascending, distinct slots
    // sanitize via parser to guarantee distinct slots
    std::vector<double> ok;
    {
        std::string err, warn;
        // build a known-good 64-bound list
        std::string list;
        double v = 1e-4; // start above the floor; the low-µs zone has coarse slots
        for (int i = 0; i < 64; i++) {
            char b[32];
            snprintf(b, sizeof(b), "%s%.9g", (i > 0 ? "," : ""), v);
            list += b;
            v *= 1.2; // 20% spacing > 1% slot width, guarantees distinct slots
        }
        bool good = prom::parse_latency_buckets(list.c_str(), ok, err, warn);
        CHECK(good, "U6: 64-bound list parses");
        if (!good) ok = bounds;
    }

    prom::text_renderer r("1.4.0", "deadbeef", labels, ok);
    struct hdr_histogram *h = new_hist();
    for (int i = 0; i < 200; i++)
        test_record_capped(h, 1000 + i * 37);

    std::string body;
    body.reserve(16384);
    metrics_snapshot s;
    zero_snapshot(s);

    r.render(body, s, h, 0.0, 0.0, 1);
    size_t cap1 = body.capacity();
    const char *ptr1 = body.data();

    for (int i = 0; i < 1000; i++) {
        r.render(body, s, h, 0.0, 0.0, (uint64_t) (i + 2));
        CHECK(body.capacity() == cap1, "U6: capacity stable after render #1");
        CHECK(body.data() == ptr1, "U6: data() pointer stable after render #1");
        if (body.capacity() != cap1 || body.data() != ptr1) break;
    }
    hdr_close(h);
}

// ---------------------------------------------------------------------------
// U7 — exposition shape, table-driven
// ---------------------------------------------------------------------------
static bool ends_with(const std::string &s, const std::string &suffix)
{
    return s.size() >= suffix.size() && s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

static void U7()
{
    prom::text_renderer r = make_default_renderer();
    struct hdr_histogram *h = new_hist();
    test_record_capped(h, 1500);
    metrics_snapshot s;
    zero_snapshot(s);
    s.counters[MT_OPS] = 123;
    std::string body;
    r.render(body, s, h, 1700000000.0, 0.25, 5);

    const prom::metric_def *defs = prom::metric_defs();
    for (size_t i = 0; i < prom::metric_defs_count(); i++) {
        const char *name = defs[i].name;
        const char *type_str = (defs[i].type == prom::MT_TYPE_COUNTER)     ? "counter"
                               : (defs[i].type == prom::MT_TYPE_HISTOGRAM) ? "histogram"
                                                                           : "gauge";
        std::string block = "# HELP " + std::string(name) + " " + prom::escape_help(defs[i].help) + "\n# TYPE " +
                            std::string(name) + " " + type_str + "\n";
        char msg[160];
        snprintf(msg, sizeof(msg), "U7: HELP+TYPE block for %s exactly once", name);
        CHECK(count_occurrences(body, block) == 1, msg);

        // counters end _total; gauges/histogram families don't carry _total name
        if (defs[i].type == prom::MT_TYPE_COUNTER) {
            snprintf(msg, sizeof(msg), "U7: counter %s ends _total", name);
            CHECK(ends_with(std::string(name), "_total"), msg);
        } else if (defs[i].type == prom::MT_TYPE_GAUGE) {
            snprintf(msg, sizeof(msg), "U7: gauge %s not _total", name);
            CHECK(!ends_with(std::string(name), "_total"), msg);
        }
    }

    // le is the last label on bucket lines; le strings parse-float-equal bounds.
    const std::vector<double> &bounds = prom::text_renderer::default_buckets();
    for (size_t j = 0; j < bounds.size(); j++) {
        std::string le = prom::format_le(r.bounds_us()[j]);
        double parsed = atof(le.c_str());
        double want = (double) r.bounds_us()[j] / 1e6;
        char msg[128];
        snprintf(msg, sizeof(msg), "U7: le %s parses to bound", le.c_str());
        CHECK(fabs(parsed - want) < 1e-12 * (want > 1 ? want : 1.0), msg);
    }

    // no trailing whitespace on any line; ends \n
    CHECK(ends_with(body, "\n"), "U7: ends \\n");
    {
        size_t pos = 0;
        bool tw = false;
        while (pos < body.size()) {
            size_t eol = body.find('\n', pos);
            if (eol == std::string::npos) eol = body.size();
            if (eol > pos && (body[eol - 1] == ' ' || body[eol - 1] == '\t')) tw = true;
            pos = eol + 1;
        }
        CHECK(!tw, "U7: no trailing whitespace");
    }

    // monotone re-render after more samples
    std::string v_before;
    CHECK(find_line_value(body, "memtier_latency_seconds_count", v_before), "U7: count line");
    for (int i = 0; i < 50; i++)
        test_record_capped(h, 2000 + i);
    std::string body2;
    r.render(body2, s, h, 1700000000.0, 0.25, 6);
    std::string v_after;
    CHECK(find_line_value(body2, "memtier_latency_seconds_count", v_after), "U7: count line 2");
    CHECK(strtoull(v_after.c_str(), NULL, 10) >= strtoull(v_before.c_str(), NULL, 10),
          "U7: _count monotone after hdr_add");

    hdr_close(h);
}

// ---------------------------------------------------------------------------
// U8 — format pins
// ---------------------------------------------------------------------------
static void U8()
{
    using namespace prom;
    // canonical division-form invariant for defaults + extra vectors.
    const std::vector<double> &bounds = text_renderer::default_buckets();
    std::vector<int64_t> us;
    for (size_t i = 0; i < bounds.size(); i++)
        us.push_back((int64_t) llround(bounds[i] * 1e6));
    int64_t extra[] = {5, 999, 600000000LL, 6123456789LL, 86399999999LL, 86400000000LL};
    for (size_t i = 0; i < sizeof(extra) / sizeof(extra[0]); i++)
        us.push_back(extra[i]);

    for (size_t i = 0; i < us.size(); i++) {
        std::string le = format_le(us[i]);
        double got = strtod(le.c_str(), NULL);
        double want = (double) us[i] / USECS_PER_SEC;
        char msg[128];
        snprintf(msg, sizeof(msg), "U8: division-form losslessness for %lld µs (le=%s)", (long long) us[i], le.c_str());
        CHECK(got == want, msg);
    }

    // default list byte-equal to the 26 literal strings.
    const char *literals[] = {"0.0001", "0.00025", "0.0005", "0.00075", "0.001", "0.0015", "0.002", "0.003", "0.004",
                              "0.005",  "0.0075",  "0.01",   "0.015",   "0.02",  "0.03",   "0.05",  "0.075", "0.1",
                              "0.25",   "0.5",     "1",      "2.5",     "5",     "10",     "30",    "60"};
    CHECK(bounds.size() == sizeof(literals) / sizeof(literals[0]), "U8: default bucket count == 26");
    for (size_t i = 0; i < bounds.size(); i++) {
        std::string le = format_le((int64_t) llround(bounds[i] * 1e6));
        char msg[128];
        snprintf(msg, sizeof(msg), "U8: default le[%zu] == '%s' (got '%s')", i, literals[i], le.c_str());
        CHECK(le == literals[i], msg);
    }

    // CONTENT_TYPE byte pin
    CHECK(std::string(CONTENT_TYPE) == "text/plain; version=0.0.4; charset=utf-8", "U8: CONTENT_TYPE pin");

    // metrics_snapshot memset+assign+memcmp (trivial copyability proxy)
    metrics_snapshot a, b;
    memset(&a, 0, sizeof(a));
    a.counters[MT_OPS] = 42;
    a.run_id = 3;
    a.connections = 7;
    a.published_at.tv_sec = 123;
    b = a;
    CHECK(memcmp(&a, &b, sizeof(a)) == 0, "U8: metrics_snapshot trivial copy memcmp-equal");
}

// U9 — prom::hdr_add_positive_delta folds each sample at most once.
// Regression guard for the gated-copy double-count fix (run_stats.cpp
// copy_inst_histogram_if_changed). Before that fix the growing branch did
// hdr_add(target, inst), re-counting samples already folded on the previous
// 1 Hz tick when two ticks landed within one worker-second. Reverting to the
// always-add behaviour makes U9b fail (target total would be 6, not 4).
static void U9()
{
    // (a) basic: only positive per-bucket deltas are folded; buckets where cur
    // shrank versus prev contribute nothing.
    {
        struct hdr_histogram *prev = new_hist();
        struct hdr_histogram *cur = new_hist();
        struct hdr_histogram *target = new_hist();
        hdr_record_value(prev, 100);
        hdr_record_values(prev, 200, 2);
        hdr_record_value(prev, 300);    // cur will have fewer here
        hdr_record_values(cur, 100, 2); // +1 over prev
        hdr_record_values(cur, 200, 3); // +1 over prev
        // value 300 absent in cur -> delta -1, must be ignored
        hdr_record_value(cur, 500); // +1 (new bucket)
        prom::hdr_add_positive_delta(target, cur, prev);
        CHECK(hdr_total_count(target) == 3, "U9a: only positive per-bucket deltas folded (+1+1+1)");
        CHECK(hdr_count_at_value(target, 100) == 1, "U9a: bucket 100 delta == 1");
        CHECK(hdr_count_at_value(target, 200) == 1, "U9a: bucket 200 delta == 1");
        CHECK(hdr_count_at_value(target, 300) == 0, "U9a: shrinking bucket 300 contributes 0");
        CHECK(hdr_count_at_value(target, 500) == 1, "U9a: new bucket 500 delta == 1");
        hdr_close(prev);
        hdr_close(cur);
        hdr_close(target);
    }

    // (b) two ticks within one worker-second with a growing (never-reset) inst
    // histogram, mirroring copy_inst_histogram_if_changed's growing branch (add
    // positive delta, then snapshot inst into last). The lifetime target must
    // equal the true sample count, not double it.
    {
        struct hdr_histogram *inst = new_hist();   // per-second inst histogram
        struct hdr_histogram *last = new_hist();   // m_prom_last_inst snapshot
        struct hdr_histogram *target = new_hist(); // lifetime histogram
        int64_t last_total = 0;

        // tick 1: 2 samples this second
        hdr_record_value(inst, 100);
        hdr_record_value(inst, 200);
        int64_t cur = hdr_total_count(inst);
        if (cur != last_total) { // growing (cur > last_total)
            prom::hdr_add_positive_delta(target, inst, last);
            hdr_reset(last);
            hdr_add(last, inst);
            last_total = cur;
        }
        // tick 2: SAME worker-second, inst grew by 2 more (no reset)
        hdr_record_values(inst, 300, 2);
        cur = hdr_total_count(inst);
        if (cur != last_total) {
            prom::hdr_add_positive_delta(target, inst, last);
            hdr_reset(last);
            hdr_add(last, inst);
            last_total = cur;
        }
        CHECK(hdr_total_count(target) == 4, "U9b: two same-second ticks fold 4 samples once (not 6)");
        CHECK(hdr_count_at_value(target, 100) == 1, "U9b: value 100 folded once");
        CHECK(hdr_count_at_value(target, 200) == 1, "U9b: value 200 folded once");
        CHECK(hdr_count_at_value(target, 300) == 2, "U9b: value 300 folded twice (2 samples)");
        hdr_close(inst);
        hdr_close(last);
        hdr_close(target);
    }
}

int main()
{
    U1();
    U1b();
    U2();
    U3();
    U4();
    U5();
    U6();
    U7();
    U8();
    U9();

    if (g_failures == 0) {
        printf("PASS: all %d checks passed (U1-U9)\n", g_checks);
        return 0;
    }
    fprintf(stderr, "FAILED: %d of %d checks failed\n", g_failures, g_checks);
    return 1;
}
