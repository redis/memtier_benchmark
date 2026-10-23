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

// run_benchmark() merges every client of every thread into one run_stats with
// a growing weight, so the merged Start/Finish time are the mean over all
// clients (issue #709). With epoch timestamps in microseconds, the running
// mean must not overflow at thousands of clients (e.g. -t 6 -c 1000).

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <assert.h>
#include <cmath>
#include <stdio.h>
#include <stdlib.h>
#include "cluster_client.h"

void benchmark_log_file_line(int, const char *, unsigned int, const char *, ...) {}
void benchmark_log(int, const char *, ...) {}
void report_connection_stage_failure(const char *)
{
    abort();
}
void report_connection_stage_success()
{
    abort();
}
bool connection_stage_steady_state_reached()
{
    abort();
}

int failure_exit_code()
{
    return 1;
}

bool is_redis_protocol(enum PROTOCOL_TYPE type)
{
    return type == PROTOCOL_REDIS_DEFAULT || type == PROTOCOL_RESP2 || type == PROTOCOL_RESP3;
}

static int failures = 0;
#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #condition);                                               \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

static const long long epoch_usec = 1792150645520149LL;

static long long usec(const struct timeval &tv)
{
    return (long long) tv.tv_sec * 1000000 + tv.tv_usec;
}

static struct timeval at(long long t)
{
    struct timeval tv;
    tv.tv_sec = (time_t) (t / 1000000);
    tv.tv_usec = (suseconds_t) (t % 1000000);
    return tv;
}

// Merge runs that start at epoch_usec + start_offsets[i] and last
// duration_usec, as merge_run_stats() does, with one counter for all of them.
static void merge_runs(benchmark_config *config, run_stats &target, const long long *start_offsets, unsigned int n,
                       long long duration_usec)
{
    unsigned int iteration = 1;
    for (unsigned int i = 0; i < n; i++) {
        run_stats client_stats(config);
        struct timeval start = at(epoch_usec + start_offsets[i]);
        struct timeval end = at(epoch_usec + start_offsets[i] + duration_usec);
        client_stats.set_start_time(&start);
        client_stats.set_end_time(&end);
        target.merge(client_stats, iteration++);
    }
}

static void test_many_clients(benchmark_config *config)
{
    // -t 6 -c 1000: past the ~5100 merges where a * (weight - 1) wrapped.
    const unsigned int n = 6000;
    static long long offsets[n];
    long long sum = 0;
    for (unsigned int i = 0; i < n; i++) {
        offsets[i] = (i * 7919LL) % 2000; // client starts spread over 2 ms
        sum += offsets[i];
    }
    run_stats merged(config);
    merge_runs(config, merged, offsets, n, 3000000);

    long long expected_start = epoch_usec + sum / n;
    long long start = usec(merged.get_start_time());
    long long duration = (long long) merged.get_duration_usec();
    fprintf(stderr, "start %lld (expected %lld), duration %lld usec\n", start, expected_start, duration);
    // Each merge floors to a whole microsecond, which drifts the mean down by
    // 935 us here (deterministic inputs). Allow 2 ms; an overflow puts it
    // decades away.
    CHECK(llabs(start - expected_start) <= 2000);
    CHECK(duration == 3000000);
}

static void test_small_weights_exact(benchmark_config *config)
{
    // Same floor rounding as (a * (w - 1) + b) / w, for later and earlier runs.
    const long long rising[] = {0, 1, 5};   // 0, 0, (0 * 2 + 5) / 3 = 1
    const long long falling[] = {10, 0, 0}; // 10, 5, (5 * 2 + 0) / 3 = 3
    run_stats a(config), b(config);
    merge_runs(config, a, rising, 3, 1000);
    merge_runs(config, b, falling, 3, 1000);
    CHECK(usec(a.get_start_time()) == epoch_usec + 1);
    CHECK(usec(b.get_start_time()) == epoch_usec + 3);
    CHECK(b.get_duration_usec() == 1000);
}

// #463: averaging seconds and microseconds separately turns a 200 us
// interval across a second boundary into 500200 us. Exercise the production
// helper through merge(), without exposing an internal function for tests.
static void test_submillisecond_merge(benchmark_config *config)
{
    run_stats merged(config);
    for (unsigned int i = 0; i < 2; ++i) {
        run_stats client_stats(config);
        timeval start = at(1700999900LL + i * 150);
        timeval end = at(1701000100LL + i * 150);
        client_stats.set_start_time(&start);
        client_stats.update_set_op(&end, 5, 5, 200);
        client_stats.set_end_time(&end);
        merged.merge(client_stats, i + 1);
    }
    CHECK(usec(merged.get_start_time()) == 1700999975LL);
    CHECK(merged.get_duration_usec() == 200);
    totals summary;
    merged.summarize(summary);
    CHECK(summary.m_ops == 2);
    CHECK(std::fabs(summary.m_ops_sec - 10000.0) < 1e-9);

    // Arithmetic progression: at every weight 2..16 the exact mean is
    // halfway between the first and last client, even across the boundary.
    run_stats series(config);
    for (unsigned int i = 0; i < 16; ++i) {
        run_stats client_stats(config);
        timeval start = at(1700999900LL + i * 32);
        timeval end = at(1701000100LL + i * 32);
        client_stats.set_start_time(&start);
        client_stats.set_end_time(&end);
        series.merge(client_stats, i + 1);
        CHECK(usec(series.get_start_time()) == 1700999900LL + i * 16);
        CHECK(series.get_duration_usec() == 200);
    }

    // A one-microsecond difference contributes less than one microsecond
    // at each later weight. Rounding the start must not erase the duration.
    long long offsets[16] = {};
    for (unsigned int i = 1; i < 16; ++i)
        offsets[i] = 1;
    for (unsigned int n = 2; n <= 16; ++n) {
        run_stats tiny(config);
        merge_runs(config, tiny, offsets, n, 1);
        CHECK(usec(tiny.get_start_time()) == epoch_usec);
        CHECK(tiny.get_duration_usec() == 1);
    }
}

static void test_retry_counters(benchmark_config *config)
{
    std::vector<run_stats> runs;
    runs.reserve(2);
    for (unsigned int i = 0; i < 2; i++) {
        runs.emplace_back(config);
        struct timeval start = at(epoch_usec);
        struct timeval end = at(epoch_usec + 1000000);
        runs.back().set_start_time(&start);
        runs.back().set_end_time(&end);
        runs.back().inc_error(i + 1);
        for (unsigned int n = 0; n < i + 3; n++)
            runs.back().inc_retry_attempt();
        for (unsigned int n = 0; n < i + 2; n++)
            runs.back().inc_retried_op();
    }

    totals fresh;
    runs[0].summarize(fresh);
    CHECK(fresh.m_errors == 1);
    CHECK(fresh.m_retry_attempts == 3);
    CHECK(fresh.m_retried_ops == 2);
    runs[1].summarize(fresh);
    CHECK(fresh.m_errors == 2);
    CHECK(fresh.m_retry_attempts == 4);
    CHECK(fresh.m_retried_ops == 3);

    run_stats merged(config), average(config);
    merged.merge(runs[0], 1);
    merged.merge(runs[1], 2);
    CHECK(merged.get_total_errors() == 3);
    CHECK(merged.get_total_retry_attempts() == 7);
    CHECK(merged.get_total_retried_ops() == 5);
    // Stored counts stay summed; JSON applies the per-run mean when writing.
    average.aggregate_average(runs);
    CHECK(average.get_total_errors() == 3);
    CHECK(average.get_total_retry_attempts() == 7);
    CHECK(average.get_total_retried_ops() == 5);

    run_stats empty(config);
    empty.summarize(fresh);
    CHECK(fresh.m_errors == 0);
    CHECK(fresh.m_retry_attempts == 0);
    CHECK(fresh.m_retried_ops == 0);
}

// Read the aggregate without calling summarize(), which rebuilds a single-run
// summary from per-second data. Exercise the same worker -> run -> series path
// as run_benchmark(), with exact samples rather than wall-clock timings.
class inspected_run_stats : public run_stats
{
public:
    explicit inspected_run_stats(benchmark_config *config) : run_stats(config) {}
    const totals &result() const { return m_totals; }
};

static void test_sample_weighted_latency(unsigned int second_count, bool arbitrary, unsigned int first_count = 100)
{
    arbitrary_command_list commands;
    if (arbitrary) {
        commands.add_command(arbitrary_command("SET a b"));
        commands.add_command(arbitrary_command("GET a"));
    }
    benchmark_config config = {};
    config.arbitrary_commands = &commands;
    std::vector<run_stats> runs;
    runs.reserve(2);
    for (unsigned int run = 0; run < 2; ++run) {
        runs.emplace_back(&config);
        for (unsigned int client = 0; client < 2; ++client) {
            run_stats worker(&config);
            timeval start = at(epoch_usec), sample = at(epoch_usec + 100000);
            // Unequal durations ensure latency weighting cannot accidentally
            // use elapsed time, while rates retain their per-run means.
            timeval end = at(epoch_usec + (run + 1) * 1000000);
            worker.set_start_time(&start);
            const unsigned int count = run ? second_count : first_count;
            const unsigned int latency = run ? 9001 : 1000;
            for (unsigned int i = client; i < count; i += 2) {
                if (arbitrary) {
                    worker.update_arbitrary_op(&sample, 5, 5, latency, 0);
                    worker.update_arbitrary_op(&sample, 5, 5, latency * 2, 1);
                } else {
                    worker.update_set_op(&sample, 5, 5, latency);
                    worker.update_get_op(&sample, 5, 5, latency * 2, 1, 0);
                    worker.update_wait_op(&sample, latency * 3);
                }
            }
            worker.set_end_time(&end);
            runs.back().merge(worker, client + 1);
        }
    }
    inspected_run_stats average(&config);
    average.aggregate_average(runs);
    const totals &t = average.result();
    const double expected = first_count + second_count
                                ? (first_count * 1000.0 + second_count * 9001.0) / (first_count + second_count) / 1000.0
                                : 0.0;
    const double multiplier = arbitrary ? 1.5 : 2.0;
    CHECK(std::fabs(t.m_latency - expected * multiplier) < 1e-9);
    CHECK(t.m_ops == (first_count + second_count) * (arbitrary ? 2 : 3));
    CHECK(std::fabs(t.m_ops_sec - (first_count + second_count / 2.0) * (arbitrary ? 2 : 3) / 2) < 1e-9);
    if (arbitrary) {
        CHECK(std::fabs(t.m_ar_commands[0].m_latency - expected) < 1e-9);
        CHECK(std::fabs(t.m_ar_commands[1].m_latency - expected * 2) < 1e-9);
    } else {
        CHECK(std::fabs(t.m_set_cmd.m_latency - expected) < 1e-9);
        CHECK(std::fabs(t.m_get_cmd.m_latency - expected * 2) < 1e-9);
        CHECK(std::fabs(t.m_wait_cmd.m_latency - expected * 3) < 1e-9);
    }
}

int main()
{
    arbitrary_command_list commands;
    benchmark_config config = {};
    config.arbitrary_commands = &commands;

    test_many_clients(&config);
    test_small_weights_exact(&config);
    test_submillisecond_merge(&config);
    test_retry_counters(&config);
    for (unsigned int count : {0U, 10U, 100U}) {
        test_sample_weighted_latency(count, false);
        test_sample_weighted_latency(count, true);
    }
    test_sample_weighted_latency(0, false, 0);
    test_sample_weighted_latency(0, true, 0);
    if (failures) {
        fprintf(stderr, "%d check(s) failed\n", failures);
        return 1;
    }
    printf("run_stats merge: OK\n");
    return 0;
}
