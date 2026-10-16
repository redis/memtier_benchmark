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

int main()
{
    arbitrary_command_list commands;
    benchmark_config config = {};
    config.arbitrary_commands = &commands;

    test_many_clients(&config);
    test_small_weights_exact(&config);
    if (failures) {
        fprintf(stderr, "%d check(s) failed\n", failures);
        return 1;
    }
    printf("run_stats merge: OK\n");
    return 0;
}
