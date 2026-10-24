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

// run_stats::summarize() merges every one_second_stats of a run into a single
// one_second_stats, so the per-command hit, miss, MOVED and ASK counters hold
// whole-run sums. They must not wrap after 2^32 events (issue #642).

#include <stdio.h>

#include "run_stats_types.h"

static int failures = 0;
#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #condition);                                               \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

static const unsigned int seconds = 5000;
static const unsigned int per_second = 1000000; // 5e9 events in total, past 2^32
static const unsigned long long expected = (unsigned long long) seconds * per_second;

static void fill_second(one_second_stats &s, unsigned int second)
{
    s.reset(second);
    s.m_get_cmd.m_hits = per_second;
    s.m_get_cmd.m_misses = per_second;
    s.m_get_cmd.m_moved = per_second;
    s.m_get_cmd.m_ask = per_second;
    s.m_total_cmd.m_moved = per_second;
    s.m_total_cmd.m_ask = per_second;
}

static void test_whole_run_totals()
{
    // Same accumulation as run_stats::summarize().
    one_second_stats totals(0);
    one_second_stats second(0);
    for (unsigned int i = 0; i < seconds; i++) {
        fill_second(second, i);
        totals.merge(second);
    }

    CHECK(totals.m_get_cmd.m_hits == expected);
    CHECK(totals.m_get_cmd.m_misses == expected);
    CHECK(totals.m_get_cmd.m_moved == expected);
    CHECK(totals.m_get_cmd.m_ask == expected);
    CHECK(totals.m_total_cmd.m_moved == expected);
    CHECK(totals.m_total_cmd.m_ask == expected);

    // MOVED/sec and ASK/sec over the run duration.
    totals_cmd result;
    result.summarize(totals.m_total_cmd, (unsigned long long) seconds * 1000000);
    CHECK(result.m_moved_sec == (double) per_second);
    CHECK(result.m_ask_sec == (double) per_second);
}

static void test_update_op_accumulation()
{
    one_sec_cmd_stats stats;
    stats.m_hits = 4294967295ULL;
    stats.m_misses = 4294967295ULL;
    stats.update_op(0, 0, 0, 1, 1);
    CHECK(stats.m_hits == 4294967296ULL);
    CHECK(stats.m_misses == 4294967296ULL);
}

int main()
{
    test_whole_run_totals();
    test_update_op_accumulation();
    if (failures) fprintf(stderr, "%d run stats counter checks failed\n", failures);
    return failures ? 1 : 0;
}
