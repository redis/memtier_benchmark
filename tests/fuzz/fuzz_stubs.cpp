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

// Tiny shim providing the logging symbols that protocol.cpp expects from
// memtier_benchmark.cpp. The fuzzers link only a handful of production TUs;
// pulling in memtier_benchmark.cpp would drag in main(), libevent loop setup,
// the whole stats stack, etc., which would defeat the in-process fuzzing
// model. We swallow log output instead.

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include "memtier_benchmark.h"
#include "obj_gen.h"

// C++ linkage to match the extern declarations in memtier_benchmark.h.
void benchmark_log_file_line(int /*level*/, const char * /*filename*/, unsigned int /*line*/, const char * /*fmt*/, ...)
{
    // No-op: silence the parser's debug/error chatter during fuzzing.
}

void benchmark_log(int /*level*/, const char * /*fmt*/, ...)
{
    // No-op.
}

// protocol.cpp also references is_redis_protocol() (defined in
// memtier_benchmark.cpp). Replicate the predicate exactly so RESP-fuzz
// PROTOCOL_RESP2/RESP3 routing works identically to production.
bool is_redis_protocol(enum PROTOCOL_TYPE type)
{
    return type == PROTOCOL_REDIS_DEFAULT || type == PROTOCOL_RESP2 || type == PROTOCOL_RESP3;
}

// Stubs for object_generator references pulled in by
// monitor_command_list::get_random_command(). The fuzzers never call any of
// these, but the linker can't dead-strip member functions whose vtable /
// typeinfo are emitted into the .o, so we satisfy the symbols here rather
// than pull in the full obj_gen.cpp / random_generator stack.
object_generator::~object_generator() {}

object_generator *object_generator::clone(void)
{
    abort();
}

const char *object_generator::get_value(unsigned long long /*key_index*/, unsigned int * /*len*/)
{
    abort();
}

unsigned int object_generator::get_expiry()
{
    abort();
}

unsigned long long object_generator::random_range(unsigned long long /*r_min*/, unsigned long long /*r_max*/)
{
    abort();
}
