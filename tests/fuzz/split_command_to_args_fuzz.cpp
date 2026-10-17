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

// libFuzzer harness for arbitrary_command::split_command_to_args().
//
// Drives arbitrary bytes into cmd.command and calls the splitter. Catches
// stack/heap overflows on the scratch buffer (see PR #405 which fixed a VLA
// stack overflow on the 19.97 MB MONITOR-input bug), \xNN decode bugs, and
// unterminated-quote loops.

#include <stddef.h>
#include <stdint.h>

#include "config_types.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    arbitrary_command cmd("X");
    cmd.command.assign(reinterpret_cast<const char *>(data), size);
    (void) cmd.split_command_to_args();
    return 0;
}
