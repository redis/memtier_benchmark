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

// libFuzzer harness for monitor_command_list::load_from_file().
//
// Writes the fuzzer input to a tmp file every iteration, then loads it through
// the production code path (getline / strchr('"') / extract_command_type).
// Catches preamble-parsing edge cases such as a missing-proxy header shape,
// missing/mismatched quotes, and multi-MB lines.

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "config_types.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    // libFuzzer drives many iterations per process; using tmpfile() keeps the
    // input on an unnamed fd backed by the kernel page cache. We rewrite each
    // iteration via a fresh tmpfile rather than reusing one to mirror
    // load_from_file()'s real I/O path (fopen by path).
    char tmpl[] = "/tmp/mbfuzz_monitor_XXXXXX";
    int fd = mkstemp(tmpl);
    if (fd < 0) {
        return 0;
    }

    if (size > 0) {
        ssize_t written = write(fd, data, size);
        (void) written; // Ignore short writes; load_from_file just sees a shorter file.
    }
    close(fd);

    monitor_command_list list;
    (void) list.load_from_file(tmpl);

    unlink(tmpl);
    return 0;
}
