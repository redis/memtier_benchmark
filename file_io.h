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

#ifndef _FILE_IO_H
#define _FILE_IO_H

#include <stdio.h>
#include "item.h"

/** Provides a mechanism to read a CSV-like memcache_dump file and extract memcache
 * items from it.
 */
class file_reader
{
protected:
    const char *m_filename; /** name of file */
    FILE *m_file;           /** handle of open file */

    unsigned int m_line; /** current line being read */

    char *read_string(unsigned int len, unsigned int alloc_len, unsigned int *actual_len);

public:
    file_reader(const char *filename);
    file_reader(const file_reader &from);
    ~file_reader();

    bool open_file(void);
    bool is_eof(void);
    memcache_item *read_item(void);
};


/** Provides a mechanism to write memcache items into a CSV-like memcache_dump file.
 */
class file_writer
{
protected:
    const char *m_filename; /** name of file */
    FILE *m_file;           /** handle of open file */

    char *get_quoted_str(char *str, int str_len, int *new_str_len);

public:
    file_writer(const char *filename);
    ~file_writer();

    bool open_file(void);
    bool write_item(memcache_item *item);
};

/** Flushes fp and, if do_close is true, closes it. Returns 0 if every write
 * to fp reached the OS, otherwise an errno value describing the failure (EIO
 * when an earlier write failed and its errno is no longer known). */
int finish_output_file(FILE *fp, bool do_close);

/** True if the write error err on the still-open fp only means that the
 * stdout reader went away (e.g. `| head`): fp is stdout or the same file, a
 * pipe or socket, and the error is EPIPE/ECONNRESET (SIGPIPE is ignored). The
 * EIO from finish_output_file() hides the real errno, which may be EAGAIN on
 * a full stdout pipe made non-blocking by a parent sharing it (e.g. Node or a
 * CI runner), so for EIO poll(): POLLERR/POLLHUP means the reader is gone.
 * Such a write error does not fail the run. */
bool stdout_reader_gone(FILE *fp, int err);

/** Flushes and closes fp, a file opened for one of the output options.
 * Returns 0 if every write reached the OS or the only failure is a stdout
 * reader that went away (stdout_reader_gone()), otherwise an errno value as
 * finish_output_file() does. */
int close_output_file(FILE *fp);

#endif /* _FILE_IO_H */
