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

#endif /* _FILE_IO_H */
