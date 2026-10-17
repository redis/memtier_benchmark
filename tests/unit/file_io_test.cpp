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

// file_reader error reporting for --data-import (#766).

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <string>

#include "file_io.h"

static int failures = 0;

#define CHECK(cond)                                                                                                    \
    do {                                                                                                               \
        if (!(cond)) {                                                                                                 \
            fprintf(stderr, "FAIL [%s:%d] %s\n", __FILE__, __LINE__, #cond);                                           \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

static const char header[] = "dumpflags, time, exptime, nbytes, nsuffix, it_flags, clsid, nkey, key, data\n";
static const char good1[] = "0, 0, 0, 5, 0, 0, 0, 3, \"k01\", \"abc\"\n";
static const char good2[] = "0, 0, 0, 5, 0, 0, 0, 3, \"k02\", \"def\"\n";

static void write_file(const char *path, const char *body)
{
    FILE *f = fopen(path, "w");
    if (f == NULL) {
        perror(path);
        exit(2);
    }
    fputs(header, f);
    fputs(body, f);
    fclose(f);
}

// Read until NULL; return the number of items.
static unsigned int drain(file_reader &r)
{
    unsigned int n = 0;
    memcache_item *i;
    while ((i = r.read_item()) != NULL) {
        delete i;
        n++;
    }
    return n;
}

int main(void)
{
    char path[] = "/tmp/memtier-file-io-test-XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0) {
        perror("mkstemp");
        return 2;
    }
    close(fd);

    file_reader r(path);
    std::string body;

    // A bad item between two good ones is an error, not the end of the file.
    body = std::string(good1) + "0, 0, 0, 9, 0, 0, 0, 3, \"k02\", \"abc\"\n" + good2;
    write_file(path, body.c_str());
    CHECK(r.open_file());
    CHECK(drain(r) == 1);
    CHECK(r.has_error());

    // open_file() clears the error: a reopened reader on a valid file reads
    // everything and reports no error.
    body = std::string(good1) + good2;
    write_file(path, body.c_str());
    CHECK(r.open_file());
    CHECK(!r.has_error());
    CHECK(drain(r) == 2);
    CHECK(!r.has_error());
    CHECK(r.is_eof());

    // A truncated last item after complete ones is treated as the end.
    body = std::string(good1) + "0, 0, 0, 5, 0, 0, 0, 3, \"k02\", \"de";
    write_file(path, body.c_str());
    CHECK(r.open_file());
    CHECK(drain(r) == 1);
    CHECK(!r.has_error());

    // ...but with no complete item it is an error.
    write_file(path, "0, 0, 0, 5, 0, 0, 0, 3, \"k02\", \"de");
    CHECK(r.open_file());
    CHECK(drain(r) == 0);
    CHECK(r.has_error());

    // Oversized lengths are rejected before any allocation.
    body = std::string(good1) + "0, 0, 0, 5, 0, 0, 0, 4294967295, \"k02\", \"def\"\n" + good2;
    write_file(path, body.c_str());
    CHECK(r.open_file());
    CHECK(drain(r) == 1);
    CHECK(r.has_error());

    unlink(path);
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("file_io_test: all checks passed\n");
    return 0;
}
