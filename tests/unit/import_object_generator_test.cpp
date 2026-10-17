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

// Import-generator copies must preserve whether their independent reader is open.
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <memory>
#include <new>
#include <type_traits>
#ifdef _WIN32
#include <windows.h>
#endif
#include "obj_gen.h"

static int failures = 0;
static void check(bool condition, const char *message)
{
    if (!condition) {
        fprintf(stderr, "FAIL: %s\n", message);
        ++failures;
    }
}

static void read_item(import_object_generator &generator, const char *key, const char *value)
{
    generator.read_next_item();
    unsigned int len = 0;
    const char *data = generator.get_value(0, &len);
    check(generator.get_key_len() == static_cast<int>(strlen(key)) &&
              memcmp(generator.get_key(), key, strlen(key)) == 0,
          "copied reader key");
    check(len == strlen(value) && memcmp(data, value, len) == 0, "copied reader value");
}

static void check_open_copy(const char *path, unsigned char fill)
{
    import_object_generator original(path, NULL, true);
    original.set_data_size_fixed(32);
    if (!original.open_file()) {
        check(false, "open original");
        return;
    }
    read_item(original, "a", "11");

    // Fill storage before construction, never overwrite a live object. This
    // makes the omitted-state regression independent of allocator contents.
    std::aligned_storage<sizeof(import_object_generator), alignof(import_object_generator)>::type storage;
    memset(&storage, fill, sizeof(storage));
    import_object_generator *first = new (&storage) import_object_generator(original);
    read_item(*first, "a", "11");
    std::unique_ptr<import_object_generator> second(first->clone());
    first->~import_object_generator();
    // Each copy starts its own stream; destroying its parent does not close it.
    read_item(*second, "a", "11");
    read_item(original, "b", "hello");
    read_item(*second, "b", "hello");
    read_item(*second, "a", "11"); // Reopen at EOF remains independent.
}

int main()
{
#ifdef _WIN32
    char dir[MAX_PATH], path[MAX_PATH];
    if (!GetTempPathA(sizeof(dir), dir) || !GetTempFileNameA(dir, "mtc", 0, path)) return 2;
#else
    char path[] = "/tmp/memtier-import-copy-XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0) return 2;
    close(fd);
#endif
    {
        // The empty fixture cannot be opened as an import file yet. Copies of
        // an unopened generator must stay unopened until explicitly requested.
        import_object_generator unopened(path, NULL, true);
        unopened.set_data_size_fixed(32);
        std::aligned_storage<sizeof(import_object_generator), alignof(import_object_generator)>::type storage;
        memset(&storage, 0xa5, sizeof(storage));
        import_object_generator *first = new (&storage) import_object_generator(unopened);
        std::unique_ptr<import_object_generator> second(first->clone());
        first->~import_object_generator();

        FILE *file = fopen(path, "wb");
        if (!file) {
            unlink(path);
            return 2;
        }
        fputs("dumpflags, time, exptime, nbytes, nsuffix, it_flags, clsid, nkey, key, data\n"
              "0, 0, 0, 4, 0, 0, 0, 1, \"a\", \"11\"\n"
              "0, 0, 0, 7, 0, 0, 0, 1, \"b\", \"hello\"\n",
              file);
        if (fclose(file)) {
            unlink(path);
            return 2;
        }
        if (second->open_file())
            read_item(*second, "a", "11");
        else
            check(false, "open previously unopened copy");
    }
    check_open_copy(path, 0);
    check_open_copy(path, 0xa5);
    unlink(path);
    if (failures) return 1;
    puts("import_object_generator_test: all checks passed");
    return 0;
}
