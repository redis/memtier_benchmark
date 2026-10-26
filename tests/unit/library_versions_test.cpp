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

#include <stdio.h>

#include "library_versions.h"

static int failures = 0;

#define CHECK(cond)                                                                                                    \
    do {                                                                                                               \
        if (!(cond)) {                                                                                                 \
            fprintf(stderr, "FAIL [%s:%d] %s\n", __FILE__, __LINE__, #cond);                                           \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

int main(void)
{
    // libevent: same series, any patch level or release status.
    CHECK(!libevent_series_differs(0x02010c00UL, 0x02010c00UL)); // 2.1.12 / 2.1.12
    CHECK(!libevent_series_differs(0x02010d00UL, 0x02010c00UL)); // 2.1.13 built, 2.1.12 run
    CHECK(!libevent_series_differs(0x02010800UL, 0x02010d00UL)); // 2.1.8 built, 2.1.13 run
    CHECK(!libevent_series_differs(0x02020201UL, 0x02020200UL)); // low byte is not part of it
    // libevent: different series.
    CHECK(libevent_series_differs(0x02020200UL, 0x02010c00UL)); // 2.2.2 headers, 2.1.12 library
    CHECK(libevent_series_differs(0x02010c00UL, 0x02020200UL));
    CHECK(libevent_series_differs(0x02001600UL, 0x02010c00UL)); // 2.0.22 / 2.1.12
    CHECK(libevent_series_differs(0x02010c00UL, 0x03010c00UL)); // major differs

    // Failed TLS bufferevent creation leaves the socket to the caller from 2.1.9 on.
    CHECK(!libevent_ssl_failure_socket_is_ours(0x02010800UL)); // 2.1.8
    CHECK(!libevent_ssl_failure_socket_is_ours(0x02001600UL)); // 2.0.22
    CHECK(libevent_ssl_failure_socket_is_ours(0x02010900UL));  // 2.1.9-beta
    CHECK(libevent_ssl_failure_socket_is_ours(0x02010c00UL));  // 2.1.12
    CHECK(libevent_ssl_failure_socket_is_ours(0x02020200UL));  // 2.2.2-alpha

    // OpenSSL 3+: a newer library of the same major is compatible, an older
    // minor version than the headers is not.
    CHECK(!openssl_series_differs(0x30000000UL, 0x300000d0UL)); // 3.0.0 / 3.0.13
    CHECK(!openssl_series_differs(0x300000d0UL, 0x30000000UL)); // 3.0.13 built, 3.0.0 run (patch)
    CHECK(!openssl_series_differs(0x30000020UL, 0x30500010UL)); // 3.0.2 built, 3.5.1 run
    CHECK(openssl_series_differs(0x30500010UL, 0x30000020UL));  // 3.5.1 built, 3.0.2 run
    CHECK(openssl_series_differs(0x30600000UL, 0x30500040UL));  // 3.6.0 built, 3.5.4 run
    CHECK(openssl_series_differs(0x30000020UL, 0x40000000UL));  // 3.0.2 / 4.0.0
    CHECK(openssl_series_differs(0x30000020UL, 0x1010117fUL));  // 3.0.2 / 1.1.1w
    // OpenSSL 1.x: compatible within major.minor.
    CHECK(!openssl_series_differs(0x1010107fUL, 0x1010117fUL)); // 1.1.1g / 1.1.1w
    CHECK(!openssl_series_differs(0x1000207fUL, 0x1000214fUL)); // 1.0.2g / 1.0.2u
    CHECK(openssl_series_differs(0x1000207fUL, 0x1010117fUL));  // 1.0.2 / 1.1.1
    // LibreSSL reports a fixed number.
    CHECK(!openssl_series_differs(0x20000000UL, 0x20000000UL));
    CHECK(openssl_series_differs(0x20000000UL, 0x30000000UL));

    if (failures != 0) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("library_versions_test: all checks passed\n");
    return 0;
}
