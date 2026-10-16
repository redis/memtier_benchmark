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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <string>
#include <event2/event.h>
#include "client.h"

void benchmark_log_file_line(int, const char *, unsigned int, const char *, ...) {}
void benchmark_log(int, const char *, ...) {}
void report_connection_stage_failure(const char *) {}
void report_connection_stage_success() {}
bool connection_stage_steady_state_reached()
{
    return true;
}
bool is_redis_protocol(enum PROTOCOL_TYPE type)
{
    return type == PROTOCOL_REDIS_DEFAULT || type == PROTOCOL_RESP2 || type == PROTOCOL_RESP3;
}

static int failures = 0;

static void check(bool condition, const char *requirement)
{
    if (!condition) {
        fprintf(stderr, "FAIL: %s\n", requirement);
        ++failures;
    }
}

static request *retry_request(const char *command, unsigned int retries)
{
    request *req = new request(rt_set, 0, NULL, 1);
    req->m_serialized = strdup(command);
    req->m_serialized_len = strlen(command);
    req->m_retries = retries;
    return req;
}

// A retry whose drain timer fired while its connection could not send stays
// queued. A later retry with a longer backoff must not push it back: it goes
// out once the connection is set up, not after the later retry's backoff.
static void test_overdue_retry_sent_after_connect(const char *path)
{
    int listener = socket(AF_UNIX, SOCK_STREAM, 0);
    sockaddr_un address = {};
    address.sun_family = AF_UNIX;
    snprintf(address.sun_path, sizeof(address.sun_path), "%s", path);
    check(listener >= 0 && bind(listener, (sockaddr *) &address, sizeof(address)) == 0 && listen(listener, 4) == 0,
          "listener");

    arbitrary_command_list commands;
    benchmark_config config = {};
    config.arbitrary_commands = &commands;
    config.clients = config.threads = 1;
    config.key_pattern = "S:S";
    // A zero-depth pipeline sends no requests of its own.
    config.pipeline = 0;
    config.protocol = PROTOCOL_REDIS_DEFAULT;
    config.unix_socket = path;
    config.retry_on_error = true;
    config.max_retries = 5;
    config.retry_backoff_ms = 20;
    config.retry_backoff_factor = 10;

    event_base *base = event_base_new();
    abstract_protocol *protocol = protocol_factory(config.protocol);
    {
        object_generator generator;
        generator.set_data_size_fixed(1);
        client manager(base, &config, protocol, &generator);
        shard_connection &connection = *manager.get_connections().front();

        // First retry: due in 20 ms. Its timer fires while the connection is
        // still disconnected, so it stays queued.
        check(connection.enqueue_retry(retry_request("*1\r\n$5\r\nFIRST\r\n", 0)), "first retry queued");
        usleep(40000);
        event_base_loop(base, EVLOOP_NONBLOCK);
        check(connection.get_connection_state() == conn_disconnected, "connection still disconnected");

        // Third resend of another request: due in 20 ms * 10^3 = 20 s.
        check(connection.enqueue_retry(retry_request("*1\r\n$6\r\nSECOND\r\n", 3)), "second retry queued");

        connect_info unused = {};
        check(connection.connect(&unused) == 0, "connect");
        int peer = accept(listener, NULL, NULL);
        check(peer >= 0, "accept");
        fcntl(peer, F_SETFL, O_NONBLOCK);
        std::string received;
        for (int i = 0; i < 50 && received.find("FIRST") == std::string::npos; i++) {
            event_base_loop(base, EVLOOP_NONBLOCK);
            char buf[256];
            ssize_t n = read(peer, buf, sizeof(buf));
            if (n > 0) received.append(buf, n);
            usleep(10000);
        }
        check(connection.get_connection_state() == conn_connected, "connection connected");
        check(received.find("FIRST") != std::string::npos, "overdue retry sent within 500 ms of connecting");
        check(received.find("SECOND") == std::string::npos, "later retry still waits for its own backoff");
        connection.disconnect();
        event_base_loop(base, EVLOOP_NONBLOCK);
        if (peer >= 0) close(peer);
    }
    delete protocol;
    event_base_free(base);
    close(listener);
    unlink(path);
}

int main()
{
    char directory[] = "/tmp/memtier-retry-drain-XXXXXX";
    if (mkdtemp(directory) == NULL) {
        perror("mkdtemp");
        return 1;
    }
    std::string path = std::string(directory) + "/socket";
    test_overdue_retry_sent_after_connect(path.c_str());
    rmdir(directory);
    if (failures != 0) {
        fprintf(stderr, "%d retry drain check(s) failed\n", failures);
        return 1;
    }
    printf("retry drain tests passed\n");
    return 0;
}
