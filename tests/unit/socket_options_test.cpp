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

#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

// Record the real descriptor without exposing production internals or replacing
// socket option calls. Only this translation unit's socket() calls are wrapped;
// libevent and the listeners below continue to use the normal system function.
static int observed_socket = -1;
static unsigned int socket_creations = 0;
static int recording_socket(int domain, int type, int protocol)
{
    observed_socket = ::socket(domain, type, protocol);
    ++socket_creations;
    return observed_socket;
}

// Include the system socket declarations above before replacing the call name.
#define socket recording_socket
#include "shard_connection.cpp"
#undef socket

// No Redis requests are sent. Fail loudly if the test starts reaching
// process-wide connection-stage reporting.
void report_connection_stage_failure(const char *)
{
    abort();
}
void report_connection_stage_success()
{
    abort();
}

// These main-owned helpers also appear in tests/fuzz/fuzz_stubs.cpp. That file's
// object_generator stubs conflict with the real objects needed for client RTTI.
void benchmark_log_file_line(int, const char *, unsigned int, const char *, ...) {}
void benchmark_log(int, const char *, ...) {}
bool is_redis_protocol(enum PROTOCOL_TYPE type)
{
    return type == PROTOCOL_REDIS_DEFAULT || type == PROTOCOL_RESP2 || type == PROTOCOL_RESP3;
}

static int failures = 0;

static bool check(bool condition, const char *context, const char *requirement)
{
    if (!condition) {
        fprintf(stderr, "FAIL [%s]: %s (errno=%d: %s)\n", context, requirement, errno, strerror(errno));
        ++failures;
    }
    return condition;
}

static void check_option(int fd, int level, int option, const char *context, const char *name)
{
    int value = -1;
    socklen_t len = sizeof(value);
    // Boolean socket options need only be nonzero: BSD can return a bit mask.
    if (check(getsockopt(fd, level, option, &value, &len) == 0, context, name) && value == 0) {
        fprintf(stderr, "FAIL [%s]: %s = 0, expected enabled\n", context, name);
        ++failures;
    }
}

static void check_connections(struct connect_info &address, const char *unix_path, bool tls, const char *context)
{
    arbitrary_command_list commands;
    benchmark_config config = {};
    config.arbitrary_commands = &commands;
    config.clients = config.threads = 1;
    config.key_pattern = "S:S";
    // A zero-depth pipeline lets connection callbacks run without sending Redis requests.
    config.pipeline = 0;
    config.protocol = PROTOCOL_REDIS_DEFAULT;
    config.unix_socket = unix_path;
#ifdef USE_TLS
    if (tls) {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
        config.openssl_ctx = SSL_CTX_new(SSLv23_client_method());
#else
        config.openssl_ctx = SSL_CTX_new(TLS_client_method());
#endif
        if (!check(config.openssl_ctx != NULL, context, "create TLS context")) return;
    }
#endif
    event_base *base = event_base_new();
    abstract_protocol *protocol = protocol_factory(config.protocol);
    if (!check(base != NULL && protocol != NULL, context, "create event base and protocol")) {
        delete protocol;
        if (base != NULL) event_base_free(base);
#ifdef USE_TLS
        if (config.openssl_ctx != NULL) SSL_CTX_free(config.openssl_ctx);
#endif
        return;
    }
    {
        // Use a real client: connection/error callbacks require its manager and statistics.
        object_generator generator;
        generator.set_data_size_fixed(1);
        client manager(base, &config, protocol, &generator);
        shard_connection &connection = *manager.get_connections().front();
        connection.set_address_port("localhost", "0");
        for (unsigned int attempt = 0; attempt != 3; ++attempt) {
            char label[128];
            snprintf(label, sizeof(label), "%s connection %u", context, attempt + 1);
            unsigned int before = socket_creations;
            if (!check(connection.connect(&address) == 0, label, "connect succeeds")) break;
            check(socket_creations == before + 1, label, "connect creates a new socket");
            int fd = observed_socket;
            int flags = fcntl(fd, F_GETFL, 0);
            check(flags >= 0 && (flags & O_NONBLOCK), label, "O_NONBLOCK enabled");
            if (unix_path == NULL) {
                check_option(fd, SOL_SOCKET, SO_KEEPALIVE, label, "SO_KEEPALIVE");
                check_option(fd, IPPROTO_TCP, TCP_NODELAY, label, "TCP_NODELAY");
                struct linger linger_value = {};
                socklen_t len = sizeof(linger_value);
                if (check(getsockopt(fd, SOL_SOCKET, SO_LINGER, &linger_value, &len) == 0, label, "SO_LINGER")) {
                    check(linger_value.l_onoff != 0 && linger_value.l_linger == 0, label, "abortive close enabled");
                }
            }
            if (unix_path != NULL) {
                // Unix connect completes immediately; dispatch its queued callback while
                // the real manager is alive, rather than relying on callback suppression.
                event_base_loop(base, EVLOOP_NONBLOCK);
                check(connection.get_connection_state() == conn_connected, label, "Unix connect callback completes");
                check(manager.get_reqs_generated() == 0, label, "connection callback sends no requests");
            }
            connection.disconnect();
            // libevent can defer bufferevent destruction until the loop runs.
            event_base_loop(base, EVLOOP_NONBLOCK);
            errno = 0;
            check(fcntl(fd, F_GETFD) == -1 && errno == EBADF, label, "disconnect closes socket");
        }
    }
    delete protocol;
    event_base_free(base);
#ifdef USE_TLS
    if (config.openssl_ctx != NULL) SSL_CTX_free(config.openssl_ctx);
#endif
}

static void test_tcp(int family)
{
    const char *label = family == AF_INET ? "IPv4" : "IPv6";
    int listener = socket(family, SOCK_STREAM, IPPROTO_TCP);
    if (listener < 0 && family == AF_INET6 && (errno == EAFNOSUPPORT || errno == EPROTONOSUPPORT)) {
        printf("SKIP IPv6: address family is unavailable\n");
        return;
    }
    if (!check(listener >= 0, label, "create listener")) return;
    sockaddr_storage storage = {};
    socklen_t len;
    if (family == AF_INET) {
        sockaddr_in *address = reinterpret_cast<sockaddr_in *>(&storage);
        address->sin_family = AF_INET;
        address->sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        len = sizeof(*address);
    } else {
        sockaddr_in6 *address = reinterpret_cast<sockaddr_in6 *>(&storage);
        address->sin6_family = AF_INET6;
        address->sin6_addr = in6addr_loopback;
        len = sizeof(*address);
    }
    int result = bind(listener, reinterpret_cast<sockaddr *>(&storage), len);
    if (result < 0 && family == AF_INET6 && errno == EADDRNOTAVAIL) {
        printf("SKIP IPv6: loopback is unavailable\n");
        close(listener);
        return;
    }
    if (!check(result == 0, label, "bind loopback listener") ||
        !check(getsockname(listener, reinterpret_cast<sockaddr *>(&storage), &len) == 0, label,
               "get listener address") ||
        !check(listen(listener, 16) == 0, label, "listen")) {
        close(listener);
        return;
    }
    connect_info address = {};
    address.ci_family = family;
    address.ci_socktype = SOCK_STREAM;
    address.ci_protocol = IPPROTO_TCP;
    address.ci_addr = reinterpret_cast<sockaddr *>(&storage);
    address.ci_addrlen = len;
    check_connections(address, NULL, false, label);
#ifdef USE_TLS
    check_connections(address, NULL, true, family == AF_INET ? "IPv4 TLS" : "IPv6 TLS");
#endif
    close(listener);
}

static void test_unix()
{
    char directory[] = "/tmp/memtier-socket-test-XXXXXX";
    if (!check(mkdtemp(directory) != NULL, "Unix", "create temporary directory")) return;
    sockaddr_un address = {};
    address.sun_family = AF_UNIX;
    snprintf(address.sun_path, sizeof(address.sun_path), "%s/socket", directory);
    int listener = socket(AF_UNIX, SOCK_STREAM, 0);
    if (check(listener >= 0, "Unix", "create listener")) {
        if (check(bind(listener, reinterpret_cast<sockaddr *>(&address), sizeof(address)) == 0, "Unix",
                  "bind listener") &&
            check(listen(listener, 16) == 0, "Unix", "listen")) {
            connect_info unused = {};
            check_connections(unused, address.sun_path, false, "Unix");
        }
        close(listener);
    }
    unlink(address.sun_path);
    rmdir(directory);
}

int main()
{
#if defined(USE_TLS) && OPENSSL_VERSION_NUMBER < 0x10100000L
    SSL_library_init();
#endif
    test_tcp(AF_INET);
    test_tcp(AF_INET6);
    test_unix();
    if (failures != 0) {
        fprintf(stderr, "%d socket option check(s) failed\n", failures);
        return 1;
    }
    printf("socket options and reconnect tests passed\n");
    return 0;
}
