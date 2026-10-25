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

#include "platform_compat.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <string>
#include <event2/buffer.h>
#include <event2/event.h>
#include "client.h"

void benchmark_log_file_line(int, const char *, unsigned int, const char *, ...) {}
void benchmark_log(int, const char *, ...) {}
void report_connection_stage_failure(const char *) {}
void report_connection_stage_success() {}
bool connection_stage_steady_state_reached()
{
    return false;
}
int failure_exit_code()
{
    return 1;
}
bool is_redis_protocol(PROTOCOL_TYPE type)
{
    return type == PROTOCOL_REDIS_DEFAULT || type == PROTOCOL_RESP2 || type == PROTOCOL_RESP3;
}

static void require(bool condition, const char *message)
{
    if (!condition) {
        fprintf(stderr, "FAIL: %s\n", message);
        abort();
    }
}

// Exercise real connection setup, parser callbacks and disconnect/reconnect.
// The topology consumer only decides whether to accept the supplied tree;
// topology validation itself belongs to the cluster-client tests.
class retention_client : public client
{
public:
    bool accept_map = true;
    bool disconnect_in_callback = false;
    unsigned int maps_seen = 0;
    retention_client(event_base *base, benchmark_config *cfg, abstract_protocol *protocol,
                     object_generator *generator) :
            client(base, cfg, protocol, generator)
    {
        m_connections.push_back(new shard_connection(1, this, cfg, base, protocol));
    }
    bool handle_cluster_slots(unsigned int id, protocol_response *response)
    {
        require(response->get_mbulk_value() != NULL, "topology consumer receives retained array");
        ++maps_seen;
        if (disconnect_in_callback) m_connections[id]->disconnect();
        return accept_map;
    }
};

class peer
{
public:
    event_base *base;
    evutil_socket_t listener;
    evutil_socket_t accepted = -1;
    sockaddr_in address = {};
    connect_info endpoint = {};

    explicit peer(event_base *event_base) : base(event_base)
    {
        listener = socket(AF_INET, SOCK_STREAM, 0);
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        socklen_t len = sizeof(address);
        require(listener >= 0 && bind(listener, (sockaddr *) &address, len) == 0 &&
                    getsockname(listener, (sockaddr *) &address, &len) == 0 && listen(listener, 4) == 0,
                "owned ephemeral loopback listener");
        evutil_make_socket_nonblocking(listener);
        endpoint.ci_family = AF_INET;
        endpoint.ci_socktype = SOCK_STREAM;
        endpoint.ci_protocol = IPPROTO_TCP;
        endpoint.ci_addr = (sockaddr *) &address;
        endpoint.ci_addrlen = sizeof(address);
    }
    ~peer()
    {
        close_connection();
        evutil_closesocket(listener);
    }
    void close_connection()
    {
        if (accepted != -1) evutil_closesocket(accepted);
        accepted = -1;
    }
    void tick()
    {
        event_base_loop(base, EVLOOP_NONBLOCK);
        usleep(1000);
    }
    void connect(shard_connection &connection)
    {
        close_connection();
        std::string port = std::to_string(ntohs(address.sin_port));
        connection.set_address_port("127.0.0.1", port.c_str());
        require(connection.connect(&endpoint) == 0, "connect starts");
        for (int i = 0; i < 1000 && accepted == -1; ++i) {
            tick();
            accepted = accept(listener, NULL, NULL);
        }
        require(accepted != -1, "accepted within one second");
        evutil_make_socket_nonblocking(accepted);
    }
    void expect(const std::string &expected)
    {
        std::string received;
        for (int i = 0; i < 1000 && received.size() < expected.size(); ++i) {
            tick();
            char data[1024];
            int n = recv(accepted, data, sizeof(data), 0);
            if (n > 0) received.append(data, n);
        }
        if (received != expected) {
            fprintf(stderr, "expected %zu bytes [%s], received %zu [%s]\n", expected.size(), expected.c_str(),
                    received.size(), received.c_str());
        }
        require(received == expected, "exact setup command on wire");
    }
    void reply(const char *response)
    {
        size_t len = strlen(response);
        require(send(accepted, response, len, 0) == (int) len, "small setup reply sent");
    }
    void drain(shard_connection &connection)
    {
        for (int i = 0; i < 1000 && connection.get_pending_resp() != 0; ++i)
            tick();
        require(connection.get_pending_resp() == 0, "response processed within one second");
    }
};

// Actual bulk and nested-array materialization, not just a policy-bit check.
// This is called while disconnected; connect() installs fresh network buffers.
static void check_values(abstract_protocol *protocol, bool retained)
{
    evbuffer *input = evbuffer_new();
    evbuffer *output = evbuffer_new();
    require(input && output, "parser buffers");
    protocol->set_buffers(input, output);
    std::string value(65536, 'Q');
    std::string bulk = "$65536\r\n" + value + "\r\n";
    require(evbuffer_add(input, bulk.data(), bulk.size()) == 0 && protocol->parse_response() == 1,
            "large bulk parsed after disconnect");
    unsigned int len = 0;
    const char *result = protocol->get_response()->get_value(&len);
    require(retained ? result && len == value.size() && memcmp(result, value.data(), len) == 0
                     : result == NULL && len == 0,
            "bulk materialization follows original policy");
    std::string array = "*2\r\n" + bulk + bulk;
    require(evbuffer_add(input, array.data(), array.size()) == 0 && protocol->parse_response() == 1,
            "nested value parsed after disconnect");
    auto *tree = protocol->get_response()->get_mbulk_value();
    require(retained ? tree && tree->mbulks_elements.size() == 2 : tree == NULL,
            "array materialization follows original policy");
    protocol->reset_state();
    protocol->set_buffers(NULL, NULL);
    evbuffer_free(input);
    evbuffer_free(output);
}

static void exercise(bool prior, int path)
{
    arbitrary_command_list commands;
    benchmark_config cfg = {};
    cfg.arbitrary_commands = &commands;
    cfg.clients = cfg.threads = 1;
    cfg.key_pattern = "S:S";
    cfg.pipeline = 0; // Setup still runs, but there is no workload traffic.
    cfg.protocol = PROTOCOL_REDIS_DEFAULT;
    cfg.cluster_mode = true;
    cfg.test_time = 60;
    cfg.authenticate = "dummy-653";
    event_base *base = event_base_new();
    require(base != NULL, "event base");
    abstract_protocol *prototype = protocol_factory(cfg.protocol);
    {
        object_generator generator;
        generator.set_data_size_fixed(1);
        retention_client manager(base, &cfg, prototype, &generator);
        shard_connection &connection = *manager.get_connections()[1];
        abstract_protocol *protocol = connection.get_protocol();
        protocol->set_keep_value(prior);
        connection.disconnect();
        require(protocol->get_keep_value() == prior, "disconnect before any refresh preserves caller policy");
        check_values(protocol, prior);
        peer server(base);
        for (int cycle = 0; cycle < 2; ++cycle) {
            connection.set_cluster_slots();
            server.connect(connection);
            server.expect("*2\r\n$4\r\nAUTH\r\n$9\r\ndummy-653\r\n"
                          "*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n");
            server.reply("+OK\r\n");
            require(protocol->get_keep_value(), "slot-map retention enabled");
            if (path == 1) {
                server.reply("*1\r\n*3\r\n:0\r\n");
                // Wait for the real parser to materialize the partial tree.
                for (int i = 0; i < 1000 && protocol->get_response()->get_mbulk_value() == NULL; ++i)
                    server.tick();
                require(protocol->get_response()->get_mbulk_value() != NULL, "partial array entered parser");
            } else if (path >= 2) {
                manager.accept_map = path != 4;
                manager.disconnect_in_callback = path == 6;
                const unsigned int previous_maps = manager.maps_seen;
                server.reply(path == 2 ? "*0\r\n" : path == 3 ? "-ERR no slots\r\n" : "*1\r\n$3\r\nmap\r\n");
                server.drain(connection);
                require(manager.maps_seen == previous_maps + (path >= 4 ? 1 : 0), "topology callback count");
                require(protocol->get_keep_value() == prior, "completed or rejected refresh restores policy");
            }
            connection.disconnect();
            connection.disconnect(); // Restoration must be idempotent.
            require(protocol->get_keep_value() == prior, "disconnect restores original policy");
            require(connection.get_cluster_slots_state() == setup_done, "abandoned refresh is not rearmed");
            check_values(protocol, prior);
            // A non-main reconnect only authenticates: no new slot map can
            // accidentally hide a missing restore on the interrupted refresh.
            server.connect(connection);
            server.expect("*2\r\n$4\r\nAUTH\r\n$9\r\ndummy-653\r\n");
            server.reply("+OK\r\n");
            server.drain(connection);
            require(protocol->get_keep_value() == prior, "non-main reconnect preserves policy");
            connection.disconnect();
            // Once the override has been released, a later caller can change
            // policy. Further teardown must not restore the old saved value.
            protocol->set_keep_value(!prior);
            connection.disconnect();
            require(protocol->get_keep_value() == !prior, "inactive override preserves later caller policy");
            check_values(protocol, !prior);
            protocol->set_keep_value(prior);
        }
    }
    delete prototype;
    event_base_free(base);
}

// A CLUSTER SLOTS retry (an empty reply without --reconnect-on-error) restarts
// the clock of a client that has not started, but not of one that has sent
// workload that is still unanswered (#833).
static void check_slots_retry_start_time(bool submitted)
{
    arbitrary_command_list commands;
    benchmark_config cfg = {};
    cfg.arbitrary_commands = &commands;
    cfg.clients = cfg.threads = 1;
    cfg.key_pattern = "S:S";
    cfg.pipeline = 0;
    cfg.protocol = PROTOCOL_REDIS_DEFAULT;
    cfg.cluster_mode = true;
    cfg.test_time = 60;
    cfg.authenticate = "dummy-833";
    event_base *base = event_base_new();
    require(base != NULL, "event base");
    abstract_protocol *prototype = protocol_factory(cfg.protocol);
    {
        object_generator generator;
        generator.set_data_size_fixed(1);
        retention_client manager(base, &cfg, prototype, &generator);
        shard_connection &connection = *manager.get_connections()[1];
        connection.disconnect();
        peer server(base);
        connection.set_cluster_slots();
        server.connect(connection);
        server.expect("*2\r\n$4\r\nAUTH\r\n$9\r\ndummy-833\r\n"
                      "*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n");
        server.reply("+OK\r\n*0\r\n");
        server.drain(connection);
        require(connection.get_cluster_slots_state() == setup_sent, "empty map leaves a retry pending");
        timeval start = {1000, 0};
        manager.set_start_time_at(&start);
        if (submitted) manager.mark_workload_submitted();
        require(manager.get_reqs_answered() == 0, "no workload reply");
        connection.handle_cluster_slots_retry_event();
        timeval stamped = manager.get_stats()->get_start_time();
        require((stamped.tv_sec == start.tv_sec && stamped.tv_usec == start.tv_usec) == submitted,
                submitted ? "retry keeps a submitting client's start time" : "retry restarts an idle client's clock");
        server.expect("*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n");
        connection.disconnect();
    }
    delete prototype;
    event_base_free(base);
}

int main()
{
#ifdef _WIN32
    WSADATA data;
    require(WSAStartup(MAKEWORD(2, 2), &data) == 0, "Winsock startup");
#endif
    for (int prior = 0; prior < 2; ++prior) {
        // Drop before/during reply, empty/error/rejected/accepted map, and
        // topology-driven disconnect from inside the completion callback.
        for (int path = 0; path < 7; ++path)
            exercise(prior != 0, path);
    }
    check_slots_retry_start_time(false);
    check_slots_retry_start_time(true);
    puts("cluster slot retention tests passed (14 cases, two refresh cycles each; 2 retry start-time cases)");
    return 0;
}
