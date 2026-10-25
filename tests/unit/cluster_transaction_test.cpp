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
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "platform_compat.h"
#include <string>
#include <event2/buffer.h>
#include "cluster_client.h"

void benchmark_log_file_line(int, const char *, unsigned int, const char *, ...) {}
void benchmark_log(int, const char *, ...) {}
void report_connection_stage_failure(const char *)
{
    abort();
}
void report_connection_stage_success()
{
    abort();
}
bool connection_stage_steady_state_reached()
{
    abort();
}

int failure_exit_code()
{
    return 1;
}

bool is_redis_protocol(enum PROTOCOL_TYPE type)
{
    return type == PROTOCOL_REDIS_DEFAULT || type == PROTOCOL_RESP2 || type == PROTOCOL_RESP3;
}

static void check(bool condition, const char *message)
{
    if (!condition) {
        fprintf(stderr, "FAIL: %s\n", message);
        exit(1);
    }
}

// Drive request generation directly and never run the event loop: the
// failures depend on the order of staging, pin release and request
// generation, not on network timing. Connections write into protocol
// buffers; the routing test only dials a local socket so that its second
// shard counts as connected.
class transaction_client : public cluster_client
{
public:
    // Keep the cap regression deterministic regardless of the integration
    // workload size or server latency.
    void check_monitor_backpressure()
    {
        check(m_connections.size() == 2, "two-shard backpressure fixture");
        check(m_config->requests == 0 && m_config->pipeline == 1,
              "backpressure fixture has no request limit and one in-flight request per shard");
        m_reqs_generated = 2;
        m_reqs_processed = 0;
        check(hold_pipeline(1), "producer pauses at the global in-flight budget");
        m_staged_monitor_commands[1].push({arbitrary_command("GET key"), 0, {}, 1});
        check(!hold_pipeline(1), "a shard with staged monitor work may drain at the budget");
        m_staged_monitor_commands[1].pop();
        m_reqs_processed = 1;
        check(!hold_pipeline(1), "one completed reply allows production to resume");
        m_reqs_generated = m_reqs_processed = 0;
    }
    explicit transaction_client(client_group *group) : cluster_client(group)
    {
        m_key_index_pools.push_back(new key_index_pool);
        m_staged_monitor_commands.emplace_back();
    }
    using cluster_client::disconnect;
    using cluster_client::reroute_staged_monitor_commands;
    using cluster_client::txn_release_pin;
    bool staged() const { return m_txn_has_staged_key; }
    unsigned long long staged_key() const { return m_txn_staged_key_index; }
    bool round_key_valid() const { return m_txn_round_key_valid; }
    unsigned long long round_key() const { return m_txn_round_key_index; }
    int pin() const { return m_txn_pinned_conn_id; }
    key_index_pool &pool() { return *m_key_index_pools[0]; }
    void next_rotation() { ++m_arbitrary_command_rotation_seq; }
    void send(unsigned int command_index)
    {
        check(send_from(0, command_index), "request was generated");
        check(pool().empty(), "request consumed its temporary key pool entry");
    }
    // Returns false when the transaction is pinned to another connection.
    bool send_from(unsigned int conn_id, unsigned int command_index)
    {
        m_executed_command_index = command_index;
        timeval timestamp = {};
        return create_arbitrary_request(command_index, timestamp, conn_id);
    }
    // Add a second, dialing shard connection and give it only `slot`; every
    // other slot stays with the main connection.
    void route_slot_to_second_shard(abstract_protocol *protocol, unsigned int slot, const char *port)
    {
        shard_connection *second = create_shard_connection(protocol);
        check(second->get_address() == NULL && second->get_port() == NULL, "new endpoint is uninitialized");
        const unsigned long long generated = get_reqs_generated();
        stage_unsent(second->get_id());
        check(connect_shard_connection(second, (char *) "127.0.0.1", (char *) port), "second shard dialed");
        check(second->get_connection_state() != conn_disconnected, "second shard is not disconnected");
        check(pool_empty(second->get_id()) && staged_monitor_count(second->get_id()) == 0,
              "fresh endpoint does not inherit queued work by numeric ID");
        check(get_reqs_generated() == generated, "fresh endpoint compensates old counted work");
        m_shard_groups.clear();
        m_shard_groups.push_back(shard_group{0, m_connections[0], {}, 0});
        m_shard_groups.push_back(shard_group{1, second, {}, 0});
        m_slot_to_shard_group.assign(m_slot_to_shard_group.size(), 0);
        m_slot_to_shard_group[slot] = 1;
    }
    void leave_slot_unowned(unsigned int slot)
    {
        m_shard_groups.resize(1);
        m_slot_to_shard_group.assign(m_slot_to_shard_group.size(), 0);
        m_slot_to_shard_group[slot] = UINT_MAX;
    }
    void second_primary_with_hole(unsigned int slot)
    {
        m_shard_groups.erase(m_shard_groups.begin());
        m_shard_groups[0].id = 0;
        m_slot_to_shard_group.assign(m_slot_to_shard_group.size(), 0);
        m_slot_to_shard_group[slot] = UINT_MAX;
    }
    bool can_route_unowned(unsigned int slot) { return check_unowned_slot(slot, false); }
    bool pool_empty(unsigned int conn_id) const { return m_key_index_pools[conn_id]->empty(); }
    void stage_unsent(unsigned int conn_id)
    {
        m_key_index_pools[conn_id]->push(SET_CMD_IDX);
        m_key_index_pools[conn_id]->push(42);
        arbitrary_command cmd("SET unit-key unit-value");
        check(cmd.split_command_to_args(), "parse staged monitor command");
        m_staged_monitor_commands[conn_id].push(staged_monitor_cmd{std::move(cmd), 0, timeval{}, 1});
        m_reqs_generated += 2;
    }
    // One counted monitor line for `slot`, staged on conn_id.
    void stage_line(unsigned int conn_id, unsigned int slot)
    {
        arbitrary_command cmd("SET unit-key unit-value");
        check(cmd.split_command_to_args(), "parse staged monitor command");
        m_staged_monitor_commands[conn_id].push(staged_monitor_cmd{std::move(cmd), 0, timeval{}, 1, slot});
        m_reqs_generated++;
    }
    size_t staged_monitor_count(unsigned int conn_id) const { return m_staged_monitor_commands[conn_id].size(); }
    void change_role(unsigned int conn_id, enum shard_role role) { set_shard_role(m_connections[conn_id], role); }
    bool redial(unsigned int conn_id, const char *address, const char *port)
    {
        m_connections[conn_id]->disconnect();
        return connect_shard_connection(m_connections[conn_id], (char *) address, (char *) port);
    }
    void skip(unsigned int command_index)
    {
        check(!send_from(0, command_index), "command of a lost MULTI was not sent");
        check(m_executed_command_index != command_index, "skipped command advanced the rotation");
    }
};

// Redis cluster key slot (CRC16/XMODEM of the whole key; no hash tags here).
static unsigned int key_slot(const std::string &key)
{
    uint16_t crc = 0;
    for (unsigned char byte : key) {
        crc ^= (uint16_t) (byte << 8);
        for (int bit = 0; bit < 8; bit++)
            crc = (crc & 0x8000) ? (uint16_t) ((crc << 1) ^ 0x1021) : (uint16_t) (crc << 1);
    }
    return crc & 16383;
}

static void make_transaction_commands(arbitrary_command_list &commands, abstract_protocol *protocol)
{
    const char *texts[] = {"MULTI", "SET __key__ value", "GET __key__", "EXEC"};
    for (const char *text : texts) {
        arbitrary_command command(text);
        check(command.split_command_to_args(), "parse command");
        command.resolve_command_meta();
        command.key_pattern = 'S';
        check(protocol->format_arbitrary_command(command), "format command");
        commands.add_command(command);
    }
}

static void make_transaction_config(benchmark_config &config, arbitrary_command_list &commands)
{
    config.arbitrary_commands = &commands;
    config.clients = config.threads = config.pipeline = 1;
    config.key_pattern = "S:S";
    config.protocol = PROTOCOL_REDIS_DEFAULT;
    config.transaction = config.cluster_mode = true;
}

// Use a fresh client: transaction routing tests can end with a failed run,
// pending requests or a pinned MULTI, which are independent hold conditions.
static void check_monitor_backpressure()
{
    evutil_socket_t listener = socket(AF_INET, SOCK_STREAM, 0);
    check(listener != static_cast<evutil_socket_t>(-1), "backpressure listener socket");
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t address_len = sizeof(address);
    check(bind(listener, (sockaddr *) &address, sizeof(address)) == 0 && listen(listener, 4) == 0 &&
              getsockname(listener, (sockaddr *) &address, &address_len) == 0,
          "backpressure listener bound");
    const std::string port = std::to_string(ntohs(address.sin_port));

    arbitrary_command_list commands;
    abstract_protocol *protocol = protocol_factory(PROTOCOL_REDIS_DEFAULT);
    make_transaction_commands(commands, protocol);
    benchmark_config config = {};
    make_transaction_config(config, commands);
    config.transaction = false;
    config.pipeline = 1;
    object_generator generator(commands.size());
    generator.set_data_size_fixed(1);
    generator.set_key_prefix("key-");
    generator.set_key_range(1, 10);
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        client.route_slot_to_second_shard(protocol, key_slot("key-1"), port.c_str());
        client.check_monitor_backpressure();
    }
    delete protocol;
    evutil_closesocket(listener);
}

static void check_pin_release(unsigned long long first_key)
{
    arbitrary_command_list commands;
    abstract_protocol *protocol = protocol_factory(PROTOCOL_REDIS_DEFAULT);
    make_transaction_commands(commands, protocol);
    benchmark_config config = {};
    make_transaction_config(config, commands);
    object_generator generator(commands.size());
    generator.set_data_size_fixed(1);
    generator.set_key_prefix("key-");
    generator.set_key_range(first_key, first_key + 10);
    evbuffer *input = evbuffer_new();
    evbuffer *output = evbuffer_new();
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        client.get_connections()[0]->get_protocol()->set_buffers(input, output);
        client.send(0); // MULTI stages the first SET key, before it is consumed.
        check(client.staged() && client.staged_key() == first_key, "lookahead stages first sequential key");

        // Existing routed pairs must remain intact, including a small key index
        // that would be silently mistaken for a valid command index.
        client.pool().push(1);
        client.pool().push(first_key + 5);
        client.txn_release_pin();
        check(client.pool().size() == 2, "pin release must not append a bare key to routed command pairs (#580)");
        check(client.pool().front() == 1, "routed command index preserved");
        client.pool().pop();
        check(client.pool().front() == first_key + 5, "routed key index preserved");
        client.pool().pop();
        check(client.staged() && client.staged_key() == first_key, "pin release retains the unsent key");
        check(client.pin() == -1 && !client.round_key_valid(), "pin and old round key released");
        client.txn_release_pin(); // A repeated release must be harmless without an owner.
        check(client.pool().empty() && client.staged(), "repeated release preserves lookahead outside the pool");

        client.send(0); // Re-pin before reaching a keyed command.
        check(client.staged_key() == first_key, "repeated lookahead retains the same key");
        client.txn_release_pin();
        evbuffer_drain(output, evbuffer_get_length(output));
        client.send(1); // Re-pin and consume the retained lookahead.
        check(!client.staged() && client.round_key() == first_key, "replacement pin uses the retained key");
        client.send(2);
        check(client.round_key() == first_key, "later keyed command uses the same rotation key");
        const std::string wire(reinterpret_cast<const char *>(evbuffer_pullup(output, -1)),
                               evbuffer_get_length(output));
        const std::string expected_key = "key-" + std::to_string(first_key) + "\r\n";
        const size_t first = wire.find(expected_key);
        check(first != std::string::npos && wire.find(expected_key, first + expected_key.size()) != std::string::npos,
              "SET and GET serialize the retained key");
        client.send(3);
        client.next_rotation();
        client.send(0);
        check(client.staged_key() == first_key + 1, "re-pinning did not advance the sequential iterator twice");
        client.send(1);
        client.txn_release_pin(); // Already consumed: no key should be returned.
        check(client.pool().empty() && !client.staged(), "release after consumption leaves no orphan key");
        check(!client.round_key_valid() && client.pin() == -1, "release after consumption drops pin and round key");
        client.next_rotation();
        client.send(0); // MULTI stages first_key + 2.
        evbuffer_drain(output, evbuffer_get_length(output));
        client.get_connections()[0]->disconnect(); // The open MULTI is lost with its socket.
        client.skip(1);
        client.skip(2);
        client.skip(3);
        check(evbuffer_get_length(output) == 0, "nothing of the lost MULTI block was sent");
        check(client.staged() && client.staged_key() == first_key + 2, "lost block keeps its unsent key");
        client.send(0);
        client.send(1);
        check(client.round_key() == first_key + 2, "next rotation sends the kept key");
        client.send(2);
        client.send(3);
        client.next_rotation();
        client.send(0);
        check(client.staged(), "lookahead present before full disconnect");
        client.disconnect();
        check(!client.staged() && !client.round_key_valid() && client.pin() == -1,
              "full disconnect clears transaction state");
    }
    evbuffer_free(input);
    evbuffer_free(output);
    delete protocol;
}

// With a populated two-shard slot map, re-pinning after a release must route
// the transaction to the owner of the retained lookahead key, not to the owner
// of some other key derived from it.
static void check_repin_routes_to_staged_owner(unsigned long long first_key)
{
    const std::string staged_key = "key-" + std::to_string(first_key);
    const unsigned int staged_slot = key_slot(staged_key);
    check(key_slot("foo") == 12182, "slot helper matches CLUSTER KEYSLOT foo");
    check(key_slot("key-" + std::to_string(first_key + 1)) != staged_slot, "neighbouring key lives in another slot");

    // A listening socket keeps the dial to the second shard pending; the
    // event loop never runs, so the connection stays conn_in_progress.
    evutil_socket_t listener = socket(AF_INET, SOCK_STREAM, 0);
    check(listener != static_cast<evutil_socket_t>(-1), "listener socket");
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t address_len = sizeof(address);
    check(bind(listener, (sockaddr *) &address, sizeof(address)) == 0 && listen(listener, 4) == 0 &&
              getsockname(listener, (sockaddr *) &address, &address_len) == 0,
          "listener bound");
    const std::string port = std::to_string(ntohs(address.sin_port));

    arbitrary_command_list commands;
    abstract_protocol *protocol = protocol_factory(PROTOCOL_REDIS_DEFAULT);
    make_transaction_commands(commands, protocol);
    benchmark_config config = {};
    make_transaction_config(config, commands);
    object_generator generator(commands.size());
    generator.set_data_size_fixed(1);
    generator.set_key_prefix("key-");
    generator.set_key_range(first_key, first_key + 10);
    evbuffer *input = evbuffer_new();
    evbuffer *output = evbuffer_new();
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        client.get_connections()[0]->get_protocol()->set_buffers(input, output);
        client.route_slot_to_second_shard(protocol, staged_slot, port.c_str());

        client.stage_unsent(1);
        const unsigned long long generated = client.get_reqs_generated();
        check(client.redial(1, "127.0.0.1", port.c_str()), "same endpoint redial starts");
        check(!client.pool_empty(1) && client.staged_monitor_count(1) == 1, "same endpoint retains both unsent queues");
        check(client.get_reqs_generated() == generated, "retained requests stay counted exactly once");
        // Same numeric connection ID, different endpoint identity: retain
        // the old reset/compensation policy rather than inherit stale work.
        check(client.redial(1, "localhost", port.c_str()), "different endpoint redial starts");
        check(client.pool_empty(1) && client.staged_monitor_count(1) == 0, "different endpoint clears both old queues");
        check(client.get_reqs_generated() == generated - 2, "reset compensates both counted requests");

        // The topology assigns the new role before redial. A demoted primary
        // and a repeated replica redial retain the existing reset policy for
        // routed keys, preventing counted reads from stranding completion on
        // a dead replica. Staged monitor lines stay, still counted: committing
        // the map moves them to their slot's primary (#836).
        client.change_role(1, role_replica);
        for (unsigned int attempt = 0; attempt < 2; attempt++) {
            client.stage_unsent(1);
            check(client.redial(1, "localhost", port.c_str()), "same replica endpoint redial starts");
            check(client.pool_empty(1) && client.staged_monitor_count(1) == attempt + 1,
                  "replica redial clears routed keys and keeps monitor lines, including demotion");
            check(client.get_reqs_generated() == generated - 2 + attempt + 1,
                  "replica reset compensates routed keys once and keeps monitor lines counted");
        }
        client.stage_unsent(1);
        client.change_role(1, role_primary);
        check(client.pool_empty(1) && client.staged_monitor_count(1) == 3,
              "promotion discards old replica keys before its role changes");
        check(client.get_reqs_generated() == generated + 1, "promotion compensates old replica keys exactly once");
        client.stage_unsent(1);
        check(client.redial(1, "localhost", port.c_str()), "promoted primary redial starts");
        check(!client.pool_empty(1) && client.staged_monitor_count(1) == 4,
              "primary retains work staged after promotion");
        check(client.get_reqs_generated() == generated + 3, "promoted primary preserves generated count");
        check(client.redial(1, "127.0.0.1", port.c_str()), "reset unit fixture endpoint");
        check(client.get_reqs_generated() == generated - 2, "fixture reset compensates retained work");

        // MULTI on the main connection stages the first key and pins to its owner.
        check(!client.send_from(0, 0), "main connection defers to the key owner");
        check(client.pin() == 1 && client.staged() && client.staged_key() == first_key,
              "first lookahead pins to the staged key's owner");
        client.txn_release_pin();
        check(client.pin() == -1 && client.staged() && client.staged_key() == first_key,
              "pin release keeps the staged key");

        // Re-pinning resolves the retained key, without generating a new one.
        const bool deferred = !client.send_from(0, 0);
        check(client.pin() == 1, "re-pin routes to the owner of the staged key (#580)");
        check(deferred, "main connection still defers to the key owner");
        check(client.staged() && client.staged_key() == first_key, "re-pin retains the staged key");

        // Staging and re-pinning have not submitted workload (#819).
        check(!client.has_submitted_workload(), "staged-only client does not participate in duration");
        // The owner sends the unit with the retained key.
        check(client.send_from(1, 0), "owner sends MULTI");
        check(client.has_submitted_workload(), "submitted MULTI participates before any reply");
        check(client.send_from(1, 1), "owner sends SET");
        check(!client.staged() && client.round_key_valid() && client.round_key() == first_key,
              "owner consumes the staged key");
        check(client.pool_empty(0) && client.pool_empty(1), "no key left in either pool");

        // A missing-slot rotation must park during a real reconnect backoff.
        // The producer is the still-dialing second connection; only the main
        // connection remains a primary, and its actual timer is armed below.
        client.txn_release_pin();
        client.next_rotation();
        client.leave_slot_unowned(key_slot("key-" + std::to_string(first_key + 1)));
        config.reconnect_on_error = true;
        config.max_reconnect_attempts = 2;
        client.get_connections()[0]->attempt_reconnect("unit-test primary drop");
        check(client.get_connections()[0]->is_reconnect_pending(), "primary reconnect timer is armed");
        check(!client.send_from(1, 0), "missing-slot transaction defers during reconnect");
        check(client.staged() && client.staged_key() == first_key + 1, "deferred rotation preserves exact key");
        check(client.hold_pipeline(1), "missing-slot wait yields to the reconnect timer (#685)");
        check(!client.run_failed(), "armed reconnect is not a terminal routing failure");
        check(client.staged() && client.staged_key() == first_key + 1, "parking does not consume another key");

        // If the reconnect is canceled with no other primary or refresh in
        // progress, the wait must fail explicitly, not spin or use a replica.
        client.get_connections()[0]->disconnect();
        check(!client.get_connections()[0]->is_reconnect_pending(), "disconnect cancels reconnect timer");
        check(client.hold_pipeline(1) && client.run_failed(), "terminal missing-slot wait fails the run");
    }
    evbuffer_free(input);
    evbuffer_free(output);
    // A shard reconnect timer has cleared, and the live seed is waiting for
    // SLOTS to redial it. No timer is armed now, but the fallback must remain
    // eligible until that refresh finishes. A disconnected seed is different.
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        client.route_slot_to_second_shard(protocol, staged_slot, port.c_str());
        client.second_primary_with_hole(staged_slot);
        client.get_connections()[1]->disconnect();
        check(client.redial(0, "127.0.0.1", port.c_str()), "seed is connecting for refresh");
        client.get_connections()[0]->set_cluster_slots();
        check(!client.get_connections()[1]->is_reconnect_pending(), "shard timer has already cleared");
        check(client.can_route_unowned(staged_slot) && !client.run_failed(),
              "live seed SLOTS refresh keeps disconnected primary eligible");
        client.get_connections()[0]->disconnect();
        check(!client.can_route_unowned(staged_slot) && client.run_failed(),
              "disconnected seed is not a pending topology refresh");
    }
    // A staged line whose slot moved to a primary that was given up on is
    // dropped and uncounted when the map is committed.
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        client.route_slot_to_second_shard(protocol, staged_slot, port.c_str());
        client.get_connections()[1]->disconnect();
        check(!client.get_connections()[1]->is_reconnect_pending(), "second shard was given up on");
        const unsigned long long generated = client.get_reqs_generated();
        client.stage_line(0, staged_slot);
        client.reroute_staged_monitor_commands();
        check(client.staged_monitor_count(0) == 0 && client.staged_monitor_count(1) == 0,
              "line for a given-up primary is dropped");
        check(client.get_reqs_generated() == generated, "dropped line is uncounted");
    }
    delete protocol;
    evutil_closesocket(listener);
}

int main()
{
#ifdef _WIN32
    WSADATA data;
    check(WSAStartup(MAKEWORD(2, 2), &data) == 0, "Winsock startup");
#endif
    check_monitor_backpressure();
    check_pin_release(1);
    check_pin_release(1221322);
    check_repin_routes_to_staged_owner(1);
    check_repin_routes_to_staged_owner(1221322);
    puts("transaction pin release tests passed");
    return 0;
}
