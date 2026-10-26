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
#include "retry_policy.h"

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

// Drive request generation directly: the failures depend on the order of
// staging, pin release and request generation, not on network timing.
// Connections write into protocol buffers. Only finish_dial() runs the event
// loop, to complete a local dial so that a shard counts as connected.

// Defined in shard_connection.cpp; schedule_fill() arms it.
void deferred_fill_pipeline_cb(evutil_socket_t, short, void *ctx);

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
    using cluster_client::fallback_primary_conn_id;
    using cluster_client::hold_pipeline;
    using cluster_client::reconnect;
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
        stage_unsent(second->get_id(), slot);
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
    void unown_slot(unsigned int slot) { m_slot_to_shard_group[slot] = UINT_MAX; }
    // A third connection, still dialing: a peer in the middle of its setup.
    void dial_peer(abstract_protocol *protocol, const char *port)
    {
        shard_connection *peer = create_shard_connection(protocol);
        check(connect_shard_connection(peer, (char *) "127.0.0.1", (char *) port), "peer dialed");
    }
    // The seed dials and asks for CLUSTER SLOTS: a refresh is in flight.
    void refresh_over_seed(const char *port)
    {
        check(connect_shard_connection(m_connections[0], (char *) "127.0.0.1", (char *) port), "seed dialed");
        m_connections[0]->set_cluster_slots();
    }
    // Primaries a then b, and `slot` owned by neither.
    void primaries_with_hole(unsigned int a, unsigned int b, unsigned int slot)
    {
        m_shard_groups.clear();
        m_shard_groups.push_back(shard_group{0, m_connections[a], {}, 0});
        m_shard_groups.push_back(shard_group{1, m_connections[b], {}, 0});
        m_slot_to_shard_group.assign(m_slot_to_shard_group.size(), 0);
        m_slot_to_shard_group[slot] = UINT_MAX;
    }
    bool pool_empty(unsigned int conn_id) const { return m_key_index_pools[conn_id]->empty(); }
    // A routed key and a monitor line for `slot`, both unsent on conn_id.
    void stage_unsent(unsigned int conn_id, unsigned int slot)
    {
        m_key_index_pools[conn_id]->push(SET_CMD_IDX);
        m_key_index_pools[conn_id]->push(42);
        arbitrary_command cmd("SET unit-key unit-value");
        check(cmd.split_command_to_args(), "parse staged monitor command");
        m_staged_monitor_commands[conn_id].push(staged_monitor_cmd{std::move(cmd), 0, timeval{}, 1, slot});
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
    // Run the give-up hook as it runs when a dial fails while
    // handle_cluster_slots() parses a reply.
    void give_up_while_parsing(unsigned int conn_id)
    {
        m_parsing_slot_map = true;
        handle_connection_given_up(conn_id);
        m_parsing_slot_map = false;
    }
    // Run the event loop until conn_id's dial completes, then until its
    // deferred fill, if one was armed, has run.
    void finish_dial(unsigned int conn_id)
    {
        for (int i = 0; i < 1000 && m_connections[conn_id]->get_connection_state() != conn_connected; i++) {
            event_base_loop(m_event_base, EVLOOP_NONBLOCK);
            usleep(1000);
        }
        check(m_connections[conn_id]->get_connection_state() == conn_connected, "peer connected");
        for (int i = 0; i < 10; i++)
            event_base_loop(m_event_base, EVLOOP_NONBLOCK);
    }
    // Whether schedule_fill() has armed conn_id's deferred fill.
    bool fill_scheduled(unsigned int conn_id)
    {
        std::pair<void *, bool> found(m_connections[conn_id], false);
        event_base_foreach_event(
            m_event_base,
            [](const event_base *, const event *ev, void *arg) {
                std::pair<void *, bool> *found = static_cast<std::pair<void *, bool> *>(arg);
                if (event_get_callback(ev) == deferred_fill_pipeline_cb && event_get_callback_arg(ev) == found->first &&
                    event_pending(ev, EV_TIMEOUT, NULL))
                    found->second = true;
                return 0;
            },
            &found);
        return found.second;
    }
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

        client.stage_unsent(1, staged_slot);
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
            client.stage_unsent(1, staged_slot);
            check(client.redial(1, "localhost", port.c_str()), "same replica endpoint redial starts");
            check(client.pool_empty(1) && client.staged_monitor_count(1) == attempt + 1,
                  "replica redial clears routed keys and keeps monitor lines, including demotion");
            check(client.get_reqs_generated() == generated - 2 + attempt + 1,
                  "replica reset compensates routed keys once and keeps monitor lines counted");
        }
        client.stage_unsent(1, staged_slot);
        client.change_role(1, role_primary);
        check(client.pool_empty(1) && client.staged_monitor_count(1) == 3,
              "promotion discards old replica keys before its role changes");
        check(client.get_reqs_generated() == generated + 1, "promotion compensates old replica keys exactly once");
        client.stage_unsent(1, staged_slot);
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
    // dropped when the map is committed: uncounted, counted as an error and
    // logged to --failed-keys-file.
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        client.route_slot_to_second_shard(protocol, staged_slot, port.c_str());
        client.get_connections()[1]->disconnect();
        check(!client.get_connections()[1]->is_reconnect_pending(), "second shard was given up on");
        const unsigned long long generated = client.get_reqs_generated();
        const unsigned long long errors = client.get_stats()->get_total_errors();
        const std::string failed_keys = "cluster_transaction_failed_keys." + std::to_string(getpid()) + ".csv";
        check(global_failed_keys_logger().open(failed_keys.c_str()), "open failed-keys file");
        client.stage_line(0, staged_slot);
        client.reroute_staged_monitor_commands();
        check(global_failed_keys_logger().close() == 0, "close failed-keys file");
        char header[64] = "", record[256] = "";
        FILE *fp = fopen(failed_keys.c_str(), "r");
        const bool logged = fp != NULL && fgets(header, sizeof(header), fp) && fgets(record, sizeof(record), fp);
        if (fp != NULL) fclose(fp);
        unlink(failed_keys.c_str());
        check(client.staged_monitor_count(0) == 0 && client.staged_monitor_count(1) == 0,
              "line for a given-up primary is dropped");
        check(client.get_reqs_generated() == generated, "dropped line is uncounted");
        check(client.get_stats()->get_total_errors() == errors + 1, "dropped line counts as an error");
        check(logged, "failed-keys file has a record");
        check(strstr(record, ",ARBITRARY,\"unit-key\",\"slot-owner-down-for-monitor-line\",0\n") != NULL,
              "dropped line is logged to --failed-keys-file");
    }
    // The same, for a line already on that primary: nothing would send it or
    // uncount it, so it would hold a --requests run open.
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        client.route_slot_to_second_shard(protocol, staged_slot, port.c_str());
        client.get_connections()[1]->disconnect();
        const unsigned long long generated = client.get_reqs_generated();
        const unsigned long long errors = client.get_stats()->get_total_errors();
        client.stage_line(1, staged_slot);
        client.reroute_staged_monitor_commands();
        check(client.staged_monitor_count(0) == 0 && client.staged_monitor_count(1) == 0,
              "line on its given-up owner is dropped");
        check(client.get_reqs_generated() == generated, "line on its given-up owner is uncounted");
        check(client.get_stats()->get_total_errors() == errors + 1, "line on its given-up owner counts as an error");
    }
    // A primary given up on while a peer is still in setup is left dead and
    // the thread goes on (--read-preference other than primary). Its staged
    // lines are dropped then, not left counted for a commit that may never
    // come: a --requests run would wait on them forever.
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        client.route_slot_to_second_shard(protocol, staged_slot, port.c_str());
        client.dial_peer(protocol, port.c_str());
        const bool reconnect_on_error = config.reconnect_on_error;
        config.reconnect_on_error = false;
        config.read_preference = rp_secondary_preferred;
        const unsigned long long generated = client.get_reqs_generated();
        const unsigned long long errors = client.get_stats()->get_total_errors();
        client.stage_line(1, staged_slot);
        client.get_connections()[1]->attempt_reconnect("unit-test primary drop");
        config.read_preference = rp_primary;
        config.reconnect_on_error = reconnect_on_error;
        check(client.get_connections()[1]->get_connection_state() == conn_disconnected &&
                  !client.get_connections()[1]->is_reconnect_pending() && !client.run_failed(),
              "primary is left dead while the thread goes on");
        check(client.staged_monitor_count(1) == 0, "given-up primary's staged line is dropped");
        check(client.get_reqs_generated() == generated, "given-up primary's staged line is uncounted");
        check(client.get_stats()->get_total_errors() == errors + 1,
              "given-up primary's staged line counts as an error");
    }
    // While the seed refreshes SLOTS, an unowned line on a given-up primary
    // still moves to the live one, even when the given-up one comes first.
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        client.route_slot_to_second_shard(protocol, staged_slot, port.c_str());
        client.dial_peer(protocol, port.c_str());
        client.refresh_over_seed(port.c_str());
        client.get_connections()[1]->disconnect();
        client.primaries_with_hole(1, 2, staged_slot);
        check(client.fallback_primary_conn_id() == 1, "fallback counts the given-up primary while the seed refreshes");
        const unsigned long long generated = client.get_reqs_generated();
        const unsigned long long errors = client.get_stats()->get_total_errors();
        client.stage_line(1, staged_slot);
        client.reroute_staged_monitor_commands();
        check(client.staged_monitor_count(1) == 0 && client.staged_monitor_count(2) == 1,
              "unowned line skips the given-up primary for the dialing one");
        check(client.get_reqs_generated() == generated + 1 && client.get_stats()->get_total_errors() == errors,
              "unowned line moved during a refresh stays counted and is not an error");
    }
    // An unowned line on a given-up primary moves to a primary that is still
    // dialing, instead of staying or being dropped.
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        client.route_slot_to_second_shard(protocol, staged_slot, port.c_str());
        client.unown_slot(staged_slot);
        check(client.get_connections()[0]->get_connection_state() == conn_disconnected &&
                  !client.get_connections()[0]->is_reconnect_pending(),
              "main primary was given up on");
        const unsigned long long generated = client.get_reqs_generated();
        const unsigned long long errors = client.get_stats()->get_total_errors();
        client.stage_line(0, staged_slot);
        client.reroute_staged_monitor_commands();
        check(client.staged_monitor_count(0) == 0 && client.staged_monitor_count(1) == 1,
              "unowned line leaves its given-up primary for a live one");
        check(client.get_reqs_generated() == generated + 1 && client.get_stats()->get_total_errors() == errors,
              "moved unowned line stays counted and is not an error");
    }
    delete protocol;
    evutil_closesocket(listener);
}

static evutil_socket_t listen_any(std::string &port)
{
    evutil_socket_t listener = socket(AF_INET, SOCK_STREAM, 0);
    check(listener != static_cast<evutil_socket_t>(-1), "listener socket");
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t address_len = sizeof(address);
    check(bind(listener, (sockaddr *) &address, sizeof(address)) == 0 && listen(listener, 4) == 0 &&
              getsockname(listener, (sockaddr *) &address, &address_len) == 0,
          "listener bound");
    port = std::to_string(ntohs(address.sin_port));
    return listener;
}

// A dropped staged line is uncounted, so a producer that paused at the
// --requests cap may produce again. Nothing else wakes it: its replies are in,
// and a primary given up on wakes no peer.
static void check_drop_wakes_producers(bool through_give_up_hook)
{
    std::string port;
    evutil_socket_t listener = listen_any(port);

    arbitrary_command_list commands;
    abstract_protocol *protocol = protocol_factory(PROTOCOL_REDIS_DEFAULT);
    make_transaction_commands(commands, protocol);
    benchmark_config config = {};
    make_transaction_config(config, commands);
    config.transaction = false;
    object_generator generator(commands.size());
    generator.set_data_size_fixed(1);
    generator.set_key_prefix("key-");
    generator.set_key_range(1, 10);
    const unsigned int producer_slot = key_slot("key-1");
    const unsigned int dropped_slot = key_slot("key-2");
    check(producer_slot != dropped_slot, "wake fixture uses two slots");
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        client.route_slot_to_second_shard(protocol, producer_slot, port.c_str());
        client.finish_dial(1);
        check(client.get_connections()[0]->get_connection_state() == conn_disconnected &&
                  !client.get_connections()[0]->is_reconnect_pending(),
              "main primary was given up on");
        client.stage_line(0, dropped_slot);
        config.requests = client.get_reqs_generated();
        check(client.hold_pipeline(1), "producer pauses at the --requests cap");
        check(!client.fill_scheduled(1), "paused producer has no fill pending");
        if (through_give_up_hook) {
            client.handle_connection_given_up(0);
        } else {
            client.reroute_staged_monitor_commands();
        }
        check(client.staged_monitor_count(0) == 0 && client.get_reqs_generated() < config.requests,
              "dropped line frees --requests");
        check(!client.hold_pipeline(1), "producer may produce again");
        check(client.fill_scheduled(1), "dropping a line wakes the paused producer");
        config.requests = 0;
    }
    delete protocol;
    evutil_closesocket(listener);
}

// A dial that fails while handle_cluster_slots() parses a reply gives its
// connection up before the reply is committed. The staged lines it holds
// follow the reply's map, not the one it replaces.
static void check_give_up_during_slots_parse()
{
    std::string port;
    evutil_socket_t listener = listen_any(port);

    arbitrary_command_list commands;
    abstract_protocol *protocol = protocol_factory(PROTOCOL_REDIS_DEFAULT);
    make_transaction_commands(commands, protocol);
    benchmark_config config = {};
    make_transaction_config(config, commands);
    config.transaction = false;
    config.reconnect_on_error = false;
    object_generator generator(commands.size());
    generator.set_data_size_fixed(1);
    generator.set_key_prefix("key-");
    generator.set_key_range(1, 10);
    const unsigned int staged_slot = key_slot("key-1");
    check(staged_slot != 0, "parse fixture leaves slot 0 to the dead node");
    // Connection 1 owns staged_slot and was given up on; the seed dials and
    // is in setup, so a primary that fails to dial is left dead.
    auto given_up_owner = [&](transaction_client &client) {
        client.route_slot_to_second_shard(protocol, staged_slot, port.c_str());
        client.refresh_over_seed(port.c_str());
        client.get_connections()[1]->disconnect();
        client.get_connections()[1]->set_address_port("224.0.0.1", "7000");
        client.stage_line(1, staged_slot);
    };
    // Slot 0 to connection 1's node, every other slot to the seed.
    const std::string slots = "*2\r\n*3\r\n:0\r\n:0\r\n*2\r\n$9\r\n224.0.0.1\r\n:7000\r\n"
                              "*3\r\n:1\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:" +
                              port + "\r\n";
    evbuffer *input = evbuffer_new();
    evbuffer *output = evbuffer_new();
    abstract_protocol *reply = protocol_factory(PROTOCOL_REDIS_DEFAULT);
    reply->set_buffers(input, output);
    reply->set_keep_value(true);
    auto parse = [&](const std::string &resp) {
        evbuffer_add(input, resp.data(), resp.size());
        check(reply->parse_response() == 1, "SLOTS reply parsed");
        return reply->get_response();
    };
    config.read_preference = rp_secondary_preferred;
#ifdef __linux__
    // Linux fails a TCP connect to a multicast address at once, so the
    // redial below gives connection 1 up inside handle_cluster_slots().
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        given_up_owner(client);
        const unsigned long long generated = client.get_reqs_generated();
        const unsigned long long errors = client.get_stats()->get_total_errors();
        check(client.handle_cluster_slots(0, parse(slots)), "SLOTS reply committed");
        check(client.get_connections()[1]->get_connection_state() == conn_disconnected &&
                  !client.get_connections()[1]->is_reconnect_pending() && !client.run_failed(),
              "failed redial left the primary dead");
        check(client.staged_monitor_count(1) == 0 && client.staged_monitor_count(0) == 1,
              "line given up on mid-parse moves to its slot's new primary");
        check(client.get_reqs_generated() == generated && client.get_stats()->get_total_errors() == errors,
              "moved line stays counted and is not an error");
    }
#endif
    // A reply another connection asked for (after -MOVED) is parsed with no
    // refresh of the main connection in flight: only the parse window keeps
    // the give-up from re-routing by the map being replaced.
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        client.route_slot_to_second_shard(protocol, staged_slot, port.c_str());
        client.get_connections()[1]->disconnect();
        client.stage_line(1, staged_slot);
        check(client.get_connections()[0]->get_connection_state() == conn_disconnected,
              "no main connection refresh is in flight");
        client.give_up_while_parsing(1);
        check(client.staged_monitor_count(1) == 1, "hook leaves lines to the commit while a reply is parsed");
    }
    // A reply with no usable shard dials nothing and commits nothing; a
    // give-up after it re-routes.
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        given_up_owner(client);
        check(!client.handle_cluster_slots(0, parse("*1\r\n*1\r\n:0\r\n")), "malformed reply ignored");
        client.handle_connection_given_up(1);
        check(client.staged_monitor_count(1) == 0 && client.staged_monitor_count(0) == 0,
              "a give-up after an uncommitted reply re-routes");
    }
    config.read_preference = rp_primary;
    delete reply;
    evbuffer_free(input);
    evbuffer_free(output);
    delete protocol;
    evutil_closesocket(listener);
}

// A primary whose reconnect timer handed its redial to a SLOTS refresh
// (cluster_client::reconnect()) is disconnected with no timer until a map
// redials it, like one given up on, but it is not: its lines stay until that
// map, or until the main connection that would ask for it is given up on.
static void check_awaiting_slots_redial()
{
    std::string seed_port, owner_port;
    evutil_socket_t seed_listener = listen_any(seed_port);
    evutil_socket_t owner_listener = listen_any(owner_port);

    arbitrary_command_list commands;
    abstract_protocol *protocol = protocol_factory(PROTOCOL_REDIS_DEFAULT);
    make_transaction_commands(commands, protocol);
    benchmark_config config = {};
    make_transaction_config(config, commands);
    config.transaction = false;
    object_generator generator(commands.size());
    generator.set_data_size_fixed(1);
    generator.set_key_prefix("key-");
    generator.set_key_range(1, 10);
    const unsigned int staged_slot = key_slot("key-1");
    check(staged_slot != 0, "redial fixture leaves slot 0 to the seed");
    evbuffer *input = evbuffer_new();
    evbuffer *output = evbuffer_new();
    abstract_protocol *reply = protocol_factory(PROTOCOL_REDIS_DEFAULT);
    reply->set_buffers(input, output);
    reply->set_keep_value(true);
    auto commit = [&](transaction_client &client, const std::string &resp) {
        evbuffer_add(input, resp.data(), resp.size());
        check(reply->parse_response() == 1, "SLOTS reply parsed");
        check(client.handle_cluster_slots(0, reply->get_response()), "SLOTS reply committed");
    };
    const std::string seed_node = "*2\r\n$9\r\n127.0.0.1\r\n:" + seed_port + "\r\n";
    // Slot 0 to the seed, every other slot to connection 1's node.
    const std::string slots = "*2\r\n*3\r\n:0\r\n:0\r\n" + seed_node +
                              "*3\r\n:1\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:" + owner_port + "\r\n";
    // Every slot to the seed: connection 1's node left the cluster.
    const std::string seed_only = "*1\r\n*3\r\n:0\r\n:16383\r\n" + seed_node;
    // Connection 1 owns staged_slot; its reconnect timer fired and left the
    // redial to the seed's refresh. Connection 2 holds a line for the same
    // slot and is given up on.
    auto await_redial = [&](transaction_client &client) {
        client.route_slot_to_second_shard(protocol, staged_slot, owner_port.c_str());
        client.dial_peer(protocol, seed_port.c_str());
        client.refresh_over_seed(seed_port.c_str());
        client.get_connections()[1]->disconnect();
        check(client.reconnect(1) == 1 && client.get_connections()[1]->is_awaiting_slots_redial(),
              "the shard leaves its redial to the seed's refresh");
        client.stage_line(1, staged_slot);
        client.stage_line(2, staged_slot);
        client.get_connections()[2]->disconnect();
    };
    // A give-up during the refresh keeps both lines, and the commit redials
    // the shard and keeps them.
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        const unsigned long long errors = client.get_stats()->get_total_errors();
        await_redial(client);
        const unsigned long long generated = client.get_reqs_generated();
        client.handle_connection_given_up(2);
        check(client.staged_monitor_count(1) == 2, "a give-up keeps lines on a shard awaiting its redial");
        commit(client, slots);
        check(client.get_connections()[1]->get_connection_state() != conn_disconnected &&
                  !client.get_connections()[1]->is_awaiting_slots_redial(),
              "the commit redials the shard");
        check(client.staged_monitor_count(1) == 2, "both lines wait on the redialed shard");
        check(client.get_reqs_generated() == generated && client.get_stats()->get_total_errors() == errors,
              "lines stay counted and are not errors");
    }
    // While the seed itself waits on its reconnect timer, no refresh is in
    // flight, but the next one still redials the shard.
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        await_redial(client);
        config.reconnect_on_error = true;
        client.get_connections()[0]->attempt_reconnect("unit-test seed drop");
        config.reconnect_on_error = false;
        check(client.get_connections()[0]->get_connection_state() == conn_disconnected &&
                  client.get_connections()[0]->is_reconnect_pending(),
              "the seed waits on its reconnect timer");
        client.handle_connection_given_up(2);
        check(client.staged_monitor_count(1) == 2, "a give-up keeps lines while the seed reconnects");
        // Staging counts the waiting shard as a primary that will send, too.
        client.primaries_with_hole(1, 2, staged_slot);
        check(client.fallback_primary_conn_id() == 1, "the unowned fallback counts the waiting shard");
    }
    // The seed is down when the shard's timer fires: reconnect() redials the
    // seed, and the shard waits on that refresh just the same.
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        client.route_slot_to_second_shard(protocol, staged_slot, owner_port.c_str());
        client.dial_peer(protocol, seed_port.c_str());
        client.refresh_over_seed(seed_port.c_str());
        client.get_connections()[0]->disconnect();
        client.get_connections()[1]->disconnect();
        server_addr seed_addr("127.0.0.1", atoi(seed_port.c_str()), AF_INET);
        config.server_addr = &seed_addr;
        config.port = atoi(seed_port.c_str());
        check(client.reconnect(1) == 1 && client.get_connections()[1]->is_awaiting_slots_redial(),
              "seed down: the shard leaves its redial to the redialed seed");
        check(client.get_connections()[0]->get_connection_state() != conn_disconnected,
              "seed down: reconnect() redialed the seed");
        client.stage_line(1, staged_slot);
        client.stage_line(2, staged_slot);
        client.get_connections()[2]->disconnect();
        client.handle_connection_given_up(2);
        check(client.staged_monitor_count(1) == 2, "seed down: a give-up keeps lines on the waiting shard");
        commit(client, slots);
        check(client.get_connections()[1]->get_connection_state() != conn_disconnected &&
                  !client.get_connections()[1]->is_awaiting_slots_redial() && client.staged_monitor_count(1) == 2,
              "seed down: the commit redials the shard and keeps its lines");
        client.get_connections()[0]->disconnect();
        config.server_addr = NULL;
        config.port = 0;
    }
    // A primary waiting on its own reconnect timer (no mark) keeps its lines
    // through a give-up elsewhere.
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        client.route_slot_to_second_shard(protocol, staged_slot, owner_port.c_str());
        client.dial_peer(protocol, seed_port.c_str());
        client.refresh_over_seed(seed_port.c_str());
        config.reconnect_on_error = true;
        config.max_reconnect_attempts = 5;
        client.get_connections()[1]->attempt_reconnect("unit-test shard drop");
        config.reconnect_on_error = false;
        config.max_reconnect_attempts = 0;
        check(client.get_connections()[1]->get_connection_state() == conn_disconnected &&
                  client.get_connections()[1]->is_reconnect_pending() &&
                  !client.get_connections()[1]->is_awaiting_slots_redial(),
              "own timer: the shard waits on its own reconnect timer");
        client.stage_line(1, staged_slot);
        client.stage_line(2, staged_slot);
        client.get_connections()[2]->disconnect();
        client.handle_connection_given_up(2);
        check(client.staged_monitor_count(1) == 2, "own timer: a give-up keeps lines on a reconnect-pending shard");
    }
    // A map that no longer lists the shard retires it: its lines move to the
    // slot's new primary.
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        await_redial(client);
        commit(client, seed_only);
        check(!client.get_connections()[1]->is_awaiting_slots_redial(), "a retired shard awaits nothing");
        check(client.staged_monitor_count(1) + client.staged_monitor_count(2) == 0 &&
                  client.staged_monitor_count(0) == 2,
              "a retired shard's lines move to the slot's new primary");
    }
    // Once the seed is given up on, no refresh will redial the shard: its
    // lines are dropped, although nothing was staged on the seed.
    {
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        await_redial(client);
        const unsigned long long generated = client.get_reqs_generated();
        client.get_connections()[0]->disconnect();
        check(client.staged_monitor_count(0) == 0, "the seed holds no line");
        client.handle_connection_given_up(0);
        check(!client.get_connections()[1]->is_awaiting_slots_redial(), "the seed's give-up ends the wait");
        check(client.staged_monitor_count(1) + client.staged_monitor_count(2) == 0,
              "the seed's give-up drops lines waiting on its refresh");
        check(client.get_reqs_generated() == generated - 2, "dropped lines are uncounted");
    }
#ifndef _WIN32
    // The seed is down when the shard's timer fires, and redialing it fails
    // at once, before reaching the event loop. The shard waits on the seed,
    // which retries on its own timer: --max-reconnect-attempts bounds that,
    // and its give-up must not leave the shard waiting on a refresh that will
    // never come. (Windows defers the error to the event loop.)
    // used: attempts the seed has already made; fires: its timer firings
    // before the give-up, or that keep retrying with cap 0.
    auto unreachable_seed = [&](unsigned int cap, unsigned int used, unsigned int fires) {
        const std::string label = "unreachable seed, cap " + std::to_string(cap) + ": ";
        client_group group(&config, protocol, &generator);
        transaction_client client(&group);
        client.route_slot_to_second_shard(protocol, staged_slot, owner_port.c_str());
        client.dial_peer(protocol, seed_port.c_str());
        client.refresh_over_seed(seed_port.c_str());
        shard_connection *seed = client.get_connections()[0];
        shard_connection *shard = client.get_connections()[1];
        config.reconnect_on_error = true;
        config.max_reconnect_attempts = cap;
        for (unsigned int attempt = 0; attempt < used; attempt++) {
            seed->attempt_reconnect("unit-test seed drop");
            // Cancels the timer: the attempt is used.
            seed->disconnect();
        }
        seed->disconnect();
        shard->disconnect();
        client.stage_line(1, staged_slot);
        const unsigned long long generated = client.get_reqs_generated();
        // TCP connect() to the limited broadcast address fails at once.
        server_addr unreachable("255.255.255.255", atoi(seed_port.c_str()), AF_INET);
        config.server_addr = &unreachable;
        config.port = atoi(seed_port.c_str());
        // Connection 2 is still dialing, so a given-up seed is left dead, not
        // the thread ended.
        config.read_preference = rp_secondary_preferred;
        client.reconnect(1);
        for (unsigned int fire = 0; fire < fires; fire++) {
            check(seed->get_connection_state() == conn_disconnected && seed->is_reconnect_pending(),
                  (label + "the seed retries on its own timer").c_str());
            check(shard->is_awaiting_slots_redial() && client.staged_monitor_count(1) == 1,
                  (label + "the shard waits on the seed with its line").c_str());
            seed->handle_reconnect_timer_event();
        }
        if (cap == 0) {
            check(seed->is_reconnect_pending() && shard->is_awaiting_slots_redial() &&
                      client.staged_monitor_count(1) == 1 && client.get_reqs_generated() == generated,
                  (label + "unlimited attempts keep retrying and keep the line").c_str());
        } else {
            check(seed->get_connection_state() == conn_disconnected && !seed->is_reconnect_pending() &&
                      !client.run_failed(),
                  (label + "the seed is given up on").c_str());
            check(!shard->is_awaiting_slots_redial(), (label + "the shard is not left waiting on its refresh").c_str());
            check(client.staged_monitor_count(1) == 0 && client.get_reqs_generated() == generated - 1,
                  (label + "the shard's line is dropped and uncounted").c_str());
        }
        seed->disconnect();
        config.read_preference = rp_primary;
        config.server_addr = NULL;
        config.port = 0;
        config.reconnect_on_error = false;
        config.max_reconnect_attempts = 0;
    };
    // Given up on inside reconnect() itself.
    unreachable_seed(1, 1, 0);
    // Given up on when its timer fires for the second time.
    unreachable_seed(2, 0, 2);
    // Never given up on: every firing re-arms the timer.
    unreachable_seed(0, 0, 3);
#endif
    delete reply;
    evbuffer_free(input);
    evbuffer_free(output);
    delete protocol;
    evutil_closesocket(owner_listener);
    evutil_closesocket(seed_listener);
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
    check_drop_wakes_producers(false);
    check_drop_wakes_producers(true);
    check_give_up_during_slots_parse();
    check_awaiting_slots_redial();
    puts("transaction pin release tests passed");
    return 0;
}
