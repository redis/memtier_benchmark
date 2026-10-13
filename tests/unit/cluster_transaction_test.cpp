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
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
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
    explicit transaction_client(client_group *group) : cluster_client(group)
    {
        m_key_index_pools.push_back(new key_index_pool);
        m_staged_monitor_commands.emplace_back();
    }
    using cluster_client::disconnect;
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
        check(connect_shard_connection(second, (char *) "127.0.0.1", (char *) port), "second shard dialed");
        check(second->get_connection_state() != conn_disconnected, "second shard is not disconnected");
        m_shard_groups.clear();
        m_shard_groups.push_back(shard_group{0, m_connections[0], {}, 0});
        m_shard_groups.push_back(shard_group{1, second, {}, 0});
        m_slot_to_shard_group.assign(m_slot_to_shard_group.size(), 0);
        m_slot_to_shard_group[slot] = 1;
    }
    bool pool_empty(unsigned int conn_id) const { return m_key_index_pools[conn_id]->empty(); }
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
    config.clients = config.threads = 1;
    config.key_pattern = "S:S";
    config.protocol = PROTOCOL_REDIS_DEFAULT;
    config.transaction = config.cluster_mode = true;
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
    int listener = socket(AF_INET, SOCK_STREAM, 0);
    check(listener >= 0, "listener socket");
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

        // The owner sends the unit with the retained key.
        check(client.send_from(1, 0), "owner sends MULTI");
        check(client.send_from(1, 1), "owner sends SET");
        check(!client.staged() && client.round_key_valid() && client.round_key() == first_key,
              "owner consumes the staged key");
        check(client.pool_empty(0) && client.pool_empty(1), "no key left in either pool");
    }
    evbuffer_free(input);
    evbuffer_free(output);
    delete protocol;
    close(listener);
}

int main()
{
    check_pin_release(1);
    check_pin_release(1221322);
    check_repin_routes_to_staged_owner(1);
    check_repin_routes_to_staged_owner(1221322);
    puts("transaction pin release tests passed");
    return 0;
}
