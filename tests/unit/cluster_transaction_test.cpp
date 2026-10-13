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
#include <stdio.h>
#include <stdlib.h>
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

// Use protocol buffers without sockets: the failure depends on the order of
// staging, pin release and request generation, not network timing.
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
        m_executed_command_index = command_index;
        timeval timestamp = {};
        check(create_arbitrary_request(command_index, timestamp, 0), "request was generated");
        check(pool().empty(), "request consumed its temporary key pool entry");
    }
};

static void check_pin_release(unsigned long long first_key)
{
    arbitrary_command_list commands;
    abstract_protocol *protocol = protocol_factory(PROTOCOL_REDIS_DEFAULT);
    const char *texts[] = {"MULTI", "SET __key__ value", "GET __key__", "EXEC"};
    for (const char *text : texts) {
        arbitrary_command command(text);
        check(command.split_command_to_args(), "parse command");
        command.key_pattern = 'S';
        check(protocol->format_arbitrary_command(command), "format command");
        commands.add_command(command);
    }
    benchmark_config config = {};
    config.arbitrary_commands = &commands;
    config.clients = config.threads = 1;
    config.key_pattern = "S:S";
    config.protocol = PROTOCOL_REDIS_DEFAULT;
    config.transaction = config.cluster_mode = true;
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

int main()
{
    check_pin_release(1);
    check_pin_release(1221322);
    puts("transaction pin release tests passed");
    return 0;
}
