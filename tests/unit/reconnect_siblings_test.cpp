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
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <string>
#include <event2/event.h>
#include "client.h"
#include "obj_gen.h"
#include "protocol.h"
#include "shard_connection.h"

// When a failed connect ends a client once the run is over, a sibling
// connection whose own reconnect is pending (e.g. another cluster shard a live
// server dropped after the deadline) must not be torn down: its held requests
// are to be resent (#656). This used to be decided by all_connections_idle(),
// which only counts pending replies. The event loop is never run: the state is
// built directly, which no cluster fixture can stage deterministically.
// end_client_after_run() is the path both a failed connect after the run and
// a rejected setup after the run take to end the client.

void benchmark_log_file_line(int, const char *, unsigned int, const char *, ...) {}
void benchmark_log(int, const char *, ...) {}
void report_connection_stage_failure(const char *) {}
void report_connection_stage_success() {}
// Keep startup active for the sibling tests, so failed connects arm a
// reconnect. The deadline tests explicitly enable steady state below.
static bool steady_state = false;
bool connection_stage_steady_state_reached()
{
    return steady_state;
}
int failure_exit_code()
{
    return 1;
}
bool is_redis_protocol(enum PROTOCOL_TYPE type)
{
    return type == PROTOCOL_REDIS_DEFAULT || type == PROTOCOL_RESP2 || type == PROTOCOL_RESP3;
}

static int failures = 0;

static void check(bool condition, const char *message)
{
    if (!condition) {
        fprintf(stderr, "FAIL: %s\n", message);
        ++failures;
    }
}

// A client with a second connection, as a cluster client has one per shard.
class two_connection_client : public client
{
public:
    two_connection_client(event_base *base, benchmark_config *config, abstract_protocol *protocol,
                          object_generator *generator) :
            client(base, config, protocol, generator)
    {
        m_connections.push_back(new shard_connection(1, this, config, base, protocol));
    }
    void mark_started(struct timeval *start)
    {
        m_reqs_answered = 1;
        m_stats.set_start_time(start);
    }
};

// A sibling that is connecting (or connected) and holds requests for replay
// will resend them (--retry-on-error): it keeps the client going. Once it is
// disconnected with nothing pending, it will never resend them.
static void test_sibling_with_replay(const char *path)
{
    int listener = socket(AF_UNIX, SOCK_STREAM, 0);
    sockaddr_un address = {};
    address.sun_family = AF_UNIX;
    snprintf(address.sun_path, sizeof(address.sun_path), "%s", path);
    check(listener >= 0 && bind(listener, (sockaddr *) &address, sizeof(address)) == 0 && listen(listener, 4) == 0,
          "unix listener");

    arbitrary_command_list commands;
    benchmark_config config = {};
    config.arbitrary_commands = &commands;
    config.clients = config.threads = 1;
    config.key_pattern = "S:S";
    config.pipeline = 0;
    config.protocol = PROTOCOL_REDIS_DEFAULT;
    config.test_time = 10;
    config.reconnect_on_error = true;
    config.retry_on_error = true;
    config.unix_socket = path;

    event_base *base = event_base_new();
    abstract_protocol *protocol = protocol_factory(config.protocol);
    {
        object_generator generator;
        generator.set_data_size_fixed(1);
        two_connection_client manager(base, &config, protocol, &generator);
        shard_connection &first = *manager.get_connections()[0];
        shard_connection &second = *manager.get_connections()[1];

        connect_info unused = {};
        check(second.connect(&unused) == 0, "the sibling connects");
        check(second.get_connection_state() != conn_disconnected, "the sibling is not disconnected");
        request *req = new request(rt_set, 0, NULL, 1);
        req->m_serialized = strdup("*1\r\n$4\r\nPING\r\n");
        req->m_serialized_len = strlen(req->m_serialized);
        second.adopt_replay_request(req);
        check(second.has_requests_to_replay(), "the sibling holds a request for replay");
        check(!second.is_reconnect_pending() && second.get_pending_resp() == 0, "nothing else keeps it busy");
        check(!first.siblings_done(), "a live sibling with requests to replay keeps the client going");

        second.disconnect();
        check(first.siblings_done(), "a disconnected sibling with no reconnect pending does not");
        event_base_loop(base, EVLOOP_NONBLOCK);
    }
    delete protocol;
    event_base_free(base);
    close(listener);
    unlink(path);
}

int main()
{
    char directory[] = "/tmp/memtier-reconnect-siblings-XXXXXX";
    if (mkdtemp(directory) == NULL) {
        perror("mkdtemp");
        return 1;
    }
    std::string path = std::string(directory) + "/socket";
    test_sibling_with_replay(path.c_str());
    rmdir(directory);

    arbitrary_command_list commands;
    benchmark_config config = {};
    config.arbitrary_commands = &commands;
    config.clients = config.threads = 1;
    config.key_pattern = "S:S";
    config.pipeline = 0;
    config.protocol = PROTOCOL_REDIS_DEFAULT;
    config.test_time = 10;
    config.reconnect_on_error = true;

    event_base *base = event_base_new();
    abstract_protocol *protocol = protocol_factory(config.protocol);
    check(base != NULL && protocol != NULL, "create event base and protocol");
    if (failures) return 1;
    {
        object_generator generator;
        generator.set_data_size_fixed(1);
        two_connection_client manager(base, &config, protocol, &generator);
        shard_connection &first = *manager.get_connections()[0];
        shard_connection &second = *manager.get_connections()[1];

        check(first.siblings_done(), "an idle, disconnected sibling does not keep the client going");

        // The second connection fails before the run has started, so it arms
        // a reconnect (the timer is never run here).
        second.attempt_reconnect("Connection dropped");
        check(second.is_reconnect_pending(), "the sibling has a reconnect pending");
        check(manager.all_connections_idle(), "no reply is pending: all_connections_idle() says idle");
        check(!first.siblings_done(), "a sibling with a reconnect pending keeps the client going");
        check(second.siblings_done(), "a connection does not wait for itself");

        // Ending the client from the first connection, as when its connect
        // fails after the run is over, must leave the busy sibling alone.
        first.end_client_after_run(true);
        check(second.is_reconnect_pending(), "ending the run left the sibling's pending reconnect alone");
        check(!manager.end_time_set(), "the client is not ended while a sibling is busy");
        // A failed run (a rejected setup) is recorded even then, so the
        // sibling's later normal end still fails the run.
        check(!manager.run_failed(), "the run is not failed by a normal end");
        first.end_client_after_run(false);
        check(manager.run_failed(), "a failed run is recorded while a sibling is busy");
        check(second.is_reconnect_pending(), "failing the run left the sibling's pending reconnect alone");

        // Once the sibling is done too, the client is torn down.
        second.disconnect();
        first.end_client_after_run(false);
        check(!second.is_reconnect_pending(), "the client is torn down once no sibling is busy");
    }
    {
        object_generator generator;
        generator.set_data_size_fixed(1);
        two_connection_client manager(base, &config, protocol, &generator);
        shard_connection &first = *manager.get_connections()[0];
        gettimeofday(&config.benchmark_start_time, NULL);
        config.benchmark_start_time.tv_sec -= config.test_time + 2;
        steady_state = true;
        config.max_reconnect_attempts = 5;
        check(first.run_is_over(), "a never-started bounded client retains the benchmark deadline");
        config.reconnect_on_error = false;
        check(first.run_is_over(), "a never-started client without reconnect retains the benchmark deadline");
        manager.mark_started(&config.benchmark_start_time);
        check(!manager.finished(), "a started client's stats clock has not advanced");
        check(!first.run_is_over(), "without reconnect a started client uses its stats clock");
        config.reconnect_on_error = true;
        check(!first.run_is_over(), "bounded reconnects use a started client's stats clock");
        config.max_reconnect_attempts = 0;
        check(first.run_is_over(), "unlimited reconnects check a started client's wall-clock deadline");
    }
    // A rejected sibling can be disconnected with no retry pending (for
    // example after a failed dial during a slot refresh). Another connection
    // ending the client must preserve the required sibling's setup failure,
    // on normal completion, a failed connect, and drain-grace expiry.
    config.retry_on_error = true;
    config.max_retry_queue = 10;
    config.max_retries = 5;
    config.retry_backoff_ms = 100;
    const enum read_pref_mode modes[] = {rp_primary, rp_secondary_preferred, rp_secondary};
    for (unsigned int mode = 0; mode < sizeof(modes) / sizeof(modes[0]); ++mode) {
        for (unsigned int end_path = 0; end_path < 3; ++end_path) {
            config.read_preference = modes[mode];
            object_generator generator;
            generator.set_data_size_fixed(1);
            two_connection_client manager(base, &config, protocol, &generator);
            shard_connection &first = *manager.get_connections()[0];
            shard_connection &second = *manager.get_connections()[1];
            manager.mark_started(&config.benchmark_start_time);
            second.set_role(role_replica);
            second.reconnect_after_setup_error("AUTH error");
            const bool required = modes[mode] == rp_secondary;
            check(second.setup_failed_since_reply() == required, "only a required replica retains the setup failure");
            check(!manager.run_failed(), "the sibling has not ended the run yet");
            second.disconnect();
            check(first.siblings_done(), "the rejected sibling has no retry or reply pending");
            if (end_path == 0) {
                manager.set_end_time();
            } else if (end_path == 1) {
                first.end_client_after_run(true);
            } else {
                struct timeval old_start = config.benchmark_start_time;
                old_start.tv_sec -= 30;
                manager.mark_started(&old_start);
                request *req = new request(rt_set, 0, NULL, 1);
                req->m_serialized = strdup("*1\r\n$4\r\nPING\r\n");
                req->m_serialized_len = strlen(req->m_serialized);
                bool queued = first.enqueue_retry(req);
                check(queued, "a held retry keeps the client waiting through drain grace");
                if (!queued) delete req;
                unsigned long long unanswered = 0, held = 0;
                check(manager.abandon_unanswered_requests(&unanswered, &held), "drain grace expires");
                check(held == 1 && unanswered == 0, "the held retry is accounted once at drain grace");
            }
            if (end_path != 0) {
                check(manager.get_stats()->get_duration_usec() == config.test_time * 1000000ULL,
                      "reconnect and drain-grace endings cap stats at the deadline");
            }
            check(manager.end_time_set(), "the client was finalized");
            check(manager.run_failed() == required, "all completion paths preserve required replica setup failure");
        }
    }
    delete protocol;
    event_base_free(base);
    if (failures != 0) {
        fprintf(stderr, "%d reconnect sibling check(s) failed\n", failures);
        return 1;
    }
    printf("reconnect sibling tests passed\n");
    return 0;
}
