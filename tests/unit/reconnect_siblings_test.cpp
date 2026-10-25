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
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "platform_compat.h"
#include <unistd.h>
#include <string>
#include <chrono>
#include <thread>
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
#ifndef _WIN32
    evutil_socket_t listener = socket(AF_UNIX, SOCK_STREAM, 0);
    sockaddr_un address = {};
    address.sun_family = AF_UNIX;
    snprintf(address.sun_path, sizeof(address.sun_path), "%s", path);
    check(listener >= 0 && bind(listener, (sockaddr *) &address, sizeof(address)) == 0 && listen(listener, 4) == 0,
          "unix listener");

    connect_info endpoint = {};

#else
    evutil_socket_t listener = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t address_len = sizeof(address);
    check(listener >= 0 && bind(listener, (sockaddr *) &address, sizeof(address)) == 0 && listen(listener, 4) == 0 &&
              getsockname(listener, (sockaddr *) &address, &address_len) == 0,
          "loopback listener");
    connect_info endpoint = {};
    endpoint.ci_family = AF_INET;
    endpoint.ci_socktype = SOCK_STREAM;
    endpoint.ci_protocol = IPPROTO_TCP;
    endpoint.ci_addr = (sockaddr *) &address;
    endpoint.ci_addrlen = address_len;

#endif
    arbitrary_command_list commands;
    benchmark_config config = {};
    config.arbitrary_commands = &commands;
    config.clients = config.threads = 1;
    config.key_pattern = "S:S";
    config.pipeline = 0;
    config.protocol = PROTOCOL_REDIS_DEFAULT;
    config.unix_socket = path;
    config.test_time = 10;
    config.reconnect_on_error = true;
    config.retry_on_error = true;

    event_base *base = event_base_new();
    abstract_protocol *protocol = protocol_factory(config.protocol);
    {
        object_generator generator;
        generator.set_data_size_fixed(1);
        two_connection_client manager(base, &config, protocol, &generator);
        shard_connection &first = *manager.get_connections()[0];
        shard_connection &second = *manager.get_connections()[1];

        second.set_address_port("127.0.0.1", "0");
        check(second.connect(&endpoint) == 0, "the sibling connects");
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
    evutil_closesocket(listener);
#ifndef _WIN32
    unlink(path);
#endif
}

// Interrupts finalize after the worker has joined. Use exact timestamps to
// exercise the deadline boundary without depending on scheduler timing.
static void test_interrupted_deadline()
{
    arbitrary_command_list commands;
    benchmark_config config = {};
    config.arbitrary_commands = &commands;
    config.clients = config.threads = 1;
    config.key_pattern = "S:S";
    config.protocol = PROTOCOL_REDIS_DEFAULT;
    config.test_time = 2;
    config.benchmark_start_time = {100, 750000};
    event_base *base = event_base_new();
    abstract_protocol *protocol = protocol_factory(config.protocol);
    struct
    {
        timeval end;
        unsigned long long duration;
        const char *name;
    } cases[] = {
        {{100, 749999}, 0, "interrupt before start clamps to start"},
        {{101, 250000}, 500000, "early interrupt keeps its actual duration"},
        {{102, 750000}, 2000000, "interrupt exactly at deadline"},
        {{102, 750001}, 2000000, "interrupt one microsecond after deadline"},
        {{105, 250000}, 2000000, "interrupt during drain caps at deadline"},
    };
    for (unsigned int i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        object_generator generator;
        client manager(base, &config, protocol, &generator);
        manager.set_start_time_at(&config.benchmark_start_time);
        manager.set_end_time_interrupted(&cases[i].end);
        check(manager.get_stats()->get_duration_usec() == cases[i].duration, cases[i].name);
        check(manager.end_time_set(), "interrupt finalizes the client");
    }
    struct timeval late = {105, 250000};
    {
        object_generator generator;
        client manager(base, &config, protocol, &generator);
        manager.set_end_time_interrupted(&late);
        check(manager.get_stats()->get_duration_usec() == 0, "never-started interrupted client has no duration");
        check(manager.end_time_set(), "never-started interrupted client is finalized");
    }
    {
        object_generator generator;
        client manager(base, &config, protocol, &generator);
        manager.set_start_time_at(&config.benchmark_start_time);
        struct timeval early = {101, 0};
        manager.set_end_time_at(&early);
        manager.set_end_time_interrupted(&late);
        check(manager.get_stats()->get_duration_usec() == 250000, "interrupt does not refinalize an ended client");
    }
    {
        config.clients_start = 1;
        object_generator generator;
        client manager(base, &config, protocol, &generator);
        manager.set_start_time();
        manager.set_end_time_interrupted(&late);
        check(manager.get_stats()->get_duration_usec() == 2000000, "staircase uses the shared benchmark deadline");
        config.clients_start = 0;
    }
    {
        config.test_time = UINT_MAX;
        struct timeval recent_start = {1792300000, 750000};
        struct timeval early = {1792300001, 250000};
        object_generator generator;
        client manager(base, &config, protocol, &generator);
        manager.set_start_time_at(&recent_start);
        manager.set_end_time_interrupted(&early);
        check(manager.get_stats()->get_duration_usec() == 500000,
              "large test time does not overflow a 32-bit timeval deadline on an early interrupt");
    }
    {
        config.test_time = 0;
        config.requests = 100;
        object_generator generator;
        client manager(base, &config, protocol, &generator);
        manager.set_start_time_at(&config.benchmark_start_time);
        manager.set_end_time_interrupted(&late);
        check(manager.get_stats()->get_duration_usec() == 4500000, "request-count interrupt keeps its actual duration");
    }
    delete protocol;
    event_base_free(base);
}

struct reconnect_clock_context
{
    event_base *base;
    timeval before_dispatch;
};

// Run inside a callback so libevent retains its time cache while the worker
// is delayed. A deadline-clamped reconnect must still be scheduled at the
// deadline, rather than earlier by the time spent in this callback.
static void test_cached_reconnect_deadline(evutil_socket_t, short, void *context)
{
    reconnect_clock_context *clock = static_cast<reconnect_clock_context *>(context);
    event_base *base = clock->base;
    arbitrary_command_list commands;
    benchmark_config config = {};
    config.arbitrary_commands = &commands;
    config.clients = config.threads = 1;
    config.key_pattern = "S:S";
    config.protocol = PROTOCOL_REDIS_DEFAULT;
    config.test_time = 2;
    config.reconnect_on_error = true;
    timeval cached, wall, elapsed = {1, 500000};
    check(event_base_gettimeofday_cached(base, &cached) == 0, "read callback's cached time");
    gettimeofday(&wall, NULL);
    // Independent wall samples bracket this event-loop iteration. libevent
    // 2.0.10 returns monotonic uptime here on Linux; self-anchoring the
    // deadline to that value would incorrectly let the regression pass.
    check(ts_diff(clock->before_dispatch, cached) >= -1000 && ts_diff(cached, wall) >= -1000,
          "cached event-loop time uses the wall-clock epoch");
    evutil_timersub(&cached, &elapsed, &config.benchmark_start_time);
    steady_state = true;
    abstract_protocol *protocol = protocol_factory(config.protocol);
    {
        object_generator generator;
        client manager(base, &config, protocol, &generator);
        shard_connection &connection = *manager.get_connections()[0];
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        const double delay = connection.next_reconnect_delay();
        check(delay >= 0.499999 && delay <= 0.500001,
              "callback work must not advance a deadline-clamped reconnect timer");
        // Also use the production contract: the run starts in wall time.
        // Bound the expected remaining delay with the independent wall
        // samples rather than assuming callback dispatch was instantaneous.
        evutil_timersub(&wall, &elapsed, &config.benchmark_start_time);
        const double wall_delay = connection.next_reconnect_delay();
        const double upper = 0.501 + ts_diff(clock->before_dispatch, wall) / 1000000.0;
        check(wall_delay >= 0.499 && wall_delay <= upper,
              "wall-anchored deadline clamps reconnect in the same clock epoch");
        config.max_reconnect_attempts = 5;
        check(connection.next_reconnect_delay() == 1.0, "bounded reconnect retains its backoff");
        config.max_reconnect_attempts = 0;
        config.test_time = 0;
        check(connection.next_reconnect_delay() == 1.0, "request-count runs retain their backoff");
        config.test_time = 2;
        steady_state = false;
        check(connection.next_reconnect_delay() == 1.0, "startup retains its backoff before steady state");
        steady_state = true;
        config.benchmark_start_time.tv_sec -= 2;
        check(connection.next_reconnect_delay() == 0.0, "an expired deadline is scheduled immediately");
    }
    steady_state = false;
    delete protocol;
}

// A client whose reconnects never succeed, and that counts them.
class counting_reconnect_client : public client
{
public:
    counting_reconnect_client(event_base *base, benchmark_config *config, abstract_protocol *protocol,
                              object_generator *generator) :
            client(base, config, protocol, generator), reconnects(0)
    {
    }
    int reconnect(unsigned int) { return ++reconnects, -1; }
    unsigned int reconnects;
};

// #852: a never-started client whose CLUSTER SLOTS retry is clamped to the
// --test-time deadline must fail the run when that retry fires. On Linux
// libevent times it with a coarse clock, so it can fire a few ms before the
// wall-clock deadline; firing it at once stages that skew. Without the fix the
// early retry reconnects, which in a real run completes setup and lets the
// client end normally (exit 0).
void cluster_client_reconnect_timer_handler(evutil_socket_t fd, short what, void *ctx);
static void test_deadline_retry_fires_early()
{
    arbitrary_command_list commands;
    benchmark_config config = {};
    config.arbitrary_commands = &commands;
    config.clients = config.threads = 1;
    config.key_pattern = "S:S";
    config.protocol = PROTOCOL_REDIS_DEFAULT;
    config.test_time = 2;
    config.reconnect_on_error = true;
    event_base *base = event_base_new();
    abstract_protocol *protocol = protocol_factory(config.protocol);
    steady_state = true;
    {
        object_generator generator;
        counting_reconnect_client manager(base, &config, protocol, &generator);
        shard_connection &connection = *manager.get_connections()[0];
        // The deadline is 100 ms away: the 1 s backoff is clamped to it.
        timeval now, ahead = {0, 100000};
        gettimeofday(&now, NULL);
        now.tv_sec -= config.test_time;
        evutil_timeradd(&now, &ahead, &config.benchmark_start_time);
        connection.reconnect_after_setup_error("CLUSTER SLOTS error", false);
        check(connection.is_reconnect_pending(), "the CLUSTER SLOTS retry is armed");
        check(!connection.run_is_over(), "the deadline has not passed by the wall clock");
        cluster_client_reconnect_timer_handler(-1, EV_TIMEOUT, &connection);
        event_base_dispatch(base);
        check(manager.reconnects == 0, "a retry due at the deadline does not reconnect");
        check(manager.run_failed(), "a never-started client fails the run at the deadline");
        check(!connection.is_reconnect_pending(), "no retry is armed after the deadline");
    }
    {
        // A retry well before the deadline still reconnects.
        object_generator generator;
        counting_reconnect_client manager(base, &config, protocol, &generator);
        shard_connection &connection = *manager.get_connections()[0];
        gettimeofday(&config.benchmark_start_time, NULL);
        connection.reconnect_after_setup_error("CLUSTER SLOTS error", false);
        check(connection.next_reconnect_delay() == 1.0, "a retry before the deadline keeps its backoff");
        cluster_client_reconnect_timer_handler(-1, EV_TIMEOUT, &connection);
        check(manager.reconnects == 1 && !manager.run_failed(), "a retry before the deadline reconnects");
        check(!connection.run_is_over(), "the run is not over before the deadline");
    }
    steady_state = false;
    delete protocol;
    event_base_free(base);
}

int main()
{
#ifdef _WIN32
    WSADATA data;
    check(WSAStartup(MAKEWORD(2, 2), &data) == 0, "Winsock startup");
    test_sibling_with_replay(NULL);
#else
    char directory[] = "/tmp/memtier-reconnect-siblings-XXXXXX";
    if (mkdtemp(directory) == NULL) {
        perror("mkdtemp");
        return 1;
    }
    std::string path = std::string(directory) + "/socket";
    test_sibling_with_replay(path.c_str());
    rmdir(directory);
#endif
    test_interrupted_deadline();
    test_deadline_retry_fires_early();
    event_base *clock_base = event_base_new();
    timeval immediately = {0, 0};
    reconnect_clock_context clock_context = {clock_base, {0, 0}};
    check(event_base_once(clock_base, -1, EV_TIMEOUT, test_cached_reconnect_deadline, &clock_context, &immediately) ==
              0,
          "schedule cached-clock regression");
    gettimeofday(&clock_context.before_dispatch, NULL);
    event_base_dispatch(clock_base);
    event_base_free(clock_base);

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
