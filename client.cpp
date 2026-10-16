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

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#ifdef HAVE_LIMITS_H
#include <limits.h>
#endif

#ifdef HAVE_ASSERT_H
#include <assert.h>
#endif

#include <math.h>
#include <algorithm>
#include <sstream>
#include <arpa/inet.h>

#include "client.h"
#include "cluster_client.h"
#include "config_types.h"
#include "rate_limiter.h"
#include "retry_policy.h"


void client::apply_arbitrary_tracking_flags(abstract_protocol *protocol)
{
    // Mirror the per-element miss-tracking decision made in setup_client onto a
    // single protocol. In cluster mode shard connections are *re-created* (not
    // reconnected in place) on topology reset -- cluster_client::disconnect()
    // deletes the non-main connections and the next cluster-slots discovery
    // rebuilds them via create_shard_connection(). Each rebuild clone()s a fresh
    // protocol with runtime flags reset to defaults, so the flag must be
    // re-applied here or miss tracking silently never engages for them. (An
    // in-place reconnect keeps the same protocol object; reset_state() preserves
    // the flag, so that path needs nothing.) See issue #476.
    if (protocol == NULL) return;
    if (m_arbitrary_needs_elem_tracking) {
        protocol->set_track_elem_misses(true);
    }
}

bool client::setup_client(benchmark_config *config, abstract_protocol *protocol, object_generator *objgen)
{
    m_config = config;
    assert(m_config != NULL);
    unsigned long long total_num_of_clients = static_cast<unsigned long long>(config->clients) * config->threads;
    m_request_rate_phase_microsecond =
        calculate_rate_limit_phase(config->request_interval_microsecond, config->next_client_idx, total_num_of_clients);

    // create main connection
    shard_connection *conn = new shard_connection(m_connections.size(), this, m_config, m_event_base, protocol,
                                                  m_request_rate_phase_microsecond);
    m_connections.push_back(conn);

    m_obj_gen = objgen->clone();
    assert(m_obj_gen != NULL);

    if (config->distinct_client_seed && config->randomize)
        m_obj_gen->set_random_seed(config->randomize + config->next_client_idx);
    else if (config->randomize)
        m_obj_gen->set_random_seed(config->randomize);
    else if (config->distinct_client_seed)
        m_obj_gen->set_random_seed(config->next_client_idx);

    m_obj_gen->fill_value_buffer();

    // Setup first arbitrary command
    if (config->arbitrary_commands->is_defined()) advance_arbitrary_command_index();

    // Enable value keeping for SCAN incremental iteration (needed to extract cursor from response)
    if (config->scan_incremental_iteration) {
        // The copied commands have matching argument indices; bound both consumers
        // of the generated-argument cache even in release builds.
        m_scan_args.resize(std::max(config->arbitrary_commands->at(0).command_args.size(),
                                    config->scan_continuation_command->command_args.size()));
        MAIN_CONNECTION->get_protocol()->set_keep_value(true);
    }

    // Enable per-element miss tracking only when a configured command needs
    // per-position reply inspection. Only ArrayPerElementNulls
    // (HMGET/MGET/ZMSCORE/GEOPOS/...) attributes hits/misses per position; the
    // parser records a per-top-level-element hit bitmap directly
    // (set_track_elem_misses), with zero reply materialization -- no malloc /
    // memcpy of element values, no mbulk tree. EmptyCollection
    // (HGETALL/SMEMBERS/LRANGE) only needs the top-level array length
    // (response->get_top_array_len()); SingleNullBulk and IntegerMembership use
    // the parser's scalar counters / status line. None of them keep values.
    if (config->arbitrary_commands->is_defined()) {
        for (size_t i = 0; i < config->arbitrary_commands->size(); ++i) {
            const arbitrary_command &cmd = config->arbitrary_commands->at(i);
            if (!cmd.miss_tracking_enabled || cmd.spec == NULL) continue;
            using memtier::command_meta::ReplyShape;
            if (cmd.spec->reply_shape == ReplyShape::ArrayPerElementNulls) {
                m_arbitrary_needs_elem_tracking = true;
                break;
            }
        }
        // Apply to the connections that exist now. Connections created later --
        // notably the per-shard connections that cluster_client builds during
        // cluster-slots discovery -- pick the flag up via
        // apply_arbitrary_tracking_flags() at creation time (see issue #476).
        for (size_t i = 0; i < m_connections.size(); ++i) {
            apply_arbitrary_tracking_flags(m_connections[i]->get_protocol());
        }
    }

    // Parallel key-pattern determined according to the first command
    if ((config->arbitrary_commands->is_defined() && config->arbitrary_commands->at(0).key_pattern == 'P') ||
        (config->key_pattern[key_pattern_set] == 'P')) {
        unsigned long long client_index = config->next_client_idx % total_num_of_clients;

        unsigned long long range = (config->key_maximum - config->key_minimum) / total_num_of_clients + 1;
        unsigned long long min = config->key_minimum + (range * client_index);
        unsigned long long max = min + range - 1;

        if (client_index == (total_num_of_clients - 1)) {
            max = config->key_maximum; // the last clients takes the leftover
        }

        m_obj_gen->set_key_range(min, max);
    }
    config->next_client_idx++;

    m_keylist = new keylist(m_config->multi_key_get + 1);
    assert(m_keylist != NULL);

    return true;
}

client::client(client_group *group) :
        m_event_base(NULL),
        m_initialized(false),
        m_end_set(false),
        m_config(NULL),
        m_obj_gen(NULL),
        m_stats(group->get_config()),
        m_reqs_processed(0),
        m_reqs_answered(0),
        m_reqs_generated(0),
        m_set_ratio_count(0),
        m_get_ratio_count(0),
        m_arbitrary_command_ratio_count(0),
        m_executed_command_index(0),
        m_arbitrary_command_rotation_seq(0),
        m_txn_rotation_key_index(0),
        m_txn_rotation_key_valid(false),
        m_txn_rotation_key_seq(0),
        m_txn_multi_sent(false),
        m_tot_set_ops(0),
        m_tot_wait_ops(0),
        m_scan_cursor("0"),
        m_scan_iteration_count(0),
        m_mget_defer(false),
        m_arbitrary_needs_elem_tracking(false),
        m_request_rate_phase_microsecond(0)
{
    m_event_base = group->get_event_base();

    if (!setup_client(group->get_config(), group->get_protocol(), group->get_obj_gen())) {
        return;
    }

    benchmark_debug_log("new client %p successfully set up.\n", this);
    m_initialized = true;
}

client::client(struct event_base *event_base, benchmark_config *config, abstract_protocol *protocol,
               object_generator *obj_gen) :
        m_event_base(NULL),
        m_initialized(false),
        m_end_set(false),
        m_config(NULL),
        m_obj_gen(NULL),
        m_stats(config),
        m_reqs_processed(0),
        m_reqs_answered(0),
        m_reqs_generated(0),
        m_set_ratio_count(0),
        m_get_ratio_count(0),
        m_arbitrary_command_ratio_count(0),
        m_executed_command_index(0),
        m_arbitrary_command_rotation_seq(0),
        m_txn_rotation_key_index(0),
        m_txn_rotation_key_valid(false),
        m_txn_rotation_key_seq(0),
        m_txn_multi_sent(false),
        m_tot_set_ops(0),
        m_tot_wait_ops(0),
        m_scan_cursor("0"),
        m_scan_iteration_count(0),
        m_keylist(NULL),
        m_mget_defer(false),
        m_arbitrary_needs_elem_tracking(false),
        m_request_rate_phase_microsecond(0)
{
    m_event_base = event_base;

    if (!setup_client(config, protocol, obj_gen)) {
        return;
    }

    benchmark_debug_log("new client %p successfully set up.\n", this);
    m_initialized = true;
}

client::~client()
{
    for (unsigned int i = 0; i < m_connections.size(); i++) {
        shard_connection *sc = m_connections[i];
        delete sc;
    }
    m_connections.clear();

    if (m_obj_gen != NULL) {
        delete m_obj_gen;
        m_obj_gen = NULL;
    }

    if (m_keylist != NULL) {
        delete m_keylist;
        m_keylist = NULL;
    }
}

bool client::initialized(void)
{
    return m_initialized;
}

void client::disconnect(void)
{
    shard_connection *sc = MAIN_CONNECTION;
    assert(sc != NULL);

    sc->disconnect();
}

void client::disconnect_all(void)
{
    unsigned long long abandoned = disconnect_and_abandon_held();
    if (abandoned) {
        benchmark_error_log("warning: %llu request(s) held for retry when the run ended were not resent; "
                            "counted as errors.\n",
                            abandoned);
    }
}

unsigned long long client::disconnect_and_abandon_held(void)
{
    // This ends the run, once it is finished and no replies are pending. A
    // request still held for resend (waiting in a retry queue for its
    // backoff, or in a replay queue for a reconnect) is not pending, so it
    // is never resent: e.g. a --test-time run whose last replies all
    // arrive, after the deadline, as retryable errors. disconnect() moves
    // the retry queue into the replay queue; account every request left
    // there instead of dropping it silently with the connection. It counts
    // as an error but, unlike a refused retry, not as an op: it never got a
    // final reply.
    unsigned long long abandoned = 0;
    for (unsigned int i = 0; i < m_connections.size(); i++) {
        shard_connection *sc = m_connections[i];
        if (sc != NULL) {
            sc->disconnect();
            abandoned += sc->abandon_replay_requests("run-ended-before-resend");
        }
    }
    return abandoned;
}

int client::connect(void)
{
    struct connect_info addr;

    // get primary connection
    shard_connection *sc = MAIN_CONNECTION;
    assert(sc != NULL);

    // get address information
    if (m_config->unix_socket == NULL) {
        if (m_config->server_addr->get_connect_info(&addr) != 0) {
            benchmark_error_log("connect: resolve error: %s\n", m_config->server_addr->get_last_error());
            return -1;
        }

        // Just in case we got domain name and not ip, we convert it
        char address[INET6_ADDRSTRLEN];
        if (addr.ci_family == PF_INET) {
            struct sockaddr_in *ipv4 = (struct sockaddr_in *) addr.ci_addr;
            inet_ntop(AF_INET, &(ipv4->sin_addr), address, INET_ADDRSTRLEN);
        } else {
            struct sockaddr_in6 *ipv6 = (struct sockaddr_in6 *) addr.ci_addr;
            inet_ntop(AF_INET6, &(ipv6->sin6_addr), address, INET6_ADDRSTRLEN);
        }

        char port_str[20];
        snprintf(port_str, sizeof(port_str) - 1, "%u", m_config->port);

        // save address and port
        sc->set_address_port(address, port_str);
    }

    // call connect
    int ret = sc->connect(&addr);
    if (ret) return ret;

    return 0;
}

bool client::finished(void)
{
    if (m_config->requests > 0 && m_reqs_processed >= m_config->requests) return true;
    if (m_config->test_time > 0) {
        if (m_config->clients_start > 0) {
            // Staircase mode: use global deadline so all clients stop together
            // Compare full timevals: whole seconds alone would end the run
            // up to 1 s early when it started late in a second.
            struct timeval now, elapsed;
            gettimeofday(&now, NULL);
            timersub(&now, &m_config->benchmark_start_time, &elapsed);
            if (elapsed.tv_sec >= (time_t) m_config->test_time) return true;
        } else {
            if (m_stats.get_duration() >= m_config->test_time) return true;
        }
    }
    return false;
}

bool client::all_connections_idle(void)
{
    for (unsigned int i = 0; i < m_connections.size(); i++) {
        shard_connection *sc = m_connections[i];
        if (sc != NULL && sc->get_pending_resp() > 0) {
            return false;
        }
    }
    return true;
}

void client::set_start_time()
{
    // In staircase mode, all clients use the global benchmark start time
    // so per-second stats are aligned to the same absolute time origin.
    if (m_config->clients_start > 0) {
        m_stats.set_start_time(&m_config->benchmark_start_time);
    } else {
        struct timeval now;
        gettimeofday(&now, NULL);
        m_stats.set_start_time(&now);
    }
}

void client::set_end_time()
{
    // update only once
    if (!m_end_set) {
        benchmark_debug_log("nothing else to do, test is finished.\n");

        m_stats.set_end_time(NULL);
        m_end_set = true;
    }
}

void client::set_end_time_interrupted(struct timeval *end_time)
{
    if (m_end_set) return;
    // A client whose connection completed in the loop iteration that the
    // interrupt ended may have started after the interrupt time.
    struct timeval start_time = m_stats.get_start_time();
    if (m_stats.has_started() && timercmp(end_time, &start_time, <)) end_time = &start_time;
    m_stats.set_end_time(end_time);
    m_end_set = true;
}

bool client::abandon_unanswered_requests(unsigned long long *unanswered, unsigned long long *held)
{
    if (m_end_set || m_config->test_time == 0) return false;
    if (m_stats.get_duration_usec() < (unsigned long long) (m_config->test_time + TEST_TIME_DRAIN_GRACE_SECS) * 1000000)
        return false;

    // Only a client that is waiting for replies, or that keeps resending
    // --retry-on-error retries that never succeed (#715): one with nothing
    // in flight ends by itself, or is waiting for a reconnect. A retry held
    // for its backoff is not pending.
    if (all_connections_idle()) {
        bool holds_retries = false;
        for (unsigned int i = 0; i < m_connections.size() && !holds_retries; i++) {
            holds_retries = m_connections[i] != NULL && m_connections[i]->holds_retries();
        }
        if (!holds_retries) return false;
    }

    // Drop them before disconnecting, so --retry-on-error does not hold them
    // for a resend that will never come (they would then be counted a
    // second time). Workload requests that never got a reply are errors,
    // not ops (drop_pending_requests() counts them through
    // abandon_requests()); setup commands are not counted.
    unsigned int abandoned = 0;
    for (unsigned int i = 0; i < m_connections.size(); i++) {
        if (m_connections[i] != NULL) abandoned += m_connections[i]->drop_pending_requests();
    }
    m_stats.add_unanswered_requests(abandoned);
    *unanswered += abandoned;

    // The run measured --test-time, not the grace period spent waiting.
    struct timeval end_time = m_stats.get_start_time();
    end_time.tv_sec += m_config->test_time;
    m_stats.set_end_time(&end_time);
    m_end_set = true;
    // The caller reports the requests held for retry with the others.
    *held += disconnect_and_abandon_held();
    return true;
}

bool client::hold_pipeline(unsigned int conn_id)
{
    // don't exceed requests
    if (m_config->requests) {
        if (m_reqs_generated >= m_config->requests) return true;
    }

    // if we have reconnect_interval stop enlarging the pipeline on time
    if (m_config->reconnect_interval) {
        if ((m_reqs_processed % m_config->reconnect_interval) + (m_reqs_generated - m_reqs_processed) >=
            m_config->reconnect_interval)
            return true;
    }

    return false;
}

get_key_response client::get_key_for_conn(unsigned int command_index, unsigned int conn_id,
                                          unsigned long long *key_index)
{
    int iter;
    if (m_config->arbitrary_commands->is_defined())
        iter = arbitrary_obj_iter_type(command_index);
    else
        iter = obj_iter_type(m_config, command_index);

    *key_index = m_obj_gen->get_key_index(iter);

    if (!m_config->data_import || m_config->generate_keys) {
        m_obj_gen->generate_key(*key_index);
    } else {
        /* For SET command we already read a completes item (see create_set_request()) */
        if (command_index == GET_CMD_IDX) dynamic_cast<import_object_generator *>(m_obj_gen)->read_next_key(*key_index);
    }

    return available_for_conn;
}

// --monitor-input "__monitor_line@__": replace the first __key__ in an
// argument with a generated key, keeping the text around it (e.g.
// {tag}__key__ or user:__key__; like the --command path, any later __key__
// in the same argument stays literal). Call it after split_command_to_args()
// and before format_arbitrary_command(). Resolved arguments are marked
// `resolved`, so format_arbitrary_command() frames them as constants without
// scanning the generated text for placeholders again, and cluster mode can
// route by the key that is actually sent. Argument 1 after resolution (a
// literal or a generated key) is kept in cmd.resolved_key for the request's
// log key: it is what cluster mode hashes to pick the shard, so
// --failed-keys-file and --retry-on-error see the same key the router used.
// A bare __data__ (other than as the first argument) is left for
// format_arbitrary_command() to mark data_type: write_monitor_command_args()
// writes the value at send time, without copying it into the command.
void client::resolve_monitor_placeholders(arbitrary_command &cmd, unsigned int command_index, unsigned int conn_id)
{
    // Argument 0 is the command name.
    for (size_t i = 1; i < cmd.command_args.size(); i++) {
        command_arg &arg = cmd.command_args[i];
        size_t pos = arg.data.find(KEY_PLACEHOLDER);
        if (pos == std::string::npos) {
            // The first argument is what cluster mode routes by, so a bare
            // __data__ there is filled in now rather than at send time.
            if (i == 1 && arg.data == DATA_PLACEHOLDER) {
                unsigned int value_len;
                const char *value = m_obj_gen->get_value(0, &value_len);
                assert(value != NULL);
                assert(value_len > 0);
                arg.data.assign(value, value_len);
                arg.resolved = true;
            }
            continue;
        }
        // get_key_for_conn() requires the out-param; the key itself is read
        // back from m_obj_gen, so the index is not needed here.
        unsigned long long key_index;
        // Base-class lookup: cluster_client routes by the resolved key
        // afterwards, so its per-shard key pool must not be used here.
        get_key_response res = client::get_key_for_conn(command_index, conn_id, &key_index);
        assert(res == available_for_conn);
        (void) res;
        arg.data.replace(pos, strlen(KEY_PLACEHOLDER), m_obj_gen->get_key(), m_obj_gen->get_key_len());
        arg.resolved = true;
    }
    if (cmd.command_args.size() >= 2) cmd.resolved_key = cmd.command_args[1].data;
}

// Write a formatted __monitor_line@__ command (see
// resolve_monitor_placeholders()) to conn_id: constant arguments as they are,
// a __data__ argument with a value generated now. Returns the bytes written.
int client::write_monitor_command_args(const arbitrary_command &cmd, unsigned int conn_id)
{
    int cmd_size = 0;
    for (unsigned int i = 0; i < cmd.command_args.size(); i++) {
        const command_arg *arg = &cmd.command_args[i];
        if (arg->type == const_type) {
            cmd_size += m_connections[conn_id]->send_arbitrary_command(arg);
        } else if (arg->type == data_type) {
            unsigned int value_len;
            const char *value = m_obj_gen->get_value(0, &value_len);
            assert(value != NULL);
            assert(value_len > 0);
            cmd_size += m_connections[conn_id]->send_arbitrary_command(arg, value, value_len);
        }
    }
    // Attach the key only to a command that is going out, so it cannot leak
    // onto the next request when nothing was written.
    if (cmd_size > 0) {
        m_connections[conn_id]->set_arbitrary_command_key(cmd.resolved_key.data(),
                                                          (unsigned int) cmd.resolved_key.size());
    }
    return cmd_size;
}

bool client::create_arbitrary_request(unsigned int command_index, struct timeval &timestamp, unsigned int conn_id)
{
    int cmd_size = 0;

    const arbitrary_command &cmd = get_arbitrary_command(command_index);

    benchmark_debug_log("%s: %s:\n", m_connections[conn_id]->get_readable_id(), cmd.command.c_str());

    // Check if this is a monitor command placeholder - handle it specially
    if (cmd.command_args.size() == 1 && cmd.command_args[0].type == monitor_random_type) {
        // Select a command from the monitor file at runtime based on the monitor pattern
        size_t selected_index = 0;
        const std::string *monitor_cmd_ptr = NULL;
        if (m_config->monitor_pattern == 'R') {
            monitor_cmd_ptr = &m_config->monitor_commands->get_random_command(m_obj_gen, &selected_index);
            benchmark_debug_log("%s: random monitor command selected (q%zu): %s\n",
                                m_connections[conn_id]->get_readable_id(),
                                selected_index + 1, // 1-based index for user display
                                monitor_cmd_ptr->c_str());
        } else {
            monitor_cmd_ptr = &m_config->monitor_commands->get_next_sequential_command(&selected_index);
            benchmark_debug_log("%s: sequential monitor command selected (q%zu): %s\n",
                                m_connections[conn_id]->get_readable_id(),
                                selected_index + 1, // 1-based index for user display
                                monitor_cmd_ptr->c_str());
        }

        const std::string &monitor_cmd = *monitor_cmd_ptr;

        // Parse and format the monitor command into a temporary arbitrary_command
        arbitrary_command temp_cmd(monitor_cmd.c_str());
        if (!temp_cmd.split_command_to_args()) {
            fprintf(stderr, "warning: skipping malformed monitor command at line %zu: %s\n", selected_index + 1,
                    monitor_cmd.c_str());
            return true; // Skip this command but continue processing
        }

        resolve_monitor_placeholders(temp_cmd, command_index, conn_id);

        // Format the command for the protocol (adds RESP headers)
        if (!m_connections[conn_id]->get_protocol()->format_arbitrary_command(temp_cmd)) {
            fprintf(stderr, "warning: skipping unformattable monitor command at line %zu: %s\n", selected_index + 1,
                    monitor_cmd.c_str());
            return true; // Skip this command but continue processing
        }

        // Send the randomly selected command
        cmd_size = write_monitor_command_args(temp_cmd, conn_id);

        // Get the stats index for the actual command type (e.g., SET, GET)
        // instead of using the placeholder's index
        size_t stats_index = m_config->monitor_commands->get_stats_index(selected_index);
        m_connections[conn_id]->send_arbitrary_command_end(stats_index, &timestamp, cmd_size);
        return true;
    }

    // Normal arbitrary command handling
    const bool scan_incremental_iteration = m_config->scan_incremental_iteration;
    for (unsigned int i = 0; i < cmd.command_args.size(); i++) {
        const command_arg *arg = &cmd.command_args[i];
        if (arg->type == const_type) {
            cmd_size += m_connections[conn_id]->send_arbitrary_command(arg);
        } else if (arg->type == key_type) {
            // --transaction: reuse one key for every __key__ in the rotation so
            // the whole WATCH/MULTI/EXEC unit resolves to a single key/slot.
            // Non-cluster path only: in --cluster-mode, cluster_client drives the
            // reuse by pushing the rotation key onto m_key_index_pools and relies
            // on this method draining it via get_key_for_conn, so the fast path
            // here would leave the pool un-drained. Also skip the data-import
            // path, which streams a distinct key per command.
            const bool txn_reuse =
                m_config->transaction && !m_config->cluster_mode && (!m_config->data_import || m_config->generate_keys);
            if (txn_reuse && m_txn_rotation_key_valid && m_txn_rotation_key_seq == m_arbitrary_command_rotation_seq) {
                // Same rotation as the cached key: regenerate the identical key
                // string without advancing the per-iter key cursor.
                m_obj_gen->generate_key(m_txn_rotation_key_index);
            } else {
                unsigned long long key_index;
                get_key_response res = get_key_for_conn(command_index, conn_id, &key_index);
                /* If key not available for this connection, we have a bug of sending partial request */
                assert(res == available_for_conn);
                if (txn_reuse) {
                    // First keyed command of the rotation: adopt this key for
                    // the rest of the rotation.
                    m_txn_rotation_key_index = key_index;
                    m_txn_rotation_key_valid = true;
                    m_txn_rotation_key_seq = m_arbitrary_command_rotation_seq;
                }
            }

            if (scan_incremental_iteration) {
                assert(i < m_scan_args.size());
                m_scan_args[i] = arg->data_prefix;
                m_scan_args[i].append(m_obj_gen->get_key(), m_obj_gen->get_key_len());
                m_scan_args[i].append(arg->data_suffix);
                cmd_size +=
                    m_connections[conn_id]->send_arbitrary_command(arg, m_scan_args[i].data(), m_scan_args[i].size());
            } else if (arg->has_key_affixes) {
                // Static data mixed with the key placeholder.
                // Pre-calculate total length to avoid reallocations
                const char *key = m_obj_gen->get_key();
                unsigned int key_len = m_obj_gen->get_key_len();
                size_t prefix_len = arg->data_prefix.length();
                size_t suffix_len = arg->data_suffix.length();
                size_t total_len = prefix_len + key_len + suffix_len;

                // Optimization: use stack buffer for small keys to avoid heap allocation
                if (total_len < KEY_BUFFER_STACK_SIZE) {
                    char stack_buffer[KEY_BUFFER_STACK_SIZE];
                    char *pos = stack_buffer;

                    // Manual copy for better performance
                    if (prefix_len > 0) {
                        memcpy(pos, arg->data_prefix.data(), prefix_len);
                        pos += prefix_len;
                    }
                    memcpy(pos, key, key_len);
                    pos += key_len;
                    if (suffix_len > 0) {
                        memcpy(pos, arg->data_suffix.data(), suffix_len);
                    }

                    cmd_size += m_connections[conn_id]->send_arbitrary_command(arg, stack_buffer, total_len);
                } else {
                    // Fallback to string for large keys
                    std::string combined_key;
                    combined_key.reserve(total_len);
                    combined_key.append(arg->data_prefix);
                    combined_key.append(key, key_len);
                    combined_key.append(arg->data_suffix);

                    cmd_size += m_connections[conn_id]->send_arbitrary_command(arg, combined_key.c_str(),
                                                                               combined_key.length());
                }
            } else {
                cmd_size +=
                    m_connections[conn_id]->send_arbitrary_command(arg, m_obj_gen->get_key(), m_obj_gen->get_key_len());
            }
        } else if (arg->type == data_type) {
            unsigned int value_len;
            const char *value = m_obj_gen->get_value(0, &value_len);

            assert(value != NULL);
            assert(value_len > 0);

            if (scan_incremental_iteration) {
                assert(i < m_scan_args.size());
                m_scan_args[i].assign(value, value_len);
            }
            cmd_size += m_connections[conn_id]->send_arbitrary_command(arg, value, value_len);
        }
    }

    m_connections[conn_id]->send_arbitrary_command_end(command_index, &timestamp, cmd_size);
    return true;
}

bool client::create_scan_continuation_request(struct timeval &timestamp, unsigned int conn_id, unsigned int stats_index)
{
    int cmd_size = 0;
    arbitrary_command *cmd = m_config->scan_continuation_command;

    benchmark_debug_log("%s: SCAN continuation cursor=%s\n", m_connections[conn_id]->get_readable_id(),
                        m_scan_cursor.c_str());

    for (unsigned int i = 0; i < cmd->command_args.size(); i++) {
        const command_arg *arg = &cmd->command_args[i];
        if (arg->type == const_type) {
            cmd_size += m_connections[conn_id]->send_arbitrary_command(arg);
        } else if (arg->type == scan_cursor_type) {
            cmd_size +=
                m_connections[conn_id]->send_arbitrary_command(arg, m_scan_cursor.c_str(), m_scan_cursor.length());
        } else if (arg->type == key_type || arg->type == data_type) {
            assert(i < m_scan_args.size());
            const std::string &value = m_scan_args[i];
            cmd_size += m_connections[conn_id]->send_arbitrary_command(arg, value.data(), value.size());
        }
    }

    m_connections[conn_id]->send_arbitrary_command_end(stats_index, &timestamp, cmd_size);
    return true;
}

bool client::create_wait_request(struct timeval &timestamp, unsigned int conn_id)
{
    unsigned int num_slaves = m_obj_gen->random_range(m_config->num_slaves.min, m_config->num_slaves.max);
    unsigned int timeout = m_obj_gen->normal_distribution(
        m_config->wait_timeout.min, m_config->wait_timeout.max, 0,
        ((m_config->wait_timeout.max - m_config->wait_timeout.min) / 2.0) + m_config->wait_timeout.min);

    m_connections[conn_id]->send_wait_command(&timestamp, num_slaves, timeout);
    return true;
}

bool client::create_set_request(struct timeval &timestamp, unsigned int conn_id)
{
    unsigned long long key_index;
    get_key_response res = get_key_for_conn(SET_CMD_IDX, conn_id, &key_index);
    if (res == not_available) return false;

    if (res == available_for_conn) {
        unsigned int value_len;
        const char *value = m_obj_gen->get_value(key_index, &value_len);

        m_connections[conn_id]->send_set_command(&timestamp, m_obj_gen->get_key(), m_obj_gen->get_key_len(), value,
                                                 value_len, m_obj_gen->get_expiry(), m_config->data_offset);
    }

    return true;
}

bool client::create_get_request(struct timeval &timestamp, unsigned int conn_id)
{
    unsigned long long key_index;
    get_key_response res = get_key_for_conn(GET_CMD_IDX, conn_id, &key_index);
    if (res == not_available) return false;

    if (res == available_for_conn) {
        m_connections[conn_id]->send_get_command(&timestamp, m_obj_gen->get_key(), m_obj_gen->get_key_len(),
                                                 m_config->data_offset);
    }

    return true;
}

bool client::create_mget_request(struct timeval &timestamp, unsigned int conn_id)
{
    unsigned long long key_index;
    unsigned int keys_count = m_config->ratio.b - m_get_ratio_count;
    if ((int) keys_count > m_config->multi_key_get) keys_count = m_config->multi_key_get;

    m_keylist->clear();
    for (unsigned int i = 0; i < keys_count; i++) {
        get_key_response res = get_key_for_conn(GET_CMD_IDX, conn_id, &key_index);
        if (res != available_for_conn) continue;

        m_keylist->add_key(m_obj_gen->get_key(), m_obj_gen->get_key_len());
    }

    if (m_keylist->get_keys_count() == 0) return false;

    m_connections[conn_id]->send_mget_command(&timestamp, m_keylist);
    return true;
}

// This function could use some urgent TLC -- but we need to do it without altering the behavior
void client::create_request(struct timeval timestamp, unsigned int conn_id)
{
    // are we using arbitrary command?
    if (m_config->arbitrary_commands->is_defined()) {
        // SCAN incremental iteration mode: cursor state drives command selection
        if (m_config->scan_incremental_iteration) {
            if (m_scan_cursor != "0") {
                // Send continuation SCAN with current cursor, stats to index 1
                if (create_scan_continuation_request(timestamp, conn_id, 1)) {
                    m_reqs_generated++;
                }
            } else {
                // Send initial SCAN 0, stats to index 0
                // Release the transaction key pin before selecting the next scan key.
                m_txn_rotation_key_valid = false;
                if (create_arbitrary_request(0, timestamp, conn_id)) {
                    m_reqs_generated++;
                }
            }
            return;
        }

        const arbitrary_command &cmd = get_arbitrary_command(m_executed_command_index);
        /* Cluster mode can fall through to here (an empty key pool), but
         * there the MULTI may sit on another connection than conn_id; its
         * own check is in cluster_client::create_arbitrary_request(). */
        if (m_config->transaction && !m_config->cluster_mode) {
            /* The connection lost this rotation's MULTI when it reconnected.
             * Sent now, the rest of the block would run outside any
             * transaction and its EXEC would fail with "EXEC without MULTI".
             * Skip it up to and including the EXEC/DISCARD; the server already
             * discarded the commands queued before. */
            if (m_txn_multi_sent && cmd.multi_effect != multi_effect_opens &&
                !m_connections[conn_id]->is_multi_open()) {
                benchmark_debug_log("--transaction: MULTI lost on reconnect; skipping command %u\n",
                                    m_executed_command_index);
                if (cmd.multi_effect == multi_effect_closes) m_txn_multi_sent = false;
                advance_arbitrary_command_index();
                return;
            }
        }
        if (create_arbitrary_request(m_executed_command_index, timestamp, conn_id)) {
            if (cmd.multi_effect == multi_effect_opens) {
                m_txn_multi_sent = true;
            } else if (cmd.multi_effect == multi_effect_closes) {
                m_txn_multi_sent = false;
            }
            advance_arbitrary_command_index();
            m_reqs_generated++;
        }
        return;
    }

    // If the Set:Wait ratio is not 0, start off with WAITs
    if (m_config->wait_ratio.b &&
        (m_tot_wait_ops == 0 || (m_tot_set_ops / m_tot_wait_ops > m_config->wait_ratio.a / m_config->wait_ratio.b))) {
        if (!create_wait_request(timestamp, conn_id)) return;

        m_reqs_generated++;
        m_tot_wait_ops++;
    }

    // are we set or get? this depends on the ratio
    else if (m_set_ratio_count < m_config->ratio.a) {
        /* Before we can create a SET request, we need to read the next imported item */
        if (m_config->data_import) {
            dynamic_cast<import_object_generator *>(m_obj_gen)->read_next_item();
        }

        if (!create_set_request(timestamp, conn_id)) return;

        m_set_ratio_count++;
        m_reqs_generated++;
        m_tot_set_ops++;
    } else if (m_get_ratio_count < m_config->ratio.b) {
        // GET command
        if (!m_config->multi_key_get) {
            if (!create_get_request(timestamp, conn_id)) return;

            m_get_ratio_count++;
            m_reqs_generated++;
            return;
        }

        // MGET command
        if (!create_mget_request(timestamp, conn_id)) {
            // Two distinct reasons for false:
            //   defer  (m_mget_defer==true):  backpressure or no-route — the
            //     destination connection is at pipeline capacity or the routing
            //     policy found no eligible replica right now. Do NOT advance the
            //     ratio counter; the next fill_pipeline tick will retry.
            //   exhausted (m_mget_defer==false): this connection owns no slots
            //     in the configured key range. Force the counter past the
            //     threshold so create_request() resets both counters on the next
            //     call instead of busy-spinning here forever.
            if (!m_mget_defer) {
                m_get_ratio_count = m_config->ratio.b;
            }
            return;
        }

        m_get_ratio_count += m_keylist->get_keys_count();
        m_reqs_generated++;
    } else {
        // overlap counters
        m_get_ratio_count = m_set_ratio_count = 0;
    }
}

int client::prepare(void)
{
    if (MAIN_CONNECTION == NULL) return -1;

    int ret = this->connect();
    if (ret < 0) {
        benchmark_error_log("prepare: failed to connect, test aborted.\n");
        return ret;
    }

    return 0;
}

// Maps internal request_type to a short uppercase string for failed-keys logs.
const char *request_type_to_name(request_type t)
{
    switch (t) {
    case rt_get:
        return "GET";
    case rt_set:
        return "SET";
    case rt_wait:
        return "WAIT";
    case rt_arbitrary:
        return "ARBITRARY";
    default:
        return "UNKNOWN";
    }
}

void client::handle_response(unsigned int conn_id, struct timeval timestamp, request *request,
                             protocol_response *response)
{
    if (response->is_error()) {
        benchmark_error_log("server %s handle error response: %s\n", m_connections[conn_id]->get_readable_id(),
                            response->get_status());

        // Retry path: only when --retry-on-error is enabled. We intercept the
        // failed response and re-enqueue the request; stats are NOT updated
        // until the request reaches a terminal outcome (success or retries
        // exhausted). The shard_connection's retry queue handles backoff and
        // hard-cap; if it refuses, we fall through to permanent-failure
        // accounting below.
        if (m_config->retry_on_error && is_retryable_error(response->get_status(), m_config->retry_on_filter)) {
            shard_connection *sc = m_connections[conn_id];
            if (sc->enqueue_retry(request)) {
                // Ownership transferred to the retry queue (signalled by
                // request->m_claimed_by_retry = true). process_response skips
                // its delete; the request is freed when the retry queue or
                // replay path drains it.
                m_stats.inc_retry_attempt();
                if (request->m_retries == 0) m_stats.inc_retried_op();
                return;
            }
            // enqueue_retry refused (max retries exceeded / queue full / no
            // captured bytes). Fall through to permanent-failure accounting.
        }

        // Permanent failure path: log to failed-keys file (if configured),
        // bump the error counter, and fall through to stats.
        if (m_config->failed_keys_file) {
            global_failed_keys_logger().log_failure(timestamp, request_type_to_name(request->m_type), request->m_key,
                                                    request->m_key_len, response->get_status(), request->m_retries);
        }
        m_stats.inc_error();

        // On SCAN error, reset cursor to restart iteration
        if (m_config->scan_incremental_iteration && request->m_type == rt_arbitrary) {
            m_scan_cursor = "0";
            m_scan_iteration_count = 0;
        }
    }
    // Stats use the first-attempt send time so retries appear as one op with
    // total latency from first send to final outcome. For non-retry runs,
    // m_first_sent_time == m_sent_time, so this is a no-op behavior change.
    struct timeval ref_sent = request->m_first_sent_time;
    switch (request->m_type) {
    case rt_get:
        m_stats.update_get_op(&timestamp, response->get_total_len(), request->m_size, ts_diff(ref_sent, timestamp),
                              response->get_hits(), request->m_keys - response->get_hits());
        break;
    case rt_set:
        m_stats.update_set_op(&timestamp, response->get_total_len(), request->m_size, ts_diff(ref_sent, timestamp));
        break;
    case rt_wait:
        m_stats.update_wait_op(&timestamp, ts_diff(ref_sent, timestamp));
        break;
    case rt_arbitrary: {
        arbitrary_request *ar = static_cast<arbitrary_request *>(request);
        m_stats.update_arbitrary_op(&timestamp, response->get_total_len(), request->m_size,
                                    ts_diff(ref_sent, timestamp), ar->index);

        // Per-key miss accounting. Only fires when:
        //   1. the command resolved to a registry entry (spec != NULL)
        //   2. miss tracking wasn't disabled by --command-miss-tracking=off
        //   3. the reply isn't an error (errors are recorded separately by the
        //      protocol layer; counting them as misses would double-book).
        if (ar->m_cmd_meta != NULL && ar->m_cmd_meta->miss_tracking_enabled && ar->m_cmd_meta->spec != NULL &&
            !response->is_error()) {
            unsigned int num_keys = request->m_keys;
            unsigned int hits = 0;
            unsigned int misses = 0;
            // Each case allocates per_key_hit at the right size once. Avoiding
            // a pre-resize here means SingleNullBulk pays only for a 1-bucket
            // vector and ArrayPerElementNulls pays for one assign() instead of
            // a resize-then-assign pair on the hot reply path.
            std::vector<bool> per_key_hit;

            using memtier::command_meta::ReplyShape;
            switch (ar->m_cmd_meta->spec->reply_shape) {
            case ReplyShape::SingleNullBulk: {
                // Hit iff the top-level reply isn't a null sentinel. We can't
                // use response->get_hits() here: the parser increments it for
                // every non-null bulk it walks, which overcounts for blocking
                // pop commands (BLPOP/BRPOP/BZPOPMAX/BZPOPMIN return [key,
                // value] arrays on success — get_hits() returns 2) and for
                // multi-element pops (LPOP key COUNT N returns an N-element
                // array). The status line carries the top-level RESP type
                // header, which is what we actually care about: $-1 (null
                // bulk), *-1 (null array), or anything else (a value).
                const char *status = response->get_status();
                // Miss sentinels: $-1 (null bulk), *-1 (null array, RESP2),
                // *0 (empty array - returned by SPOP/SRANDMEMBER with a
                // count argument when the key is absent), and "_" (RESP3
                // null, parsed via single_type and stored verbatim as the
                // status line without the CRLF). Anything else is a value
                // (or a non-empty container) and counts as a hit.
                bool is_null = (status != NULL && (strcmp(status, "$-1") == 0 || strcmp(status, "*-1") == 0 ||
                                                   strcmp(status, "*0") == 0 || strcmp(status, "_") == 0));
                // Always one bucket for SingleNullBulk: variadic-key blocking
                // commands like BLPOP carry the winning key in the reply but
                // we don't parse it, so per-key attribution beyond hit/miss
                // isn't available.
                per_key_hit.assign(1, false);
                num_keys = 1;
                hits = is_null ? 0 : 1;
                misses = is_null ? 1 : 0;
                if (!is_null) per_key_hit[0] = true;
                break;
            }
            case ReplyShape::ArrayPerElementNulls: {
                // The parser recorded a per-top-level-element hit/miss bitmap at
                // parse time (set_track_elem_misses) with no materialization --
                // no value malloc/memcpy, no mbulk tree. Bucket count is the
                // reply element count, not the spec key count: HMGET / ZMSCORE
                // have 1 Redis key but produce one reply element per
                // field/member; MGET (multi-key spec) matches reply size 1:1.
                //
                // The bitmap reproduces the old materialized walk's semantics:
                //   - bulk leaf: hit iff $N with N>0 (null $-1 and empty $0 are
                //     misses); RESP3 null '_' is a miss; other single types are
                //     hits (see track_leaf in protocol.cpp).
                //   - nested sub-array element (GEOPOS/SORT_RO/...): hit iff the
                //     sub-array is non-empty (*0 / *-1 are misses).
                if (response->get_top_array_len() < 0) {
                    // Top-level reply isn't an array (+OK, -ERR, a single bulk,
                    // or any non-array misshape). One miss bucket, matching the
                    // previous get_mbulk_value()==NULL guard.
                    per_key_hit.assign(1, false);
                    num_keys = 1;
                    misses = 1;
                } else {
                    const std::vector<uint8_t> &eh = response->get_elem_hits();
                    size_t n = eh.size();
                    per_key_hit.assign(n, false);
                    num_keys = (unsigned int) n;
                    for (size_t i = 0; i < n; ++i) {
                        if (eh[i]) {
                            per_key_hit[i] = true;
                            hits++;
                        }
                    }
                    misses = (num_keys > hits) ? (num_keys - hits) : 0;
                }
                break;
            }
            case ReplyShape::EmptyCollection: {
                // Heuristic: empty array == missing key. EmptyCollection
                // commands (SMEMBERS, LRANGE, HGETALL, ...) virtually always
                // carry exactly 1 key.
                //
                // We only need the top-level element count, not the element
                // values, so we avoid set_keep_value() materialization (see the
                // keep_value gate in setup_client) and read the declared
                // aggregate length the parser captured from the reply header.
                // get_top_array_len() is:
                //   >0  for a populated array/map/set  -> hit (regardless of
                //       whether the elements are zero-length: the count comes
                //       from the *N/%N/~N header, not a per-value walk),
                //    0  for an empty (*0) or null (*-1) collection -> miss,
                //   -1  when the reply was not a top-level aggregate at all
                //       (a scalar/bulk misshape) -> miss, matching the previous
                //       get_mbulk_value()==NULL guard.
                // This reproduces the old structural check exactly while
                // staying alloc-free, and is independent of keep_value (it is
                // captured whether or not an ArrayPerElementNulls command on
                // the same connection forced materialization on).
                bool empty = (response->get_top_array_len() <= 0);
                per_key_hit.assign(num_keys, !empty);
                hits = empty ? 0 : num_keys;
                misses = empty ? num_keys : 0;
                break;
            }
            case ReplyShape::IntegerMembership: {
                // Reply is ":N\r\n" where N is the count of present keys.
                // Per-position attribution is impossible from this reply, so
                // we mark the first N buckets as hits and the rest as misses;
                // the aggregate is correct, the per-position is conventional.
                const char *status = response->get_status();
                unsigned int n = 0;
                if (status != NULL && status[0] == ':') {
                    long v = strtol(status + 1, NULL, 10);
                    if (v > 0) n = (unsigned int) v;
                }
                if (n > num_keys) n = num_keys;
                hits = n;
                misses = num_keys - n;
                per_key_hit.assign(num_keys, false);
                for (unsigned int i = 0; i < hits; ++i)
                    per_key_hit[i] = true;
                break;
            }
            default:
                // Unreachable today: miss_tracking_enabled is only set for the
                // four miss-bearing shapes above. Skip the stats call entirely
                // here so a future shape addition doesn't silently record
                // zero-valued hits/misses with an empty per_key_hit; the build
                // will fail to compile (missing case label) only if -Wswitch
                // catches it, so the guard makes the intent explicit.
                break;
            }

            if (!per_key_hit.empty()) {
                m_stats.update_arbitrary_op_misses(ar->index, hits, misses, per_key_hit);
            }
        }

        // Extract cursor from SCAN response for incremental iteration
        if (m_config->scan_incremental_iteration && !response->is_error()) {
            mbulk_size_el *top = response->get_mbulk_value();
            if (top && top->mbulks_elements.size() >= 1) {
                bulk_el *cursor_el = top->mbulks_elements[0]->as_bulk();
                if (cursor_el && cursor_el->value && cursor_el->value_len > 0) {
                    m_scan_cursor.assign(cursor_el->value, cursor_el->value_len);
                } else {
                    m_scan_cursor = "0";
                }
            } else {
                m_scan_cursor = "0";
            }

            // Only count continuation SCANs (index 1), not the initial SCAN 0 (index 0)
            if (ar->index == 1) {
                m_scan_iteration_count++;
            }
            if (m_scan_cursor == "0" || (m_config->scan_incremental_max_iterations > 0 &&
                                         m_scan_iteration_count >= m_config->scan_incremental_max_iterations)) {
                m_scan_cursor = "0";
                m_scan_iteration_count = 0;
            }
        }
        break;
    }
    default:
        assert(0);
        break;
    }
}

///////////////////////////////////////////////////////////////////////////

verify_client::verify_client(struct event_base *event_base, benchmark_config *config, abstract_protocol *protocol,
                             object_generator *obj_gen) :
        client(event_base, config, protocol, obj_gen), m_finished(false), m_verified_keys(0), m_errors(0)
{
    MAIN_CONNECTION->get_protocol()->set_keep_value(true);
}

unsigned long long int verify_client::get_verified_keys(void)
{
    return m_verified_keys;
}

unsigned long long int verify_client::get_errors(void)
{
    return m_errors;
}

bool verify_client::create_wait_request(struct timeval &timestamp, unsigned int conn_id)
{
    // Nothing to do
    return true;
}

bool verify_client::create_set_request(struct timeval &timestamp, unsigned int conn_id)
{
    unsigned long long key_index;
    get_key_response res = get_key_for_conn(SET_CMD_IDX, conn_id, &key_index);
    if (res == not_available) return false;

    if (res == available_for_conn) {
        unsigned int value_len;
        const char *value = m_obj_gen->get_value(key_index, &value_len);

        m_connections[conn_id]->send_verify_get_command(&timestamp, m_obj_gen->get_key(), m_obj_gen->get_key_len(),
                                                        value, value_len, m_config->data_offset);
    }

    return true;
}

bool verify_client::create_get_request(struct timeval &timestamp, unsigned int conn_id)
{
    // Just Keep object generator synced
    unsigned long long key_index;
    get_key_for_conn(GET_CMD_IDX, conn_id, &key_index);

    return true;
}

bool verify_client::create_mget_request(struct timeval &timestamp, unsigned int conn_id)
{
    // Just Keep object generator synced
    unsigned long long key_index;
    unsigned int keys_count = m_config->ratio.b - m_get_ratio_count;
    if ((int) keys_count > m_config->multi_key_get) keys_count = m_config->multi_key_get;

    m_keylist->clear();
    for (unsigned int i = 0; i < keys_count; i++) {
        get_key_for_conn(GET_CMD_IDX, conn_id, &key_index);
    }

    return true;
}

void verify_client::handle_response(unsigned int conn_id, struct timeval timestamp, request *request,
                                    protocol_response *response)
{
    unsigned int rvalue_len;
    const char *rvalue = response->get_value(&rvalue_len);
    verify_request *vr = static_cast<verify_request *>(request);

    assert(vr->m_type == rt_get);
    if (response->is_error()) {
        // Same retry policy as the normal client: retry transient errors;
        // permanent errors (and value-mismatch below) always count.
        if (m_config->retry_on_error && is_retryable_error(response->get_status(), m_config->retry_on_filter)) {
            shard_connection *sc = m_connections[conn_id];
            if (sc->enqueue_retry(request)) {
                m_stats.inc_retry_attempt();
                if (request->m_retries == 0) m_stats.inc_retried_op();
                return;
            }
            // enqueue refused — fall through to terminal-failure accounting.
        }
        if (m_config->failed_keys_file) {
            global_failed_keys_logger().log_failure(timestamp, "GET", vr->m_key, vr->m_key_len, response->get_status(),
                                                    vr->m_retries);
        }
        benchmark_error_log("error: request for key [%.*s] failed: %s\n", vr->m_key_len, vr->m_key,
                            response->get_status());
        m_stats.inc_error();
        m_errors++;
    } else {
        if (!rvalue || rvalue_len != vr->m_value_len || memcmp(rvalue, vr->m_value, rvalue_len) != 0) {
            benchmark_error_log("error: key [%.*s]: expected [%.*s], got [%.*s]\n", vr->m_key_len, vr->m_key,
                                vr->m_value_len, vr->m_value, rvalue_len, rvalue);
            m_errors++;
        } else {
            benchmark_debug_log("key: [%.*s] verified successfuly.\n", vr->m_key_len, vr->m_key);
            m_verified_keys++;
        }
    }
}

bool verify_client::finished(void)
{
    if (m_finished) return true;
    if (m_config->requests > 0 && m_reqs_processed >= m_config->requests) return true;
    return false;
}

///////////////////////////////////////////////////////////////////////////

client_group::client_group(benchmark_config *config, abstract_protocol *protocol, object_generator *obj_gen) :
        m_base(NULL),
        m_config(config),
        m_protocol(protocol),
        m_obj_gen(obj_gen),
        m_staircase_timer(NULL),
        m_staircase_active_clients(0),
        m_staircase_steps(0),
        m_interrupt_requested(false),
        m_worker_tid_set(false),
        m_joined(false)
{
    m_interrupt_time.tv_sec = 0;
    m_interrupt_time.tv_usec = 0;
    m_base = event_base_new();
    assert(m_base != NULL);

    assert(protocol != NULL);
    assert(obj_gen != NULL);
}

client_group::~client_group(void)
{
    if (m_staircase_timer != NULL) {
        event_del(m_staircase_timer);
        event_free(m_staircase_timer);
        m_staircase_timer = NULL;
    }

    for (std::vector<client *>::iterator i = m_clients.begin(); i != m_clients.end(); i++) {
        client *c = *i;
        delete c;
    }
    m_clients.clear();

    if (m_base != NULL) event_base_free(m_base);
    m_base = NULL;
}

int client_group::create_clients(int num)
{
    for (int i = 0; i < num; i++) {
        client *c;

        if (m_config->cluster_mode)
            c = new cluster_client(this);
        else
            c = new client(this);

        assert(c != NULL);

        if (!c->initialized()) {
            delete c;
            return i;
        }

        m_clients.push_back(c);

        // Add jitter between connection creation (except for the last connection)
        if (i < num - 1 && m_config->thread_conn_start_max_jitter_micros > 0) {
            unsigned int jitter_range =
                m_config->thread_conn_start_max_jitter_micros - m_config->thread_conn_start_min_jitter_micros;
            unsigned int jitter_micros = m_config->thread_conn_start_min_jitter_micros;

            if (jitter_range > 0) {
                jitter_micros += rand() % (jitter_range + 1);
            }

            usleep(jitter_micros);
        }
    }

    return num;
}

int client_group::prepare(void)
{
    return prepare_count(m_clients.size());
}

int client_group::prepare_count(unsigned int count)
{
    if (count > m_clients.size()) count = m_clients.size();

    for (unsigned int i = 0; i < count; i++) {
        int ret = m_clients[i]->prepare();
        if (ret < 0) {
            return ret;
        }
    }

    m_staircase_active_clients.store(count, std::memory_order_release);
    return 0;
}

unsigned int client_group::active_client_count(void)
{
    if (m_config->clients_start > 0) {
        return m_staircase_active_clients.load(std::memory_order_acquire);
    }
    return m_clients.size();
}

void client_group::run(void)
{
    m_worker_tid = pthread_self();
    m_worker_tid_set.store(true, std::memory_order_release);
    if (m_config->clients_start > 0) {
        setup_staircase_timer();
    }
    event_base_dispatch(m_base);

    // The loop can end while requests are still held for resend: a
    // connection that lost its server for good (no --reconnect-on-error, or
    // out of reconnect attempts) ends the whole thread. The other
    // connections on it may have moved their in-flight requests to their
    // replay queues on the same outage, or still have them in flight, their
    // own EOF not handled yet. Nothing resends them now; count them as
    // errors instead of freeing them silently with the connections. A run
    // that ended normally has already emptied them (disconnect_all()). On
    // Ctrl+C, requests still in flight are dropped as before.
    const bool interrupted = !m_clients.empty() && m_clients[0]->get_stats()->get_interrupted();
    unsigned long long abandoned = 0;
    for (unsigned int i = 0; i < m_clients.size(); i++) {
        std::vector<shard_connection *> &conns = m_clients[i]->get_connections();
        for (unsigned int j = 0; j < conns.size(); j++) {
            shard_connection *sc = conns[j];
            if (sc == NULL) continue;
            // Moves the requests still in flight to the replay queue.
            if (m_config->retry_on_error && !interrupted && sc->get_pending_resp() > 0) sc->disconnect();
            const char *reason = sc->get_thread_end_reason();
            abandoned += sc->abandon_replay_requests(reason ? reason : "thread-ended-before-resend");
        }
    }
    if (abandoned) {
        benchmark_error_log("warning: %llu request(s) held for resend when the thread ended were not resent; "
                            "counted as errors.\n",
                            abandoned);
    }
}

void client_group::staircase_timer_cb(evutil_socket_t fd, short what, void *arg)
{
    (void) fd;
    (void) what;
    client_group *cg = (client_group *) arg;
    cg->handle_staircase_step();
}

void client_group::setup_staircase_timer(void)
{
    struct timeval interval = {(long) m_config->step_duration, 0};
    m_staircase_timer = event_new(m_base, -1, EV_PERSIST, staircase_timer_cb, (void *) this);
    event_add(m_staircase_timer, &interval);
}

void client_group::handle_staircase_step(void)
{
    // Check if the test deadline has passed. Without this, the EV_PERSIST
    // timer keeps event_base_dispatch alive even after all clients' finished()
    // returns true, extending the run far beyond test_time.
    // Two checks, either one stops:
    // - The step count: step k is due at k * step_duration. The clock alone
    //   can't decide this: the timer runs on libevent's monotonic clock and
    //   can fire a hair before the deadline by gettimeofday(), which would add
    //   clients that the deadline stops at once.
    // - The clock: after a stall of the event loop (SIGSTOP, VM pause), the
    //   timer is rescheduled from "now", so the count lags behind and a step
    //   could fire after the deadline. Compare full timevals, as finished()
    //   does.
    m_staircase_steps++;
    if (m_config->test_time > 0) {
        struct timeval now, elapsed;
        gettimeofday(&now, NULL);
        timersub(&now, &m_config->benchmark_start_time, &elapsed);
        if ((unsigned long long) m_staircase_steps * m_config->step_duration >= m_config->test_time ||
            elapsed.tv_sec >= (time_t) m_config->test_time) {
            if (m_staircase_timer != NULL) {
                event_del(m_staircase_timer);
                event_free(m_staircase_timer);
                m_staircase_timer = NULL;
            }
            return;
        }
    }

    unsigned int current = m_staircase_active_clients.load(std::memory_order_relaxed);
    unsigned int target = current + m_config->clients_step;
    if (target > m_config->clients) target = m_config->clients;

    if (current >= target) {
        // Already at max, remove the timer
        if (m_staircase_timer != NULL) {
            event_del(m_staircase_timer);
            event_free(m_staircase_timer);
            m_staircase_timer = NULL;
        }
        return;
    }

    // Prepare (connect) pre-created clients from index 'current' to 'target'
    unsigned int connected = current;
    for (unsigned int i = current; i < target && i < m_clients.size(); i++) {
        int ret = m_clients[i]->prepare();
        if (ret < 0) {
            benchmark_error_log("staircase: failed to connect client %u.\n", i);
            break;
        }
        connected++;
    }

    m_staircase_active_clients.store(connected, std::memory_order_release);

    benchmark_debug_log("staircase: activated %u clients, now at %u/%u per thread.\n", connected - current, connected,
                        m_config->clients);

    // If we've reached max, remove the timer
    if (connected >= m_config->clients) {
        if (m_staircase_timer != NULL) {
            event_del(m_staircase_timer);
            event_free(m_staircase_timer);
            m_staircase_timer = NULL;
        }
    }
}

// Called from the main thread once a --test-time run is
// TEST_TIME_DRAIN_GRACE_SECS past its deadline and this thread has not ended.
// The clients belong to the worker thread, so hand the work to its event loop
// (this also wakes it from a poll that has nothing left to deliver).
void client_group::end_stalled_clients(void)
{
    struct timeval zero = {0, 0};
    event_base_once(m_base, -1, EV_TIMEOUT, end_stalled_clients_cb, (void *) this, &zero);
}

void client_group::end_stalled_clients_cb(evutil_socket_t fd, short what, void *arg)
{
    (void) fd;
    (void) what;
    ((client_group *) arg)->handle_end_stalled_clients();
}

void client_group::handle_end_stalled_clients(void)
{
    // A client is reported as waiting for replies if it was (even if only
    // for setup commands, which are not counted), and as retrying if it
    // held requests for retry; one that had both is in both lines.
    unsigned long long unanswered = 0, held = 0;
    unsigned int no_reply = 0, retrying = 0, ended = 0;
    unsigned int count = active_client_count();
    for (unsigned int i = 0; i < count && i < m_clients.size(); i++) {
        if (m_clients[i]->end_time_set()) continue;
        unsigned long long client_unanswered = 0, client_held = 0;
        if (!m_clients[i]->abandon_unanswered_requests(&client_unanswered, &client_held)) continue;
        ended++;
        if (client_unanswered > 0 || client_held == 0) {
            no_reply++;
            unanswered += client_unanswered;
        }
        if (client_held > 0) {
            retrying++;
            held += client_held;
        }
    }
    if (ended > 0) {
        // The deadline has passed, so the ramp is over. Do not let its
        // persistent timer keep the loop alive until the next step.
        if (m_staircase_timer != NULL) {
            event_del(m_staircase_timer);
            event_free(m_staircase_timer);
            m_staircase_timer = NULL;
        }
        if (no_reply > 0) {
            benchmark_error_log("warning: %u client(s) got no reply to %llu request(s) %u seconds after the test time "
                                "ended; abandoned them (counted as errors, \"Unanswered requests\" in the JSON "
                                "Runtime).\n",
                                no_reply, unanswered, TEST_TIME_DRAIN_GRACE_SECS);
        }
        if (retrying > 0) {
            benchmark_error_log("warning: %u client(s) were still retrying %llu request(s) %u seconds after the test "
                                "time ended; ended them (counted as errors, not resent).\n",
                                retrying, held, TEST_TIME_DRAIN_GRACE_SECS);
        }
    }
}

void client_group::interrupt(void)
{
    // The end time of the clients that have not ended yet. Taken first, so
    // that it does not include the time the loop takes to stop.
    if (!m_interrupt_requested) {
        gettimeofday(&m_interrupt_time, NULL);
        m_interrupt_requested = true;
    }
    // Mark all clients as interrupted
    set_all_clients_interrupted();
    // Break the event loop to stop processing.
    //
    // We pair loopbreak() (terminate after the current iteration) with a
    // loopexit(timeval{0,0}) (schedule an immediate timer that wakes the
    // poll). Without the loopexit wake-up, a worker that is sitting idle
    // in epoll_wait() with no live event sources (e.g. a connection that
    // failed setup but never disconnected) will block forever, so
    // pthread_join() in the parent hangs indefinitely. This is the same
    // pattern libevent recommends for terminating an idle loop from
    // another thread; loopbreak alone is documented to only take effect
    // *between* iterations.
    event_base_loopbreak(m_base);
    struct timeval zero = {0, 0};
    event_base_loopexit(m_base, &zero);
    // Do not finalize the clients here: the worker may still be in a callback
    // that records into the same run_stats (roll_cur_stats() and
    // set_end_time() both fold and append the current second).
    // finalize_interrupted_clients() does it after pthread_join(), with the
    // end time taken here.
}

void client_group::finalize_interrupted_clients(void)
{
    if (m_interrupt_requested) finalize_all_clients(&m_interrupt_time);
}

void client_group::check_stats_owner(const char *what)
{
    if (m_joined.load(std::memory_order_acquire)) return;
    if (m_worker_tid_set.load(std::memory_order_acquire) && pthread_equal(pthread_self(), m_worker_tid)) return;
    // Another thread would race with the worker recording into the same
    // run_stats (roll_cur_stats() vs. set_end_time()). Always on: the race
    // itself only shows up under a sanitizer, and then not every time.
    fprintf(stderr, "BUG: %s off the worker thread before join\n", what);
    abort();
}

void client_group::finalize_all_clients(struct timeval *end_time)
{
    check_stats_owner("finalize_all_clients()");
    // Iterate ALL clients, not just active ones. During staircase mode,
    // a client may have been prepare()-d but not yet counted in the atomic
    // (race window in handle_staircase_step). Using m_clients.size() ensures
    // no client is missed. Calling set_end_time() on never-started clients
    // is harmless — they won't be included in merge (which uses active_client_count).
    for (unsigned int i = 0; i < m_clients.size(); i++) {
        if (end_time != NULL)
            m_clients[i]->set_end_time_interrupted(end_time);
        else
            m_clients[i]->set_end_time();
    }
}

// Connected (prepare()-d) clients that have not reached their stop condition
// when the event loop returns. The missing end time establishes incomplete work,
// not its cause. Clients a staircase ramp never connected are not counted.
unsigned int client_group::count_unended_clients(void)
{
    unsigned int count = 0;
    unsigned int active = active_client_count();
    for (unsigned int i = 0; i < active && i < m_clients.size(); i++) {
        if (!m_clients[i]->end_time_set()) count++;
    }
    return count;
}

void client_group::set_all_clients_interrupted(void)
{
    // Iterate ALL clients for the same reason as finalize_all_clients.
    for (unsigned int i = 0; i < m_clients.size(); i++) {
        m_clients[i]->get_stats()->set_interrupted(true);
    }
}

unsigned long long int client_group::get_total_bytes(void)
{
    unsigned long long int total_bytes = 0;
    unsigned int count = active_client_count();
    for (unsigned int i = 0; i < count; i++) {
        total_bytes += m_clients[i]->get_stats()->get_total_bytes();
    }

    return total_bytes;
}

unsigned long long int client_group::get_total_bytes_rx(void)
{
    unsigned long long int total_bytes_rx = 0;
    unsigned int count = active_client_count();
    for (unsigned int i = 0; i < count; i++) {
        total_bytes_rx += m_clients[i]->get_stats()->get_total_bytes_rx();
    }

    return total_bytes_rx;
}

unsigned long long int client_group::get_total_bytes_tx(void)
{
    unsigned long long int total_bytes_tx = 0;
    unsigned int count = active_client_count();
    for (unsigned int i = 0; i < count; i++) {
        total_bytes_tx += m_clients[i]->get_stats()->get_total_bytes_tx();
    }

    return total_bytes_tx;
}

unsigned long long int client_group::get_total_ops(void)
{
    unsigned long long int total_ops = 0;
    unsigned int count = active_client_count();
    for (unsigned int i = 0; i < count; i++) {
        total_ops += m_clients[i]->get_stats()->get_total_ops();
    }

    return total_ops;
}

double client_group::get_total_latency(void)
{
    double total_latency = 0;
    unsigned int count = active_client_count();
    for (unsigned int i = 0; i < count; i++) {
        total_latency += m_clients[i]->get_stats()->get_total_latency();
    }

    return total_latency;
}

unsigned long long int client_group::get_duration_usec(void)
{
    unsigned long long int duration = 0;
    unsigned int thread_counter = 1;
    unsigned int count = active_client_count();
    for (unsigned int i = 0; i < count; i++) {
        if (!m_clients[i]->get_stats()->has_started()) continue;
        float factor = ((float) (thread_counter - 1) / thread_counter);
        duration = factor * duration + (float) m_clients[i]->get_stats()->get_duration_usec() / thread_counter;
        thread_counter++;
    }

    return duration;
}

unsigned long long int client_group::get_total_connection_errors(void)
{
    unsigned long long int total_errors = 0;
    unsigned int count = active_client_count();
    for (unsigned int i = 0; i < count; i++) {
        total_errors += m_clients[i]->get_stats()->get_total_connection_errors();
    }

    return total_errors;
}

unsigned long long int client_group::get_total_hits(void)
{
    unsigned long long int total = 0;
    unsigned int count = active_client_count();
    for (unsigned int i = 0; i < count; i++) {
        total += m_clients[i]->get_stats()->get_total_hits();
    }
    return total;
}

unsigned long long int client_group::get_total_misses(void)
{
    unsigned long long int total = 0;
    unsigned int count = active_client_count();
    for (unsigned int i = 0; i < count; i++) {
        total += m_clients[i]->get_stats()->get_total_misses();
    }
    return total;
}

unsigned long long client_group::get_total_arbitrary_hits(void)
{
    unsigned long long total = 0;
    unsigned int count = active_client_count();
    for (unsigned int i = 0; i < count; i++) {
        total += m_clients[i]->get_stats()->get_total_arbitrary_hits();
    }
    return total;
}

unsigned long long client_group::get_total_arbitrary_misses(void)
{
    unsigned long long total = 0;
    unsigned int count = active_client_count();
    for (unsigned int i = 0; i < count; i++) {
        total += m_clients[i]->get_stats()->get_total_arbitrary_misses();
    }
    return total;
}

unsigned long long client_group::get_total_arbitrary_aborts(void)
{
    unsigned long long total = 0;
    unsigned int count = active_client_count();
    for (unsigned int i = 0; i < count; i++) {
        total += m_clients[i]->get_stats()->get_total_arbitrary_aborts();
    }
    return total;
}

unsigned long long int client_group::get_total_retry_attempts(void)
{
    unsigned long long int total = 0;
    unsigned int count = active_client_count();
    for (unsigned int i = 0; i < count; i++) {
        total += m_clients[i]->get_stats()->get_total_retry_attempts();
    }
    return total;
}

unsigned long long int client_group::get_total_retried_ops(void)
{
    unsigned long long int total = 0;
    unsigned int count = active_client_count();
    for (unsigned int i = 0; i < count; i++) {
        total += m_clients[i]->get_stats()->get_total_retried_ops();
    }
    return total;
}

unsigned long long int client_group::get_total_errors(void)
{
    unsigned long long int total = 0;
    unsigned int count = active_client_count();
    for (unsigned int i = 0; i < count; i++) {
        total += m_clients[i]->get_stats()->get_total_errors();
    }
    return total;
}

void client_group::merge_run_stats(run_stats *target, unsigned int &iteration_counter)
{
    assert(target != NULL);
    // Iterate ALL clients (not just active_client_count) to avoid dropping
    // stats from clients that were prepare()-d but not yet counted in the
    // atomic when the benchmark ended. Clients that never started
    // (set_start_time never called) are merged too, for the connection errors
    // and interrupted flag they recorded; run_stats::merge() leaves their
    // zeroed m_start_time out of the factorial-averaged timestamps, so they
    // do not advance the iteration counter. The caller owns the counter
    // (starting at 1) and passes the same one to every client_group merged
    // into the same target, so the timestamps average over all their clients.
    for (unsigned int i = 0; i < m_clients.size(); i++) {
        run_stats *stats = m_clients[i]->get_stats();
        target->merge(*stats, iteration_counter);
        if (stats->has_started()) iteration_counter++;
    }
}

void client_group::aggregate_inst_histogram(hdr_histogram *target)
{
    for (unsigned int i = 0; i < m_clients.size(); i++) {
        if (!m_clients[i]->get_stats()->has_started()) continue;
        m_clients[i]->get_stats()->copy_inst_histogram(target);
    }
}

void client_group::drain_prom_latency_histogram(hdr_histogram *target, bool run_end)
{
    for (unsigned int i = 0; i < m_clients.size(); i++) {
        if (!m_clients[i]->get_stats()->has_started()) continue;
        m_clients[i]->get_stats()->drain_prom_latency_histogram(target, run_end);
    }
}


void client_group::write_client_stats(const char *prefix)
{
    for (unsigned int i = 0; i < m_clients.size(); i++) {
        if (!m_clients[i]->get_stats()->has_started()) continue;
        char filename[PATH_MAX];

        snprintf(filename, sizeof(filename) - 1, "%s-%u.csv", prefix, i);
        if (!m_clients[i]->get_stats()->save_csv(filename, m_config)) {
            fprintf(stderr, "error: %s: failed to write client stats.\n", filename);
        }
    }
}
