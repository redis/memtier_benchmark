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

#ifndef MEMTIER_BENCHMARK_CLIENT_DATA_MANAGER_H
#define MEMTIER_BENCHMARK_CLIENT_DATA_MANAGER_H

#include <vector>

class shard_connection;

class connections_manager
{
    // Lifetime of the logical client, including deleted/recreated cluster
    // connections. Written by its worker, read only after that worker joins.
    bool m_workload_submitted = false;

public:
    void mark_workload_submitted() { m_workload_submitted = true; }
    bool has_submitted_workload() const { return m_workload_submitted; }

    virtual unsigned long long get_reqs_processed(void) = 0;
    virtual void inc_reqs_processed(void) = 0;
    virtual unsigned long long get_reqs_answered(void) = 0;
    // timestamp: when the reply was read, as passed to handle_response().
    // A reply whose request was claimed for retry (--retry-on-error) is not
    // final, so it is not the last reply that ends a --test-time client.
    virtual void inc_reqs_answered(const struct timeval &timestamp, bool is_final) = 0;
    virtual unsigned long long get_reqs_generated(void) = 0;
    virtual void inc_reqs_generated(void) = 0;
    virtual void dec_reqs_generated(unsigned long long count) = 0;
    // Requests sent and then given up for good under --retry-on-error (never
    // resent, never answered): uncount them and count each as an error.
    virtual void abandon_requests(unsigned long long count) = 0;
    virtual bool finished(void) = 0;
    // Whether a started client's --test-time deadline has passed by now,
    // which finished() may not show yet: its clock only moves with replies
    // and connection errors. Checked before workload is issued.
    virtual bool deadline_passed_by(const struct timeval &now) = 0;
    virtual bool all_connections_idle(void) = 0;
    // Whether a connection holds --retry-on-error retries for their backoff.
    virtual bool holds_retries(void) = 0;

    virtual void set_start_time(void) = 0;
    virtual void set_end_time(void) = 0;

    // Returns true if the reply produced a usable topology that was committed
    // to the manager's internal slot map; false if the reply was rejected
    // (malformed, empty, or every shard was skipped during parsing). The
    // caller -- shard_connection's CLUSTER SLOTS response handler -- uses
    // the return value to decide whether to transition m_cluster_slots to
    // setup_done. A rejected reply must NOT advance setup_done, otherwise the
    // worker enters steady-state routing with an empty topology and every
    // slot lookup returns UINT_MAX. Non-cluster clients (no override) return
    // false because they never produce a topology.
    virtual bool handle_cluster_slots(unsigned int conn_id, protocol_response *r) = 0;
    virtual void handle_response(unsigned int conn_id, struct timeval timestamp, request *request,
                                 protocol_response *response) = 0;

    virtual void create_request(struct timeval timestamp, unsigned int conn_id) = 0;
    virtual bool hold_pipeline(unsigned int conn_id) = 0;
    // Called only with a drained pipeline. Commit a planned transaction
    // reconnect if its interval and full command-rotation boundary are due.
    virtual bool prepare_transaction_reconnect() { return false; }

    // Routed requests can be waiting on another connection's rate budget.
    virtual void handle_rate_limit_refill(unsigned int) {}
    virtual void handle_connection_disconnect(unsigned int) {}
    // A connection was closed for good (a replica not needed): hand off the
    // requests it holds for resend. Only cluster clients have replicas.
    virtual void handle_connection_retired(unsigned int) {}

    virtual int connect(void) = 0;
    // Reconnect one dropped connection: returns 0 once a connect is issued,
    // 1 if the reconnect was left to a later step, -1 on failure. Only the
    // main connection exists outside cluster mode, so the default reconnects it.
    virtual int reconnect(unsigned int) { return connect(); }
    // --transaction: true while the given connection holds the pin of the
    // in-flight command rotation, i.e. it will send the rotation's EXEC.
    virtual bool is_transaction_pinned(unsigned int) { return false; }
    virtual void disconnect(void) = 0;
    virtual void disconnect_all(void) = 0;

    // Read-preference helpers: shard_connection's READONLY response handler
    // needs to wake peers that may be blocked in hold_pipeline waiting for a
    // live replica. Exposing get_connections via the abstract interface keeps
    // shard_connection decoupled from client / cluster_client concrete types.
    virtual std::vector<shard_connection *> &get_connections(void) = 0;
};


#endif // MEMTIER_BENCHMARK_CLIENT_DATA_MANAGER_H
