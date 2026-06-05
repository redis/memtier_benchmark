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
public:
    virtual unsigned long long get_reqs_processed(void) = 0;
    virtual void inc_reqs_processed(void) = 0;
    virtual unsigned long long get_reqs_generated(void) = 0;
    virtual void inc_reqs_generated(void) = 0;
    virtual bool finished(void) = 0;
    virtual bool all_connections_idle(void) = 0;

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
    virtual bool handle_cluster_slots(protocol_response *r) = 0;
    virtual void handle_response(unsigned int conn_id, struct timeval timestamp, request *request,
                                 protocol_response *response) = 0;

    virtual void create_request(struct timeval timestamp, unsigned int conn_id) = 0;
    virtual bool hold_pipeline(unsigned int conn_id) = 0;

    virtual int connect(void) = 0;
    virtual void disconnect(void) = 0;
    virtual void disconnect_all(void) = 0;

    // Read-preference helpers: shard_connection's READONLY response handler
    // needs to wake peers that may be blocked in hold_pipeline waiting for a
    // live replica. Exposing get_connections via the abstract interface keeps
    // shard_connection decoupled from client / cluster_client concrete types.
    virtual std::vector<shard_connection *> &get_connections(void) = 0;
};


#endif // MEMTIER_BENCHMARK_CLIENT_DATA_MANAGER_H
