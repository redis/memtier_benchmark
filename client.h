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

#ifndef _CLIENT_H
#define _CLIENT_H

#include <stdlib.h>
#include <stdio.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <vector>
#include <queue>
#include <iterator>
#include <event2/event.h>
#include <event2/buffer.h>

#include "protocol.h"
#include "config_types.h"
#include "shard_connection.h"
#include "connections_manager.h"
#include "obj_gen.h"
#include "memtier_benchmark.h"
#include "run_stats.h"

#define MAIN_CONNECTION m_connections[0]

// forward declarations
class client;
class client_group;
struct benchmark_config;
class object_generator;

#define SET_CMD_IDX 0
#define GET_CMD_IDX 2

// Stack buffer size for key operations to avoid heap allocation
#define KEY_BUFFER_STACK_SIZE 512

enum get_key_response { not_available, available_for_conn, available_for_other_conn };

class client : public connections_manager {
protected:

    std::vector<shard_connection*> m_connections;

    struct event_base* m_event_base;
    bool m_initialized;
    bool m_end_set;

    // test related
    benchmark_config* m_config;
    object_generator* m_obj_gen;
    run_stats m_stats;

    unsigned long long m_reqs_processed;          // requests processed (responses received)
    unsigned long long m_reqs_generated;          // requests generated (wait for responses)
    unsigned int m_set_ratio_count;               // number of sets counter (overlaps on ratio)
    unsigned int m_get_ratio_count;               // number of gets counter (overlaps on ratio)
    unsigned int m_arbitrary_command_ratio_count; // number of arbitrary commands counter (overlaps on ratio)
    unsigned int m_executed_command_index;        // current arbitrary command executed

    unsigned long long m_tot_set_ops;             // Total number of SET ops
    unsigned long long m_tot_wait_ops;            // Total number of WAIT ops

    keylist *m_keylist;                           // used to construct multi commands

public:
    client(client_group* group);
    client(struct event_base *event_base, benchmark_config *config, abstract_protocol *protocol, object_generator *obj_gen);
    virtual ~client();
    bool setup_client(benchmark_config *config, abstract_protocol *protocol, object_generator *obj_gen);
    int prepare(void);
    bool initialized(void);
    run_stats* get_stats(void) { return &m_stats; }

    virtual get_key_response get_key_for_conn(unsigned int command_index, unsigned int conn_id, unsigned long long* key_index);
    virtual bool create_arbitrary_request(unsigned int command_index, struct timeval& timestamp, unsigned int conn_id);
    virtual bool create_wait_request(struct timeval& timestamp, unsigned int conn_id);
    virtual bool create_set_request(struct timeval& timestamp, unsigned int conn_id);
    virtual bool create_get_request(struct timeval& timestamp, unsigned int conn_id);
    virtual bool create_mget_request(struct timeval& timestamp, unsigned int conn_id);

    // client manager api's
    unsigned long long get_reqs_processed() {
        return m_reqs_processed;
    }

    void inc_reqs_processed() {
        m_reqs_processed++;
    }

    unsigned long long get_reqs_generated() {
        return m_reqs_generated;
    }

    void inc_reqs_generated() {
        m_reqs_generated++;
    }

    virtual void handle_cluster_slots(protocol_response *r) {
        assert(false && "handle_cluster_slots not supported");
    }

    virtual void handle_response(unsigned int conn_id, struct timeval timestamp,
                                 request *request, protocol_response *response);
    virtual bool finished(void);
    virtual void set_start_time();
    virtual void set_end_time();
    virtual void create_request(struct timeval timestamp, unsigned int conn_id);
    virtual bool hold_pipeline(unsigned int conn_id);
    virtual int connect(void);
    virtual void disconnect(void);
    //

    /* Get current executed arbitrary command */
    const arbitrary_command & get_arbitrary_command(unsigned int command_index) {
        return m_config->arbitrary_commands->at(command_index);
    }

    /* Get connections for crash reporting */
    std::vector<shard_connection*>& get_connections(void) { return m_connections; }

    /* Set the arbitrary command index to the next to be executed */
    void advance_arbitrary_command_index() {
        while(true) {
            if (m_arbitrary_command_ratio_count < get_arbitrary_command(m_executed_command_index).ratio) {
                m_arbitrary_command_ratio_count++;
                return;
            } else {
                m_arbitrary_command_ratio_count = 0;
                m_executed_command_index++;
                if (m_executed_command_index == m_config->arbitrary_commands->size()) {
                    m_executed_command_index = 0;
                }
            }
        }

    }
    // Utility function to get the object iterator type based on the config
    inline int obj_iter_type(benchmark_config *cfg, unsigned char index)
    {
        if (cfg->key_pattern[index] == 'R') {
            return OBJECT_GENERATOR_KEY_RANDOM;
        } else if (cfg->key_pattern[index] == 'G') {
            return OBJECT_GENERATOR_KEY_GAUSSIAN;
        } else if (cfg->key_pattern[index] == 'Z') {
            return OBJECT_GENERATOR_KEY_ZIPFIAN;
        } else {
            if (index == key_pattern_set)
                return OBJECT_GENERATOR_KEY_SET_ITER;
            else
                return OBJECT_GENERATOR_KEY_GET_ITER;
        }
    }

    inline int arbitrary_obj_iter_type(unsigned int index) {
        const arbitrary_command& cmd = get_arbitrary_command(index);
        if (cmd.key_pattern == 'R') {
            return OBJECT_GENERATOR_KEY_RANDOM;
        } else if (cmd.key_pattern == 'G') {
            return OBJECT_GENERATOR_KEY_GAUSSIAN;
        } else if (cmd.key_pattern == 'Z') {
            return OBJECT_GENERATOR_KEY_ZIPFIAN;
        } else {
            return index;
        }
    }
};

class verify_client : public client {
protected:
    bool m_finished;
    unsigned long long int m_verified_keys;
    unsigned long long int m_errors;

    virtual bool finished(void);
    virtual bool create_wait_request(struct timeval& timestamp, unsigned int conn_id);
    virtual bool create_set_request(struct timeval& timestamp, unsigned int conn_id);
    virtual bool create_get_request(struct timeval& timestamp, unsigned int conn_id);
    virtual bool create_mget_request(struct timeval& timestamp, unsigned int conn_id);
    virtual void handle_response(unsigned int conn_id, struct timeval timestamp,
                                 request *request, protocol_response *response);
public:
    verify_client(struct event_base *event_base, benchmark_config *config, abstract_protocol *protocol, object_generator *obj_gen);
    unsigned long long int get_verified_keys(void);
    unsigned long long int get_errors(void);
};

class client_group {
protected:
    struct event_base* m_base;
    benchmark_config *m_config;
    abstract_protocol* m_protocol;
    object_generator* m_obj_gen;
    std::vector<client*> m_clients;
public:
    client_group(benchmark_config *cfg, abstract_protocol *protocol, object_generator* obj_gen);
    ~client_group();

    int create_clients(int count);
    int prepare(void);
    void run(void);
    void interrupt(void);
    void finalize_all_clients(void);
    void set_all_clients_interrupted(void);

    void write_client_stats(const char *prefix);

    struct event_base *get_event_base(void) { return m_base; }
    benchmark_config *get_config(void) { return m_config; }
    abstract_protocol* get_protocol(void) { return m_protocol; }
    object_generator* get_obj_gen(void) { return m_obj_gen; }
    std::vector<client*>& get_clients(void) { return m_clients; }

    unsigned long int get_total_bytes(void);
    unsigned long int get_total_ops(void);
    unsigned long int get_total_latency(void);
    unsigned long int get_duration_usec(void);
    unsigned long int get_total_connection_errors(void);

    void merge_run_stats(run_stats* target);
    void aggregate_inst_histogram(hdr_histogram* target);
};


#endif	/* _CLIENT_H */
