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

#ifndef MEMTIER_BENCHMARK_SHARD_CONNECTION_H
#define MEMTIER_BENCHMARK_SHARD_CONNECTION_H

#include <atomic>
#include <climits>
#include <queue>
#include <string>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>

#include "protocol.h"

// forward decleration
class connections_manager;
struct benchmark_config;
class abstract_protocol;
class object_generator;
struct arbitrary_command;

enum connection_state
{
    conn_disconnected,
    conn_in_progress,
    conn_connected
};
enum setup_state
{
    setup_none,
    setup_sent,
    setup_done
};

// Topology role of a shard connection. Primary == owns slots and accepts writes.
// Replica == read-only mirror of a primary; requires READONLY after HELLO before
// it will serve any user traffic in cluster mode. Cluster mode only; standalone
// replicas don't get this treatment (see P2 design brief).
enum shard_role
{
    role_primary,
    role_replica
};

enum request_type
{
    rt_unknown,
    rt_set,
    rt_get,
    rt_wait,
    rt_arbitrary,
    rt_auth,
    rt_select_db,
    rt_cluster_slots,
    rt_hello,
    rt_readonly
};
struct request
{
    request_type m_type;
    struct timeval m_sent_time; // most-recent attempt; updated on each replay
    // First-attempt send time. Stats are computed against this so retries appear
    // as a single op spanning first-send to final outcome.
    struct timeval m_first_sent_time;
    unsigned int m_size;
    unsigned int m_keys;

    // Retry bookkeeping. Populated by the retry path; ignored when --retry-on-error is off.
    unsigned int m_retries; // number of resends so far (0 = first attempt)
    // Set true by enqueue_retry when ownership has transferred to the retry
    // queue. process_response checks this flag and skips its delete, leaving
    // the request to be freed when the retry queue drains.
    bool m_claimed_by_retry;

    // Captured serialized command bytes for resend. Owned; freed in dtor.
    // NULL/0 when retry is disabled or capture failed. Stored as opaque protocol
    // bytes so any command type can be replayed without per-type re-serialization.
    char *m_serialized;
    size_t m_serialized_len;

    // Optional key string for failed-keys logging. NULL/0 for commands without
    // a key (WAIT, setup) or when key capture was not attempted.
    char *m_key;
    unsigned int m_key_len;

    request(request_type type, unsigned int size, struct timeval *sent_time, unsigned int keys);
    virtual ~request(void);

    // Attach serialized command bytes (takes ownership). Safe to call once.
    void set_serialized(const char *data, size_t len);
    // Attach a key for failed-keys logging (copies bytes; takes ownership).
    void set_key_for_log(const char *key, unsigned int key_len);
};

struct arbitrary_request : public request
{
    size_t index;
    // Nullable pointer to the source command's metadata (NULL for memcached
    // and any path where lookup is unavailable). When non-null and
    // miss_tracking_enabled is true, the response handler walks the reply to
    // record hits/misses per key bucket.
    const arbitrary_command *m_cmd_meta;

    arbitrary_request(size_t request_index, request_type type, unsigned int size, struct timeval *sent_time,
                      const arbitrary_command *cmd_meta = NULL);
    virtual ~arbitrary_request(void) {}
};

struct verify_request : public request
{
    char *m_value;
    unsigned int m_value_len;

    verify_request(request_type type, unsigned int size, struct timeval *sent_time, unsigned int keys, const char *key,
                   unsigned int key_len, const char *value, unsigned int value_len);
    virtual ~verify_request(void);
};

class shard_connection
{
    friend void cluster_client_timer_handler(evutil_socket_t fd, short what, void *ctx);
    friend void deferred_fill_pipeline_cb(evutil_socket_t fd, short what, void *ctx);
    friend void cluster_client_read_handler(bufferevent *bev, void *ctx);
    friend void cluster_client_event_handler(bufferevent *bev, short events, void *ctx);

public:
    shard_connection(unsigned int id, connections_manager *conn_man, benchmark_config *config,
                     struct event_base *event_base, abstract_protocol *abs_protocol);
    ~shard_connection();

    void set_address_port(const char *address, const char *port);
    const char *get_readable_id();

    int connect(struct connect_info *addr);
    void disconnect();

    void send_wait_command(struct timeval *sent_time, unsigned int num_slaves, unsigned int timeout);
    void send_set_command(struct timeval *sent_time, const char *key, int key_len, const char *value, int value_len,
                          int expiry, unsigned int offset);
    void send_get_command(struct timeval *sent_time, const char *key, int key_len, unsigned int offset);
    void send_mget_command(struct timeval *sent_time, const keylist *key_list);
    void send_verify_get_command(struct timeval *sent_time, const char *key, int key_len, const char *value,
                                 int value_len, unsigned int offset);
    int send_arbitrary_command(const command_arg *arg);
    int send_arbitrary_command(const command_arg *arg, const char *val, int val_len);
    void send_arbitrary_command_end(size_t command_index, struct timeval *sent_time, int cmd_size);

    void set_cluster_slots() { m_cluster_slots = setup_none; }
    void schedule_fill(void);

    enum setup_state get_cluster_slots_state() { return m_cluster_slots; }

    unsigned int get_id() { return m_id; }

    abstract_protocol *get_protocol() { return m_protocol; }

    const char *get_address() { return m_address; }

    const char *get_port() { return m_port; }

    enum connection_state get_connection_state() { return m_connection_state; }

    // Topology role accessors. Defaults to role_primary; cluster_client flips
    // replicas to role_replica after CLUSTER SLOTS reveals them.
    enum shard_role get_role() const { return m_role; }
    void set_role(enum shard_role role) { m_role = role; }
    bool is_replica() const { return m_role == role_replica; }

    // READONLY ladder state accessor. setup_done means the server has
    // acknowledged the READONLY command for this connection. Primary
    // connections skip the ladder (m_readonly_state starts and stays
    // setup_done) so this always returns setup_done for them.
    enum setup_state get_readonly_state() const { return m_readonly_state; }

    // Re-arm and immediately send READONLY on an already-connected replica
    // connection whose role was just flipped from primary → replica by a live
    // CLUSTER SLOTS refresh. Re-sets m_readonly_state to setup_none (so
    // is_conn_setup_done() returns false) and fires the READONLY wire bytes
    // immediately. If the connection is not yet in conn_connected state the
    // call is a no-op: connect() will arm the ladder normally.
    void rearm_readonly();

    // True iff this connection is ready to serve user-level reads:
    // TCP connected, cluster-slots ladder done, and (for replicas) the
    // READONLY ladder also done. Primaries satisfy the last condition
    // trivially because m_readonly_state is initialised to setup_done.
    bool is_ready_for_reads() const
    {
        if (m_connection_state != conn_connected) return false;
        if (m_cluster_slots != setup_done) return false;
        // Replicas must have completed the READONLY handshake before the
        // server will serve reads; sending a read before READONLY gets
        // a -READONLY error from the server.
        if (m_role == role_replica && m_readonly_state != setup_done) return false;
        return true;
    }

    // ----------------------------------------------------------------------
    // Read-preference observability hooks
    // ----------------------------------------------------------------------
    //
    // Per-endpoint EWMA latency in microseconds. Updated on every successful
    // response via `update_latency_ewma`. Until `m_latency_samples` reaches
    // `LATENCY_EWMA_MIN_SAMPLES` the EWMA is treated as +inf by
    // `nearest`-mode selection, so cold or unproven endpoints never win the
    // lowest-latency tiebreak.
    //
    // Per-role op counters track how many user-level requests were routed
    // to this endpoint. They feed the "Endpoints" array in mb.json and the
    // "Ops from Primary"/"Ops from Replica" counts in the per-command "Read
    // Routing" sub-object. The counters live on the connection (not on
    // run_stats) because routing is per-conn, and the connection's role can
    // be flipped at topology refresh time.
    // C++11 forbids in-class initializers for static constexpr doubles
    // (technically allowed but odr-use would require an out-of-class
    // definition, which is brittle); wrap the constants in static inline
    // accessors instead.
    static double latency_ewma_alpha() { return 0.1; }
    static unsigned int latency_ewma_min_samples() { return 10; }

    double get_latency_ewma_us() const { return m_latency_ewma_us; }
    unsigned int get_latency_samples() const { return m_latency_samples; }
    bool latency_ewma_warm() const { return m_latency_samples >= latency_ewma_min_samples(); }
    void update_latency_ewma(double sample_us)
    {
        if (m_latency_samples == 0)
            m_latency_ewma_us = sample_us;
        else {
            const double a = latency_ewma_alpha();
            m_latency_ewma_us = a * sample_us + (1.0 - a) * m_latency_ewma_us;
        }
        if (m_latency_samples < UINT_MAX) m_latency_samples++;
    }

    void inc_routed_ops() { m_routed_ops++; }
    unsigned long long get_routed_ops() const { return m_routed_ops; }

    // Snapshot the pending-response counter. Read by the crash-handler signal
    // path (print_client_list) which races with worker-thread push_req/pop_req
    // mutations; std::atomic with relaxed ordering gives TSAN a clean
    // happens-before edge and is signal-safe for lock-free integer atomics.
    int get_pending_resp() { return m_pending_resp.load(std::memory_order_relaxed); }

    // Get local port for crash reporting
    int get_local_port();

    // Get last command type for crash reporting
    const char *get_last_request_type();

    void handle_reconnect_timer_event();
    void handle_connection_timeout_event();

    // Reset retry backoff after a successful response (so a recovered SUT
    // returns to immediate retries on the next transient burst).
    void reset_retry_backoff() { m_current_retry_backoff_ms = (double) m_config->retry_backoff_ms; }

    // Retry / replay machinery.
    void handle_retry_drain_event();
    // Push `req` back onto the wire (or onto the retry queue if a backoff is
    // configured). Caller must guarantee req->m_serialized is non-NULL.
    // Returns false if max_retries exceeded or queue full (caller must finalize).
    bool enqueue_retry(request *req);
    // Replay a request (write the captured bytes to the output buffer + push_req).
    // Bumps req->m_retries and updates req->m_sent_time.
    void replay_request(request *req);
    // Drain any pending replay-on-reconnect requests back into the pipeline.
    void drain_replay_queue_after_reconnect();
    // Capture bytes added between `before_pos` and the current evbuffer length
    // into req->m_serialized. No-op when retry is globally disabled.
    void capture_serialized_bytes(size_t before_pos, request *req);

    // Hard cap on retry queue depth. True == cap hit, caller must hold the
    // pipeline until the queue drains.
    bool retry_queue_full() const;

private:
    void setup_event(int sockfd);
    int setup_socket(struct connect_info *addr);
    void set_readable_id();

    bool is_conn_setup_done();
    void send_conn_setup_commands(struct timeval timestamp);

    // True iff any peer connection on this client is still climbing the
    // setup ladder (TCP-up or in-progress, but not yet ready for reads).
    // Used by attempt_reconnect's terminal-else to avoid tearing down the
    // whole worker thread when ONE connection has exhausted its reconnect
    // budget but its sibling connections are still mid-HELLO/READONLY on
    // surviving nodes — cluster_client's routing path can fall back to
    // those peers via the existing is_ready_for_reads() gate.
    bool peer_client_has_any_setup_in_progress() const;

    request *pop_req();
    void push_req(request *req);

    void process_response(void);
    void process_subsequent_requests(void);
    void process_first_request();
    void fill_pipeline(void);

    void handle_event(short evtype);
    void handle_timer_event(void);
    void attempt_reconnect(const char *error_context);

    unsigned int m_id;
    connections_manager *m_conns_manager;
    benchmark_config *m_config;

    char *m_address;
    char *m_port;
    std::string m_readable_id;

    struct sockaddr_un *m_unix_sockaddr;
    struct bufferevent *m_bev;
    struct event_base *m_event_base;
    struct event *m_event_timer;

    abstract_protocol *m_protocol;
    std::queue<request *> *m_pipeline;
    unsigned int m_request_per_cur_interval; // number requests to send during the current interval

    // Pending-response counter. Mutated only by the connection's owning worker
    // thread (push_req/pop_req on the libevent callback) but read from the
    // crash-handler signal context on a foreign thread. std::atomic<int>
    // serializes those accesses cleanly under TSAN; on every supported
    // platform std::atomic<int> is lock-free and the loads/stores are
    // async-signal-safe.
    std::atomic<int> m_pending_resp;

    // Snapshot of the most recently pushed request's type. Updated on every
    // push_req() and read by get_last_request_type() from the crash-handler
    // signal context. std::atomic<int> (lock-free, signal-safe) replaces the
    // earlier `volatile int` which was not sufficient to silence TSAN's
    // foreign-thread read race report.
    std::atomic<int> m_last_pushed_req_type;

    enum connection_state m_connection_state;
    // Topology role; defaults to role_primary. Cluster_client sets it to
    // role_replica after CLUSTER SLOTS reveals the connection is a replica node.
    // Connection-scoped (preserved across reconnects so the READONLY ladder
    // re-fires on every reconnect).
    enum shard_role m_role;

    enum setup_state m_hello;
    enum setup_state m_authentication;
    enum setup_state m_db_selection;
    enum setup_state m_cluster_slots;
    // READONLY ladder stage. Only ever leaves setup_done when m_role ==
    // role_replica; primaries skip the stage entirely. Re-armed on every
    // reconnect because READONLY is connection-scoped on the server side.
    enum setup_state m_readonly_state;

    // Reconnection state tracking
    unsigned int m_reconnect_attempts;
    double m_current_backoff_delay;
    struct event *m_reconnect_timer;
    bool m_reconnecting;

    // Connection timeout tracking
    struct event *m_connection_timeout_timer;

    // Retry plumbing.
    //   m_retry_queue       requests waiting for backoff before resending
    //   m_replay_queue      requests that were in-flight when the socket died,
    //                       to be replayed after reconnect succeeds
    //   m_retry_drain_timer single libevent timer that drains m_retry_queue
    //                       (avoids one-timer-per-request)
    //   m_current_retry_backoff_ms  next-retry backoff delay (exponential per
    //                       retry-backoff-factor; resets to retry_backoff_ms
    //                       on a successful response)
    std::queue<request *> *m_retry_queue;
    std::queue<request *> *m_replay_queue;
    struct event *m_retry_drain_timer;
    double m_current_retry_backoff_ms;

    // Cancelable timer for deferred fill_pipeline (replaces event_base_once to
    // avoid UAF when the connection is freed before the callback fires).
    struct event *m_deferred_fill_timer;

    // Tracks whether the bufferevent was paused via bufferevent_disable(EV_READ|
    // EV_WRITE) by fill_pipeline's idle-disable branch. fill_pipeline used to
    // call bufferevent_enable() unconditionally on every invocation to recover
    // from a --transaction hold_pipeline pause; that fired per-response and
    // took libevent's BEV_LOCK in the hot path. The flag lets the resume be
    // conditional. Invariant: m_bev_paused == true iff m_bev is currently in
    // the EV_READ|EV_WRITE-disabled state.
    bool m_bev_paused;

    // EWMA of per-response latency in microseconds; see public accessors above
    // (LATENCY_EWMA_ALPHA / LATENCY_EWMA_MIN_SAMPLES). Driven by
    // update_latency_ewma() from the response handler. Read by the
    // `nearest` selector in cluster_client. Per-conn, single-threaded: no
    // synchronization needed.
    double m_latency_ewma_us = 0.0;
    unsigned int m_latency_samples = 0;

    // Number of user-facing requests this endpoint has handled (after the
    // routing decision lands at send-time). Distinct from m_pending_resp,
    // which is in-flight depth, and from run_stats counters, which aggregate
    // across endpoints and don't know about role. Bumped at send-time, not
    // at response time, so per-endpoint Ops reflects what we routed there
    // even when a request later fails.
    unsigned long long m_routed_ops = 0;
};

#endif // MEMTIER_BENCHMARK_SHARD_CONNECTION_H
