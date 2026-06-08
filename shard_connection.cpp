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

#include "shard_connection.h"
#include "obj_gen.h"
#include "memtier_benchmark.h"
#include "connections_manager.h"
#include "client.h"
#include "retry_policy.h"
#include "event2/bufferevent.h"

// Maximum backoff delay enforced after every exponential-backoff multiplication.
// Without a cap, a factor of 2.0 and --max-reconnect-attempts=0 (unlimited)
// causes the delay to double on every attempt: after ~30 doublings the next
// scheduled reconnect or retry fires in ~34 years. event_add() silently absorbs
// that value, making the benchmark go effectively dark instead of surfacing the
// underlying failure.  60 s is a practical upper bound: tight enough to remain
// responsive, large enough to avoid thundering-herd reconnect storms.
static const double MEMTIER_BACKOFF_CAP_SEC = 60.0;
static const double MEMTIER_BACKOFF_CAP_MS = 60000.0;

#ifdef USE_TLS
#include <mutex>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include "event2/bufferevent_ssl.h"
#endif

void cluster_client_timer_handler(evutil_socket_t fd, short what, void *ctx)
{
    shard_connection *sc = (shard_connection *) ctx;
    assert(sc != NULL);
    sc->handle_timer_event();
}

void cluster_client_reconnect_timer_handler(evutil_socket_t fd, short what, void *ctx)
{
    shard_connection *sc = (shard_connection *) ctx;
    assert(sc != NULL);
    sc->handle_reconnect_timer_event();
}

void deferred_fill_pipeline_cb(evutil_socket_t, short, void *ctx)
{
    static_cast<shard_connection *>(ctx)->fill_pipeline();
}

void cluster_client_connection_timeout_handler(evutil_socket_t fd, short what, void *ctx)
{
    shard_connection *sc = (shard_connection *) ctx;
    assert(sc != NULL);
    sc->handle_connection_timeout_event();
}

void cluster_client_retry_drain_handler(evutil_socket_t fd, short what, void *ctx)
{
    shard_connection *sc = (shard_connection *) ctx;
    assert(sc != NULL);
    sc->handle_retry_drain_event();
}

void cluster_client_read_handler(bufferevent *bev, void *ctx)
{
    shard_connection *sc = (shard_connection *) ctx;
    assert(sc != NULL);
    sc->process_response();
}

void cluster_client_event_handler(bufferevent *bev, short events, void *ctx)
{
    shard_connection *sc = (shard_connection *) ctx;
    assert(sc != NULL);
    sc->handle_event(events);
}

request::request(request_type type, unsigned int size, struct timeval *sent_time, unsigned int keys) :
        m_type(type),
        m_size(size),
        m_keys(keys),
        m_retries(0),
        m_claimed_by_retry(false),
        m_serialized(NULL),
        m_serialized_len(0),
        m_key(NULL),
        m_key_len(0)
{
    if (sent_time != NULL)
        m_sent_time = *sent_time;
    else {
        gettimeofday(&m_sent_time, NULL);
    }
    m_first_sent_time = m_sent_time;
}

request::~request(void)
{
    if (m_serialized) {
        free(m_serialized);
        m_serialized = NULL;
        m_serialized_len = 0;
    }
    if (m_key) {
        free(m_key);
        m_key = NULL;
        m_key_len = 0;
    }
}

void request::set_serialized(const char *data, size_t len)
{
    if (m_serialized) {
        free(m_serialized);
        m_serialized = NULL;
        m_serialized_len = 0;
    }
    if (!data || len == 0) return;
    m_serialized = (char *) malloc(len);
    if (!m_serialized) return; // best-effort capture; replay just won't work
    memcpy(m_serialized, data, len);
    m_serialized_len = len;
}

void request::set_key_for_log(const char *key, unsigned int key_len)
{
    if (m_key) {
        free(m_key);
        m_key = NULL;
        m_key_len = 0;
    }
    if (!key || key_len == 0) return;
    m_key = (char *) malloc(key_len);
    if (!m_key) return;
    memcpy(m_key, key, key_len);
    m_key_len = key_len;
}

arbitrary_request::arbitrary_request(size_t request_index, request_type type, unsigned int size,
                                     struct timeval *sent_time, const arbitrary_command *cmd_meta) :
        request(type, size, sent_time,
                // m_keys is the number of expected key buckets. Prefer the
                // spec-resolved positions when available so per-key totals match
                // what the parser will see; otherwise fall back to the user's
                // __key__ placeholder count, then 1 as a conservative default.
                (cmd_meta != NULL && !cmd_meta->spec_key_positions.empty())
                    ? (unsigned int) cmd_meta->spec_key_positions.size()
                : (cmd_meta != NULL && cmd_meta->keys_count > 0) ? cmd_meta->keys_count
                                                                 : 1),
        index(request_index),
        m_cmd_meta(cmd_meta)
{
}

verify_request::verify_request(request_type type, unsigned int size, struct timeval *sent_time, unsigned int keys,
                               const char *key, unsigned int key_len, const char *value, unsigned int value_len) :
        request(type, size, sent_time, keys), m_value(NULL), m_value_len(0)
{
    // base class holds the key for both verification + failed-keys logging.
    set_key_for_log(key, key_len);
    m_value_len = value_len;
    m_value = (char *) malloc(value_len);
    memcpy(m_value, value, m_value_len);
}

verify_request::~verify_request(void)
{
    if (m_value != NULL) {
        free((void *) m_value);
        m_value = NULL;
    }
}

shard_connection::shard_connection(unsigned int id, connections_manager *conns_man, benchmark_config *config,
                                   struct event_base *event_base, abstract_protocol *abs_protocol) :
        m_address(NULL),
        m_port(NULL),
        m_unix_sockaddr(NULL),
        m_bev(NULL),
        m_event_timer(NULL),
        m_request_per_cur_interval(0),
        m_pending_resp(0),
        m_last_pushed_req_type(-1),
        m_connection_state(conn_disconnected),
        m_role(role_primary),
        m_hello(setup_done),
        m_authentication(setup_done),
        m_db_selection(setup_done),
        m_cluster_slots(setup_done),
        m_readonly_state(setup_done),
        m_reconnect_attempts(0),
        m_current_backoff_delay(1.0),
        m_reconnect_timer(NULL),
        m_reconnecting(false),
        m_connection_timeout_timer(NULL),
        m_retry_queue(NULL),
        m_replay_queue(NULL),
        m_retry_drain_timer(NULL),
        m_current_retry_backoff_ms(0.0),
        m_deferred_fill_timer(NULL),
        m_bev_paused(false)
{
    m_id = id;
    m_conns_manager = conns_man;
    m_config = config;
    m_event_base = event_base;

    if (m_config->unix_socket) {
        m_unix_sockaddr = (struct sockaddr_un *) malloc(sizeof(struct sockaddr_un));
        assert(m_unix_sockaddr != NULL);

        m_unix_sockaddr->sun_family = AF_UNIX;
        strncpy(m_unix_sockaddr->sun_path, m_config->unix_socket, sizeof(m_unix_sockaddr->sun_path) - 1);
        m_unix_sockaddr->sun_path[sizeof(m_unix_sockaddr->sun_path) - 1] = '\0';
    }

    m_protocol = abs_protocol->clone();
    assert(m_protocol != NULL);

    m_pipeline = new std::queue<request *>;
    assert(m_pipeline != NULL);

    if (m_config->retry_on_error) {
        m_retry_queue = new std::queue<request *>;
        m_replay_queue = new std::queue<request *>;
        m_current_retry_backoff_ms = (double) m_config->retry_backoff_ms;
    }
}

shard_connection::~shard_connection()
{
    if (m_address != NULL) {
        free(m_address);
        m_address = NULL;
    }

    if (m_port != NULL) {
        free(m_port);
        m_port = NULL;
    }

    if (m_unix_sockaddr != NULL) {
        free(m_unix_sockaddr);
        m_unix_sockaddr = NULL;
    }

    if (m_bev != NULL) {
        bufferevent_free(m_bev);
        m_bev = NULL;
    }

    if (m_event_timer != NULL) {
        event_free(m_event_timer);
        m_event_timer = NULL;
    }

    if (m_reconnect_timer != NULL) {
        event_free(m_reconnect_timer);
        m_reconnect_timer = NULL;
    }

    if (m_connection_timeout_timer != NULL) {
        event_free(m_connection_timeout_timer);
        m_connection_timeout_timer = NULL;
    }

    if (m_protocol != NULL) {
        delete m_protocol;
        m_protocol = NULL;
    }

    if (m_pipeline != NULL) {
        delete m_pipeline;
        m_pipeline = NULL;
    }

    if (m_retry_drain_timer != NULL) {
        event_free(m_retry_drain_timer);
        m_retry_drain_timer = NULL;
    }

    if (m_deferred_fill_timer != NULL) {
        event_free(m_deferred_fill_timer);
        m_deferred_fill_timer = NULL;
    }

    if (m_retry_queue != NULL) {
        while (!m_retry_queue->empty()) {
            delete m_retry_queue->front();
            m_retry_queue->pop();
        }
        delete m_retry_queue;
        m_retry_queue = NULL;
    }

    if (m_replay_queue != NULL) {
        while (!m_replay_queue->empty()) {
            delete m_replay_queue->front();
            m_replay_queue->pop();
        }
        delete m_replay_queue;
        m_replay_queue = NULL;
    }
}

void shard_connection::setup_event(int sockfd)
{
    if (m_bev) {
        bufferevent_free(m_bev);
    }
    // Fresh bufferevent below starts enabled by default (after we call
    // bufferevent_enable in handle_event on BEV_EVENT_CONNECTED); reset the
    // pause flag so we don't carry stale state from a prior socket.
    m_bev_paused = false;

#ifdef USE_TLS
    if (m_config->openssl_ctx) {
        SSL *ctx = SSL_new(m_config->openssl_ctx);
        assert(ctx != NULL);

        if (m_config->tls_sni) {
            SSL_set_tlsext_host_name(ctx, m_config->tls_sni);
        }

        m_bev = bufferevent_openssl_socket_new(m_event_base, sockfd, ctx, BUFFEREVENT_SSL_CONNECTING,
                                               BEV_OPT_CLOSE_ON_FREE);
    } else {
#endif
        m_bev = bufferevent_socket_new(m_event_base, sockfd, BEV_OPT_CLOSE_ON_FREE);
#ifdef USE_TLS
    }
#endif

    assert(m_bev != NULL);
    bufferevent_setcb(m_bev, cluster_client_read_handler, NULL, cluster_client_event_handler, (void *) this);
    m_protocol->set_buffers(bufferevent_get_input(m_bev), bufferevent_get_output(m_bev));
}

int shard_connection::setup_socket(struct connect_info *addr)
{
    int flags;
    int sockfd;

    if (m_unix_sockaddr != NULL) {
        sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (sockfd < 0) {
            return -1;
        }
    } else {
        // initialize socket
        sockfd = socket(addr->ci_family, addr->ci_socktype, addr->ci_protocol);
        if (sockfd < 0) {
            return -1;
        }


        int error = setsockopt(sockfd, SOL_SOCKET, SO_KEEPALIVE, (void *) &flags, sizeof(flags));
        assert(error == 0);

        /*
         * Configure socket behavior:
         * If l_onoff is non-zero and l_linger is zero:
         *   The socket will discard any unsent data and the close() call will return immediately.
         */
        struct linger ling;
        ling.l_onoff = 1;  // Enable SO_LINGER
        ling.l_linger = 0; // Discard any unsent data and close immediately
        error = setsockopt(sockfd, SOL_SOCKET, SO_LINGER, (void *) &ling, sizeof(ling));
        assert(error == 0);

        error = setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (void *) &flags, sizeof(flags));
        assert(error == 0);
    }

    // set non-blocking behavior
    flags = 1;
    if ((flags = fcntl(sockfd, F_GETFL, 0)) < 0 || fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        close(sockfd);
        return -1;
    }

    return sockfd;
}

int shard_connection::connect(struct connect_info *addr)
{
    // Belt-and-suspenders: disconnect() already calls reset_state(), but if
    // this object was constructed and never went through a disconnect (very
    // first connect on a fresh shard_connection) we still want a clean
    // parser cursor. Cheap and idempotent.
    if (m_protocol != NULL) {
        m_protocol->reset_state();
    }

    // set required setup commands
    m_authentication = m_config->authenticate ? setup_none : setup_done;
    m_db_selection = m_config->select_db ? setup_none : setup_done;
    m_hello = (m_config->protocol == PROTOCOL_RESP2 || m_config->protocol == PROTOCOL_RESP3) ? setup_none : setup_done;
    // Replica connections need READONLY before they will serve user reads;
    // cluster mode only (standalone replicas need a different ladder per P2
    // design brief). Re-armed on every reconnect because the flag is
    // connection-scoped on the server.
    m_readonly_state = (m_role == role_replica && m_config->cluster_mode && is_redis_protocol(m_config->protocol))
                           ? setup_none
                           : setup_done;

    // setup socket
    int sockfd = setup_socket(addr);
    if (sockfd < 0) {
        fprintf(stderr, "Failed to setup socket: %s\n", strerror(errno));
        return -1;
    }

    // set up bufferevent
    setup_event(sockfd);

    // set readable id
    set_readable_id();

    // call connect
    m_connection_state = conn_in_progress;

    if (bufferevent_socket_connect(m_bev, m_unix_sockaddr ? (struct sockaddr *) m_unix_sockaddr : addr->ci_addr,
                                   m_unix_sockaddr ? sizeof(struct sockaddr_un) : addr->ci_addrlen) == -1) {
        disconnect();

        benchmark_error_log("connect failed, error = %s\n", strerror(errno));
        return -1;
    }

    // Start connection timeout timer (only if enabled)
    if (m_config->connection_timeout > 0) {
        struct timeval timeout;
        timeout.tv_sec = m_config->connection_timeout;
        timeout.tv_usec = 0;

        m_connection_timeout_timer =
            event_new(m_event_base, -1, 0, cluster_client_connection_timeout_handler, (void *) this);
        event_add(m_connection_timeout_timer, &timeout);
    }

    return 0;
}

void shard_connection::disconnect()
{
    if (m_bev) {
        bufferevent_free(m_bev);
        m_bev = NULL;
    }
    // After free, any future bufferevent starts in the default (enabled) state.
    m_bev_paused = false;

    if (m_event_timer != NULL) {
        event_free(m_event_timer);
        m_event_timer = NULL;
    }

    if (m_reconnect_timer != NULL) {
        event_free(m_reconnect_timer);
        m_reconnect_timer = NULL;
    }

    if (m_connection_timeout_timer != NULL) {
        event_free(m_connection_timeout_timer);
        m_connection_timeout_timer = NULL;
    }

    // Drain pipeline. With --retry-on-error, move in-flight requests into the
    // replay queue so they get resent after reconnect. Otherwise, discard them
    // as before.
    if (m_config->retry_on_error && m_replay_queue != NULL) {
        while (m_pending_resp) {
            request *req = pop_req();
            // Only setup commands have no serialized capture (we never attempt
            // capture for those) — drop those.
            if (req->m_type == rt_auth || req->m_type == rt_select_db || req->m_type == rt_cluster_slots ||
                req->m_type == rt_hello || req->m_type == rt_readonly) {
                delete req;
                continue;
            }
            if (req->m_serialized && req->m_serialized_len > 0) {
                m_replay_queue->push(req);
            } else {
                delete req;
            }
        }
        // Also rescue requests sitting in the per-connection retry queue
        // (waiting for a backoff timer to fire). Without this, the backoff
        // timer's connection check (handle_retry_drain_event) would silently
        // leave them stranded after reconnect.
        if (m_retry_queue != NULL) {
            while (!m_retry_queue->empty()) {
                request *req = m_retry_queue->front();
                m_retry_queue->pop();
                if (req->m_serialized && req->m_serialized_len > 0) {
                    m_replay_queue->push(req);
                } else {
                    delete req;
                }
            }
        }
        // Cancel any pending drain timer — it has nothing to drain now and
        // will be re-armed after reconnect by drain_replay_queue_after_reconnect.
        if (m_retry_drain_timer != NULL && evtimer_pending(m_retry_drain_timer, NULL)) {
            evtimer_del(m_retry_drain_timer);
        }
    } else {
        while (m_pending_resp)
            delete pop_req();
    }

    if (m_deferred_fill_timer != NULL && evtimer_pending(m_deferred_fill_timer, NULL)) {
        evtimer_del(m_deferred_fill_timer);
    }

    m_connection_state = conn_disconnected;

    // Reset rate limiting state during disconnection
    m_request_per_cur_interval = 0;

    // Clear the reconnect-in-progress flag. If a teardown races a pending
    // reconnect timer (the timer was freed above), leaving m_reconnecting
    // true would make every future attempt_reconnect call no-op silently.
    m_reconnecting = false;

    // by default no need to send any setup request
    m_authentication = setup_done;
    m_db_selection = setup_done;
    m_cluster_slots = setup_done;
    m_hello = setup_done;
    m_readonly_state = setup_done;

    // Drop any partial RESP parser state. A TCP RST received mid-bulk
    // (especially while draining a RESP3 push frame) would otherwise leave
    // m_response_state / m_total_bulks_count / m_push / m_attribute /
    // m_bulk_len at intermediate values, and the next reconnect's first
    // bytes would parse under stale cursor state. The setup ladder above
    // is already reset; this finishes the job for the protocol layer.
    if (m_protocol != NULL) {
        m_protocol->reset_state();
    }
}

void shard_connection::set_address_port(const char *address, const char *port)
{
    if (m_address != NULL) {
        free(m_address);
    }
    m_address = strdup(address);

    if (m_port != NULL) {
        free(m_port);
    }
    m_port = strdup(port);
}

void shard_connection::set_readable_id()
{
    if (m_unix_sockaddr != NULL) {
        m_readable_id.assign(m_config->unix_socket);
    } else {
        m_readable_id.assign(m_address);
        m_readable_id.append(":");
        m_readable_id.append(m_port);
    }
}

const char *shard_connection::get_readable_id()
{
    return m_readable_id.c_str();
}

int shard_connection::get_local_port()
{
    if (!m_bev) {
        return -1;
    }

    int fd = bufferevent_getfd(m_bev);
    if (fd < 0) {
        return -1;
    }

    struct sockaddr_storage local_addr;
    socklen_t addr_len = sizeof(local_addr);

    if (getsockname(fd, (struct sockaddr *) &local_addr, &addr_len) != 0) {
        return -1;
    }

    if (local_addr.ss_family == AF_INET) {
        struct sockaddr_in *addr_in = (struct sockaddr_in *) &local_addr;
        return ntohs(addr_in->sin_port);
    } else if (local_addr.ss_family == AF_INET6) {
        struct sockaddr_in6 *addr_in6 = (struct sockaddr_in6 *) &local_addr;
        return ntohs(addr_in6->sin6_port);
    }

    return -1;
}

const char *shard_connection::get_last_request_type()
{
    // Read the cached most-recently-pushed type set by push_req(). Called from
    // the crash-handler signal context on a foreign (main) thread, racing with
    // the connection's worker thread; std::atomic<int> with relaxed ordering
    // gives TSAN a clean happens-before edge and is signal-safe because the
    // load is lock-free on every supported platform. We never deref the
    // queue's request* (which a worker thread might be popping/freeing
    // concurrently).
    int t = m_last_pushed_req_type.load(std::memory_order_relaxed);
    switch (t) {
    case rt_set:
        return "SET";
    case rt_get:
        return "GET";
    case rt_wait:
        return "WAIT";
    case rt_arbitrary:
        return "ARBITRARY";
    case rt_auth:
        return "AUTH";
    case rt_select_db:
        return "SELECT";
    case rt_cluster_slots:
        return "CLUSTER_SLOTS";
    case rt_hello:
        return "HELLO";
    case rt_readonly:
        return "READONLY";
    default:
        return "none";
    }
}

request *shard_connection::pop_req()
{
    request *req = m_pipeline->front();
    m_pipeline->pop();

    // Worker-thread mutation; relaxed is sufficient because all mutations
    // happen on the connection's owning event-loop thread. The signal-handler
    // reader uses an atomic load purely to establish a TSAN happens-before
    // edge for the foreign-thread read.
    m_pending_resp.fetch_sub(1, std::memory_order_relaxed);
    assert(m_pending_resp.load(std::memory_order_relaxed) >= 0);

    return req;
}

void shard_connection::push_req(request *req)
{
    m_pipeline->push(req);
    // Worker-thread mutation (see pop_req comment).
    m_pending_resp.fetch_add(1, std::memory_order_relaxed);
    // Snapshot the type for the crash handler (which can't safely deref the
    // queue front without racing with worker-thread pops/destructors).
    m_last_pushed_req_type.store((int) req->m_type, std::memory_order_relaxed);
    // Per-endpoint routed_ops: count anything that's not a connection-setup
    // request. This is the canonical "how many user-level requests did we
    // route here" tally used by the "Endpoints" array in mb.json.
    switch (req->m_type) {
    case rt_get:
    case rt_set:
    case rt_wait:
    case rt_arbitrary:
        m_routed_ops++;
        break;
    case rt_unknown:
    case rt_auth:
    case rt_select_db:
    case rt_cluster_slots:
    case rt_hello:
    case rt_readonly:
        break;
    }
    if (m_config->request_rate) {
        // Handle race condition during reconnection - don't assert if interval is 0
        if (m_request_per_cur_interval > 0) {
            m_request_per_cur_interval--;
        } else {
            // Rate limit exceeded, but don't crash - just log debug info
            benchmark_debug_log("Rate limit interval exhausted during request push (connection %u)\n", m_id);
        }
    }
}

void shard_connection::capture_serialized_bytes(size_t before_pos, request *req)
{
    if (!m_config->retry_on_error || !m_bev || !req) return;

    struct evbuffer *out = bufferevent_get_output(m_bev);
    size_t after_pos = evbuffer_get_length(out);
    if (after_pos <= before_pos) return;
    size_t len = after_pos - before_pos;

    char *buf = (char *) malloc(len);
    if (!buf) {
        benchmark_debug_log("retry: failed to allocate %zu bytes for capture (conn %u)\n", len, m_id);
        return;
    }

    struct evbuffer_ptr p;
    if (evbuffer_ptr_set(out, &p, before_pos, EVBUFFER_PTR_SET) != 0) {
        free(buf);
        return;
    }

    struct evbuffer_iovec vecs[8];
    int n = evbuffer_peek(out, (ev_ssize_t) len, &p, vecs, 8);
    if (n < 0) {
        free(buf);
        return;
    }
    if (n > 8) {
        struct evbuffer_iovec *dyn = (struct evbuffer_iovec *) malloc((size_t) n * sizeof(*dyn));
        if (!dyn) {
            free(buf);
            return;
        }
        int n2 = evbuffer_peek(out, (ev_ssize_t) len, &p, dyn, n);
        if (n2 == n) {
            size_t off = 0;
            for (int i = 0; i < n2 && off < len; i++) {
                size_t take = dyn[i].iov_len;
                if (off + take > len) take = len - off;
                memcpy(buf + off, dyn[i].iov_base, take);
                off += take;
            }
            req->m_serialized = buf;
            req->m_serialized_len = len;
            buf = NULL; // ownership transferred
        }
        free(dyn);
        if (buf) free(buf);
        return;
    }

    size_t off = 0;
    for (int i = 0; i < n && off < len; i++) {
        size_t take = vecs[i].iov_len;
        if (off + take > len) take = len - off;
        memcpy(buf + off, vecs[i].iov_base, take);
        off += take;
    }
    req->m_serialized = buf;
    req->m_serialized_len = len;
}

bool shard_connection::retry_queue_full() const
{
    if (!m_retry_queue) return false;
    unsigned int cap = m_config->max_retry_queue;
    if (cap == 0) {
        // Auto cap: pipeline * 4, floor of 64.
        cap = m_config->pipeline * 4;
        if (cap < 64) cap = 64;
    }
    return m_retry_queue->size() >= cap;
}

bool shard_connection::enqueue_retry(request *req)
{
    if (!m_config->retry_on_error || !m_retry_queue) return false;
    if (!req || !req->m_serialized || req->m_serialized_len == 0) return false;

    // Honor max_retries (always counts; MOVED/ASK count too).
    if (m_config->max_retries >= 0 && (int) req->m_retries >= m_config->max_retries) {
        return false;
    }

    if (retry_queue_full()) {
        // Caller treats this as terminal: log + finalize.
        return false;
    }

    req->m_claimed_by_retry = true;
    m_retry_queue->push(req);

    // (Re)schedule the drain timer if we have a backoff configured. With zero
    // backoff we still go through the timer with a 0 ms delay to keep the
    // ordering predictable and the libevent integration simple.
    if (m_retry_drain_timer == NULL) {
        m_retry_drain_timer = event_new(m_event_base, -1, 0, cluster_client_retry_drain_handler, (void *) this);
    }
    if (m_retry_drain_timer != NULL) {
        // Only (re)add if not pending.
        if (!evtimer_pending(m_retry_drain_timer, NULL)) {
            double ms = m_current_retry_backoff_ms;
            struct timeval delay;
            delay.tv_sec = (long) (ms / 1000.0);
            delay.tv_usec = (long) ((ms - delay.tv_sec * 1000.0) * 1000.0);
            event_add(m_retry_drain_timer, &delay);
        }
    }

    // Exponential backoff for the *next* retry on this connection.
    if (m_config->retry_backoff_factor > 0.0) {
        m_current_retry_backoff_ms *= m_config->retry_backoff_factor;
        if (m_current_retry_backoff_ms > MEMTIER_BACKOFF_CAP_MS) m_current_retry_backoff_ms = MEMTIER_BACKOFF_CAP_MS;
    }

    return true;
}

void shard_connection::replay_request(request *req)
{
    if (!req || !req->m_serialized || !m_bev) return;
    struct evbuffer *out = bufferevent_get_output(m_bev);
    evbuffer_add(out, req->m_serialized, req->m_serialized_len);
    gettimeofday(&req->m_sent_time, NULL);
    req->m_retries++;
    // Back in the pipeline: ownership returns to the normal flow.
    req->m_claimed_by_retry = false;
    push_req(req);
}

void shard_connection::handle_retry_drain_event()
{
    if (!m_retry_queue || m_retry_queue->empty()) return;
    // Only drain if the connection is actually usable.
    if (m_connection_state != conn_connected || !m_bev) {
        // Will retry once we reconnect (handled by drain_replay_queue_after_reconnect).
        return;
    }
    while (!m_retry_queue->empty()) {
        request *req = m_retry_queue->front();
        m_retry_queue->pop();
        replay_request(req);
    }
}

void shard_connection::drain_replay_queue_after_reconnect()
{
    if (!m_replay_queue) return;
    while (!m_replay_queue->empty()) {
        request *req = m_replay_queue->front();
        m_replay_queue->pop();
        // Each replay counts toward max_retries.
        if (m_config->max_retries >= 0 && (int) req->m_retries >= m_config->max_retries) {
            struct timeval now;
            gettimeofday(&now, NULL);
            global_failed_keys_logger().log_failure(now, "REPLAY", req->m_key, req->m_key_len, "connection-dropped",
                                                    req->m_retries);
            delete req;
            continue;
        }
        if (!req->m_serialized || req->m_serialized_len == 0) {
            // Can't replay — capture failed earlier. Drop with a log line.
            struct timeval now;
            gettimeofday(&now, NULL);
            global_failed_keys_logger().log_failure(now, "REPLAY", req->m_key, req->m_key_len,
                                                    "no-captured-bytes-for-replay", req->m_retries);
            delete req;
            continue;
        }
        replay_request(req);
    }
}

bool shard_connection::is_conn_setup_done()
{
    return m_authentication == setup_done && m_db_selection == setup_done && m_cluster_slots == setup_done &&
           m_hello == setup_done && m_readonly_state == setup_done;
}

bool shard_connection::peer_client_has_any_setup_in_progress() const
{
    // Walk every shard_connection on the same client. A peer "is mid-setup"
    // if it is either climbing the setup ladder (conn_in_progress) OR
    // already TCP-connected but not yet ready for reads (HELLO/CLUSTER
    // SLOTS/READONLY pending). Dead/disconnected peers don't count — they
    // either gave up too, or they're racing us toward their own terminal
    // exit. Reconnect-pending peers (conn_disconnected but with
    // m_reconnect_attempts > 0 and m_reconnecting true) also count because
    // their timer is armed and they may yet rejoin the setup ladder.
    if (m_conns_manager == NULL) return false;
    const std::vector<shard_connection *> &peers = m_conns_manager->get_connections();
    for (size_t i = 0; i < peers.size(); i++) {
        const shard_connection *peer = peers[i];
        if (peer == NULL || peer == this) continue;
        const enum connection_state st = peer->m_connection_state;
        if (st == conn_in_progress) {
            return true;
        }
        if (st == conn_connected && !peer->is_ready_for_reads()) {
            return true;
        }
        if (st == conn_disconnected && peer->m_reconnecting) {
            // Peer has a reconnect timer armed; treat as in-flight.
            return true;
        }
    }
    return false;
}

void shard_connection::send_conn_setup_commands(struct timeval timestamp)
{
    if (m_authentication == setup_none) {
        benchmark_debug_log("sending authentication command.\n");
        m_protocol->authenticate(m_config->authenticate);
        push_req(new request(rt_auth, 0, &timestamp, 0));
        m_authentication = setup_sent;
    }

    if (m_db_selection == setup_none) {
        benchmark_debug_log("sending db selection command.\n");
        m_protocol->select_db(m_config->select_db);
        push_req(new request(rt_select_db, 0, &timestamp, 0));
        m_db_selection = setup_sent;
    }

    if (m_hello == setup_none) {
        benchmark_debug_log("sending HELLO command.\n");
        m_protocol->configure_protocol(m_config->protocol);
        push_req(new request(rt_hello, 0, &timestamp, 0));
        m_hello = setup_sent;
    }

    // READONLY: replica connections only (cluster mode). Sent after AUTH/SELECT/
    // HELLO so any earlier failure is surfaced first, and before CLUSTER SLOTS
    // so the slot-discovery command itself is allowed (the server otherwise
    // rejects user-visible traffic on a replica connection that hasn't opted
    // into reads).
    if (m_readonly_state == setup_none) {
        benchmark_debug_log("sending READONLY command (replica connection).\n");
        // Use bufferevent_write rather than going through protocol::write_command_readonly
        // which adds to the bufferevent's output evbuffer directly. For a connection whose
        // FD was attached to the bufferevent via bufferevent_socket_new + a subsequent
        // bufferevent_socket_connect (the path A1 uses for replicas discovered through
        // CLUSTER SLOTS), the evbuffer notify callback does not re-arm EPOLLOUT on the
        // first user-level send, so the bytes sit in the output buffer forever and the
        // connect-callback handle_event path has no chance to flush them. bufferevent_write
        // routes through bufferevent_trigger_nolock_ which forces the EPOLLOUT registration.
        // Primaries don't hit this because all setup states default to setup_done, so
        // send_conn_setup_commands is a no-op for them.
        static const char READONLY_CMD[] = "*1\r\n$8\r\nREADONLY\r\n";
        bufferevent_write(m_bev, READONLY_CMD, sizeof(READONLY_CMD) - 1);
        push_req(new request(rt_readonly, 0, &timestamp, 0));
        m_readonly_state = setup_sent;
    }

    if (m_cluster_slots == setup_none) {
        benchmark_debug_log("sending cluster slots command.\n");

        // in case we send CLUSTER SLOTS command, we need to keep the response to parse it
        m_protocol->set_keep_value(true);
        m_protocol->write_command_cluster_slots();
        push_req(new request(rt_cluster_slots, 0, &timestamp, 0));
        m_cluster_slots = setup_sent;
    }
}

// Called by cluster_client::handle_cluster_slots when a live CLUSTER SLOTS
// refresh promotes an already-connected primary to a replica (role flip).
// Re-arms the READONLY ladder and sends the wire command immediately so the
// connection becomes eligible for reads without a full reconnect.
void shard_connection::rearm_readonly()
{
    // Only cluster-mode Redis replica connections need READONLY.
    if (m_role != role_replica || !m_config->cluster_mode || !is_redis_protocol(m_config->protocol)) return;
    // Skip if the connection is not yet fully up; connect() will arm the
    // ladder through the normal path.
    if (m_connection_state != conn_connected) return;
    // Skip if READONLY was already sent or acknowledged (e.g. a second refresh
    // that sees the same replica twice).
    if (m_readonly_state != setup_done) return;

    benchmark_debug_log("rearm_readonly: re-sending READONLY on live role-flip connection.\n");

    // Re-arm the ladder so is_conn_setup_done() returns false and
    // fill_pipeline holds new user traffic until the ACK arrives.
    m_readonly_state = setup_none;

    struct timeval now;
    gettimeofday(&now, NULL);

    // Under --read-preference=secondary, primary connections get paused via
    // bufferevent_disable(EV_READ | EV_WRITE) by fill_pipeline once they have
    // no work. On a role flip primary->replica, bufferevent_write below would
    // force EPOLLOUT but EV_READ would stay disabled; the +OK reply would
    // land in the kernel buffer with no readcb to consume it, m_readonly_state
    // would stay at setup_sent, is_conn_setup_done() would never return true,
    // and the connection would deadlock for reads. Re-enable I/O and clear
    // the paused flag, matching the BEV_EVENT_CONNECTED handler and
    // schedule_fill paths.
    if (m_bev_paused) {
        bufferevent_enable(m_bev, EV_READ | EV_WRITE);
        m_bev_paused = false;
    }

    // Send the READONLY bytes via bufferevent_write (same path as the normal
    // ladder in send_conn_setup_commands) to force EPOLLOUT registration.
    static const char READONLY_CMD[] = "*1\r\n$8\r\nREADONLY\r\n";
    bufferevent_write(m_bev, READONLY_CMD, sizeof(READONLY_CMD) - 1);
    push_req(new request(rt_readonly, 0, &now, 0));
    m_readonly_state = setup_sent;
}

void shard_connection::process_response(void)
{
    int ret;
    bool responses_handled = false;

    struct timeval now;
    gettimeofday(&now, NULL);

    while ((ret = m_protocol->parse_response()) > 0) {
        bool error = false;
        protocol_response *r = m_protocol->get_response();

        request *req = pop_req();
        switch (req->m_type) {
        case rt_auth:
            if (r->is_error()) {
                benchmark_error_log("error: authentication failed [%s]\n", r->get_status());
                {
                    // Forward the server-side status to the connection-stage
                    // supervisor so --connection-stage-timeout has actionable
                    // context to surface (e.g. "called without any password
                    // configured for the default user.").
                    char buf[256];
                    snprintf(buf, sizeof(buf), "AUTH failed: %s", r->get_status() ? r->get_status() : "");
                    report_connection_stage_failure(buf);
                }
                error = true;
            } else {
                m_authentication = setup_done;
                benchmark_debug_log("authentication successful.\n");
            }
            break;
        case rt_select_db:
            if (strcmp(r->get_status(), "+OK") != 0) {
                benchmark_error_log("database selection failed.\n");
                {
                    char buf[256];
                    snprintf(buf, sizeof(buf), "SELECT failed: %s", r->get_status() ? r->get_status() : "");
                    report_connection_stage_failure(buf);
                }
                error = true;
            } else {
                benchmark_debug_log("database selection successful.\n");
                m_db_selection = setup_done;
            }
            break;
        case rt_cluster_slots:
            if (r->get_mbulk_value() == NULL || r->get_mbulk_value()->mbulks_elements.size() == 0) {
                benchmark_error_log("cluster slot failed.\n");
                report_connection_stage_failure("CLUSTER SLOTS failed (server returned empty or non-cluster reply; "
                                                "is the server actually cluster-enabled?)");
                error = true;
            } else {
                // Parse the reply. handle_cluster_slots returns true only when
                // at least one valid shard parsed and the new topology was
                // committed; on a malformed / all-skipped reply it returns
                // false and the prior topology (empty on bootstrap) is left
                // in place.
                //
                // When the build is rejected we MUST NOT advance
                // m_cluster_slots to setup_done. Doing so would let the worker
                // enter steady-state routing with an empty topology
                // (m_shard_groups empty on bootstrap), and every slot lookup
                // would return UINT_MAX. Leave m_cluster_slots at its prior
                // value (setup_sent during the bootstrap ladder) so subsequent
                // protocol-error / reconnect / retry paths can re-arm. The
                // write-side hold_pipeline spin guard added in the previous
                // commit still prevents fill_pipeline from busy-spinning if
                // the worker somehow reaches steady-state with an empty
                // topology via a different code path.
                bool committed = m_conns_manager->handle_cluster_slots(r);
                m_protocol->set_keep_value(false);

                if (committed) {
                    m_cluster_slots = setup_done;
                    benchmark_debug_log("cluster slot command successful\n");
                } else {
                    benchmark_debug_log("cluster slot reply rejected; leaving m_cluster_slots unchanged\n");
                    // Surface the rejection to the connection-stage supervisor
                    // so the failure-streak detector arms regardless of whether
                    // the peer half-closes after the malformed reply. The fuzz
                    // fixture relied on BEV_EVENT_EOF to trigger reconnect, but
                    // a real server that keeps the socket open after every
                    // shard's mbulk fails validation would otherwise stall the
                    // worker in fill_pipeline's setup branch until
                    // --connection-stage-timeout fired via run-elapsed (default
                    // 30s), or hang indefinitely with
                    // --connection-stage-timeout=0. Mirrors how AUTH / SELECT /
                    // HELLO / READONLY rejections are already surfaced above.
                    report_connection_stage_failure("CLUSTER SLOTS reply rejected (every shard malformed)");
                }
            }
            break;
        case rt_hello:
            if (r->is_error()) {
                benchmark_error_log("error: HELLO failed [%s]\n", r->get_status());
                {
                    char buf[256];
                    snprintf(buf, sizeof(buf), "HELLO failed: %s", r->get_status() ? r->get_status() : "");
                    report_connection_stage_failure(buf);
                }
                error = true;
            } else {
                m_hello = setup_done;
                benchmark_debug_log("HELLO successful.\n");
            }
            break;
        case rt_readonly:
            if (r->is_error()) {
                benchmark_error_log("error: READONLY failed [%s]\n", r->get_status());
                {
                    char buf[256];
                    snprintf(buf, sizeof(buf), "READONLY failed: %s", r->get_status() ? r->get_status() : "");
                    report_connection_stage_failure(buf);
                }
                // READONLY recovery policy:
                //
                // * --reconnect-on-error ON: schedule a backoff-armed
                //   reconnect so the setup state machine re-arms from
                //   scratch. attempt_reconnect() tears down the
                //   bufferevent + state and reschedules connect() on the
                //   standard --max-reconnect-attempts / backoff ladder.
                //
                // * --reconnect-on-error OFF (default): bare disconnect()
                //   (idle the replica connection) and rely on the next
                //   CLUSTER SLOTS refresh (driven by any peer) to revive it.
                //   Unconditionally calling attempt_reconnect() here would
                //   fall through to the supervisor-trip arm and call
                //   event_base_loopbreak(), so a single transient
                //   -NOREPLICATION would kill the entire worker thread. The
                //   peer-wake loop below covers both branches so the
                //   bootstrap "no live replica yet" gate releases.
                if (m_config->reconnect_on_error) {
                    attempt_reconnect("READONLY error");
                } else {
                    // Mirror attempt_reconnect's stat bump so the OFF path
                    // is observable: without this, --reconnect-on-error=off
                    // READONLY errors would silently idle the replica with
                    // no signal in the connection-error counter.
                    struct timeval err_now;
                    gettimeofday(&err_now, NULL);
                    client *c = static_cast<client *>(m_conns_manager);
                    c->get_stats()->update_connection_error(&err_now);
                    disconnect();
                }
                // Wake peer connections (the primary may be parked in the
                // "no live replica yet" gate); mirror the success arm so a
                // stalled READONLY does not deadlock the bootstrap. In the
                // disconnect-only path this is the only mechanism by which
                // the failed replica gets re-evaluated — a peer's next
                // CLUSTER SLOTS refresh will rebuild topology and may
                // re-bring this replica online.
                if (m_role == role_replica) {
                    for (size_t i = 0; i < m_conns_manager->get_connections().size(); i++) {
                        shard_connection *peer = m_conns_manager->get_connections()[i];
                        if (peer != this && peer->get_connection_state() == conn_connected) {
                            peer->schedule_fill();
                        }
                    }
                }
                error = true;
            } else {
                m_readonly_state = setup_done;
                benchmark_debug_log("READONLY successful.\n");
                // Wake any peer connection that may have been holding for
                // a live replica. The bootstrap primary conn will be sitting
                // in hold_pipeline's "no live replica yet" gate; nudge it.
                if (m_role == role_replica) {
                    for (size_t i = 0; i < m_conns_manager->get_connections().size(); i++) {
                        shard_connection *peer = m_conns_manager->get_connections()[i];
                        if (peer != this && peer->get_connection_state() == conn_connected) {
                            peer->schedule_fill();
                        }
                    }
                }
            }
            break;
        default:
            benchmark_debug_log("server %s: handled response (first line): %s, %d hits, %d misses\n", get_readable_id(),
                                r->get_status(), r->get_hits(), req->m_keys - r->get_hits());

            m_conns_manager->handle_response(m_id, now, req, r);
            m_conns_manager->inc_reqs_processed();
            // First *successful* post-setup response observed: tell the
            // supervisor we reached steady state. Idempotent.
            //
            // We deliberately do NOT count error responses as steady state
            // — a -ERR can fire on the very first request (e.g. the
            // 'invalid bulk length' loop from #426 #8) and then close the
            // connection, dropping the worker into an infinite reconnect
            // loop that the supervisor would otherwise be disarmed against.
            if (!r->is_error()) {
                report_connection_stage_success();
                // Steady state reached: reset the per-connection reconnect
                // attempts counter now (not at BEV_EVENT_CONNECTED). The
                // READONLY-error reconnect storm completes TCP before the
                // -NOREPLICATION lands, so resetting on connect would
                // defeat --max-reconnect-attempts; resetting here ties the
                // counter to setup-ladder lifetime instead. Idempotent —
                // subsequent successful responses just re-assign 0.
                if (m_reconnect_attempts > 0) {
                    benchmark_debug_log(
                        "Setup ladder + first response succeeded after %u reconnection attempts; resetting counter.\n",
                        m_reconnect_attempts);
                }
                m_reconnect_attempts = 0;
            } else {
                // Forward the error so a later abort can attribute it.
                char buf[256];
                snprintf(buf, sizeof(buf), "server error: %s", r->get_status() ? r->get_status() : "");
                report_connection_stage_failure(buf);
            }
            responses_handled = true;
            break;
        }
        // The retry path may have claimed ownership of req for resend; in that
        // case the retry queue / replay path is responsible for freeing it.
        if (!req->m_claimed_by_retry) {
            delete req;
        }
        if (error) {
            return;
        }
    }

    if (ret == -1) {
        benchmark_error_log("error: response parsing failed.\n");
        // A parser failure during the initial probe (e.g. talking memcache_text
        // to a Redis server, or memcache_binary to redis) puts the worker
        // into an unrecoverable spin: the bytes never align, the response
        // never completes, and we never call inc_reqs_processed(). Report it
        // as a connection-stage failure so the supervisor bounds the spin.
        // Once steady state is reached this is a no-op.
        report_connection_stage_failure("response parsing failed (protocol mismatch?)");
    }

    if (m_config->reconnect_interval > 0 && responses_handled) {
        if ((m_config->requests != m_conns_manager->get_reqs_processed()) &&
            ((m_conns_manager->get_reqs_processed() % m_config->reconnect_interval) == 0)) {
            assert(m_pipeline->size() == 0);
            benchmark_debug_log("reconnecting, m_reqs_processed = %u\n", m_conns_manager->get_reqs_processed());

            // client manage connection & disconnection of shard
            m_conns_manager->disconnect();
            ret = m_conns_manager->connect();
            if (ret != 0) {
                benchmark_error_log("failed to reconnect.\n");
                exit(1);
            }
            return;
        }
    }

    fill_pipeline();

    if (m_conns_manager->finished()) {
        m_conns_manager->set_end_time();
    }
}

void shard_connection::process_first_request()
{
    m_conns_manager->set_start_time();
    fill_pipeline();
}

void shard_connection::fill_pipeline(void)
{
    struct timeval now;
    gettimeofday(&now, NULL);

    // Re-enable I/O in case a prior idle period disabled the bufferevent
    // (e.g. a --transaction non-pin connection that was held by hold_pipeline).
    // Gate on m_bev_paused so this lock-taking libevent call only fires when
    // we actually need to undo a prior pause -- fill_pipeline runs per
    // response received, so an unconditional enable here showed up as a ~4%
    // ops/sec regression in the canonical SET/GET micro-bench.
    if (m_bev != NULL && m_bev_paused && get_connection_state() == conn_connected) {
        bufferevent_enable(m_bev, EV_READ | EV_WRITE);
        m_bev_paused = false;
    }

    while (!m_conns_manager->finished() && m_pipeline->size() < m_config->pipeline) {
        if (!is_conn_setup_done()) {
            send_conn_setup_commands(now);
            return;
        }

        // don't exceed requests
        if (m_conns_manager->hold_pipeline(m_id)) {
            break;
        }

        // Hold new work while the retry queue is at its hard cap. The drain
        // timer will reschedule fill_pipeline via the event loop once it makes
        // progress.
        if (retry_queue_full()) {
            break;
        }

        // that's enough, we reached the rate limit
        if (m_config->request_rate && m_request_per_cur_interval == 0) {
            // return and skip on update events
            return;
        }

        // client manage requests logic
        m_conns_manager->create_request(now, m_id);
    }

    // Check if done: no pending responses and output buffer empty
    if (m_bev != NULL) {
        if ((m_pending_resp == 0) && (evbuffer_get_length(bufferevent_get_output(m_bev)) == 0)) {
            benchmark_debug_log("%s Done, no requests to send no response to wait for\n", get_readable_id());

            if (m_conns_manager->finished() && m_conns_manager->all_connections_idle()) {
                m_conns_manager->set_end_time();
                m_conns_manager->disconnect_all();
            } else if (!m_config->request_rate) {
                bufferevent_disable(m_bev, EV_WRITE | EV_READ);
                m_bev_paused = true;
            }
        }
    }
}

void shard_connection::handle_event(short events)
{
    // connect() returning to us?  normally we expect EV_WRITE, but for UNIX domain
    // sockets we workaround since connect() returned immediately, but we don't want
    // to do any I/O from the client::connect() call...

    if ((get_connection_state() == conn_in_progress) && (events & BEV_EVENT_CONNECTED)) {
        m_connection_state = conn_connected;
        bufferevent_enable(m_bev, EV_READ | EV_WRITE);
        m_bev_paused = false;

#ifdef USE_TLS
        // Log the negotiated TLS version and cipher exactly once for the whole
        // run (not per connection/thread/shard) on the first completed handshake.
        // All connections share one SSL_CTX and hit the same server config, so
        // one line is representative. std::call_once makes this thread-safe
        // across the per-thread event loops.
        if (m_config->openssl_ctx != NULL && m_bev != NULL) {
            static std::once_flag tls_info_logged;
            std::call_once(tls_info_logged, [this]() {
                SSL *ssl = bufferevent_openssl_get_ssl(m_bev);
                if (ssl != NULL) {
                    // SSL_get_version/SSL_get_cipher return stable static strings;
                    // safe to stash on the (shared) config for the JSON output.
                    m_config->tls_negotiated_version = SSL_get_version(ssl);
                    m_config->tls_negotiated_cipher = SSL_get_cipher(ssl);
                    fprintf(stderr, "TLS connection established: protocol %s, cipher %s\n",
                            m_config->tls_negotiated_version, m_config->tls_negotiated_cipher);
                }
            });
        }
#endif

        // Cancel connection timeout timer on successful connection
        if (m_connection_timeout_timer != NULL) {
            event_free(m_connection_timeout_timer);
            m_connection_timeout_timer = NULL;
        }

        // Log progress on successful TCP connect, but do NOT reset the
        // attempts counter here. The READONLY-error reconnect storm always
        // completes TCP before -NOREPLICATION arrives — resetting on every
        // BEV_EVENT_CONNECTED defeated --max-reconnect-attempts and produced
        // an unbounded spin loop. The counter is now reset only when the
        // setup ladder reaches steady state (see the rt_get/rt_arbitrary
        // success branch in handle_response). Clear m_reconnecting and the
        // backoff delay here so the next backoff schedule starts fresh if
        // the setup ladder fails again before steady state.
        if (m_reconnect_attempts > 0) {
            benchmark_debug_log("TCP connection established (attempt %u); awaiting setup-ladder success.\n",
                                m_reconnect_attempts);
        }
        m_current_backoff_delay = 1.0;
        m_reconnecting = false;

        /* Set timer for request rate (create or recreate after reconnect) */
        if (m_config->request_rate && m_event_timer == NULL) {
            struct timeval interval = {0, (int) m_config->request_interval_microsecond};
            m_request_per_cur_interval = m_config->request_per_interval;
            m_event_timer = event_new(m_event_base, -1, EV_PERSIST, cluster_client_timer_handler, (void *) this);
            event_add(m_event_timer, &interval);
        }

        // After (re)connect: replay any in-flight requests that survived the
        // disconnect. This must happen *before* fill_pipeline() so the old
        // requests get back on the wire first; otherwise pipeline ordering
        // would shuffle replayed work behind fresh work.
        if (m_config->retry_on_error && m_replay_queue && !m_replay_queue->empty()) {
            drain_replay_queue_after_reconnect();
        }

        if (!m_conns_manager->get_reqs_processed()) {
            process_first_request();
        } else {
            benchmark_debug_log("reconnection complete, proceeding with test\n");
            fill_pipeline();
        }

        return;
    }

    if (events & BEV_EVENT_ERROR) {
        bool ssl_error = false;
#ifdef USE_TLS
        unsigned long sslerr;
        while ((sslerr = bufferevent_get_openssl_error(m_bev))) {
            ssl_error = true;
            benchmark_error_log("TLS connection error: %s\n", ERR_reason_error_string(sslerr));
        }
#endif
        if (!ssl_error && errno) {
            benchmark_error_log("Connection error: %s\n", strerror(errno));
        }

        attempt_reconnect("Connection error");
        return;
    }

    if (events & BEV_EVENT_EOF) {
        benchmark_error_log("connection dropped.\n");
        attempt_reconnect("Connection dropped");
        return;
    }
}

void shard_connection::handle_timer_event(void)
{
    m_request_per_cur_interval = m_config->request_per_interval;

    if (m_conns_manager->finished() && m_conns_manager->all_connections_idle()) {
        m_conns_manager->set_end_time();
        m_conns_manager->disconnect_all();
        return;
    }

    fill_pipeline();
}

void shard_connection::schedule_fill(void)
{
    if (m_connection_state != conn_connected || m_bev == NULL) {
        return;
    }
    // Re-enable I/O in case fill_pipeline silenced this connection while it
    // was blocked by transaction-mode hold_pipeline.
    bufferevent_enable(m_bev, EV_READ | EV_WRITE);
    m_bev_paused = false;
    if (m_deferred_fill_timer == NULL) {
        m_deferred_fill_timer = event_new(m_event_base, -1, 0, deferred_fill_pipeline_cb, this);
        if (m_deferred_fill_timer == NULL) {
            return;
        }
    }
    if (!evtimer_pending(m_deferred_fill_timer, NULL)) {
        struct timeval zero = {0, 0};
        event_add(m_deferred_fill_timer, &zero);
    }
}

void shard_connection::attempt_reconnect(const char *error_context)
{
    // Update connection error statistics
    struct timeval now;
    gettimeofday(&now, NULL);
    client *c = static_cast<client *>(m_conns_manager);
    c->get_stats()->update_connection_error(&now);

    // Attempt reconnection if enabled and not already reconnecting
    if (m_config->reconnect_on_error && !m_reconnecting &&
        (m_config->max_reconnect_attempts == 0 || m_reconnect_attempts < m_config->max_reconnect_attempts)) {
        disconnect();
        // Snapshot the backoff delay before the multiplication so we can
        // restore it cleanly if event_new fails below.
        const double prev_backoff_delay = m_current_backoff_delay;
        m_reconnect_attempts++;
        if (m_config->reconnect_backoff_factor > 0.0) {
            m_current_backoff_delay *= m_config->reconnect_backoff_factor;
            if (m_current_backoff_delay > MEMTIER_BACKOFF_CAP_SEC) m_current_backoff_delay = MEMTIER_BACKOFF_CAP_SEC;
        }

        if (m_config->max_reconnect_attempts == 0) {
            benchmark_error_log("%s, attempting reconnection %u (unlimited) in %.2f seconds...\n", error_context,
                                m_reconnect_attempts, m_current_backoff_delay);
        } else {
            benchmark_error_log("%s, attempting reconnection %u/%u in %.2f seconds...\n", error_context,
                                m_reconnect_attempts, m_config->max_reconnect_attempts, m_current_backoff_delay);
        }

        // Schedule reconnection attempt
        struct timeval delay;
        delay.tv_sec = (long) m_current_backoff_delay;
        delay.tv_usec = (long) ((m_current_backoff_delay - delay.tv_sec) * 1000000);

        m_reconnect_timer = event_new(m_event_base, -1, 0, cluster_client_reconnect_timer_handler, (void *) this);
        if (m_reconnect_timer == NULL) {
            benchmark_error_log("error: event_new failed in attempt_reconnect\n");
            // Roll back the bookkeeping so the next attempt does not
            // count an attempt that never armed, and leave m_reconnecting
            // false so a subsequent error can re-enter this path. Break
            // out of the event loop — there is no way to recover this
            // connection without a working libevent timer.
            if (m_reconnect_attempts > 0) m_reconnect_attempts--;
            m_current_backoff_delay = prev_backoff_delay;
            event_base_loopbreak(m_event_base);
            return;
        }
        event_add(m_reconnect_timer, &delay);
        m_reconnecting = true;
    } else if (m_config->reconnect_on_error && m_reconnecting) {
        // A reconnect is already pending for this connection. The event loop
        // can deliver multiple connection-error callbacks per dead connection
        // (e.g. an EOF followed by stray TLS read errors during a node
        // failover storm), and every one of them lands here while the first
        // one's reconnect timer is still pending.
        //
        // Treat the duplicates as no-ops — the in-flight reconnect will run
        // and decide what to do. Tearing the thread down here would mean a
        // single dead connection always kills the whole benchmark thread
        // under realistic failover conditions, regardless of how high
        // --max-reconnect-attempts is set.
        return;
    } else {
        // Under --read-preference != primary, ONE dead replica should not
        // tear down the whole worker thread while sibling connections are
        // still mid-setup-ladder on the surviving primary/replicas. The
        // RESP3 setup ladder (HELLO 3 ack + READONLY ack) is heavy enough
        // that a Connection-refused storm from the shut-down replica races
        // ahead of those acks; if we loopbreak here, the peers never finish
        // setup and zero GETs ever fly. Fall back to the cluster_client
        // routing path which already gates on is_ready_for_reads() and
        // handles dead-conn cases via the bootstrap-warming hold.
        if (m_config->read_preference != rp_primary && peer_client_has_any_setup_in_progress()) {
            benchmark_error_log("Maximum reconnection attempts (%u) exceeded for %s on conn %u; peers still mid-setup, "
                                "leaving connection dead and continuing.\n",
                                m_config->max_reconnect_attempts, error_context, m_id);
            disconnect();
            return;
        }
        benchmark_error_log("Maximum reconnection attempts (%u) exceeded for %s, triggering thread restart.\n",
                            m_config->max_reconnect_attempts, error_context);
        disconnect();
        // Break the event loop to trigger thread restart
        event_base_loopbreak(m_event_base);
    }
}

void shard_connection::handle_reconnect_timer_event()
{
    // Clean up the timer
    if (m_reconnect_timer != NULL) {
        event_free(m_reconnect_timer);
        m_reconnect_timer = NULL;
    }

    m_reconnecting = false;

    // Attempt to reconnect
    int ret = m_conns_manager->connect();
    if (ret != 0) {
        // Reconnection failed, try again if we haven't exceeded max attempts
        if (m_config->max_reconnect_attempts == 0 || m_reconnect_attempts < m_config->max_reconnect_attempts) {
            const double prev_backoff_delay = m_current_backoff_delay;
            m_reconnect_attempts++;
            if (m_config->reconnect_backoff_factor > 0.0) {
                m_current_backoff_delay *= m_config->reconnect_backoff_factor;
                if (m_current_backoff_delay > MEMTIER_BACKOFF_CAP_SEC)
                    m_current_backoff_delay = MEMTIER_BACKOFF_CAP_SEC;
            }

            benchmark_error_log("Reconnection attempt %u failed, retrying in %.2f seconds...\n", m_reconnect_attempts,
                                m_current_backoff_delay);

            // Schedule next reconnection attempt
            struct timeval delay;
            delay.tv_sec = (long) m_current_backoff_delay;
            delay.tv_usec = (long) ((m_current_backoff_delay - delay.tv_sec) * 1000000);

            m_reconnect_timer = event_new(m_event_base, -1, 0, cluster_client_reconnect_timer_handler, (void *) this);
            if (m_reconnect_timer == NULL) {
                benchmark_error_log("error: event_new failed in handle_reconnect_timer_event retry path\n");
                // Roll back the bookkeeping so the next attempt does not
                // count an attempt that never armed, and leave m_reconnecting
                // false so a subsequent error can re-enter this path. Break
                // out of the event loop -- there is no way to recover this
                // connection without a working libevent timer.
                if (m_reconnect_attempts > 0) m_reconnect_attempts--;
                m_current_backoff_delay = prev_backoff_delay;
                event_base_loopbreak(m_event_base);
                return;
            }
            event_add(m_reconnect_timer, &delay);
            m_reconnecting = true;
        } else {
            // Mirror attempt_reconnect's terminal-else policy: under
            // --read-preference != primary, do not tear down the worker
            // thread if any sibling connection is still climbing its setup
            // ladder. The cluster_client routing path will skip this dead
            // connection via is_ready_for_reads() and route around it.
            if (m_config->read_preference != rp_primary && peer_client_has_any_setup_in_progress()) {
                benchmark_error_log("Maximum reconnection attempts (%u) exceeded on conn %u; peers still mid-setup, "
                                    "leaving connection dead and continuing.\n",
                                    m_config->max_reconnect_attempts, m_id);
                // disconnect() already ran when the prior attempt failed;
                // m_connection_state is conn_disconnected and routing will
                // skip it. Just return — no loopbreak, no further retries.
                return;
            }
            benchmark_error_log("Maximum reconnection attempts (%u) exceeded, triggering thread restart.\n",
                                m_config->max_reconnect_attempts);
            // Break the event loop to trigger thread restart. No state reset
            // needed here: the thread is torn down and the shard_connection
            // object destroyed before any field could be read again.
            event_base_loopbreak(m_event_base);
        }
    } else {
        benchmark_error_log("Reconnection successful after %u attempts.\n", m_reconnect_attempts);
        // State reset (m_reconnect_attempts, m_current_backoff_delay) is now
        // done by report_connection_stage_success() once the full setup ladder
        // completes and the first user-level response arrives. Resetting here
        // would mask --max-reconnect-attempts under a READONLY-error storm
        // where TCP-connect succeeds but READONLY repeatedly fails.
    }
}

void shard_connection::handle_connection_timeout_event()
{
    // Clean up the timer
    if (m_connection_timeout_timer != NULL) {
        event_free(m_connection_timeout_timer);
        m_connection_timeout_timer = NULL;
    }

    benchmark_error_log("Connection timeout after %u seconds.\n", m_config->connection_timeout);
    attempt_reconnect("Connection timeout");
}

void shard_connection::send_wait_command(struct timeval *sent_time, unsigned int num_slaves, unsigned int timeout)
{
    int cmd_size = 0;

    benchmark_debug_log("WAIT num_slaves=%u timeout=%u\n", num_slaves, timeout);

    size_t before = (m_bev && m_config->retry_on_error) ? evbuffer_get_length(bufferevent_get_output(m_bev)) : 0;
    cmd_size = m_protocol->write_command_wait(num_slaves, timeout);
    request *req = new request(rt_wait, cmd_size, sent_time, 0);
    if (m_config->retry_on_error) capture_serialized_bytes(before, req);
    push_req(req);
}

void shard_connection::send_set_command(struct timeval *sent_time, const char *key, int key_len, const char *value,
                                        int value_len, int expiry, unsigned int offset)
{
    int cmd_size = 0;

    benchmark_debug_log("server %s: SET key=[%.*s] value_len=%u expiry=%u\n", get_readable_id(), key_len, key,
                        value_len, expiry);

    size_t before = (m_bev && m_config->retry_on_error) ? evbuffer_get_length(bufferevent_get_output(m_bev)) : 0;
    cmd_size = m_protocol->write_command_set(key, key_len, value, value_len, expiry, offset);

    request *req = new request(rt_set, cmd_size, sent_time, 1);
    if (m_config->retry_on_error) {
        capture_serialized_bytes(before, req);
        if (key_len > 0) req->set_key_for_log(key, (unsigned int) key_len);
    }
    push_req(req);
}


void shard_connection::send_get_command(struct timeval *sent_time, const char *key, int key_len, unsigned int offset)
{
    int cmd_size = 0;

    benchmark_debug_log("server %s: GET key=[%.*s]\n", get_readable_id(), key_len, key);
    size_t before = (m_bev && m_config->retry_on_error) ? evbuffer_get_length(bufferevent_get_output(m_bev)) : 0;
    cmd_size = m_protocol->write_command_get(key, key_len, offset);

    request *req = new request(rt_get, cmd_size, sent_time, 1);
    if (m_config->retry_on_error) {
        capture_serialized_bytes(before, req);
        if (key_len > 0) req->set_key_for_log(key, (unsigned int) key_len);
    }
    push_req(req);
}

void shard_connection::send_mget_command(struct timeval *sent_time, const keylist *key_list)
{
    int cmd_size = 0;

    const char *first_key, *last_key;
    unsigned int first_key_len, last_key_len;
    first_key = key_list->get_key(0, &first_key_len);
    last_key = key_list->get_key(key_list->get_keys_count() - 1, &last_key_len);

    benchmark_debug_log("MGET %d keys [%.*s] .. [%.*s]\n", key_list->get_keys_count(), first_key_len, first_key,
                        last_key_len, last_key);

    size_t before = (m_bev && m_config->retry_on_error) ? evbuffer_get_length(bufferevent_get_output(m_bev)) : 0;
    cmd_size = m_protocol->write_command_multi_get(key_list);
    request *req = new request(rt_get, cmd_size, sent_time, key_list->get_keys_count());
    if (m_config->retry_on_error) {
        capture_serialized_bytes(before, req);
        // Log the first key only — MGET keys are listed in the same record.
        if (first_key_len > 0) req->set_key_for_log(first_key, first_key_len);
    }
    push_req(req);
}

void shard_connection::send_verify_get_command(struct timeval *sent_time, const char *key, int key_len,
                                               const char *value, int value_len, unsigned int offset)
{
    int cmd_size = 0;

    benchmark_debug_log("Verify GET key=[%.*s] value_len=%u\n", key_len, key, value_len);

    size_t before = (m_bev && m_config->retry_on_error) ? evbuffer_get_length(bufferevent_get_output(m_bev)) : 0;
    cmd_size = m_protocol->write_command_get(key, key_len, offset);
    verify_request *vr = new verify_request(rt_get, cmd_size, sent_time, 1, key, key_len, value, value_len);
    // verify_request constructor already stored the key via base set_key_for_log.
    if (m_config->retry_on_error) capture_serialized_bytes(before, vr);
    push_req(vr);
}

/*
 * arbitrary command:
 *
 * we send the arbitrary command in several iterations, where on each iteration
 * different type of argument can be sent (const/randomized).
 *
 * since we do it on several iterations, we call to arbitrary_command_end() to mark that
 * all the command sent
 */

int shard_connection::send_arbitrary_command(const command_arg *arg)
{
    int cmd_size = 0;

    cmd_size = m_protocol->write_arbitrary_command(arg);

    return cmd_size;
}

int shard_connection::send_arbitrary_command(const command_arg *arg, const char *val, int val_len)
{
    int cmd_size = 0;

    if (arg->type == key_type) {
        benchmark_debug_log("key=[%.*s]\n", val_len, val);
    } else if (arg->type == scan_cursor_type) {
        benchmark_debug_log("scan_cursor=[%.*s]\n", val_len, val);
    } else {
        benchmark_debug_log("value_len=%u\n", val_len);
    }

    cmd_size = m_protocol->write_arbitrary_command(val, val_len);

    return cmd_size;
}

void shard_connection::send_arbitrary_command_end(size_t command_index, struct timeval *sent_time, int cmd_size)
{
    // Look up the source command's metadata so the reply handler can route
    // per-key miss accounting. Safe to be NULL (we tolerate it downstream).
    const arbitrary_command *meta = NULL;
    if (m_config && m_config->arbitrary_commands && command_index < m_config->arbitrary_commands->size()) {
        meta = &m_config->arbitrary_commands->at(command_index);
    }
    arbitrary_request *req = new arbitrary_request(command_index, rt_arbitrary, cmd_size, sent_time, meta);
    if (m_config->retry_on_error && m_bev && cmd_size > 0) {
        // Bytes were written across N calls to send_arbitrary_command; use
        // cmd_size to recover the start offset.
        size_t after = evbuffer_get_length(bufferevent_get_output(m_bev));
        if (after >= (size_t) cmd_size) {
            capture_serialized_bytes(after - (size_t) cmd_size, req);
        }
    }
    push_req(req);
}
