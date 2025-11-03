/*
 * Copyright (C) 2011-2017 Redis Labs Ltd.
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
#include "event2/bufferevent.h"

#ifdef USE_TLS
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

request::request(request_type type, unsigned int size, struct timeval* sent_time, unsigned int keys)
        : m_type(type), m_size(size), m_keys(keys)
{
    if (sent_time != NULL)
        m_sent_time = *sent_time;
    else {
        gettimeofday(&m_sent_time, NULL);
    }
}

arbitrary_request::arbitrary_request(size_t request_index, request_type type,
                                     unsigned int size, struct timeval* sent_time) :
        request(type, size, sent_time, 1),
        index(request_index) {
}

verify_request::verify_request(request_type type,
                               unsigned int size,
                               struct timeval* sent_time,
                               unsigned int keys,
                               const char *key,
                               unsigned int key_len,
                               const char *value,
                               unsigned int value_len) :
        request(type, size, sent_time, keys),
        m_key(NULL), m_key_len(0),
        m_value(NULL), m_value_len(0)
{
    m_key_len = key_len;
    m_key = (char *)malloc(key_len);
    memcpy(m_key, key, m_key_len);

    m_value_len = value_len;
    m_value = (char *)malloc(value_len);
    memcpy(m_value, value, m_value_len);
}

verify_request::~verify_request(void)
{
    if (m_key != NULL) {
        free((void *) m_key);
        m_key = NULL;
    }
    if (m_value != NULL) {
        free((void *) m_value);
        m_value = NULL;
    }
}

shard_connection::shard_connection(unsigned int id, connections_manager* conns_man, benchmark_config* config,
                                   struct event_base* event_base, abstract_protocol* abs_protocol) :
        m_address(NULL), m_port(NULL), m_unix_sockaddr(NULL),
        m_bev(NULL), m_event_timer(NULL), m_request_per_cur_interval(0), m_pending_resp(0), m_connection_state(conn_disconnected),
        m_hello(setup_done), m_authentication(setup_done), m_db_selection(setup_done), m_cluster_slots(setup_done) {
    m_id = id;
    m_conns_manager = conns_man;
    m_config = config;
    m_event_base = event_base;

    if (m_config->unix_socket) {
        m_unix_sockaddr = (struct sockaddr_un *) malloc(sizeof(struct sockaddr_un));
        assert(m_unix_sockaddr != NULL);

        m_unix_sockaddr->sun_family = AF_UNIX;
        strncpy(m_unix_sockaddr->sun_path, m_config->unix_socket, sizeof(m_unix_sockaddr->sun_path)-1);
        m_unix_sockaddr->sun_path[sizeof(m_unix_sockaddr->sun_path)-1] = '\0';
    }

    m_protocol = abs_protocol->clone();
    assert(m_protocol != NULL);

    m_pipeline = new std::queue<request *>;
    assert(m_pipeline != NULL);

}

shard_connection::~shard_connection() {
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

    if (m_protocol != NULL) {
        delete m_protocol;
        m_protocol = NULL;
    }

    if (m_pipeline != NULL) {
        delete m_pipeline;
        m_pipeline = NULL;
    }

}

void shard_connection::setup_event(int sockfd) {
    if (m_bev) {
        bufferevent_free(m_bev);
    }

#ifdef USE_TLS
    if (m_config->openssl_ctx) {
        SSL *ctx = SSL_new(m_config->openssl_ctx);
        assert(ctx != NULL);

        if (m_config->tls_sni) {
          SSL_set_tlsext_host_name(ctx, m_config->tls_sni);
        }

        m_bev = bufferevent_openssl_socket_new(m_event_base,
                sockfd, ctx, BUFFEREVENT_SSL_CONNECTING, BEV_OPT_CLOSE_ON_FREE);
    } else {
#endif
        m_bev = bufferevent_socket_new(m_event_base, sockfd, BEV_OPT_CLOSE_ON_FREE);
#ifdef USE_TLS
    }
#endif

    assert(m_bev != NULL);
    bufferevent_setcb(m_bev, cluster_client_read_handler,
        NULL, cluster_client_event_handler, (void *)this);
    m_protocol->set_buffers(bufferevent_get_input(m_bev), bufferevent_get_output(m_bev));
}

int shard_connection::setup_socket(struct connect_info* addr) {
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
    if ((flags = fcntl(sockfd, F_GETFL, 0)) < 0 ||
        fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        close(sockfd);
        return -1;
    }

    return sockfd;
}

int shard_connection::connect(struct connect_info* addr) {
    // set required setup commands
    m_authentication = m_config->authenticate ? setup_none : setup_done;
    m_db_selection = m_config->select_db ? setup_none : setup_done;
    m_hello = (m_config->protocol == PROTOCOL_RESP2 || m_config->protocol == PROTOCOL_RESP3) ? setup_none : setup_done;

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

    if (bufferevent_socket_connect(m_bev,
                  m_unix_sockaddr ? (struct sockaddr *) m_unix_sockaddr : addr->ci_addr,
                  m_unix_sockaddr ? sizeof(struct sockaddr_un) : addr->ci_addrlen) == -1) {
        disconnect();

        benchmark_error_log("connect failed, error = %s\n", strerror(errno));
        return -1;
    }

    return 0;
}

void shard_connection::disconnect() {
    if (m_bev) {
        bufferevent_free(m_bev);
        m_bev = NULL;
    }

    if (m_event_timer != NULL) {
        event_free(m_event_timer);
        m_event_timer = NULL;
    }

    // empty pipeline
    while (m_pending_resp)
        delete pop_req();

    m_connection_state = conn_disconnected;

    // by default no need to send any setup request
    m_authentication = setup_done;
    m_db_selection = setup_done;
    m_cluster_slots = setup_done;
    m_hello = setup_done;
}

void shard_connection::set_address_port(const char* address, const char* port) {
    if (m_address != NULL) {
        free(m_address);
    }
    m_address = strdup(address);

    if (m_port != NULL) {
        free(m_port);
    }
    m_port = strdup(port);
}

void shard_connection::set_readable_id() {
    if (m_unix_sockaddr != NULL) {
        m_readable_id.assign(m_config->unix_socket);
    } else {
        m_readable_id.assign(m_address);
        m_readable_id.append(":");
        m_readable_id.append(m_port);
    }
}

const char* shard_connection::get_readable_id() {
    return m_readable_id.c_str();
}

request* shard_connection::pop_req() {
    request* req = m_pipeline->front();
    m_pipeline->pop();

    m_pending_resp--;
    assert(m_pending_resp >= 0);

    return req;
}

void shard_connection::push_req(request* req) {
    m_pipeline->push(req);
    m_pending_resp++;
    if (m_config->request_rate) {
        assert(m_request_per_cur_interval > 0);
        m_request_per_cur_interval--;
    }
}

bool shard_connection::is_conn_setup_done() {
    return m_authentication == setup_done &&
           m_db_selection == setup_done &&
           m_cluster_slots == setup_done &&
           m_hello == setup_done;
}

void shard_connection::send_conn_setup_commands(struct timeval timestamp) {
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

    if (m_cluster_slots == setup_none) {
        benchmark_debug_log("sending cluster slots command.\n");

        // in case we send CLUSTER SLOTS command, we need to keep the response to parse it
        m_protocol->set_keep_value(true);
        m_protocol->write_command_cluster_slots();
        push_req(new request(rt_cluster_slots, 0, &timestamp, 0));
        m_cluster_slots = setup_sent;
    }
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

        request* req = pop_req();
        switch (req->m_type)
        {
        case rt_auth:
            if (r->is_error()) {
                benchmark_error_log("error: authentication failed [%s]\n", r->get_status());
                error = true;
            } else {
                m_authentication = setup_done;
                benchmark_debug_log("authentication successful.\n");
            }
            break;
        case rt_select_db:
            if (strcmp(r->get_status(), "+OK") != 0) {
                benchmark_error_log("database selection failed.\n");
                error = true;
            } else {
                benchmark_debug_log("database selection successful.\n");
                m_db_selection = setup_done;
            }
            break;
        case rt_cluster_slots:
            if (r->get_mbulk_value() == NULL || r->get_mbulk_value()->mbulks_elements.size() == 0) {
                benchmark_error_log("cluster slot failed.\n");
                error = true;
            } else {
                // parse response
                m_conns_manager->handle_cluster_slots(r);
                m_protocol->set_keep_value(false);

                m_cluster_slots = setup_done;
                benchmark_debug_log("cluster slot command successful\n");
            }
            break;
        case rt_hello:
            if (r->is_error()) {
                benchmark_error_log("error: HELLO failed [%s]\n", r->get_status());
                error = true;
            } else {
                m_hello = setup_done;
                benchmark_debug_log("HELLO successful.\n");
            }
            break;
        default:
            benchmark_debug_log("server %s: handled response (first line): %s, %d hits, %d misses\n",
                                get_readable_id(),
                                r->get_status(),
                                r->get_hits(),
                                req->m_keys - r->get_hits());

            m_conns_manager->handle_response(m_id, now, req, r);
            m_conns_manager->inc_reqs_processed();
            responses_handled = true;
            break;
        }
        delete req;
        if (error) {
            return;
        }
    }

    if (ret == -1) {
        benchmark_error_log("error: response parsing failed.\n");
    }

    if (m_config->reconnect_interval > 0 && responses_handled) {
        if ((m_config->requests != m_conns_manager->get_reqs_processed()) && ((m_conns_manager->get_reqs_processed() % m_config->reconnect_interval) == 0)) {
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

void shard_connection::process_first_request() {
    m_conns_manager->set_start_time();
    fill_pipeline();
}

void shard_connection::fill_pipeline(void)
{
    struct timeval now;
    gettimeofday(&now, NULL);

    while (!m_conns_manager->finished() && m_pipeline->size() < m_config->pipeline) {
        if (!is_conn_setup_done()) {
            send_conn_setup_commands(now);
            return;
        }

        // don't exceed requests
        if (m_conns_manager->hold_pipeline(m_id)) {
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

    // Flush any partial bulk when test finishes
    if (m_conns_manager->finished()) {
        flush_bulk();
    }

    // update events
    if (m_bev != NULL) {
        // no pending response (nothing to read) and output buffer empty (nothing to write)
        if ((m_pending_resp == 0) && (evbuffer_get_length(bufferevent_get_output(m_bev)) == 0)) {
            benchmark_debug_log("%s Done, no requests to send no response to wait for\n", get_readable_id());
            bufferevent_disable(m_bev, EV_WRITE|EV_READ);
            if (m_config->request_rate) {
                event_del(m_event_timer);
            }
        }
    }
}

void shard_connection::flush_bulk(void)
{
    // If there's a partial bulk in progress, finalize it
    if (m_protocol->has_partial_bulk()) {
        m_protocol->finalize_partial_bulk();
    }
}

void shard_connection::handle_event(short events)
{
    // connect() returning to us?  normally we expect EV_WRITE, but for UNIX domain
    // sockets we workaround since connect() returned immediately, but we don't want
    // to do any I/O from the client::connect() call...

    if ((get_connection_state() == conn_in_progress) && (events & BEV_EVENT_CONNECTED)) {
        m_connection_state = conn_connected;
        bufferevent_enable(m_bev, EV_READ|EV_WRITE);

        if (!m_conns_manager->get_reqs_processed()) {
            /* Set timer for request rate */
            if (m_config->request_rate) {
                struct timeval interval = { 0, (int)m_config->request_interval_microsecond };
                m_request_per_cur_interval = m_config->request_per_interval;
                m_event_timer = event_new(m_event_base, -1, EV_PERSIST, cluster_client_timer_handler, (void *)this);
                event_add(m_event_timer, &interval);
            }

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
            benchmark_error_log("TLS connection error: %s\n",
                    ERR_reason_error_string(sslerr));
        }
#endif
        if (!ssl_error && errno) {
            benchmark_error_log("Connection error: %s\n", strerror(errno));
        }
        disconnect();

        return;
    }

    if (events & BEV_EVENT_EOF) {
        benchmark_error_log("connection dropped.\n");
        disconnect();

        return;
    }
}

void shard_connection::handle_timer_event() {
    m_request_per_cur_interval = m_config->request_per_interval;
    fill_pipeline();
}

void shard_connection::send_wait_command(struct timeval* sent_time,
                                         unsigned int num_slaves, unsigned int timeout) {
    int cmd_size = 0;

    benchmark_debug_log("WAIT num_slaves=%u timeout=%u\n", num_slaves, timeout);

    cmd_size = m_protocol->write_command_wait(num_slaves, timeout);
    push_req(new request(rt_wait, cmd_size, sent_time, 0));
}

void shard_connection::send_set_command(struct timeval* sent_time, const char *key, int key_len,
                                        const char *value, int value_len, int expiry, unsigned int offset) {
    int cmd_size = 0;

    benchmark_debug_log("server %s: SET key=[%.*s] value_len=%u expiry=%u\n",
                        get_readable_id(), key_len, key, value_len, expiry);

    cmd_size = m_protocol->write_command_set(key, key_len, value, value_len,
                                             expiry, offset);

    push_req(new request(rt_set, cmd_size, sent_time, 1));
}


void shard_connection::send_get_command(struct timeval* sent_time,
                                        const char *key, int key_len, unsigned int offset) {
    int cmd_size = 0;

    benchmark_debug_log("server %s: GET key=[%.*s]\n", get_readable_id(), key_len, key);
    cmd_size = m_protocol->write_command_get(key, key_len, offset);

    push_req(new request(rt_get, cmd_size, sent_time, 1));
}

void shard_connection::send_mget_command(struct timeval* sent_time, const keylist* key_list) {
    int cmd_size = 0;

    const char *first_key, *last_key;
    unsigned int first_key_len, last_key_len;
    first_key = key_list->get_key(0, &first_key_len);
    last_key = key_list->get_key(key_list->get_keys_count()-1, &last_key_len);

    benchmark_debug_log("MGET %d keys [%.*s] .. [%.*s]\n",
                        key_list->get_keys_count(), first_key_len, first_key, last_key_len, last_key);

    cmd_size = m_protocol->write_command_multi_get(key_list);
    push_req(new request(rt_get, cmd_size, sent_time, key_list->get_keys_count()));
}

void shard_connection::send_verify_get_command(struct timeval* sent_time, const char *key, int key_len,
                                               const char *value, int value_len, unsigned int offset) {
    int cmd_size = 0;

    benchmark_debug_log("Verify GET key=[%.*s] value_len=%u\n", key_len, key, value_len);

    cmd_size = m_protocol->write_command_get(key, key_len, offset);
    push_req(new verify_request(rt_get, cmd_size, sent_time, 1, key, key_len, value, value_len));
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

int shard_connection::send_arbitrary_command(const command_arg *arg) {
    int cmd_size = 0;

    cmd_size = m_protocol->write_arbitrary_command(arg);

    return cmd_size;
}

int shard_connection::send_arbitrary_command(const command_arg *arg, const char *val, int val_len) {
    int cmd_size = 0;

    if (arg->type == key_type) {
        benchmark_debug_log("key=[%.*s]\n",  val_len, val);
    } else {
        benchmark_debug_log("value_len=%u\n",  val_len);
    }

    cmd_size = m_protocol->write_arbitrary_command(val, val_len);

    return cmd_size;
}

void shard_connection::send_arbitrary_command_end(size_t command_index, struct timeval* sent_time, int cmd_size) {
    push_req(new arbitrary_request(command_index, rt_arbitrary, cmd_size, sent_time));
}
