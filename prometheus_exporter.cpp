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

#include "prometheus_exporter.h"

#ifdef HAVE_EVHTTP

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <event2/event.h>
#include <event2/http.h>
#include <event2/buffer.h>
#include <event2/keyvalq_struct.h>

#include "version.h" // PACKAGE_VERSION / MEMTIER_GIT_SHA1 (impure layer only)
#include "run_stats_types.h"
#include "deps/hdr_histogram/hdr_histogram.h"

// PACKAGE_VERSION lives in config.h; MEMTIER_GIT_SHA1 in version.h.
#ifndef PACKAGE_VERSION
#define PACKAGE_VERSION "unknown"
#endif
#ifndef MEMTIER_GIT_SHA1
#define MEMTIER_GIT_SHA1 "unknown"
#endif

// PROM_HDR_* (prometheus_metrics.h) mirror the run_stats SEC HDR params; assert
// the mirror is exact so the exporter histograms are layout-identical to the
// benchmark's instantaneous histograms (Decisions #22). The named macros are
// referenced verbatim — never re-derived.
static_assert(PROM_HDR_MIN_VALUE == LATENCY_HDR_MIN_VALUE, "PROM_HDR_MIN_VALUE drifted from LATENCY_HDR_MIN_VALUE");
static_assert(PROM_HDR_MAX_VALUE == LATENCY_HDR_SEC_MAX_VALUE,
              "PROM_HDR_MAX_VALUE drifted from LATENCY_HDR_SEC_MAX_VALUE");
static_assert(PROM_HDR_SIGDIGITS == LATENCY_HDR_SEC_SIGDIGTS,
              "PROM_HDR_SIGDIGITS drifted from LATENCY_HDR_SEC_SIGDIGTS");

namespace
{

const int kDefaultMaxInflight = 8;
const int kIdleTimeoutSec = 5;
const int kMaxHeadersSize = 4096;

// CLOCK_MONOTONIC delta in seconds.
double ts_delta_sec(const struct timespec &a, const struct timespec &b)
{
    return (double) (b.tv_sec - a.tv_sec) + (double) (b.tv_nsec - a.tv_nsec) / 1e9;
}

// MEMTIER_PROM_MAX_INFLIGHT test seam (Decisions #47): fully-consumed token in
// [0,8] accepted; unset/empty/junk/out-of-range => default 8. Resolved exactly
// once in start() before pthread_create; immutable afterwards.
int resolve_max_inflight()
{
    const char *env = getenv("MEMTIER_PROM_MAX_INFLIGHT");
    if (env == NULL || env[0] == '\0') return kDefaultMaxInflight;
    errno = 0;
    char *end = NULL;
    long v = strtol(env, &end, 10);
    if (errno != 0 || end == env || *end != '\0' || v < 0 || v > kDefaultMaxInflight) return kDefaultMaxInflight;
    return (int) v;
}

// Translate text_renderer's run-label storage type (it uses its own typedef).
std::vector<prom::text_renderer::label> to_renderer_labels(const std::vector<std::pair<std::string, std::string> > &in)
{
    std::vector<prom::text_renderer::label> out;
    out.reserve(in.size());
    for (size_t i = 0; i < in.size(); i++)
        out.push_back(prom::text_renderer::label(in[i].first, in[i].second));
    return out;
}

} // namespace

prometheus_exporter::prometheus_exporter(const options &opts) :
        m_opts(opts),
        m_start_time_seconds(0.0),
        m_bound_port(0),
        m_max_inflight(kDefaultMaxInflight),
        m_base(NULL),
        m_http(NULL),
        m_bound(NULL),
        m_stop_ev(NULL),
        m_thread((pthread_t) 0),
        m_thread_started(false),
        m_joined(false),
        m_stop(false),
        m_inflight(0),
        m_publish_seq(0),
        m_lifetime_hist(NULL),
        m_tick_hist(NULL),
        m_renderer(PACKAGE_VERSION, MEMTIER_GIT_SHA1, to_renderer_labels(opts.run_labels), opts.latency_buckets),
        m_render_hist(NULL),
        m_have_cached(false),
        m_renders(0)
{
    pthread_mutex_init(&m_snap_mutex, NULL);

    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    m_start_time_seconds = (double) now.tv_sec + (double) now.tv_nsec / 1e9;

    m_last_render.tv_sec = 0;
    m_last_render.tv_nsec = 0;

    m_accum.init(opts.n_threads);

    // Publish the idle zero snapshot: run_id 0, counters 0, and 0 active threads
    // — no worker threads exist until the first run's publish_run_start(), so the
    // gauge must read 0 here (matches run-end behavior; avoids advertising live
    // threads during --verify-only or pre-run startup). run_count is config.
    metrics_snapshot s;
    memset(&s, 0, sizeof(s));
    s.run_count = opts.run_count;
    s.active_threads = 0;
    publish(s, NULL);
}

prometheus_exporter::~prometheus_exporter()
{
    stop_and_join(); // idempotent; frees libevent state if start() ran.

    // hdr_close is NULL-safe (deps/hdr_histogram/hdr_histogram.c:416-422); the
    // three HDRs are ctor-NULL'd and only allocated in start(), so a never- or
    // partially-started exporter closes cleanly here.
    if (m_lifetime_hist != NULL) hdr_close(m_lifetime_hist);
    if (m_tick_hist != NULL) hdr_close(m_tick_hist);
    if (m_render_hist != NULL) hdr_close(m_render_hist);

    pthread_mutex_destroy(&m_snap_mutex);
}

// ---------------------------------------------------------------------------
// start() / teardown — PLAN.md section 3.2
// ---------------------------------------------------------------------------

bool prometheus_exporter::start()
{
    // 1. event_base_new(). evthread_use_pthreads() must already have run in
    //    main() (memtier_benchmark.cpp) before this so event_active() from the
    //    main thread is thread-safe against the listener loop.
    m_base = event_base_new();
    if (m_base == NULL) {
        fprintf(stderr, "error: prometheus exporter: event_base_new failed\n");
        free_libevent_state();
        return false;
    }

    // 2. evhttp_new().
    m_http = evhttp_new(m_base);
    if (m_http == NULL) {
        fprintf(stderr, "error: prometheus exporter: evhttp_new failed\n");
        free_libevent_state();
        return false;
    }

    // 3. Hardening (PLAN.md section 3.2 step 3). GET-only => POST/HEAD auto-501.
    evhttp_set_allowed_methods(m_http, EVHTTP_REQ_GET);
    evhttp_set_max_headers_size(m_http, kMaxHeadersSize);
    evhttp_set_max_body_size(m_http, 0);
    evhttp_set_timeout(m_http, kIdleTimeoutSec);
    evhttp_set_cb(m_http, "/metrics", metrics_cb, this);
    evhttp_set_gencb(m_http, gencb, this); // unknown path -> 404, zero URI echo

    // Resolve the in-flight cap exactly once, before the listener thread starts.
    m_max_inflight = resolve_max_inflight();

    // 4. bind.
    m_bound = evhttp_bind_socket_with_handle(m_http, m_opts.bind_addr.c_str(), (ev_uint16_t) m_opts.port);
    if (m_bound == NULL) {
        fprintf(stderr, "error: prometheus exporter: failed to bind %s:%d: %s\n", m_opts.bind_addr.c_str(), m_opts.port,
                strerror(errno));
        free_libevent_state();
        return false;
    }

    // 5. Port resolution & announce, IPv6-complete (Decisions #26).
    {
        evutil_socket_t fd = evhttp_bound_socket_get_fd(m_bound);
        struct sockaddr_storage ss;
        socklen_t slen = sizeof(ss);
        memset(&ss, 0, sizeof(ss));
        if (getsockname(fd, (struct sockaddr *) &ss, &slen) != 0) {
            fprintf(stderr, "error: prometheus exporter: getsockname failed: %s\n", strerror(errno));
            free_libevent_state();
            return false;
        }
        if (ss.ss_family == AF_INET6)
            m_bound_port = (int) ntohs(((struct sockaddr_in6 *) &ss)->sin6_port);
        else
            m_bound_port = (int) ntohs(((struct sockaddr_in *) &ss)->sin_port);
        // NB: the listen-URL announce is deferred to the end of start() (after
        // pthread_create succeeds) so a failure in a later step never leaves
        // stdout advertising a /metrics URL that was never brought up (Bugbot).
    }

    // 6. HDR allocation — :3058 triplet verbatim (Decisions #22). The
    //    whitespace-normalized form is byte-pinned by the gate (S10).
    if (hdr_init(LATENCY_HDR_MIN_VALUE, LATENCY_HDR_SEC_MAX_VALUE, LATENCY_HDR_SEC_SIGDIGTS, &m_lifetime_hist) != 0) {
        fprintf(stderr, "error: prometheus exporter: hdr_init (lifetime) failed\n");
        free_libevent_state();
        return false;
    }
    if (hdr_init(LATENCY_HDR_MIN_VALUE, LATENCY_HDR_SEC_MAX_VALUE, LATENCY_HDR_SEC_SIGDIGTS, &m_render_hist) != 0) {
        fprintf(stderr, "error: prometheus exporter: hdr_init (render) failed\n");
        free_libevent_state();
        return false;
    }
    if (hdr_init(LATENCY_HDR_MIN_VALUE, LATENCY_HDR_SEC_MAX_VALUE, LATENCY_HDR_SEC_SIGDIGTS, &m_tick_hist) != 0) {
        fprintf(stderr, "error: prometheus exporter: hdr_init (tick) failed\n");
        free_libevent_state();
        return false;
    }

    // Re-publish the zero snapshot so its counter total reflects the lifetime
    // HDR being live; the snapshot itself is unchanged. (No-op functionally;
    // the ctor already published.) Reserve the render buffer once here.
    m_cached_body.reserve(16384);

    // 7. The manual-activation stop event — never event_add'ed (Decisions #20).
    m_stop_ev = event_new(m_base, -1, 0, on_stop_event, this);
    if (m_stop_ev == NULL) {
        fprintf(stderr, "error: prometheus exporter: event_new (stop) failed\n");
        free_libevent_state();
        return false;
    }

    // 8. Launch the listener thread.
    if (pthread_create(&m_thread, NULL, thread_entry, this) != 0) {
        fprintf(stderr, "error: prometheus exporter: pthread_create failed: %s\n", strerror(errno));
        free_libevent_state();
        return false;
    }
    m_thread_started = true;

    // Announce only now that the listener is fully up (bind + HDR + stop event +
    // thread all succeeded). Bracket IPv6 literals (RFC 3986). Exactly one
    // flushed stdout line — the RLTest discovery contract (Decisions #9).
    if (m_opts.bind_addr.find(':') != std::string::npos)
        printf("Prometheus exporter listening on http://[%s]:%d/metrics\n", m_opts.bind_addr.c_str(), m_bound_port);
    else
        printf("Prometheus exporter listening on http://%s:%d/metrics\n", m_opts.bind_addr.c_str(), m_bound_port);
    fflush(stdout);
    return true;
}

void *prometheus_exporter::thread_entry(void *arg)
{
    static_cast<prometheus_exporter *>(arg)->run_loop();
    return NULL;
}

void prometheus_exporter::run_loop() // ENTIRE listener thread body
{
    int rc = event_base_dispatch(m_base);
    if (!m_stop.load(std::memory_order_acquire))
        fprintf(stderr,
                "warning: prometheus exporter event loop exited unexpectedly (rc=%d); "
                "/metrics is no longer served for the rest of the process\n",
                rc);
    // No frees, no parking, no retry: the thread simply exits; pthread_join
    // returns immediately.
}

void prometheus_exporter::on_stop_event(evutil_socket_t, short, void *arg) // ON listener thread
{
    // The loop is running at callback time => the break cannot be lost.
    event_base_loopbreak(static_cast<prometheus_exporter *>(arg)->m_base);
}

void prometheus_exporter::stop_and_join() // main thread
{
    if (m_joined) return; // idempotent
    m_joined = true;
    if (m_thread_started) {
        m_stop.store(true, std::memory_order_release); // strictly before activation
        // event_active inserts m_stop_ev into the base's ACTIVE QUEUE, which —
        // unlike the loopbreak flag — is NOT cleared at event_base_loop entry;
        // any activation (before, during, or racing dispatch entry) is processed
        // on the first iteration, where on_stop_event issues loopbreak from
        // inside the running loop, where it is always honored. A listener that
        // already exited makes the join instant; the stray activation is
        // harmless (base lives until the post-join frees). Thread-safe under
        // evthread_use_pthreads() (called in main() before the base was made).
        event_active(m_stop_ev, EV_TIMEOUT, 1);
        pthread_join(m_thread, NULL);
        m_thread_started = false;
    }
    free_libevent_state();
}

void prometheus_exporter::free_libevent_state() // main thread; only while no listener can run
{
    // NULL discipline (Decisions #49): every free is NULL-guarded and NULLs its
    // member; this is the ONLY place libevent state is freed; on libevent 2.1.x
    // event_free(NULL) and evhttp_free(NULL) SIGSEGV, so the guards are
    // load-bearing. Free order is fixed: the event, then the evhttp (which owns
    // m_bound), then the base last.
    if (m_stop_ev != NULL) {
        event_free(m_stop_ev);
        m_stop_ev = NULL;
    }
    if (m_http != NULL) {
        evhttp_free(m_http);
        m_http = NULL;
        m_bound = NULL; // m_bound is owned by m_http (a handle, never freed itself)
    }
    if (m_base != NULL) {
        event_base_free(m_base);
        m_base = NULL;
    }
}

// ---------------------------------------------------------------------------
// Request handling & render — PLAN.md section 3.3 (HTTP thread only)
// ---------------------------------------------------------------------------

void prometheus_exporter::metrics_cb(struct evhttp_request *req, void *arg)
{
    prometheus_exporter *self = static_cast<prometheus_exporter *>(arg);
    if (!self->begin_request(req)) return; // 503 already sent
    self->handle_metrics(req);
}

void prometheus_exporter::gencb(struct evhttp_request *req, void *arg)
{
    prometheus_exporter *self = static_cast<prometheus_exporter *>(arg);
    if (!self->begin_request(req)) return; // 503 already sent (cap precedes routing, Decisions #34)
    self->handle_not_found(req);
}

void prometheus_exporter::on_request_complete(struct evhttp_request *, void *arg)
{
    prometheus_exporter *self = static_cast<prometheus_exporter *>(arg);
    self->m_inflight.fetch_sub(1, std::memory_order_relaxed);
}

bool prometheus_exporter::begin_request(struct evhttp_request *req)
{
    // In-flight cap (Decisions #7/#47). Cap applies to /metrics and the gencb
    // alike. 503s are NOT counted and get no complete-cb; memory stays bounded
    // by the 5 s idle timeout * tiny bodies.
    if (m_inflight.load(std::memory_order_relaxed) >= m_max_inflight) {
        struct evkeyvalq *hdrs = evhttp_request_get_output_headers(req);
        evhttp_add_header(hdrs, "Connection", "close");
        evhttp_add_header(hdrs, "Content-Type", "text/plain; charset=utf-8");
        struct evbuffer *out = evhttp_request_get_output_buffer(req);
        evbuffer_add(out, "exporter busy\n", 14);
        evhttp_send_reply(req, 503, "Service Unavailable", out);
        return false;
    }
    m_inflight.fetch_add(1, std::memory_order_relaxed);
    evhttp_request_set_on_complete_cb(req, on_request_complete, this);
    return true;
}

void prometheus_exporter::handle_not_found(struct evhttp_request *req)
{
    // Fixed body, zero URI echo (PLAN.md section 6).
    struct evkeyvalq *hdrs = evhttp_request_get_output_headers(req);
    evhttp_add_header(hdrs, "Connection", "close");
    evhttp_add_header(hdrs, "Content-Type", "text/plain; charset=utf-8");
    struct evbuffer *out = evhttp_request_get_output_buffer(req);
    evbuffer_add(out, "not found\n", 10);
    evhttp_send_reply(req, 404, "Not Found", out);
}

void prometheus_exporter::handle_metrics(struct evhttp_request *req)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    const std::string &body = render_body(now);

    struct evkeyvalq *hdrs = evhttp_request_get_output_headers(req);
    evhttp_add_header(hdrs, "Content-Type", prom::CONTENT_TYPE);
    evhttp_add_header(hdrs, "Connection", "close");
    struct evbuffer *out = evhttp_request_get_output_buffer(req);
    evbuffer_add(out, body.data(), body.size());
    evhttp_send_reply(req, 200, "OK", out);
}

const std::string &prometheus_exporter::render_body(const struct timespec &now)
{
    if (m_have_cached && ts_delta_sec(m_last_render, now) < 1.0)
        return m_cached_body; // TTL hit: byte-identical body (F9)

    metrics_snapshot snap;
    pthread_mutex_lock(&m_snap_mutex);
    snap = m_snap; // POD copy
    hdr_reset(m_render_hist);
    hdr_add(m_render_hist, m_lifetime_hist); // bounded counts-array copy
    pthread_mutex_unlock(&m_snap_mutex);

    double age = ts_delta_sec(snap.published_at, now); // computed at render time (F10)
    if (age < 0.0)
        age = 0.0; // 'now' is sampled before the snapshot mutex; a publish()
                   // in that sub-ms window can make published_at > now. Clamp
                   // so the gauge is never transiently negative.
    ++m_renders;   // HTTP-thread-local -> renders_total
    m_renderer.render(m_cached_body, snap, m_render_hist, m_start_time_seconds, age, m_renders);
    m_last_render = now;
    m_have_cached = true;
    return m_cached_body;
}

// ---------------------------------------------------------------------------
// Producer API — PLAN.md section 3.7 (MAIN THREAD ONLY)
// ---------------------------------------------------------------------------

void prometheus_exporter::publish(const metrics_snapshot &snap, const struct hdr_histogram *inst)
{
    pthread_mutex_lock(&m_snap_mutex);
    m_snap = snap;
    m_snap.seq = ++m_publish_seq;
    // test_time is process-immutable config; stamp it here so every snapshot
    // (tick, run-start/_end, ctor zero) carries it without touching fill sites.
    m_snap.test_time = m_opts.test_time;
    clock_gettime(CLOCK_MONOTONIC, &m_snap.published_at);
    if (inst != NULL && m_lifetime_hist != NULL && hdr_total_count(inst) > 0) hdr_add(m_lifetime_hist, inst);
    pthread_mutex_unlock(&m_snap_mutex);
}

void prometheus_exporter::publish_run_start(uint32_t run_id, uint32_t run_count)
{
    metrics_snapshot s;
    memset(&s, 0, sizeof(s));
    s.run_id = run_id;
    s.run_count = run_count;
    s.active_threads = m_opts.n_threads;
    m_accum.fill(s.counters);
    publish(s, NULL);
}

void prometheus_exporter::publish_run_end(uint32_t run_id, uint32_t run_count)
{
    metrics_snapshot s;
    memset(&s, 0, sizeof(s));
    s.run_id = run_id;
    s.run_count = run_count;
    // Worker threads have joined by run-end; report 0 active threads (and thus
    // 0 connections, already memset) so a scrape between runs doesn't claim the
    // configured thread count is still live (Bugbot review, PR #468).
    s.active_threads = 0;
    s.progress_pct = 100.0;
    m_accum.fill(s.counters);
    publish(s, NULL);
}

#endif // HAVE_EVHTTP
