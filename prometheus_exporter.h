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

/*
 * prometheus_exporter.{h,cpp} is the IMPURE layer of the Prometheus exporter
 * (PLAN.md v5 sections 3.1, 3.2, 3.3, 3.7, 3.8). It owns the libevent
 * event_base + evhttp listener thread, the snapshot mutex + published POD, the
 * exporter-lifetime HDR histogram, the render-TTL cache and the renders
 * counter. It supplies PACKAGE_VERSION / MEMTIER_GIT_SHA1 (via config.h /
 * version.h, included in the .cpp only) to the pure prom::text_renderer.
 *
 * The whole class is compiled only when HAVE_EVHTTP is defined (Decisions #1).
 * With the flag off this header declares nothing and the .cpp is empty, so the
 * translation unit still compiles and links cleanly under --disable-prometheus.
 *
 * Thread ownership (PLAN.md section 3.8): the producer API
 * (accumulator/tick_histogram/publish/publish_run_start/_end) is MAIN-THREAD
 * ONLY; the listener thread only ever reads published copies under m_snap_mutex
 * and owns the render state lock-free (evhttp serializes request callbacks).
 */

#ifndef _PROMETHEUS_EXPORTER_H
#define _PROMETHEUS_EXPORTER_H

#ifdef HAVE_EVHTTP

#include <pthread.h>
#include <stdint.h>
#include <time.h>
#include <atomic>
#include <string>
#include <vector>
#include <utility>

#include <event2/util.h> // evutil_socket_t

#include "prometheus_metrics.h"

// libevent struct tags are forward-declared so this header pulls in no
// libevent implementation headers beyond <event2/util.h> (PLAN.md section 3.1).
struct event_base;
struct event;
struct evhttp;
struct evhttp_bound_socket;
struct evhttp_request;
struct hdr_histogram;

class prometheus_exporter
{
public:
    // Exporter configuration, filled by main() from cfg (PLAN.md section 3.1).
    struct options
    {
        std::string bind_addr;                                        // bracket-free numeric literal (CLI strips [])
        int port;                                                     // 0 = ephemeral
        std::vector<std::pair<std::string, std::string> > run_labels; // raw; renderer escapes
        std::vector<double> latency_buckets;                          // seconds; empty => default 26-bound list
        uint32_t run_count;                                           // --run-count
        int test_time;      // --test-time seconds (0 when running by --requests)
        uint32_t n_threads; // --threads

        options() : port(0), run_count(0), test_time(0), n_threads(0) {}
    };

    // ctor: no libevent calls, cannot fail; NULL-inits all libevent members and
    // publishes the zero snapshot (run_id = 0, run_count from options).
    explicit prometheus_exporter(const options &opts);

    // dtor: idempotent stop_and_join(); NULL-safe hdr_close of the three HDRs.
    ~prometheus_exporter();

    // start(): main thread, no workers yet. Creates the base + evhttp, applies
    // the HTTP hardening, binds the socket, announces the listen URL, allocates
    // the HDRs + the stop-event, and launches the listener thread. Each failure
    // path prints one stderr line, frees via free_libevent_state(), returns
    // false. Bind failure is fatal & loud (Decisions #8). Returns true on
    // success.
    bool start();

    // stop_and_join(): main thread, idempotent. Wakes the listener via the
    // stop-event protocol (Decisions #20), joins it, then frees libevent state.
    void stop_and_join();

    // The resolved listen port (post-bind; 0 before start() or on failure).
    int bound_port() const { return m_bound_port; }

    // ----- producer API (MAIN THREAD ONLY) -----------------------------------
    // accumulator(): the monotonic counter accumulator (PLAN.md section 3.5).
    monotonic_accumulator &accumulator() { return m_accum; }

    // tick_histogram(): main-thread-only scratch HDR for the gated 1 Hz
    // aggregate (PLAN.md section 3.7). Reset+refilled each tick by the producer.
    hdr_histogram *tick_histogram() { return m_tick_hist; }

    // publish(snap, inst): copy the snapshot under m_snap_mutex, stamp seq +
    // CLOCK_MONOTONIC published_at, and fold `inst` (if non-empty) into the
    // exporter-lifetime cumulative HDR. `inst` may be NULL.
    void publish(const metrics_snapshot &snap, const struct hdr_histogram *inst);

    // publish_run_start/_end: memset a snapshot, set ids, fill carried counter
    // totals from the accumulator, and publish() it (gauges/rates zero; run-end
    // sets progress_pct = 100.0). PLAN.md section 3.7.
    void publish_run_start(uint32_t run_id, uint32_t run_count);
    void publish_run_end(uint32_t run_id, uint32_t run_count);

private:
    // Non-copyable.
    prometheus_exporter(const prometheus_exporter &);
    prometheus_exporter &operator=(const prometheus_exporter &);

    // listener-thread entry + bodies (PLAN.md section 3.2 stop-event protocol).
    static void *thread_entry(void *arg);
    void run_loop();
    static void on_stop_event(evutil_socket_t fd, short what, void *arg);

    // request handlers (HTTP thread only).
    static void metrics_cb(struct evhttp_request *req, void *arg);
    static void gencb(struct evhttp_request *req, void *arg);
    static void on_request_complete(struct evhttp_request *req, void *arg);
    void handle_metrics(struct evhttp_request *req);
    void handle_not_found(struct evhttp_request *req);
    bool begin_request(struct evhttp_request *req); // false => 503 sent, caller returns
    const std::string &render_body(const struct timespec &now);

    // free_libevent_state(): the ONLY place libevent members are freed
    // (Decisions #49). NULL-guarded, NULLs each member, idempotent. Main thread,
    // only while no listener can run.
    void free_libevent_state();

    // ----- immutable after start() (both threads, no lock) -------------------
    options m_opts;
    double m_start_time_seconds; // unix start time, stamped at ctor
    int m_bound_port;            // resolved listen port (0 until bind)
    int m_max_inflight;          // in-flight cap (default 8; MEMTIER_PROM_MAX_INFLIGHT seam)

    // ----- libevent state: created on main pre-thread; listener-only while the
    //       loop may run; freed+NULLed only in free_libevent_state() -----------
    struct event_base *m_base;
    struct evhttp *m_http;
    struct evhttp_bound_socket *m_bound; // owned by m_http (a handle; never freed itself)
    struct event *m_stop_ev;             // manual-activation stop event (never event_add'ed)

    pthread_t m_thread;
    bool m_thread_started; // main-thread-only
    bool m_joined;         // main-thread-only

    // ----- cross-thread atomics ----------------------------------------------
    // m_stop: main release-store, listener acquire-load post-dispatch.
    // m_inflight: HTTP-thread-only in practice, atomic for TSan triviality.
    std::atomic<bool> m_stop;
    std::atomic<int> m_inflight;

    // ----- published snapshot, guarded by m_snap_mutex -----------------------
    pthread_mutex_t m_snap_mutex;
    metrics_snapshot m_snap;
    uint64_t m_publish_seq;
    struct hdr_histogram *m_lifetime_hist; // exporter-lifetime cumulative; never reset

    // ----- producer-side, main-thread-only -----------------------------------
    monotonic_accumulator m_accum;
    struct hdr_histogram *m_tick_hist; // gated 1 Hz aggregate scratch

    // ----- HTTP-thread-only render state (lock-free) -------------------------
    prom::text_renderer m_renderer;
    struct hdr_histogram *m_render_hist; // per-render copy target of m_lifetime_hist
    std::string m_cached_body;
    struct timespec m_last_render;
    bool m_have_cached;
    uint64_t m_renders;
};

#endif // HAVE_EVHTTP

#endif // _PROMETHEUS_EXPORTER_H
