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

// One read event on a plaintext socket bufferevent takes at most 16 KiB on every
// libevent series: 2.2 reads that much itself, and memtier reads past 2.1's
// 4 KiB cap (libevent_read_cap.h, issue #535).

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <string.h>

#include "libevent_read_cap.h"

#ifdef _WIN32
int main(void)
{
    printf("libevent_read_cap_test: skipped (Windows keeps libevent's reads)\n");
    return 0;
}
#else

#include <event2/bufferevent.h>
#include <sys/socket.h>
#include <unistd.h>

static int failures = 0;

#define CHECK_EQ(actual, expected)                                                                                     \
    do {                                                                                                               \
        long long a_ = (long long) (actual), e_ = (long long) (expected);                                              \
        if (a_ != e_) {                                                                                                \
            fprintf(stderr, "FAIL [%s:%d] %s: got %lld, expected %lld\n", __FILE__, __LINE__, #actual, a_, e_);        \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

struct reader
{
    struct event_base *base;
    bool read_past_cap;
    // Input length after each read event, in order.
    size_t lengths[8];
    int reads;
    bool input_frozen;
    bool eof;
    // Per read event (the first four): bytes written to the peer before
    // read_past_libevent_cap() runs (data that arrives after libevent's read),
    // bytes of input left unparsed afterwards, and bytes written to the peer
    // once the event is done (for the next event to read).
    size_t inject_before[4];
    size_t keep[4];
    size_t deliver_after[4];
    int peer;
};

static char payload[65536];

static void read_cb(struct bufferevent *bev, void *arg)
{
    reader *r = (reader *) arg;
    struct evbuffer *input = bufferevent_get_input(bev);
    int ev = r->reads;
    if (ev < 4 && r->inject_before[ev] &&
        write(r->peer, payload, r->inject_before[ev]) != (ssize_t) r->inject_before[ev]) {
        perror("write");
        failures++;
    }
    if (r->read_past_cap) read_past_libevent_cap(input, bufferevent_getfd(bev));
    if (r->reads < 8) r->lengths[r->reads] = evbuffer_get_length(input);
    r->reads++;
    // libevent keeps the input's end frozen outside its own read.
    r->input_frozen = evbuffer_add(input, "x", 1) != 0;
    if (!r->input_frozen) evbuffer_drain(input, 1);
    size_t len = evbuffer_get_length(input);
    size_t keep = ev < 4 ? r->keep[ev] : 0;
    evbuffer_drain(input, len > keep ? len - keep : 0);
    if (ev < 4 && r->deliver_after[ev] &&
        write(r->peer, payload, r->deliver_after[ev]) != (ssize_t) r->deliver_after[ev]) {
        perror("write");
        failures++;
    }
    event_base_loopbreak(r->base);
}

static void event_cb(struct bufferevent *, short events, void *arg)
{
    reader *r = (reader *) arg;
    if (events & BEV_EVENT_EOF) r->eof = true;
    event_base_loopbreak(r->base);
}

// Queue `queued` bytes (then EOF if `close_after`) and run the loop until
// `events` read or EOF callbacks have run.
static void run(reader *r, size_t queued, bool close_after, int events, const reader *cfg = NULL)
{
    int fds[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        perror("socketpair");
        failures++;
        return;
    }
    evutil_make_socket_nonblocking(fds[0]);
    // Queue all the data before the first read: raise the buffer sizes (macOS
    // gives Unix sockets 8 KiB by default), and write without blocking so that
    // a short write fails the test instead of hanging it.
    int size = 65536;
    setsockopt(fds[1], SOL_SOCKET, SO_SNDBUF, &size, sizeof(size));
    setsockopt(fds[0], SOL_SOCKET, SO_RCVBUF, &size, sizeof(size));
    evutil_make_socket_nonblocking(fds[1]);
    memset(payload, 'v', sizeof(payload));
    if (write(fds[1], payload, queued) != (ssize_t) queued) {
        perror("write");
        failures++;
    }
    if (close_after) shutdown(fds[1], SHUT_WR);

    memset(r, 0, sizeof(*r));
    if (cfg) {
        memcpy(r->inject_before, cfg->inject_before, sizeof(r->inject_before));
        memcpy(r->keep, cfg->keep, sizeof(r->keep));
        memcpy(r->deliver_after, cfg->deliver_after, sizeof(r->deliver_after));
    }
    r->peer = fds[1];
    r->base = event_base_new();
    struct bufferevent *bev = bufferevent_socket_new(r->base, fds[0], BEV_OPT_CLOSE_ON_FREE);
    r->read_past_cap = libevent_caps_reads();
    bufferevent_setcb(bev, read_cb, NULL, event_cb, r);
    bufferevent_enable(bev, EV_READ);
    for (int i = 0; i < events; i++)
        event_base_dispatch(r->base);
    bufferevent_free(bev);
    event_base_free(r->base);
    close(fds[1]);
}

int main(void)
{
    // A missed read event would hang the loop; fail the test instead.
    alarm(60);
    reader r;

    // More than 16 KiB queued: the first read event takes exactly 16 KiB.
    run(&r, 40000, false, 1);
    CHECK_EQ(r.reads, 1);
    CHECK_EQ(r.lengths[0], libevent_2_2_read_size);
    CHECK_EQ(r.input_frozen, true);

    // Less than 4 KiB: one ordinary read.
    run(&r, 1000, false, 1);
    CHECK_EQ(r.lengths[0], 1000);

    // Exactly 4 KiB: libevent's read is not capped and nothing more is queued.
    run(&r, libevent_read_cap, false, 1);
    CHECK_EQ(r.lengths[0], libevent_read_cap);
    CHECK_EQ(r.input_frozen, true);

    // Exactly one cap, then EOF: nothing more is read, and libevent's next
    // read reports the EOF.
    run(&r, libevent_read_cap, true, 2);
    CHECK_EQ(r.lengths[0], libevent_read_cap);
    CHECK_EQ(r.eof, true);

    // Data then EOF: all the data in one read event, and the EOF is still
    // reported by libevent's next read.
    run(&r, 6000, true, 2);
    CHECK_EQ(r.reads, 1);
    CHECK_EQ(r.lengths[0], 6000);
    CHECK_EQ(r.eof, true);

    // The check is a hint (an input of at least one cap may hold leftover
    // bytes), and these cases pin both of its edges. They need the read cap.
    if (libevent_caps_reads()) {
        reader cfg;

        // A: exactly one cap in the input, and more arrives after libevent's
        // read: it is read on.
        memset(&cfg, 0, sizeof(cfg));
        cfg.inject_before[0] = 100;
        run(&r, 4096, false, 1, &cfg);
        CHECK_EQ(r.lengths[0], 4096 + 100);

        // B: one byte under the cap: nothing is read on; the later bytes are
        // left for the next read event.
        memset(&cfg, 0, sizeof(cfg));
        cfg.inject_before[0] = 100;
        run(&r, libevent_read_cap - 1, false, 2, &cfg);
        CHECK_EQ(r.lengths[0], libevent_read_cap - 1);
        CHECK_EQ(r.lengths[1], 100);

        // C: 3000 bytes left unparsed, then 1500 more arrive: the input holds
        // more than a cap although libevent's read was not capped. Nothing is
        // queued, so nothing more is read.
        memset(&cfg, 0, sizeof(cfg));
        cfg.keep[0] = 3000;
        cfg.deliver_after[0] = 1500;
        run(&r, 3000, false, 2, &cfg);
        CHECK_EQ(r.lengths[0], 3000);
        CHECK_EQ(r.lengths[1], 4500);

        // C2: the same, with 50 bytes arriving after libevent's read: they are read.
        cfg.inject_before[1] = 50;
        run(&r, 3000, false, 2, &cfg);
        CHECK_EQ(r.lengths[1], 4550);

        // D: 100 bytes left unparsed, then 1000 more and 50 after libevent's
        // read: the input is under a cap, so the 50 stay for the next event.
        memset(&cfg, 0, sizeof(cfg));
        cfg.keep[0] = 100;
        cfg.deliver_after[0] = 1000;
        cfg.inject_before[1] = 50;
        run(&r, 100, false, 2, &cfg);
        CHECK_EQ(r.lengths[1], 1100);

        // E: 100 bytes left unparsed, then 40000 queued: libevent reads a cap,
        // memtier at most 12 KiB more, so one event adds 16 KiB of new data.
        memset(&cfg, 0, sizeof(cfg));
        cfg.keep[0] = 100;
        cfg.deliver_after[0] = 40000;
        run(&r, 100, false, 2, &cfg);
        CHECK_EQ(r.lengths[1], 100 + libevent_2_2_read_size);

        // F: a failing FIONREAD (bad descriptor) reads nothing.
        struct evbuffer *buf = evbuffer_new();
        evbuffer_add(buf, payload, 5000);
        read_past_libevent_cap(buf, -1);
        CHECK_EQ(evbuffer_get_length(buf), 5000);
        evbuffer_free(buf);
    }

    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("libevent_read_cap_test: all checks passed (libevent %s, %s)\n", event_get_version(),
           libevent_caps_reads() ? "reads past the 4 KiB cap" : "uncapped reads");
    return 0;
}
#endif
