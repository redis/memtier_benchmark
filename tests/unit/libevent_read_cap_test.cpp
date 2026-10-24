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

// One read event on a plaintext socket bufferevent takes 16 KiB on every
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
    size_t last_added;
    bool tracked;
    // Input length after each read event, in order.
    size_t lengths[8];
    int reads;
    bool input_frozen;
    bool eof;
};

static void read_cb(struct bufferevent *bev, void *arg)
{
    reader *r = (reader *) arg;
    struct evbuffer *input = bufferevent_get_input(bev);
    if (r->tracked) read_past_libevent_cap(input, bufferevent_getfd(bev), &r->last_added);
    if (r->reads < 8) r->lengths[r->reads] = evbuffer_get_length(input);
    r->reads++;
    // libevent keeps the input's end frozen outside its own read.
    r->input_frozen = evbuffer_add(input, "x", 1) != 0;
    if (!r->input_frozen) evbuffer_drain(input, 1);
    evbuffer_drain(input, evbuffer_get_length(input));
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
static void run(reader *r, size_t queued, bool close_after, int events)
{
    int fds[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        perror("socketpair");
        failures++;
        return;
    }
    evutil_make_socket_nonblocking(fds[0]);
    static char payload[65536];
    memset(payload, 'v', sizeof(payload));
    if (write(fds[1], payload, queued) != (ssize_t) queued) {
        perror("write");
        failures++;
    }
    if (close_after) shutdown(fds[1], SHUT_WR);

    memset(r, 0, sizeof(*r));
    r->base = event_base_new();
    struct bufferevent *bev = bufferevent_socket_new(r->base, fds[0], BEV_OPT_CLOSE_ON_FREE);
    r->tracked = libevent_caps_reads();
    if (r->tracked) track_libevent_reads(bufferevent_get_input(bev), &r->last_added);
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

    // Data then EOF: all the data in one read event, and the EOF is still
    // reported by libevent's next read.
    run(&r, 6000, true, 2);
    CHECK_EQ(r.reads, 1);
    CHECK_EQ(r.lengths[0], 6000);
    CHECK_EQ(r.eof, true);

    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("libevent_read_cap_test: all checks passed (libevent %s, %s)\n", event_get_version(),
           libevent_caps_reads() ? "reads past the 4 KiB cap" : "uncapped reads");
    return 0;
}
#endif
