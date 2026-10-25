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

#ifndef MEMTIER_LIBEVENT_READ_CAP_H
#define MEMTIER_LIBEVENT_READ_CAP_H

// libevent before 2.2 (2.0.11, the oldest configure accepts, to 2.1.13) reads
// at most 4 KiB (EVBUFFER_MAX_READ) from a socket per read event, so
// pipelined replies are read and parsed 4 KiB at a time;
// libevent 2.2 reads up to 16 KiB (issue #535). For a plaintext socket
// bufferevent, read on after a capped read so that a read event takes the
// same amount of data on both series. TLS bufferevents read whole records
// and are left alone. Windows keeps libevent's reads: the Winsock version
// (ioctlsocket(), and IOCP bufferevents) is neither written nor tested.
// This relies on two libevent 2.1 details: a capped read adds exactly
// EVBUFFER_MAX_READ (4096) bytes, and the input's end is frozen outside
// libevent's own read. With a libevent built with a different
// EVBUFFER_MAX_READ, a read adds exactly 4096 bytes only by coincidence, finds
// nothing more queued, and memtier keeps libevent's reads, as before.

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <event2/buffer.h>
#include <event2/event.h>
#include <event2/util.h>
#ifndef _WIN32
#include <sys/ioctl.h>
#include <sys/socket.h>
#endif

static const size_t libevent_read_cap = 4096;
static const size_t libevent_2_2_read_size = 16384;

// The cap is in the loaded library, so ask its (runtime) version.
static inline bool libevent_caps_reads(void)
{
#ifdef _WIN32
    return false;
#else
    return event_get_version_number() < 0x02020000;
#endif
}

static inline void libevent_read_added_cb(struct evbuffer *, const struct evbuffer_cb_info *info, void *arg)
{
    if (info->n_added > 0) *(size_t *) arg = info->n_added;
}

// Record in *last_added how much each read adds to input. A callback rather
// than comparing lengths around process_response(), which can delete the
// connection (e.g. a cluster reconnect frees non-main connections). Our own
// read below triggers it too; libevent's next read overwrites that value
// before the next read callback, as long as only libevent's reads run the
// read callback (memtier never calls bufferevent_trigger() on it).
static inline void track_libevent_reads(struct evbuffer *input, size_t *last_added)
{
    *last_added = 0;
    evbuffer_add_cb(input, libevent_read_added_cb, last_added);
}

// Call from the read callback of a socket bufferevent whose input is tracked
// with track_libevent_reads(). When libevent's read filled the cap, more data
// is usually queued: read on, up to libevent_2_2_read_size in all. A read that
// added exactly the cap without being capped finds nothing more queued.
static inline void read_past_libevent_cap(struct evbuffer *input, evutil_socket_t fd, size_t *last_added)
{
#ifdef _WIN32
    (void) input;
    (void) fd;
    (void) last_added;
#else
    if (*last_added != libevent_read_cap) return;
    *last_added = 0;

    // Read only what is already queued, as libevent does, so that a pending
    // EOF or socket error is left for libevent's next read to report.
    int queued = 0;
    if (ioctl(fd, FIONREAD, &queued) < 0 || queued <= 0) return;
    if ((size_t) queued > libevent_2_2_read_size - libevent_read_cap) {
        queued = (int) (libevent_2_2_read_size - libevent_read_cap);
    }

    struct evbuffer_iovec vec;
    // The socket bufferevent keeps the end of its input frozen except while
    // it reads into it (bufferevent_readcb); do the same around this read.
    evbuffer_unfreeze(input, 0);
    if (evbuffer_reserve_space(input, queued, &vec, 1) == 1) {
        ssize_t n = recv(fd, vec.iov_base, queued, 0);
        vec.iov_len = n > 0 ? (size_t) n : 0;
        // Committing the single vector just reserved cannot fail. If it
        // ever did, bytes already taken from the socket would be lost and
        // the parser would wait for them forever, so stop instead.
        if (evbuffer_commit_space(input, &vec, 1) != 0) {
            fprintf(stderr, "error: evbuffer_commit_space failed after reading %zu bytes\n", vec.iov_len);
            abort();
        }
    }
    evbuffer_freeze(input, 0);
#endif
}

#endif // MEMTIER_LIBEVENT_READ_CAP_H
