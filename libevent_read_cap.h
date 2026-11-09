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

// libevent before 2.2 (2.0.11, the oldest configure accepts, through 2.1.x) reads
// at most 4 KiB (EVBUFFER_MAX_READ) from a socket per read event, so
// pipelined replies are read and parsed 4 KiB at a time;
// libevent 2.2 reads up to 16 KiB (issue #535). For a plaintext socket
// bufferevent, read on when the input holds a full cap, so that a read event
// takes at most the amount of data libevent 2.2 does. TLS bufferevents read
// whole records and are left alone. Windows keeps libevent's reads: the
// Winsock version (ioctlsocket(), and IOCP bufferevents) is neither written
// nor tested.
// This relies on two libevent 2.1 details: a capped read adds at most
// EVBUFFER_MAX_READ (4096) bytes, and the input's end is frozen outside
// libevent's own read. The 16 KiB bound holds for the default 4096. A libevent
// built with a larger EVBUFFER_MAX_READ already reads more than 4 KiB itself;
// memtier then only reads on when at least 4096 bytes are in the input and
// more is queued. The direct recv() bypasses libevent's read watermark and
// rate-limit accounting, which memtier does not use on these sockets.

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

// Call from the read callback of a plaintext socket bufferevent, before parsing.
// libevent's read added at most libevent_read_cap bytes, so an input shorter
// than the cap cannot hold a capped read: return at once, which is the usual
// case for small replies and costs one length read per event. An input of at
// least one cap is only a hint, since it may also hold unparsed bytes from
// earlier reads. FIONREAD then says whether more is queued: if it is, read on;
// if not (libevent's read was not capped), nothing is read, at the price of
// one ioctl. A read event therefore takes at most libevent_2_2_read_size of new
// data (libevent's 4 KiB plus at most the rest of that size here), whatever the
// input already holds.
static inline void read_past_libevent_cap(struct evbuffer *input, evutil_socket_t fd)
{
#ifdef _WIN32
    (void) input;
    (void) fd;
#else
    if (evbuffer_get_length(input) < libevent_read_cap) return;

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
        // FIONREAD reported queued data, which a socket returns before any
        // pending error; should recv() still fail, commit nothing and leave
        // the error for libevent's next read.
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
