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

// libFuzzer harness for redis_protocol::parse_response().
//
// Feeds bytes through an evbuffer into the redis_protocol parser. The
// concrete redis_protocol class lives in protocol.cpp's translation unit
// (not exposed via protocol.h), so we route through the existing
// protocol_factory(PROTOCOL_RESP2) entry point. Catches strtol overflow on
// bulk-length headers, mbulk leak paths, RESP3 map/attribute mishandling,
// and truncated-frame loops.

#include <stddef.h>
#include <stdint.h>
#include <cassert>

#include <event2/buffer.h>

#include "protocol.h"
#include "memtier_benchmark.h"

// Drive the parser to completion (or a hard iteration cap so a pathological
// input can't spin libFuzzer past its -timeout=25s backstop). parse_response()
// returns:
//   1 -> a complete response was produced; loop and try the next frame.
//   0 -> need more bytes; we're done with this input.
//  <0 -> protocol error; bail.
static const int kMaxParseIterations = 64;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    // Pick RESP2 vs RESP3 based on one driver byte so the fuzzer can explore
    // both code paths. RESP3 unlocks map/attribute/push branches (`%`, `|`,
    // `>`) which RESP2 rejects.
    enum PROTOCOL_TYPE proto = PROTOCOL_RESP2;
    if (size > 0 && (data[0] & 0x01)) {
        proto = PROTOCOL_RESP3;
    }
    const uint8_t *payload = (size > 0) ? data + 1 : data;
    size_t payload_size = (size > 0) ? size - 1 : 0;

    abstract_protocol *proto_inst = protocol_factory(proto);
    if (proto_inst == NULL) {
        return 0;
    }

    struct evbuffer *read_buf = evbuffer_new();
    struct evbuffer *write_buf = evbuffer_new();
    if (read_buf == NULL || write_buf == NULL) {
        if (read_buf) evbuffer_free(read_buf);
        if (write_buf) evbuffer_free(write_buf);
        delete proto_inst;
        return 0;
    }

    proto_inst->set_buffers(read_buf, write_buf);
    // Exercise the mbulk-retention path: forces the parser to allocate
    // mbulk_size_el / bulk_el nodes which is where leak paths historically
    // surface.
    proto_inst->set_keep_value(true);

    if (payload_size > 0) {
        evbuffer_add(read_buf, payload, payload_size);
    }

    for (int i = 0; i < kMaxParseIterations; ++i) {
        int rc = proto_inst->parse_response();
        if (rc <= 0) {
            break;
        }
        // A complete response was parsed; clear keep-value state for the next
        // frame so the response object isn't left holding stale pointers when
        // the destructor runs.
    }

    evbuffer_free(read_buf);
    evbuffer_free(write_buf);
    delete proto_inst;
    return 0;
}
