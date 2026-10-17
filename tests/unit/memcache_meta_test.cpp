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
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include "protocol.h"

void benchmark_log_file_line(int, const char *, unsigned int, const char *, ...) {}
void benchmark_log(int, const char *, ...) {}
bool is_redis_protocol(enum PROTOCOL_TYPE type)
{
    return type == PROTOCOL_REDIS_DEFAULT || type == PROTOCOL_RESP2 || type == PROTOCOL_RESP3;
}

static int failures = 0;
static void check(bool condition, const char *message)
{
    if (!condition) {
        fprintf(stderr, "FAIL: %s\n", message);
        ++failures;
    }
}


struct fixture
{
    abstract_protocol *protocol;
    evbuffer *input;
    evbuffer *output;
    fixture(bool keep) :
            protocol(protocol_factory(PROTOCOL_MEMCACHE_META)), input(evbuffer_new()), output(evbuffer_new())
    {
        protocol->set_buffers(input, output);
        protocol->set_keep_value(keep);
    }
    ~fixture()
    {
        delete protocol;
        evbuffer_free(input);
        evbuffer_free(output);
    }
    void issue(unsigned int gets)
    {
        if (!gets)
            protocol->write_command_set("k", 1, "v", 1, 0, 0);
        else if (gets == 1)
            protocol->write_command_get("k", 1, 0);
        else {
            keylist keys(gets);
            for (unsigned int i = 0; i < gets; ++i)
                keys.add_key("k", 1);
            protocol->write_command_multi_get(&keys);
        }
    }
};

static void error_tests()
{
    const char *errors[] = {"ERROR", "CLIENT_ERROR bad command", "SERVER_ERROR out of memory"};
    for (int keep = 0; keep < 2; ++keep) {
        for (const char *error : errors) {
            for (unsigned int gets = 0; gets <= 3; ++gets) {
                fixture f(keep != 0);
                f.issue(gets);
                f.issue(1);
                std::string wire;
                if (gets >= 2) wire = "VA 1\r\nx\r\n";
                wire += error;
                wire += "\r\n";
                if (gets == 3) wire += "CLIENT_ERROR later\r\n";
                wire += "MN\r\n";
                evbuffer_add(f.input, wire.data(), wire.size());
                check(f.protocol->parse_response() == 1, "defined error completes through MN");
                check(f.protocol->get_response()->is_error(), "defined error flag retained");
                check(strcmp(f.protocol->get_response()->get_status(), error) == 0,
                      "first error overrides prior valid status");
                check(f.protocol->get_response()->get_total_len() == wire.size(), "error response bytes exact");
                evbuffer_add(f.input, "EN\r\nMN\r\n", 8);
                check(f.protocol->parse_response() == 1, "following reply still aligns");
                check(!f.protocol->get_response()->is_error(), "error flag does not leak");
            }
        }
    }
}

static void framing_tests()
{
    struct reply_case
    {
        const char *wire;
        unsigned int gets;
    };
    const reply_case invalid[] = {{"SERVER_ERRORjunk\r\nMN\r\n", 1}, {"CLIENT_ERRORjunk\r\nMN\r\n", 1},
                                  {"ERROR junk\r\nMN\r\n", 1},       {"HD\r\nHD\r\nMN\r\n", 1},
                                  {"VA 1\r\nx\r\nMN\r\n", 0},        {"EN\r\nMN\r\n", 2},
                                  {"EN\r\nEN\r\nMN\r\n", 1},         {"VA 1\r\nx\r\nVA 1\r\ny\r\nMN\r\n", 1},
                                  {"HD\r\nHD\r\nMN\r\n", 0},         {"MN\r\n", 1},
                                  {"VA 2147483648\r\n", 1},          {"VA 4294967295\r\n", 1}};
    {
        fixture f(false);
        f.issue(1);
        const char *header = "VA 2147483647\r\n";
        evbuffer_add(f.input, header, strlen(header));
        check(f.protocol->parse_response() == 0, "INT_MAX value header remains valid and incomplete");
    }
    {
        fixture f(false);
        f.issue(1);
        f.protocol->reset_state();
        f.protocol->prepare_request_replay(false, 1);
        f.protocol->prepare_request_replay(true, 2);
        f.issue(1);
        const char *stream = "HD\r\nMN\r\nEN\r\nVA 1\r\nx\r\nMN\r\nEN\r\nMN\r\n";
        evbuffer_add(f.input, stream, strlen(stream));
        check(f.protocol->parse_response() == 1, "replayed SET retains kind");
        check(f.protocol->parse_response() == 1, "replayed MGET retains key count");
        check(f.protocol->get_response()->get_hits() == 1, "replayed MGET has one hit");
        check(f.protocol->parse_response() == 1, "new GET follows replayed requests");
        check(f.protocol->get_response()->get_hits() == 0, "new GET has no stale hit");
    }
    for (const auto &item : invalid) {
        fixture f(false);
        f.issue(item.gets);
        evbuffer_add(f.input, item.wire, strlen(item.wire));
        check(f.protocol->parse_response() == -1, "reject wrong response kind/cardinality");
    }
    struct valid_case
    {
        std::string wire;
        unsigned int gets;
        unsigned int hits;
    };
    const valid_case valid[] = {{"HD\r\nMN\r\n", 0, 0},
                                {"EN\r\nMN\r\n", 1, 0},
                                {"VA 0\r\n\r\nMN\r\n", 1, 1},
                                {std::string("VA 4\r\n") + std::string("a\0\r\n", 4) + "\r\nMN\r\n", 1, 1},
                                {"VA 1\r\nx\r\nEN\r\nMN\r\n", 2, 1}};
    for (int keep = 0; keep < 2; ++keep) {
        for (const auto &item : valid) {
            for (size_t split = 0; split < item.wire.size(); ++split) {
                fixture f(keep != 0);
                f.issue(item.gets);
                evbuffer_add(f.input, item.wire.data(), split);
                int rc = f.protocol->parse_response();
                check(rc == 0, "fragment needs remaining bytes including value CRLF");
                if (rc != 0) continue;
                evbuffer_add(f.input, item.wire.data() + split, item.wire.size() - split);
                check(f.protocol->parse_response() == 1, "fragmented reply completes");
                check(f.protocol->get_response()->get_hits() == item.hits, "fragmented hits exact");
                check(f.protocol->get_response()->get_total_len() == item.wire.size(), "fragmented bytes exact");
            }
        }
        fixture f(keep != 0);
        f.issue(0);
        f.issue(2);
        f.issue(1);
        const char *stream = "HD\r\nMN\r\nEN\r\nVA 1\r\nx\r\nMN\r\nEN\r\nMN\r\n";
        evbuffer_add(f.input, stream, strlen(stream));
        check(f.protocol->parse_response() == 1, "mixed pipeline SET");
        check(f.protocol->parse_response() == 1, "mixed pipeline MGET");
        check(f.protocol->get_response()->get_hits() == 1, "mixed pipeline MGET hits");
        check(f.protocol->parse_response() == 1, "mixed pipeline GET");
        check(f.protocol->get_response()->get_hits() == 0, "mixed pipeline GET miss");
        check(f.protocol->parse_response() == 0, "no phantom reply");
    }
}

int main()
{
    framing_tests();
    error_tests();
    // A new connection must not inherit either a partially read value or
    // accumulated bytes/hits from a reply whose MN never arrived.
    const char *partial[] = {"V", "VA 100\r\nx", "VA 1\r\nx\r\n", "EN\r\n", "HD\r\n"};
    for (int keep = 0; keep < 2; ++keep) {
        for (size_t i = 0; i < sizeof(partial) / sizeof(partial[0]); ++i) {
            abstract_protocol *protocol = protocol_factory(PROTOCOL_MEMCACHE_META);
            evbuffer *input = evbuffer_new();
            evbuffer *output = evbuffer_new();
            protocol->set_buffers(input, output);
            protocol->set_keep_value(keep != 0);
            if (i == 4)
                protocol->write_command_set("k", 1, "v", 1, 0, 0);
            else
                protocol->write_command_get("k", 1, 0);
            protocol->write_command_set("stale", 5, "v", 1, 0, 0);
            evbuffer_add(input, partial[i], strlen(partial[i]));
            check(protocol->parse_response() == 0, "partial reply needs more bytes");
            evbuffer_drain(input, evbuffer_get_length(input));
            protocol->reset_state();
            protocol->write_command_get("fresh", 5, 0);
            evbuffer_add(input, "EN\r\nMN\r\n", 8);
            check(protocol->parse_response() == 1, "new connection parses a complete miss");
            check(protocol->get_response()->get_hits() == 0, "old hits were discarded");
            check(protocol->get_response()->get_total_len() == 8, "old response bytes were discarded");
            unsigned int length = 99;
            check(protocol->get_response()->get_value(&length) == NULL, "old value was discarded");
            delete protocol;
            evbuffer_free(input);
            evbuffer_free(output);
        }
    }
    return failures ? 1 : 0;
}
