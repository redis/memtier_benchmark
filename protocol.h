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

#ifndef _PROTOCOL_H
#define _PROTOCOL_H

#include <event2/buffer.h>
#include <vector>
#include "memtier_benchmark.h"

enum mbulk_element_type
{
    mbulk_element_mbulk_size,
    mbulk_element_bulk
};

// forward deceleration
class mbulk_size_el;
class bulk_el;

class mbulk_element
{
public:
    mbulk_element(mbulk_element_type t) : type(t) { ; }
    virtual ~mbulk_element() { ; }

    virtual mbulk_size_el *as_mbulk_size() = 0;
    virtual bulk_el *as_bulk() = 0;

    // Safe type predicates so callers can avoid as_bulk() / as_mbulk_size()
    // assertions when the element kind is unknown (e.g. when walking a
    // top-level reply array whose elements may be either bulks or nested
    // arrays, as in GEOPOS, COMMAND INFO, SORT_RO).
    bool is_bulk() const { return type == mbulk_element_bulk; }
    bool is_mbulk_size() const { return type == mbulk_element_mbulk_size; }

protected:
    mbulk_element_type type;
};

class mbulk_size_el : public mbulk_element
{
public:
    mbulk_size_el() : mbulk_element(mbulk_element_mbulk_size), upper_level(NULL), bulks_count(0) { ; }
    virtual ~mbulk_size_el()
    {
        // Iterative teardown. A naive recursive `delete el` on each child
        // would blow the stack when a (malicious or buggy) server sends
        // deeply nested arrays (`*1\r\n*1\r\n*1\r\n…`). We walk the tree
        // non-recursively by stealing each visited node's children into a
        // worklist; once a node's `mbulks_elements` has been swapped out
        // its destructor's body is a no-op so the eventual `delete` does
        // not recurse.
        std::vector<mbulk_element *> worklist;
        worklist.swap(mbulks_elements);
        for (size_t i = 0; i < worklist.size(); i++) {
            mbulk_element *el = worklist[i];
            if (el == NULL) continue;
            if (el->is_mbulk_size()) {
                mbulk_size_el *m = el->as_mbulk_size();
                std::vector<mbulk_element *> stolen;
                stolen.swap(m->mbulks_elements);
                for (size_t j = 0; j < stolen.size(); j++) {
                    worklist.push_back(stolen[j]);
                }
            }
            delete el;
        }
    }

    virtual mbulk_size_el *as_mbulk_size() { return this; }

    virtual bulk_el *as_bulk() { assert(0); }

    void add_new_element(mbulk_element *new_el)
    {
        mbulks_elements.push_back(new_el);
        bulks_count--;
    }

    // return the next mbulk size element, that new element should be pushed to
    mbulk_size_el *get_next_mbulk()
    {
        mbulk_size_el *next = this;
        while (next != NULL) {
            if (next->bulks_count == 0) {
                next = next->upper_level;
            } else {
                break;
            }
        }

        return next;
    }

    mbulk_size_el *upper_level;
    int bulks_count;
    std::vector<mbulk_element *> mbulks_elements;
};

class bulk_el : public mbulk_element
{
public:
    bulk_el() : mbulk_element(mbulk_element_bulk), value(NULL), value_len(0), is_resp3_null(false) { ; }
    virtual ~bulk_el()
    {
        free(value);
        value_len = 0;
    }

    virtual bulk_el *as_bulk() { return this; }

    virtual mbulk_size_el *as_mbulk_size() { assert(0); }

    char *value;
    unsigned int value_len;
    // True only when this element was produced by the single_type parser
    // branch for the RESP3 null type byte '_'. Distinguishes a genuine
    // RESP3 null from a legitimate bulk string whose content happens to be
    // the single character '_' (parsed via blob_type / rs_read_bulk).
    bool is_resp3_null;
};

struct protocol_response
{
protected:
    const char *m_status;
    mbulk_size_el *m_mbulk_value;
    const char *m_value;
    unsigned int m_value_len;
    unsigned int m_total_len;
    unsigned int m_hits;
    bool m_error;

public:
    protocol_response();
    virtual ~protocol_response();

    void set_status(const char *status);
    const char *get_status(void);

    void set_error();
    bool is_error(void);

    void set_value(const char *value, unsigned int value_len);
    const char *get_value(unsigned int *value_len);

    void set_total_len(unsigned int total_len);
    unsigned int get_total_len(void);

    void incr_hits(void);
    unsigned int get_hits(void);

    void clear();

    void set_mbulk_value(mbulk_size_el *element);
    mbulk_size_el *get_mbulk_value();
};

class keylist
{
protected:
    struct key_entry
    {
        char *key_ptr;
        unsigned int key_len;
    };

    char *m_buffer;
    char *m_buffer_ptr;
    unsigned int m_buffer_size;

    key_entry *m_keys;
    unsigned int m_keys_size;
    unsigned int m_keys_count;

public:
    keylist(unsigned int max_keys);
    ~keylist();

    bool add_key(const char *key, unsigned int key_len);
    unsigned int get_keys_count(void) const;
    const char *get_key(unsigned int index, unsigned int *key_len) const;

    void clear(void);
};

class abstract_protocol
{
protected:
    struct evbuffer *m_read_buf;
    struct evbuffer *m_write_buf;

    bool m_keep_value;
    struct protocol_response m_last_response;

public:
    abstract_protocol();
    virtual ~abstract_protocol();
    virtual abstract_protocol *clone(void) = 0;
    void set_buffers(struct evbuffer *read_buf, struct evbuffer *write_buf);
    void set_keep_value(bool flag);

    virtual int select_db(int db) = 0;
    virtual int authenticate(const char *credentials) = 0;
    virtual int configure_protocol(enum PROTOCOL_TYPE type) = 0;
    virtual int write_command_cluster_slots() = 0;
    virtual int write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry,
                                  unsigned int offset) = 0;
    virtual int write_command_get(const char *key, int key_len, unsigned int offset) = 0;
    virtual int write_command_multi_get(const keylist *keylist) = 0;
    virtual int write_command_wait(unsigned int num_slaves, unsigned int timeout) = 0;
    virtual int parse_response() = 0;

    // handle arbitrary command
    virtual bool format_arbitrary_command(arbitrary_command &cmd) = 0;
    virtual int write_arbitrary_command(const command_arg *arg) = 0;
    virtual int write_arbitrary_command(const char *val, int val_len) = 0;

    struct protocol_response *get_response(void) { return &m_last_response; }
};

class abstract_protocol *protocol_factory(enum PROTOCOL_TYPE type);

#endif /* _PROTOCOL_H */
