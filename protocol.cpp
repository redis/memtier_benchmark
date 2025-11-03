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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <queue>
#include <unistd.h>
#ifdef HAVE_ASSERT_H
#include <assert.h>
#endif

#include "protocol.h"
#include "memtier_benchmark.h"
#include "shard_connection.h"
#include "libmemcached_protocol/binary.h"

/////////////////////////////////////////////////////////////////////////

abstract_protocol::abstract_protocol() :
    m_read_buf(NULL), m_write_buf(NULL), m_keep_value(false), m_read_limit(0)
{
}

abstract_protocol::~abstract_protocol()
{
}

void abstract_protocol::set_buffers(struct evbuffer* read_buf, struct evbuffer* write_buf)
{
    m_read_buf = read_buf;
    m_write_buf = write_buf;
}

void abstract_protocol::set_keep_value(bool flag)
{
    m_keep_value = flag;
}

size_t abstract_protocol::get_available_bytes()
{
    size_t available = evbuffer_get_length(m_read_buf);
    if (m_read_limit > 0 && available > m_read_limit) {
        return m_read_limit;
    }
    return available;
}

/////////////////////////////////////////////////////////////////////////

protocol_response::protocol_response()
    : m_status(NULL), m_mbulk_value(NULL), m_value(NULL), m_value_len(0), m_hits(0), m_error(false)
{
}

protocol_response::~protocol_response()
{
    clear();
}

void protocol_response::set_error()
{
    m_error = true;
}

bool protocol_response::is_error(void)
{
    return m_error;
}

void protocol_response::set_status(const char* status)
{
    if (m_status != NULL)
        free((void *)m_status);
    m_status = status;
}

const char* protocol_response::get_status(void)
{
    return m_status;
}

void protocol_response::set_value(const char* value, unsigned int value_len)
{
    if (m_value != NULL)
        free((void *)m_value);
    m_value = value;
    m_value_len = value_len;
}

const char* protocol_response::get_value(unsigned int* value_len)
{
    assert(value_len != NULL);

    *value_len = m_value_len;
    return m_value;
}

void protocol_response::set_total_len(unsigned int total_len)
{
    m_total_len = total_len;
}

unsigned int protocol_response::get_total_len(void)
{
    return m_total_len;
}

void protocol_response::incr_hits(void)
{
    m_hits++;
}

unsigned int protocol_response::get_hits(void)
{
    return m_hits;
}

void protocol_response::clear(void)
{
    if (m_status != NULL) {
        free((void *)m_status);
        m_status = NULL;
    }
    if (m_value != NULL) {
        free((void *)m_value);
        m_value = NULL;
    }
    if (m_mbulk_value != NULL) {
        delete m_mbulk_value;
        m_mbulk_value = NULL;
    }

    m_value_len = 0;
    m_total_len = 0;
    m_hits = 0;
    m_error = 0;
}

void protocol_response::set_mbulk_value(mbulk_size_el* element) {
    m_mbulk_value = element;
}

mbulk_size_el* protocol_response::get_mbulk_value() {
    return m_mbulk_value;
}

/////////////////////////////////////////////////////////////////////////

class redis_protocol : public abstract_protocol {
protected:
    enum response_state { rs_initial, rs_read_bulk, rs_read_line, rs_end_bulk };
    response_state m_response_state;
    long m_bulk_len;
    size_t m_response_len;

    unsigned int m_total_bulks_count;
    mbulk_size_el* m_current_mbulk;
    bool m_resp3;
    bool m_attribute;

    bool aggregate_type(char c);
    bool blob_type(char c);
    bool single_type(char c);
    bool response_ended();

public:
    redis_protocol() : m_response_state(rs_initial), m_bulk_len(0), m_response_len(0), m_total_bulks_count(0), m_current_mbulk(NULL), m_resp3(false), m_attribute(false) { }
    virtual redis_protocol* clone(void) { return new redis_protocol(); }
    virtual int select_db(int db);
    virtual int authenticate(const char *credentials);
    virtual int configure_protocol(enum PROTOCOL_TYPE type);
    virtual int write_command_cluster_slots();
    virtual int write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset);
    virtual int write_command_get(const char *key, int key_len, unsigned int offset);
    virtual int write_command_multi_get(const keylist *keylist);
    virtual int write_command_wait(unsigned int num_slaves, unsigned int timeout);
    virtual int parse_response(void);

    // handle arbitrary command
    virtual bool format_arbitrary_command(arbitrary_command &cmd);
    int write_arbitrary_command(const command_arg *arg);
    int write_arbitrary_command(const char *val, int val_len);
};

// Fast header protocol - prepends a 10-byte header before RESP3 payload
// Header format: designator(1) | length(4) | ncmd(1) | slot(2) | client_idx(2)
#define FAST_HEADER_DESIGNATOR 0x80
#define FAST_HEADER_SIZE 10
#define MAX_CLUSTER_HSLOT 16383

// CRC16 lookup table for Redis cluster slot calculation
static const uint16_t fast_header_crc16tab[256] = {
    0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
    0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
    0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
    0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
    0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
    0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
    0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
    0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
    0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
    0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
    0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
    0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
    0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
    0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
    0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
    0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
    0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
    0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
    0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
    0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
    0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
    0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
    0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
    0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
    0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
    0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
    0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
    0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
    0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
    0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
    0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
    0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
};

class fast_header_protocol : public redis_protocol {
private:
    uint16_t m_client_idx;

    // Bulk support state
    uint8_t m_accumulated_commands;      // Number of commands accumulated in current bulk
    uint32_t m_bulk_payload_size;        // Total size of commands in current bulk
    uint16_t m_current_slot;             // Slot ID for current bulk
    struct evbuffer* m_bulk_buffer;      // Temporary buffer for accumulating commands in current bulk
    uint8_t m_bulk_size;                 // Target bulk size (from config)
    bool m_no_header;                    // Whether to skip header when sending bulk

    // Response header support
    struct pending_request {
        uint16_t client_idx;
        uint8_t expected_responses;
        struct timeval sent_time;
    };
    std::queue<pending_request> m_pending_requests;

    // Response parsing state
    enum response_header_state { rhs_initial, rhs_read_header, rhs_read_payload };
    response_header_state m_response_header_state;
    uint32_t m_response_payload_length;
    uint8_t m_response_ncmd_flags;
    uint16_t m_response_client_idx;
    uint8_t m_responses_to_parse;  // Number of RESP responses to parse from current header
    uint32_t m_payload_bytes_consumed;  // Bytes consumed from current fast header payload

    // Track setup commands that don't use fast header protocol
    bool m_expecting_setup_response;     // True when expecting auth/select_db/hello response

    static inline uint16_t fast_header_crc16(const char *buf, size_t len) {
        size_t counter;
        uint16_t crc = 0;
        for (counter = 0; counter < len; counter++)
            crc = (crc << 8) ^ fast_header_crc16tab[((crc >> 8) ^ *buf++) & 0x00FF];
        return crc;
    }

    static inline uint16_t fast_header_calc_slot(const char *key, size_t key_len) {
        // Handle Redis hash tags: if key contains {}, only hash the part inside {}
        const char *hash_tag_start = (const char *)memchr(key, '{', key_len);

        if (hash_tag_start != NULL) {
            // Found opening brace, look for closing brace
            size_t offset_from_start = hash_tag_start - key;
            size_t remaining_len = key_len - offset_from_start;

            const char *hash_tag_end = (const char *)memchr(hash_tag_start + 1, '}', remaining_len - 1);

            if (hash_tag_end != NULL && hash_tag_end > hash_tag_start + 1) {
                // Found valid hash tag, hash only the content between { and }
                size_t hash_tag_len = hash_tag_end - hash_tag_start - 1;
                return (uint16_t)(fast_header_crc16(hash_tag_start + 1, hash_tag_len) & MAX_CLUSTER_HSLOT);
            }
        }

        // No valid hash tag found, hash the entire key
        return (uint16_t)(fast_header_crc16(key, key_len) & MAX_CLUSTER_HSLOT);
    }

    int write_fast_header(uint16_t slot, uint32_t payload_size, uint8_t batch_count = 1) {
        unsigned char header[FAST_HEADER_SIZE];

        // New 10-byte header format: designator(1) | length(4) | ncmd(1) | slot(2) | client_idx(2)
        header[0] = FAST_HEADER_DESIGNATOR;
        header[1] = (payload_size >> 24) & 0xFF;  // length (4 bytes, network byte order)
        header[2] = (payload_size >> 16) & 0xFF;
        header[3] = (payload_size >> 8) & 0xFF;
        header[4] = payload_size & 0xFF;
        header[5] = batch_count;                  // ncmd (1 byte)
        header[6] = (slot >> 8) & 0xFF;          // slot (2 bytes, network byte order)
        header[7] = slot & 0xFF;
        header[8] = (m_client_idx >> 8) & 0xFF;  // client_idx (2 bytes, network byte order)
        header[9] = m_client_idx & 0xFF;

        // Increment client_idx for next request
        m_client_idx++;

        return evbuffer_add(m_write_buf, header, FAST_HEADER_SIZE);
    }

public:
    fast_header_protocol() : redis_protocol(), m_client_idx(rand() & 0xFFFF),
                             m_accumulated_commands(0), m_bulk_payload_size(0),
                             m_current_slot(0),
                             m_bulk_buffer(NULL), m_bulk_size(1), m_no_header(false),
                             m_response_header_state(rhs_initial), m_response_payload_length(0),
                             m_response_ncmd_flags(0), m_response_client_idx(0), m_responses_to_parse(0),
                             m_payload_bytes_consumed(0),
                             m_expecting_setup_response(false) {
        m_bulk_buffer = evbuffer_new();
    }

    virtual ~fast_header_protocol() {
        if (m_bulk_buffer) {
            evbuffer_free(m_bulk_buffer);
        }
    }
    virtual fast_header_protocol* clone(void) {
        fast_header_protocol* cloned = new fast_header_protocol();
        cloned->m_bulk_size = m_bulk_size;
        cloned->m_no_header = m_no_header;
        // Don't copy m_client_idx - let each clone start with its own random value
        return cloned;
    }

    virtual int parse_response(void);

    // Override setup commands to track when we expect non-fast-header responses
    virtual int authenticate(const char *credentials);
    virtual int select_db(int db);
    virtual int configure_protocol(enum PROTOCOL_TYPE type);

    virtual int write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset);
    virtual int write_command_get(const char *key, int key_len, unsigned int offset);
    virtual int write_command_multi_get(const keylist *keylist);
    virtual int write_arbitrary_command(const command_arg *arg);
    virtual int write_arbitrary_command(const char *val, int val_len);

    // Bulk support methods
    virtual bool has_partial_bulk() { return m_accumulated_commands > 0; }
    virtual void finalize_partial_bulk();
    virtual void set_bulk_size(unsigned int bulk_size) { m_bulk_size = bulk_size; }
    virtual void set_no_header(bool no_header) { m_no_header = no_header; }
};

int fast_header_protocol::write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset) {
    assert(key != NULL);
    assert(key_len > 0);
    assert(value != NULL);
    assert(value_len > 0);

    uint16_t slot = fast_header_calc_slot(key, key_len);

    // Always use bulk accumulation path for fast-header protocol
    // If this is the first command in a new bulk, initialize bulk state
    if (m_accumulated_commands == 0) {
        m_current_slot = slot;
        m_bulk_payload_size = 0;
        evbuffer_drain(m_bulk_buffer, evbuffer_get_length(m_bulk_buffer));
    }

    // Write the RESP3 command to the temporary bulk buffer
    struct evbuffer *orig_buf = m_write_buf;
    m_write_buf = m_bulk_buffer;
    int command_size = redis_protocol::write_command_set(key, key_len, value, value_len, expiry, offset);
    m_write_buf = orig_buf;

    m_bulk_payload_size += command_size;
    m_accumulated_commands++;

    // If bulk is full, finalize it
    if (m_accumulated_commands >= m_bulk_size) {
        finalize_partial_bulk();
    }

    // Return the command size only (not including header)
    // The header will be added when the bulk is finalized
    return command_size;
}

int fast_header_protocol::write_command_get(const char *key, int key_len, unsigned int offset) {
    assert(key != NULL);
    assert(key_len > 0);

    uint16_t slot = fast_header_calc_slot(key, key_len);

    // Always use bulk accumulation path for fast-header protocol
    // If this is the first command in a new bulk, initialize bulk state
    if (m_accumulated_commands == 0) {
        m_current_slot = slot;
        m_bulk_payload_size = 0;
        evbuffer_drain(m_bulk_buffer, evbuffer_get_length(m_bulk_buffer));
    }

    // Write the RESP3 command to the temporary bulk buffer
    struct evbuffer *orig_buf = m_write_buf;
    m_write_buf = m_bulk_buffer;
    int command_size = redis_protocol::write_command_get(key, key_len, offset);
    m_write_buf = orig_buf;

    m_bulk_payload_size += command_size;
    m_accumulated_commands++;

    // If bulk is full, finalize it
    if (m_accumulated_commands >= m_bulk_size) {
        finalize_partial_bulk();
    }

    // Return the command size only (not including header)
    // The header will be added when the bulk is finalized
    return command_size;
}

void fast_header_protocol::finalize_partial_bulk() {
    if (m_accumulated_commands == 0) {
        return;  // No partial bulk to finalize
    }

    // Track this request for response validation
    pending_request req;
    req.expected_responses = m_accumulated_commands;
    gettimeofday(&req.sent_time, NULL);

    // If no_header is set, skip writing the header
    if (!m_no_header) {
        // Create header with actual batch_count and payload_size
        unsigned char header[FAST_HEADER_SIZE];

        // New 10-byte header format: designator(1) | length(4) | ncmd(1) | slot(2) | client_idx(2)
        header[0] = FAST_HEADER_DESIGNATOR;
        header[1] = (m_bulk_payload_size >> 24) & 0xFF;  // length (4 bytes, network byte order)
        header[2] = (m_bulk_payload_size >> 16) & 0xFF;
        header[3] = (m_bulk_payload_size >> 8) & 0xFF;
        header[4] = m_bulk_payload_size & 0xFF;
        header[5] = m_accumulated_commands;              // ncmd (1 byte)
        header[6] = (m_current_slot >> 8) & 0xFF;        // slot (2 bytes, network byte order)
        header[7] = m_current_slot & 0xFF;
        header[8] = (m_client_idx >> 8) & 0xFF;          // client_idx (2 bytes, network byte order)
        header[9] = m_client_idx & 0xFF;

        // Store client_idx for this request
        req.client_idx = m_client_idx;

        // Increment client_idx for next request
        m_client_idx++;

        // Write header to main buffer
        evbuffer_add(m_write_buf, header, FAST_HEADER_SIZE);
    } else {
        // When no header is sent, we don't expect headers in responses
        req.client_idx = 0;  // No client_idx to validate
    }

    // Add request to tracking queue
    m_pending_requests.push(req);

    // Write all accumulated commands from bulk buffer to main buffer
    evbuffer_add_buffer(m_write_buf, m_bulk_buffer);

    // Reset bulk state for next bulk
    m_accumulated_commands = 0;
    m_bulk_payload_size = 0;
}

int fast_header_protocol::write_command_multi_get(const keylist *keylist) {
    // MGET can be bulked like any other command
    // If this is the first command in a new bulk, initialize bulk state
    if (m_accumulated_commands == 0) {
        m_current_slot = 0;  // MGET doesn't have a specific slot
        m_bulk_payload_size = 0;
        evbuffer_drain(m_bulk_buffer, evbuffer_get_length(m_bulk_buffer));
    }

    // Write the RESP3 command to the temporary bulk buffer
    struct evbuffer *orig_buf = m_write_buf;
    m_write_buf = m_bulk_buffer;
    int command_size = redis_protocol::write_command_multi_get(keylist);
    m_write_buf = orig_buf;

    m_bulk_payload_size += command_size;
    m_accumulated_commands++;

    // If bulk is full, finalize it
    if (m_accumulated_commands >= m_bulk_size) {
        finalize_partial_bulk();
    }

    // Return the command size only (not including header)
    return command_size;
}

int fast_header_protocol::write_arbitrary_command(const command_arg *arg) {
    // Arbitrary commands can be bulked like any other command
    // If this is the first command in a new bulk, initialize bulk state
    if (m_accumulated_commands == 0) {
        m_current_slot = 0;  // Arbitrary commands don't have a specific slot
        m_bulk_payload_size = 0;
        evbuffer_drain(m_bulk_buffer, evbuffer_get_length(m_bulk_buffer));
    }

    // Write the RESP3 command to the temporary bulk buffer
    struct evbuffer *orig_buf = m_write_buf;
    m_write_buf = m_bulk_buffer;
    int command_size = redis_protocol::write_arbitrary_command(arg);
    m_write_buf = orig_buf;

    m_bulk_payload_size += command_size;
    m_accumulated_commands++;

    // If bulk is full, finalize it
    if (m_accumulated_commands >= m_bulk_size) {
        finalize_partial_bulk();
    }

    // Return the command size only (not including header)
    return command_size;
}

int fast_header_protocol::write_arbitrary_command(const char *val, int val_len) {
    // Arbitrary commands can be bulked like any other command
    // If this is the first command in a new bulk, initialize bulk state
    if (m_accumulated_commands == 0) {
        m_current_slot = 0;  // Arbitrary commands don't have a specific slot
        m_bulk_payload_size = 0;
        evbuffer_drain(m_bulk_buffer, evbuffer_get_length(m_bulk_buffer));
    }

    // Write the RESP3 command to the temporary bulk buffer
    struct evbuffer *orig_buf = m_write_buf;
    m_write_buf = m_bulk_buffer;
    int command_size = redis_protocol::write_arbitrary_command(val, val_len);
    m_write_buf = orig_buf;

    m_bulk_payload_size += command_size;
    m_accumulated_commands++;

    // If bulk is full, finalize it
    if (m_accumulated_commands >= m_bulk_size) {
        finalize_partial_bulk();
    }

    // Return the command size only (not including header)
    return command_size;
}

// Override setup commands to set flag for expecting non-fast-header responses
int fast_header_protocol::authenticate(const char *credentials) {
    m_expecting_setup_response = true;
    return redis_protocol::authenticate(credentials);
}

int fast_header_protocol::select_db(int db) {
    m_expecting_setup_response = true;
    return redis_protocol::select_db(db);
}

int fast_header_protocol::configure_protocol(enum PROTOCOL_TYPE type) {
    m_expecting_setup_response = true;
    return redis_protocol::configure_protocol(type);
}

int fast_header_protocol::parse_response(void) {
    // If we're expecting a setup response (auth/select_db/hello), use regular RESP parsing
    if (m_expecting_setup_response) {
        int result = redis_protocol::parse_response();
        if (result > 0) {
            // Setup response parsed successfully, reset flag
            m_expecting_setup_response = false;
        }
        return result;
    }

    while (true) {
        switch (m_response_header_state) {
            case rhs_initial:
                // Clear last response
                m_last_response.clear();
                m_response_len = 0;
                m_response_header_state = rhs_read_header;
                break;

            case rhs_read_header: {
                // If no_header is set, we don't expect response headers
                if (m_no_header) {
                    // Use parent redis_protocol parsing directly
                    return redis_protocol::parse_response();
                }

                // Check if we have enough data for header (8 bytes for response)
                if (evbuffer_get_length(m_read_buf) < 8) {
                    return 0;  // Need more data
                }

                // Read the 8-byte response header
                unsigned char header[8];
                int ret = evbuffer_remove(m_read_buf, header, 8);
                if (ret != 8) {
                    return -1;
                }

                // Parse header fields
                uint8_t desig = header[0];
                m_response_payload_length = (header[1] << 24) | (header[2] << 16) | (header[3] << 8) | header[4];
                m_response_ncmd_flags = header[5];
                m_response_client_idx = (header[6] << 8) | header[7];

                // Validate designator
                if (desig != FAST_HEADER_DESIGNATOR) {
                    return -1;
                }

                // Validate client_idx against pending requests
                if (m_pending_requests.empty()) {
                    return -1;
                }

                pending_request& expected = m_pending_requests.front();

                if (m_response_client_idx != expected.client_idx) {
                    return -1;
                }

                // Check protocol error flag
                if (m_response_ncmd_flags & 0x80) {
                    m_last_response.set_error();
                }

                // Extract number of replies (mask out error bit)
                uint8_t num_replies = m_response_ncmd_flags & 0x7F;

                if (num_replies == 0 || num_replies > expected.expected_responses) {
                    return -1;
                }

                // Store how many RESP responses we need to parse from this header
                m_responses_to_parse = num_replies;
                m_payload_bytes_consumed = 0;  // Reset payload byte counter for new header

                // Update expected responses count
                expected.expected_responses -= num_replies;
                if (expected.expected_responses == 0) {
                    m_pending_requests.pop();  // This request is complete
                }

                m_response_len += 8;  // Count header bytes
                m_response_header_state = rhs_read_payload;

                // Initialize RESP parser state for the first response in this payload
                m_response_state = rs_initial;
                break;
            }

            case rhs_read_payload: {
                // Parse RESP responses incrementally from the payload
                // We need to physically limit the buffer to prevent evbuffer_readln from reading beyond the payload

                size_t bytes_remaining_in_payload = m_response_payload_length - m_payload_bytes_consumed;
                size_t available = evbuffer_get_length(m_read_buf);

                // If buffer has more data than the remaining payload, we need to temporarily swap buffers
                struct evbuffer* original_buf = NULL;
                if (available > bytes_remaining_in_payload) {
                    // Strategy: Swap m_read_buf with a new buffer containing only the payload
                    // 1. Save the original m_read_buf pointer
                    // 2. Create a new buffer with only the payload data
                    // 3. Set m_read_buf to point to the new buffer
                    // 4. Parse
                    // 5. Restore m_read_buf to the original, with consumed bytes drained

                    original_buf = m_read_buf;

                    // Create new buffer with only the payload
                    m_read_buf = evbuffer_new();
                    if (m_read_buf == NULL) {
                        m_read_buf = original_buf;
                        return -1;
                    }

                    // Copy only the payload bytes to the new buffer
                    char* payload_data = (char*)malloc(bytes_remaining_in_payload);
                    if (payload_data == NULL) {
                        evbuffer_free(m_read_buf);
                        m_read_buf = original_buf;
                        return -1;
                    }

                    evbuffer_copyout(original_buf, payload_data, bytes_remaining_in_payload);
                    evbuffer_add(m_read_buf, payload_data, bytes_remaining_in_payload);
                    free(payload_data);
                }

                // Track buffer size before parsing
                size_t buffer_before = evbuffer_get_length(m_read_buf);

                // Use the parent redis_protocol to parse individual RESP responses
                // Note: We DON'T reset m_response_state here because the RESP parser
                // might be in the middle of parsing a response (e.g., waiting for more bulk data)
                // The state will be reset to rs_initial after each successful parse below

                int parse_result = redis_protocol::parse_response();

                // Track how many bytes were consumed by this RESP parse
                size_t buffer_after = evbuffer_get_length(m_read_buf);
                size_t bytes_consumed_this_parse = buffer_before - buffer_after;

                // Restore the original buffer if we swapped it
                if (original_buf != NULL) {
                    // Free the temporary buffer we created
                    evbuffer_free(m_read_buf);

                    // Restore the original buffer
                    m_read_buf = original_buf;

                    // Drain the consumed bytes from the original buffer
                    evbuffer_drain(m_read_buf, bytes_consumed_this_parse);
                }

                // IMPORTANT: Always update payload bytes consumed, even if parse_result <= 0
                // The RESP parser may consume bytes even when it returns 0 (need more data)
                m_payload_bytes_consumed += bytes_consumed_this_parse;

                if (parse_result <= 0) {
                    return parse_result;  // Error or need more data
                }

                // One RESP response parsed successfully
                m_responses_to_parse--;

                // Check if we need to parse more RESP responses from this header
                if (m_responses_to_parse > 0) {
                    // More responses to parse, stay in payload state but reset for next RESP
                    m_response_state = rs_initial;
                } else {
                    // All responses from this header parsed, reset to initial state
                    m_response_header_state = rhs_initial;
                }

                return 1;
            }

            default:
                return -1;
        }
    }
}

int redis_protocol::select_db(int db)
{
    int size = 0;
    char db_str[20];

    snprintf(db_str, sizeof(db_str)-1, "%d", db);
    size = evbuffer_add_printf(m_write_buf,
        "*2\r\n"
        "$6\r\n"
        "SELECT\r\n"
        "$%u\r\n"
        "%s\r\n",
        (unsigned int)strlen(db_str), db_str);
    return size;
}

int redis_protocol::authenticate(const char *credentials)
{
    int size = 0;
    assert(credentials != NULL);

    /* Credentials may be one of:
     * <PASSWORD>           For Redis <6.0 simple AUTH commands.
     * <USER>:<PASSWORD>    For Redis 6.0+ AUTH with both username and password.
     *
     * A :<PASSWORD> will be handled as a special case of quoting a password that
     * contains a colon.
     */

    const char *user = NULL;
    const char *password;

    if (credentials[0] == ':') {
        password = credentials + 1;
    } else {
        password = strchr(credentials, ':');
        if (!password) {
            password = credentials;
        } else {
            user = credentials;
            password++;
        }
    }

    if (!user) {
        size = evbuffer_add_printf(m_write_buf,
            "*2\r\n"
            "$4\r\n"
            "AUTH\r\n"
            "$%zu\r\n"
            "%s\r\n",
            strlen(password), password);
    } else {
        size_t user_len = password - user - 1;
        size = evbuffer_add_printf(m_write_buf,
            "*3\r\n"
            "$4\r\n"
            "AUTH\r\n"
            "$%zu\r\n"
            "%.*s\r\n"
            "$%zu\r\n"
            "%s\r\n",
            user_len,
            (int) user_len,
            user,
            strlen(password),
            password);
    }
    return size;
}

int redis_protocol::configure_protocol(enum PROTOCOL_TYPE type) {
    int size = 0;
    if (type == PROTOCOL_RESP2 || type == PROTOCOL_RESP3) {
        m_resp3 = type == PROTOCOL_RESP3;
        size = evbuffer_add_printf(m_write_buf,
                                   "*2\r\n"
                                   "$5\r\n"
                                   "HELLO\r\n"
                                   "$1\r\n"
                                   "%d\r\n",
                                   type == PROTOCOL_RESP2 ? 2 : 3);
    }
    return size;
}

int redis_protocol::write_command_cluster_slots()
{
    int size = 0;

    size = evbuffer_add(m_write_buf,
                        "*2\r\n"
                        "$7\r\n"
                        "CLUSTER\r\n"
                        "$5\r\n"
                        "SLOTS\r\n",
                        28);

    return size;
}

int redis_protocol::write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);
    assert(value != NULL);
    assert(value_len > 0);
    int size = 0;

    if (!expiry && !offset) {
        size = evbuffer_add_printf(m_write_buf,
            "*3\r\n"
            "$3\r\n"
            "SET\r\n"
            "$%u\r\n", key_len);
        evbuffer_add(m_write_buf, key, key_len);
        size += key_len;
        size += evbuffer_add_printf(m_write_buf,
            "\r\n"
            "$%u\r\n", value_len);
    } else if(offset) {
        char offset_str[30];
        snprintf(offset_str, sizeof(offset_str)-1, "%u", offset);

        size = evbuffer_add_printf(m_write_buf,
            "*4\r\n"
            "$8\r\n"
            "SETRANGE\r\n"
            "$%u\r\n", key_len);
        evbuffer_add(m_write_buf, key, key_len);
        size += key_len;
        size += evbuffer_add_printf(m_write_buf,
            "\r\n"
            "$%u\r\n"
            "%s\r\n"
            "$%u\r\n", (unsigned int) strlen(offset_str), offset_str, value_len);
    } else {
        char expiry_str[30];
        snprintf(expiry_str, sizeof(expiry_str)-1, "%u", expiry);

        size = evbuffer_add_printf(m_write_buf,
            "*4\r\n"
            "$5\r\n"
            "SETEX\r\n"
            "$%u\r\n", key_len);
        evbuffer_add(m_write_buf, key, key_len);
        size += key_len;
        size += evbuffer_add_printf(m_write_buf,
            "\r\n"
            "$%u\r\n"
            "%s\r\n"
            "$%u\r\n", (unsigned int) strlen(expiry_str), expiry_str, value_len);
    }
    evbuffer_add(m_write_buf, value, value_len);
    evbuffer_add(m_write_buf, "\r\n", 2);
    size += value_len + 2;

    return size;
}

int redis_protocol::write_command_multi_get(const keylist *keylist)
{
    fprintf(stderr, "error: multi get not implemented for redis yet!\n");
    assert(0);
}

int redis_protocol::write_command_get(const char *key, int key_len, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);
    int size = 0;

    if (!offset) {
        size = evbuffer_add_printf(m_write_buf,
            "*2\r\n"
            "$3\r\n"
            "GET\r\n"
            "$%u\r\n", key_len);
        evbuffer_add(m_write_buf, key, key_len);
        evbuffer_add(m_write_buf, "\r\n", 2);
        size += key_len + 2;
    } else {
        char offset_str[30];
        snprintf(offset_str, sizeof(offset_str)-1, "%u", offset);

        size = evbuffer_add_printf(m_write_buf,
            "*4\r\n"
            "$8\r\n"
            "GETRANGE\r\n"
            "$%u\r\n", key_len);
        evbuffer_add(m_write_buf, key, key_len);
        size += key_len;
        size += evbuffer_add_printf(m_write_buf,
            "\r\n"
            "$%u\r\n"
            "%s\r\n"
            "$2\r\n"
            "-1\r\n", (unsigned int) strlen(offset_str), offset_str);
    }

    return size;
}

/*
 * Utility function to get the number of digits in a number
 */
static int get_number_length(unsigned int num)
{
    if (num < 10) return 1;
    if (num < 100) return 2;
    if (num < 1000) return 3;
    if (num < 10000) return 4;
    if (num < 100000) return 5;
    if (num < 1000000) return 6;
    if (num < 10000000) return 7;
    if (num < 100000000) return 8;
    if (num < 1000000000) return 9;
    return 10;
}

int redis_protocol::write_command_wait(unsigned int num_slaves, unsigned int timeout)
{
    int size = 0;
    size = evbuffer_add_printf(m_write_buf,
                               "*3\r\n"
                               "$4\r\n"
                               "WAIT\r\n"
                               "$%u\r\n"
                               "%u\r\n"
                               "$%u\r\n"
                               "%u\r\n",
                               get_number_length(num_slaves), num_slaves,
                               get_number_length(timeout), timeout);
    return size;
}

bool redis_protocol::aggregate_type(char c) {
    if (c == '*')
        return true;

    if (m_resp3 && (c == '%' || c == '~' || c == '|'))
        return true;

    return false;
}

bool redis_protocol::blob_type(char c) {
    if (c == '$')
        return true;

    if (m_resp3 && (c == '!' || c == '='))
        return true;

    return false;
}

bool redis_protocol::single_type(char c) {
    if (c == '+' || c == '-' || c == ':')
        return true;

    if (m_resp3 && (c == '_' || c == ',' || c == '#' || c == '('))
        return true;

    return false;
}

bool redis_protocol::response_ended() {
    if (m_total_bulks_count != 0)
        return false;

    if (m_attribute) {
        m_attribute = false;
        return false;
    }

    return true;
}

int redis_protocol::parse_response(void)
{
    char *line;
    size_t res_len;

    while (true) {
        switch (m_response_state) {
            case rs_initial:
                // clear last response
                m_last_response.clear();
                m_response_len = 0;
                m_total_bulks_count = 0;
                m_attribute = 0;
                m_response_state = rs_read_line;

                break;
            case rs_read_line:
                line = evbuffer_readln(m_read_buf, &res_len, EVBUFFER_EOL_CRLF_STRICT);

                // maybe we didn't get it yet?
                if (line == NULL) {
                    return 0;
                }

                // count CRLF
                m_response_len += res_len + 2;

                if (aggregate_type(line[0])) {
                    int count = strtol(line + 1, NULL, 10);

                    // in case of nested mbulk, the mbulk is one of the total bulks
                    if (m_total_bulks_count > 0) {
                        m_total_bulks_count--;
                    }

                    // from bulks counter perspective every count < 0 is equal to 0, because it's not followed by bulks.
                    if (count < 0) {
                        count = 0;
                    }

                    if (line[0] == '|') {
                        // Nested attribute?
                        assert(!m_attribute);

                        m_attribute = true;
                    }

                    // Map or Attribute contain key-value pair
                    if (line[0] == '%' || line[0] == '|') {
                        count *= 2;
                    }

                    if (m_keep_value) {
                        mbulk_size_el* new_mbulk_size = new mbulk_size_el();
                        new_mbulk_size->bulks_count = count;
                        new_mbulk_size->upper_level = m_current_mbulk;

                        // update first mbulk as the response mbulk, or insert it to current mbulk
                        if (m_last_response.get_mbulk_value() == NULL) {
                            m_last_response.set_mbulk_value(new_mbulk_size);
                        } else {
                            m_current_mbulk->add_new_element(new_mbulk_size);
                        }

                        // update current mbulk
                        m_current_mbulk = new_mbulk_size->get_next_mbulk();
                    }

                    m_last_response.set_status(line);
                    m_total_bulks_count += count;

                    if (response_ended()) {
                        m_last_response.set_total_len(m_response_len);
                        m_response_state = rs_initial;
                        return 1;
                    }
                } else if (blob_type(line[0])) {
                    // if it's single bulk (not part of mbulk), we count it here
                    if (m_total_bulks_count == 0) {
                        m_total_bulks_count++;
                    }

                    m_bulk_len = strtol(line + 1, NULL, 10);
                    m_last_response.set_status(line);

                    if (line[0] == '!')
                        m_last_response.set_error();

                    /*
                     * only on negative bulk, the data ends right after the first CRLF ($-1\r\n), so
                     * we skip on rs_read_bulk and jump into rs_end_bulk
                     */
                    if (m_bulk_len < 0) {
                        m_response_state = rs_end_bulk;
                    } else {
                        m_response_state = rs_read_bulk;
                    }
                } else if (single_type(line[0])) {
                    // if it's single bulk (not part of mbulk), we count it here
                    if (m_total_bulks_count == 0) {
                        m_total_bulks_count++;
                    }

                    // if we are not inside mbulk, the status will be kept in m_status anyway
                    if (m_keep_value && m_current_mbulk) {
                        char *bulk_value = strdup(line);
                        assert(bulk_value != NULL);

                        bulk_el* new_bulk = new bulk_el();
                        new_bulk->value = bulk_value;
                        new_bulk->value_len = strlen(bulk_value);

                        // insert it to current mbulk
                        m_current_mbulk->add_new_element(new_bulk);
                        m_current_mbulk = m_current_mbulk->get_next_mbulk();
                    }

                    if (line[0] == '-')
                        m_last_response.set_error();

                    m_last_response.set_status(line);
                    m_total_bulks_count--;

                    if (response_ended()) {
                        m_last_response.set_total_len(m_response_len);
                        m_response_state = rs_initial;
                        return 1;
                    }
                } else {
                    benchmark_debug_log("unsupported response: '%s'.\n", line);
                    free(line);
                    return -1;
                }
                break;
            case rs_read_bulk:
                if (get_available_bytes() >= (unsigned long)(m_bulk_len + 2)) {
                    m_response_len += m_bulk_len + 2;

                    /*
                     * KNOWN ISSUE:
                     * in case of key with zero size (SET X "") that will return $0,
                     * we are not counting it as "hit", and the report will be wrong.
                     * currently this is a limitation because GETRANGE returns $0 for
                     * such key as well as non existing key or existing key without data
                     * in the requested range
                     */
                    if (m_bulk_len > 0) {
                        m_last_response.incr_hits();
                    }

                    m_response_state = rs_end_bulk;
                } else {
                    return 0;
                }
                break;
            case rs_end_bulk:
                if (m_keep_value) {
                    /*
                     * keep bulk value - in case we need to save bulk value it depends
                     * if it's inside a mbulk or not.
                     * in case of receiving just bulk as a response, we save it directly to the m_last_response,
                     * otherwise we insert it to the current mbulk element
                     */
                    char *bulk_value = NULL;
                    int ret;
                    if (m_bulk_len > 0) {
                        bulk_value = (char *) malloc(m_bulk_len);
                        assert(bulk_value != NULL);

                        ret = evbuffer_remove(m_read_buf, bulk_value, m_bulk_len);
                        assert(ret != -1);
                    }

                    // drain last CRLF, zero bulk also includes it ($0\r\n\r\n)
                    if (m_bulk_len >= 0) {
                        ret = evbuffer_drain(m_read_buf, 2);
                        assert(ret != -1);
                    }

                    // in case we are inside mbulk
                    if (m_current_mbulk) {
                        bulk_el* new_bulk = new bulk_el();
                        new_bulk->value = bulk_value;
                        // negative bulk len counted as empty bulk
                        new_bulk->value_len = m_bulk_len > 0 ? m_bulk_len : 0;

                        // insert it to current mbulk
                        m_current_mbulk->add_new_element(new_bulk);
                        m_current_mbulk = m_current_mbulk->get_next_mbulk();
                    } else {
                        // negative bulk len counted as empty bulk
                        m_last_response.set_value(bulk_value, m_bulk_len > 0 ? m_bulk_len : 0);
                    }
                } else {
                    // just drain the buffer, include the CRLF
                    if (m_bulk_len >= 0) {
                        int ret = evbuffer_drain(m_read_buf, m_bulk_len + 2);
                        assert(ret != -1);
                    }
                }

                m_total_bulks_count--;

                if (response_ended()) {
                    m_last_response.set_total_len(m_response_len);
                    m_response_state = rs_initial;
                    return 1;
                } else {
                    m_response_state = rs_read_line;
                }
                break;
            default:
                return -1;
        }
    }

    return -1;
}

int redis_protocol::write_arbitrary_command(const command_arg *arg) {
    evbuffer_add(m_write_buf, arg->data.c_str(), arg->data.length());

    return arg->data.length();
}

int redis_protocol::write_arbitrary_command(const char *rand_val, int rand_val_len) {
    int size = 0;

    size = evbuffer_add_printf(m_write_buf, "$%d\r\n", rand_val_len);
    evbuffer_add(m_write_buf, rand_val, rand_val_len);
    size += rand_val_len;
    evbuffer_add(m_write_buf, "\r\n", 2);
    size += 2;

    return size;
}

bool redis_protocol::format_arbitrary_command(arbitrary_command &cmd) {
    for (unsigned int i = 0; i < cmd.command_args.size(); i++) {
        command_arg* current_arg = &cmd.command_args[i];
        current_arg->type = const_type;

        // check arg type
        if (current_arg->data.find(KEY_PLACEHOLDER) != std::string::npos) {
            if (current_arg->data.length() != strlen(KEY_PLACEHOLDER)) {
                benchmark_error_log("error: key placeholder can't combined with other data\n");
                return false;
            }
            cmd.keys_count++;
            current_arg->type = key_type;
        } else if (current_arg->data.find(DATA_PLACEHOLDER) != std::string::npos) {
            if (current_arg->data.length() != strlen(DATA_PLACEHOLDER)) {
                benchmark_error_log("error: data placeholder can't combined with other data\n");
                return false;
            }

            current_arg->type = data_type;
        }

        // we expect that first arg is the COMMAND name
        assert(i != 0 || (i == 0 && current_arg->type == const_type && "first arg is not command name?"));

        if (current_arg->type == const_type) {
            char buffer[20];
            int buffer_len;

            // if it's first arg we add also the mbulk size
            if (i == 0) {
                buffer_len = snprintf(buffer, 20, "*%zd\r\n$%zd\r\n", cmd.command_args.size(), current_arg->data.length());
            } else {
                buffer_len = snprintf(buffer, 20, "$%zd\r\n", current_arg->data.length());
            }

            current_arg->data.insert(0, buffer, buffer_len);
            current_arg->data += "\r\n";
        }
    }

    return true;
}

/////////////////////////////////////////////////////////////////////////

class memcache_text_protocol : public abstract_protocol {
protected:
    enum response_state { rs_initial, rs_read_section, rs_read_value, rs_read_end };
    response_state m_response_state;
    unsigned int m_value_len;
    size_t m_response_len;
public:
    memcache_text_protocol() : m_response_state(rs_initial), m_value_len(0), m_response_len(0) { }
    virtual memcache_text_protocol* clone(void) { return new memcache_text_protocol(); }
    virtual int select_db(int db);
    virtual int authenticate(const char *credentials);
    virtual int configure_protocol(enum PROTOCOL_TYPE type);
    virtual int write_command_cluster_slots();
    virtual int write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset);
    virtual int write_command_get(const char *key, int key_len, unsigned int offset);
    virtual int write_command_multi_get(const keylist *keylist);
    virtual int write_command_wait(unsigned int num_slaves, unsigned int timeout);
    virtual int parse_response(void);

    // handle arbitrary command
    virtual bool format_arbitrary_command(arbitrary_command& cmd);
    virtual int write_arbitrary_command(const command_arg *arg);
    virtual int write_arbitrary_command(const char *val, int val_len);

};

int memcache_text_protocol::select_db(int db)
{
    assert(0);
}

int memcache_text_protocol::authenticate(const char *credentials)
{
    assert(0);
}

int memcache_text_protocol::configure_protocol(enum PROTOCOL_TYPE type)
{
    assert(0);
}

int memcache_text_protocol::write_command_cluster_slots()
{
    assert(0);
}

int memcache_text_protocol::write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);
    assert(value != NULL);
    assert(value_len > 0);
    int size = 0;

    size = evbuffer_add_printf(m_write_buf,
        "set %.*s 0 %u %u\r\n", key_len, key, expiry, value_len);
    evbuffer_add(m_write_buf, value, value_len);
    evbuffer_add(m_write_buf, "\r\n", 2);
    size += value_len + 2;

    return size;
}

int memcache_text_protocol::write_command_get(const char *key, int key_len, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);
    int size = 0;

    size = evbuffer_add_printf(m_write_buf,
        "get %.*s\r\n", key_len, key);
    return size;
}

int memcache_text_protocol::write_command_multi_get(const keylist *keylist)
{
    assert(keylist != NULL);
    assert(keylist->get_keys_count() > 0);

    int n = 0;
    int size = 0;

    n = evbuffer_add(m_write_buf, "get", 3);
    assert(n != -1);
    size = 3;

    for (unsigned int i = 0; i < keylist->get_keys_count(); i++) {
        const char *key;
        unsigned int key_len;

        n = evbuffer_add(m_write_buf, " ", 1);
        assert(n != -1);
        size++;

        key = keylist->get_key(i, &key_len);
        assert(key != NULL);

        n = evbuffer_add(m_write_buf, key, key_len);
        assert(n != -1);
        size += key_len;
    }

    n = evbuffer_add(m_write_buf, "\r\n", 2);
    assert(n != -1);
    size += 2;

    return size;
}

int memcache_text_protocol::write_command_wait(unsigned int num_slaves, unsigned int timeout)
{
    fprintf(stderr, "error: WAIT command not implemented for memcache!\n");
    assert(0);
}

int memcache_text_protocol::parse_response(void)
{
    char *line;
    size_t tmplen;

    while (true) {
        switch (m_response_state) {
            case rs_initial:
                m_last_response.clear();
                m_response_state = rs_read_section;
                m_response_len = 0;
                break;

            case rs_read_section:
                line = evbuffer_readln(m_read_buf, &tmplen, EVBUFFER_EOL_CRLF_STRICT);
                if (!line)
                    return 0;

                m_response_len += tmplen + 2;   // For CRLF
                if (m_last_response.get_status() == NULL) {
                    m_last_response.set_status(line);
                }
                m_last_response.set_total_len((unsigned int) m_response_len);   // for now...

                if (memcmp(line, "VALUE", 5) == 0) {
                    char prefix[50];
                    char key[256];
                    unsigned int flags;
                    unsigned int cas;

                    int res = sscanf(line, "%s %s %u %u %u", prefix, key, &flags, &m_value_len, &cas);
                    if (res < 4|| res > 5) {
                        benchmark_debug_log("unexpected VALUE response: %s\n", line);
                        if (m_last_response.get_status() != line)
                            free(line);
                        return -1;
                    }

                    m_response_state = rs_read_value;
                    continue;
                } else if (memcmp(line, "END", 3) == 0 ||
                           memcmp(line, "STORED", 6) == 0) {
                    if (m_last_response.get_status() != line)
                        free(line);
                    m_response_state = rs_read_end;
                    break;
                } else {
                    m_last_response.set_error();
                    benchmark_debug_log("unknown response: %s\n", line);
                    return -1;
                }
                break;

            case rs_read_value:
                if (evbuffer_get_length(m_read_buf) >= m_value_len + 2) {
                    if (m_keep_value) {
                        char *value = (char *) malloc(m_value_len);
                        assert(value != NULL);

                        int ret = evbuffer_remove(m_read_buf, value, m_value_len);
                        assert((unsigned int) ret == 0);

                        m_last_response.set_value(value, m_value_len);
                    } else {
                        int ret = evbuffer_drain(m_read_buf, m_value_len);
                        assert((unsigned int) ret == 0);
                    }

                    int ret = evbuffer_drain(m_read_buf, 2);
                    assert((unsigned int) ret == 0);

                    m_last_response.incr_hits();
                    m_response_len += m_value_len + 2;
                    m_response_state = rs_read_section;
                } else {
                    return 0;
                }
                break;
            case rs_read_end:
                m_response_state = rs_initial;
                return 1;

            default:
                benchmark_debug_log("unknown response state %d.\n", m_response_state);
                return -1;
        }
    }

    return -1;
}

bool memcache_text_protocol::format_arbitrary_command(arbitrary_command& cmd) {
    assert(0);
}

int memcache_text_protocol::write_arbitrary_command(const command_arg *arg) {
    assert(0);
}

int memcache_text_protocol::write_arbitrary_command(const char *val, int val_len) {
    assert(0);
}

/////////////////////////////////////////////////////////////////////////

class memcache_binary_protocol : public abstract_protocol {
protected:
    enum response_state { rs_initial, rs_read_body };
    response_state m_response_state;
    protocol_binary_response_no_extras m_response_hdr;
    size_t m_response_len;

    const char* status_text(void);
public:
    memcache_binary_protocol() : m_response_state(rs_initial), m_response_len(0) { }
    virtual memcache_binary_protocol* clone(void) { return new memcache_binary_protocol(); }
    virtual int select_db(int db);
    virtual int authenticate(const char *credentials);
    virtual int configure_protocol(enum PROTOCOL_TYPE type);
    virtual int write_command_cluster_slots();
    virtual int write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset);
    virtual int write_command_get(const char *key, int key_len, unsigned int offset);
    virtual int write_command_multi_get(const keylist *keylist);
    virtual int write_command_wait(unsigned int num_slaves, unsigned int timeout);
    virtual int parse_response(void);

    // handle arbitrary command
    virtual bool format_arbitrary_command(arbitrary_command& cmd);
    virtual int write_arbitrary_command(const command_arg *arg);
    virtual int write_arbitrary_command(const char *val, int val_len);
};

int memcache_binary_protocol::select_db(int db)
{
    assert(0);
}

int memcache_binary_protocol::authenticate(const char *credentials)
{
    protocol_binary_request_no_extras req;
    char nullbyte = '\0';
    const char mechanism[] = "PLAIN";
    int mechanism_len = sizeof(mechanism) - 1;
    const char *colon;
    const char *user;
    int user_len;
    const char *passwd;
    int passwd_len;

    assert(credentials != NULL);
    colon = strchr(credentials, ':');
    assert(colon != NULL);

    user = credentials;
    user_len = colon - user;
    passwd = colon + 1;
    passwd_len = strlen(passwd);

    memset(&req, 0, sizeof(req));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_SASL_AUTH;
    req.message.header.request.keylen = htons(mechanism_len);
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.bodylen = htonl(mechanism_len + user_len + passwd_len + 2);
    evbuffer_add(m_write_buf, &req, sizeof(req));
    evbuffer_add(m_write_buf, mechanism, mechanism_len);
    evbuffer_add(m_write_buf, &nullbyte, 1);
    evbuffer_add(m_write_buf, user, user_len);
    evbuffer_add(m_write_buf, &nullbyte, 1);
    evbuffer_add(m_write_buf, passwd, passwd_len);

    return sizeof(req) + user_len + passwd_len + 2 + sizeof(mechanism) - 1;
}

int memcache_binary_protocol::configure_protocol(enum PROTOCOL_TYPE type) {
    assert(0);
}

int memcache_binary_protocol::write_command_cluster_slots()
{
    assert(0);
}

int memcache_binary_protocol::write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);
    assert(value != NULL);
    assert(value_len > 0);

    protocol_binary_request_set req;

    memset(&req, 0, sizeof(req));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_SET;
    req.message.header.request.keylen = htons(key_len);
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.bodylen = htonl(sizeof(req.message.body) + value_len + key_len);
    req.message.header.request.extlen = sizeof(req.message.body);
    req.message.body.expiration = htonl(expiry);

    evbuffer_add(m_write_buf, &req, sizeof(req));
    evbuffer_add(m_write_buf, key, key_len);
    evbuffer_add(m_write_buf, value, value_len);

    return sizeof(req) + key_len + value_len;
}

int memcache_binary_protocol::write_command_get(const char *key, int key_len, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);

    protocol_binary_request_get req;

    memset(&req, 0, sizeof(req));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_GET;
    req.message.header.request.keylen = htons(key_len);
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.bodylen = htonl(key_len);
    req.message.header.request.extlen = 0;

    evbuffer_add(m_write_buf, &req, sizeof(req));
    evbuffer_add(m_write_buf, key, key_len);

    return sizeof(req) + key_len;
}

int memcache_binary_protocol::write_command_multi_get(const keylist *keylist)
{
    fprintf(stderr, "error: multi get not implemented for binary memcache yet!\n");
    assert(0);
}

const char* memcache_binary_protocol::status_text(void)
{
    int status;
    static const char* status_str_00[] = {
        "PROTOCOL_BINARY_RESPONSE_SUCCESS",
        "PROTOCOL_BINARY_RESPONSE_KEY_ENOENT",
        "PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS",
        "PROTOCOL_BINARY_RESPONSE_E2BIG",
        "PROTOCOL_BINARY_RESPONSE_EINVAL",
        "PROTOCOL_BINARY_RESPONSE_NOT_STORED",
        "PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL",
        "PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET",
    };
    static const char* status_str_20[] = {
        "PROTOCOL_BINARY_RESPONSE_AUTH_ERROR",
        "PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE"
    };
    static const char* status_str_80[] = {
        NULL,
        "PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND",
        "PROTOCOL_BINARY_RESPONSE_ENOMEM",
        "PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED",
        "PROTOCOL_BINARY_RESPONSE_EINTERNAL",
        "PROTOCOL_BINARY_RESPONSE_EBUSY",
        "PROTOCOL_BINARY_RESPONSE_ETMPFAIL"
    };

    status = ntohs(m_response_hdr.message.header.response.status);
    if (status <= 0x07) {
        return status_str_00[status];
    } else if (status >= 0x20 && status <= 0x21) {
        return status_str_20[status - 0x20];
    } else if (status >= 0x80 && status <= 0x86) {
        return status_str_80[status - 0x80];
    } else {
        return NULL;
    }
}

int memcache_binary_protocol::write_command_wait(unsigned int num_slaves, unsigned int timeout)
{
    fprintf(stderr, "error: WAIT command not implemented for memcache!\n");
    assert(0);
}

int memcache_binary_protocol::parse_response(void)
{
    while (true) {
        int ret;
        int status;

        switch (m_response_state) {
            case rs_initial:
                if (evbuffer_get_length(m_read_buf) < sizeof(m_response_hdr))
                    return 0;               // no header yet?

                ret = evbuffer_remove(m_read_buf, (void *)&m_response_hdr, sizeof(m_response_hdr));
                assert(ret == sizeof(m_response_hdr));

                if (m_response_hdr.message.header.response.magic != PROTOCOL_BINARY_RES) {
                    benchmark_error_log("error: invalid memcache response header magic.\n");
                    return -1;
                }

                m_response_len = sizeof(m_response_hdr);
                m_last_response.clear();
                if (status_text()) {
                    m_last_response.set_status(strdup(status_text()));
                }

                status = ntohs(m_response_hdr.message.header.response.status);
                if (status == PROTOCOL_BINARY_RESPONSE_AUTH_ERROR ||
                    status == PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE ||
                    status == PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED ||
                    status == PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND ||
                    status == PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED ||
                    status == PROTOCOL_BINARY_RESPONSE_EBUSY) {
                    m_last_response.set_error();
                }

                if (ntohl(m_response_hdr.message.header.response.bodylen) > 0) {
                    m_response_hdr.message.header.response.bodylen = ntohl(m_response_hdr.message.header.response.bodylen);
                    m_response_hdr.message.header.response.keylen = ntohs(m_response_hdr.message.header.response.keylen);

                    m_response_state = rs_read_body;
                    continue;
                }

                return 1;
                break;
            case rs_read_body:
                if (evbuffer_get_length(m_read_buf) >= m_response_hdr.message.header.response.bodylen) {
                    // get rid of extras and key, we don't care about them
                    ret = evbuffer_drain(m_read_buf,
                        m_response_hdr.message.header.response.extlen +
                        m_response_hdr.message.header.response.keylen);
                    assert((unsigned int) ret == 0);

                    int actual_body_len = m_response_hdr.message.header.response.bodylen -
                        m_response_hdr.message.header.response.extlen -
                        m_response_hdr.message.header.response.keylen;
                    if (m_keep_value) {
                        char *value = (char *) malloc(actual_body_len);
                        assert(value != NULL);
                        ret = evbuffer_remove(m_read_buf, value, actual_body_len);
                        m_last_response.set_value(value, actual_body_len);
                    } else {
                        int ret = evbuffer_drain(m_read_buf, actual_body_len);
                        assert((unsigned int) ret == 0);
                    }

                    if (m_response_hdr.message.header.response.status == PROTOCOL_BINARY_RESPONSE_SUCCESS)
                        m_last_response.incr_hits();

                    m_response_len += m_response_hdr.message.header.response.bodylen;
                    m_response_state = rs_initial;

                    return 1;
                } else {
                    return 0;
                }
                break;
            default:
                benchmark_debug_log("unknown response state.\n");
                return -1;
        }
    }

    return -1;
}

bool memcache_binary_protocol::format_arbitrary_command(arbitrary_command& cmd) {
    assert(0);
}

int memcache_binary_protocol::write_arbitrary_command(const command_arg *arg) {
    assert(0);
}

int memcache_binary_protocol::write_arbitrary_command(const char *val, int val_len) {
    assert(0);
}

/////////////////////////////////////////////////////////////////////////

class abstract_protocol *protocol_factory(enum PROTOCOL_TYPE type)
{
    if (type == PROTOCOL_REDIS_FAST_HEADER) {
        return new fast_header_protocol();
    } else if (is_redis_protocol(type)) {
        return new redis_protocol();
    } else if (type == PROTOCOL_MEMCACHE_TEXT) {
        return new memcache_text_protocol();
    } else if (type == PROTOCOL_MEMCACHE_BINARY) {
        return new memcache_binary_protocol();
    } else {
        benchmark_error_log("Error: unknown protocol type: %d.\n", type);
        return NULL;
    }
}

/////////////////////////////////////////////////////////////////////////

keylist::keylist(unsigned int max_keys) :
    m_buffer(NULL), m_buffer_ptr(NULL), m_buffer_size(0),
    m_keys(NULL), m_keys_size(0), m_keys_count(0)
{
    m_keys_size = max_keys;
    m_keys = (key_entry *) malloc(m_keys_size * sizeof(key_entry));
    assert(m_keys != NULL);
    memset(m_keys, 0, m_keys_size * sizeof(key_entry));

    /* allocate buffer for actual keys */
    m_buffer_size = 256 * m_keys_size;
    m_buffer = (char *) malloc(m_buffer_size);
    assert(m_buffer != NULL);
    memset(m_buffer, 0, m_buffer_size);

    m_buffer_ptr = m_buffer;
}

keylist::~keylist()
{
    if (m_buffer != NULL) {
        free(m_buffer);
        m_buffer = NULL;
    }
    if (m_keys != NULL) {
        free(m_keys);
        m_keys = NULL;
    }
}

bool keylist::add_key(const char *key, unsigned int key_len)
{
    // have room?
    if (m_keys_count >= m_keys_size)
        return false;

    // have buffer?
    if (m_buffer_ptr + key_len >= m_buffer + m_buffer_size) {
        while (m_buffer_ptr + key_len >= m_buffer + m_buffer_size) {
            m_buffer_size *= 2;
        }
        m_buffer = (char *)realloc(m_buffer, m_buffer_size);
        assert(m_buffer != NULL);
    }

    // copy key
    memcpy(m_buffer_ptr, key, key_len);
    m_buffer_ptr[key_len] = '\0';
    m_keys[m_keys_count].key_ptr = m_buffer_ptr;
    m_keys[m_keys_count].key_len = key_len;

    m_buffer_ptr += key_len + 1;
    m_keys_count++;

    return true;
}

unsigned int keylist::get_keys_count(void) const
{
    return m_keys_count;
}

const char *keylist::get_key(unsigned int index, unsigned int *key_len) const
{
    if (index < 0 || index >= m_keys_count)
        return NULL;
    if (key_len != NULL)
        *key_len = m_keys[index].key_len;
    return m_keys[index].key_ptr;
}

void keylist::clear(void)
{
    m_keys_count = 0;
    m_buffer_ptr = m_buffer;
}

