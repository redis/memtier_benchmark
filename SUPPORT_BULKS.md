# Bulk Support for Skip Header Protocol in memtier_benchmark

## Overview

Currently, memtier_benchmark has a skip header protocol implementation that sends one command per header (see SKIP_HEADER_PROTOCOL.md).

This document specifies the enhancement to support **bulk commands**: multiple commands sent with a **single 16-byte header**.

## New Command-Line Arguments

### `--bulk-size`
- **Type**: unsigned integer
- **Default**: 1 (no bulk accumulation)
- **Description**: Number of commands to accumulate in one bulk before sending with a single header
- **Constraint**: Must be >= 1
- **Pipeline Interaction**: When `--bulk-size > 1`, the pipeline size is automatically set to match `--bulk-size` (if not explicitly specified by user). This ensures the benchmark generates enough commands to fill the bulk before waiting for responses. If user explicitly sets `--pipeline` to a value smaller than `--bulk-size`, an error is returned.

### `--bulk-slots`
- **Type**: unsigned integer
- **Default**: 16384 (number of Redis cluster slots)
- **Description**: Number of different slots to cycle through when generating keys
- **Constraint**: Must be >= 1

## Header Format with Bulks

The 16-byte header remains the same, but now serves multiple commands:

```
Byte 0:      Magic byte (0xAE)
Byte 1:      Version (0x01)
Bytes 2-3:   Slot (uint16_t, big-endian) - Redis cluster slot (0-16383)
Bytes 4-7:   Payload size (uint32_t, big-endian) - Total size of ALL RESP3 payloads (without header)
Byte 8:      Batch count (uint8_t) - Number of commands in this bulk
Bytes 9-12:  Request ID (uint32_t, big-endian) - One ID per header (applies to entire bulk)
Bytes 13-15: Reserved (3 bytes, set to 0)
```

**Key changes:**
- **Payload size**: Total size of all RESP3 commands in the bulk (sum of individual command sizes)
- **Batch count**: Number of commands in this bulk
- **Request ID**: One per header (not per command)

## Rule 1: Key Count Limit and Fixed-Size Keys

### Constraint
User specifies key range: `--key-minimum M --key-maximum N`

Maximum unique keys that can be generated: `(N - M + 1) / bulk_slots`

This maximum **must be >= bulk_size**, otherwise error.

**Error condition:**
```
(key_max - key_min + 1) / bulk_slots < bulk_size
```

### Key Generation Algorithm

Keys are generated with format: `{slot_id}:key_suffix`

**Slot ID Assignment**:
- Each command in one bulk will have the same `slot_id`
- `slot_id` can be in the range of `[0, bulk_slots)`
- The first bulk starts from some random number from this range (it is important that each client starts from a different number)
- Each next bulk will use the next number in a cyclic manner: `slot_id = (initial_slot_id + bulk_number) % bulk_slots`

**Key Suffix Assignment**:
- `key_suffix` is a number from the range `[0, keys_per_slot_id)`
- The very first command in the very first bulk will start from some random number from this range
- The next command will have the next `key_suffix` in a cyclic manner, **crossing bulks**
- `key_suffix` increments continuously: `0, 1, 2, ..., (keys_per_slot_id - 1), 0, 1, ...`

**Total unique keys**: `key_max - key_min + 1` (as specified by user)

**Keys per slot_id**: `keys_per_slot_id = (key_max - key_min + 1) / bulk_slots`

**Key pattern**: `{slot_id}:key_suffix` where:
- `slot_id` cycles from 0 to (bulk_slots - 1), one per bulk
- `key_suffix` continues sequentially from 0 to (keys_per_slot_id - 1), then repeats, across all bulks

### Example

- `--key-minimum 0 --key-maximum 999`
- `--bulk-size 6`
- `--bulk-slots 10`

Calculation:
- `keys_per_slot = (999 - 0 + 1) / 10 = 100`
- Check: `100 >= 6` ✓ (valid)
- Total keys to generate: 1000
- Assume first bulk starts at `slot_id = 3` and first command starts at `key_suffix = 7`

Key generation:
```
Bulk 1:   {3}:7,   {3}:8,   {3}:9,   {3}:10,  {3}:11,  {3}:12
Bulk 2:   {4}:13,  {4}:14,  {4}:15,  {4}:16,  {4}:17,  {4}:18
Bulk 3:   {5}:19,  {5}:20,  {5}:21,  {5}:22,  {5}:23,  {5}:24
...
Bulk 10:  {2}:61,  {2}:62,  {2}:63,  {2}:64,  {2}:65,  {2}:66
Bulk 11:  {3}:67,  {3}:68,  {3}:69,  {3}:70,  {3}:71,  {3}:72
Bulk 12:  {4}:73,  {4}:74,  {4}:75,  {4}:76,  {4}:77,  {4}:78
...
Bulk 16:  {1}:97,  {1}:98,  {1}:99,  {1}:0,   {1}:1,   {1}:2    (key_suffix wraps around)
Bulk 17:  {2}:3,   {2}:4,   {2}:5,   {2}:6,   {2}:7,   {2}:8
...
```

**Total bulks**: `1000 / 6 = 166.67`
- 166 full bulks × 6 keys = 996 keys
- 1 partial bulk × 4 keys = 4 keys


## Rule 3: Slot Consistency

**All commands in a single bulk MUST have keys from the same slot.**

This is enforced by the key generation algorithm (all keys in a bulk share the same `{slot_id}` prefix).


## Rule 6: Statistics Accuracy

**Critical requirement**: Statistics must be accurate and reflect real performance.

- **Count commands individually**: Each command in a bulk is counted separately in statistics
- **KB/sec calculation**: Include header bytes (16 bytes per bulk) in sent data size
  - Total bytes sent = (sum of all RESP3 command payloads) + (number of bulks × 16 bytes)
  - KB/sec = Total bytes sent / elapsed time
- **ops/sec**: Count total commands sent, not bulks
- **Statistics must match** the version without bulk support (when `--bulk-size 1`)

## Implementation Strategy: Temporary Buffer Accumulation

### Problem Statement

When implementing bulk support, we face a fundamental challenge:
- The header must precede the commands in the output buffer
- The header contains `payload_size` (total size of all commands in the bulk)
- We don't know the total payload size until all commands are generated
- For the last bulk, we may have fewer commands than `bulk_size`, making it a partial bulk

### Solution: Temporary Buffer per Bulk

**Algorithm**:

When `bulk_size > 1`:

1. **Accumulate commands in temporary buffer**:
   - For each command in the bulk:
     - Write the RESP3 command to the temporary buffer
     - Track the total payload size
     - Increment `m_accumulated_commands`

2. **When bulk is full OR test ends**:
   - Create the header with actual `batch_count` and `payload_size`
   - Write header to main output buffer
   - Write all accumulated commands from temporary buffer to main output buffer using `evbuffer_add_buffer()`
   - Reset temporary buffer and counters

**Key insight**: Commands must be accumulated in temporary buffer first because:
- We don't know the total `payload_size` until all commands are generated
- We don't know the final `batch_count` until all commands are generated
- The header must be correct before sending to the server

### Data Structure

Add to `skip_header_protocol` class:
```cpp
private:
    uint32_t m_bulk_payload_size;        // Total size of commands in current bulk
    uint8_t m_accumulated_commands;      // Number of commands accumulated in current bulk
    uint8_t m_bulk_size;                 // Target bulk size (from config)
    uint16_t m_current_slot;             // Slot ID for current bulk
    struct evbuffer* m_bulk_buffer;      // Temporary buffer for accumulating commands
```

### Return Values

- When accumulating (not yet sending): Return the command size (but nothing is sent to server yet)
- When finalizing and sending: Header + commands are written to main output buffer

## Response Tracking with Bulks

### Current Response Tracking Mechanism

**Current flow** (regardless of bulk_size):
1. `send_set_command()` or `send_get_command()` is called
2. Protocol writes the command to the output buffer (with header prepended)
3. A `request` object is created and pushed to the pipeline queue
4. When a response arrives, `process_response()` is called
5. `m_protocol->parse_response()` parses one response from the input buffer
6. `pop_req()` removes one request from the pipeline queue
7. The request and response are matched 1:1
8. Statistics are updated: `m_conns_manager->handle_response()`
9. Request is deleted

**Key principle**: One command sent = One request in pipeline = One response from server

**Current request structure**:
```cpp
struct request {
    request_type m_type;           // rt_set, rt_get, rt_wait, etc.
    struct timeval m_sent_time;    // When the command was sent
    unsigned int m_size;           // Size of the command
    unsigned int m_keys;           // Number of keys (1 for SET/GET, N for MGET)
};
```

### Why Bulks Don't Change Response Tracking

**Key insight**: Bulks are purely a transmission optimization. They don't change the fundamental request/response matching:

- We send X commands → X requests pushed to pipeline
- Redis receives X commands → Sends back X responses
- We receive X responses → X requests popped from pipeline
- Each response matches exactly one request (1:1 matching preserved)

**The only difference**: Multiple commands share a single 16-byte header for transmission efficiency.

**Example with bulk_size=6**:
```
Sending phase:
  write_command_set(key1) → Write header + command 1, push request 1
  write_command_set(key2) → Write command 2, push request 2
  write_command_set(key3) → Write command 3, push request 3
  write_command_set(key4) → Write command 4, push request 4
  write_command_set(key5) → Write command 5, push request 5
  write_command_set(key6) → Write command 6, push request 6
  (All 6 commands + 1 header buffered together in evbuffer)

Transmission phase:
  Event loop sends entire bulk to Redis

Receiving phase:
  Response 1 arrives → pop_req() → match with request 1
  Response 2 arrives → pop_req() → match with request 2
  Response 3 arrives → pop_req() → match with request 3
  Response 4 arrives → pop_req() → match with request 4
  Response 5 arrives → pop_req() → match with request 5
  Response 6 arrives → pop_req() → match with request 6
```

### No Changes to Response Processing Logic

**The existing `process_response()` flow remains unchanged**:
```cpp
while ((ret = m_protocol->parse_response()) > 0) {
    protocol_response *r = m_protocol->get_response();

    // Pop request (exactly as before)
    request* req = pop_req();

    // Process response (exactly as before)
    m_conns_manager->handle_response(m_id, now, req, r);
    m_conns_manager->inc_reqs_processed();

    // Delete request (exactly as before)
    delete req;
}
```

**No peek-not-pop needed**: Each response immediately matches one request, so we can pop immediately.

### Implementation Details

**In `write_command_set()` and `write_command_get()`**:

When `bulk_size > 1`:
1. **First command in bulk** (`m_accumulated_commands == 0`):
   - Reserve 16 bytes for header in evbuffer
   - Write placeholder header (batch_count=0, payload_size=0)
   - Write RESP3 command to evbuffer
   - Increment `m_accumulated_commands` to 1
   - Return size (16 + command_size)

2. **Subsequent commands** (`0 < m_accumulated_commands < m_bulk_size`):
   - Write RESP3 command to evbuffer
   - Increment `m_accumulated_commands`
   - Return command_size

3. **Last command in bulk** (`m_accumulated_commands == m_bulk_size`):
   - Write RESP3 command to evbuffer
   - Update header in-place with correct `payload_size` and `batch_count`
   - Reset `m_accumulated_commands` to 0
   - Return command_size

**In `send_set_command()` and `send_get_command()`**:
- No changes needed
- Still call `write_command_set()` or `write_command_get()`
- Still push one request per command
- Still use existing `push_req()` function

**In `process_response()`**:
- No changes needed
- Still pop one request per response
- Still use existing `pop_req()` function
- Still match 1:1 as before

### Handling Partial Bulks

**Problem 1**: When the test ends (by reaching request count or time limit), there may be a partial bulk in the buffer that hasn't been sent yet.

**Example**:
- `bulk_size = 6`
- Test ends after 14 commands
- First bulk: 6 commands (complete)
- Second bulk: 6 commands (complete)
- Third bulk: 2 commands (partial, needs to be flushed)

**Problem 2**: The `finished()` function was checking `m_reqs_processed` (responses received) instead of `m_reqs_generated` (commands generated). This caused a deadlock with partial bulks:
- All commands generated, but last bulk is partial (not sent yet)
- `finished()` returns false because not all responses received
- `fill_pipeline()` doesn't flush the partial bulk
- Benchmark waits for responses that will never come (commands not sent)

**Solution**:
1. Change `finished()` to check `m_reqs_generated >= m_config->requests` instead of `m_reqs_processed`
2. Add a flush mechanism to finalize partial bulks when test finishes

**In `client.cpp`** (client::finished() function):

Change from checking `m_reqs_processed` to `m_reqs_generated`:
```cpp
bool client::finished(void)
{
    if (m_config->requests > 0 && m_reqs_generated >= m_config->requests)
        return true;
    if (m_config->test_time > 0 && m_stats.get_duration() >= m_config->test_time)
        return true;
    return false;
}
```

**In `shard_connection.cpp`**:

Add a new method `flush_bulk()`:
```cpp
void shard_connection::flush_bulk() {
    // If there's a partial bulk in progress, finalize it
    if (m_protocol->has_partial_bulk()) {
        m_protocol->finalize_partial_bulk();
    }
}
```

**In `fill_pipeline()` function** (after the while loop):
```cpp
void shard_connection::fill_pipeline(void) {
    struct timeval now;
    gettimeofday(&now, NULL);

    while (!m_conns_manager->finished() && m_pipeline->size() < m_config->pipeline) {
        // ... existing code ...
    }

    // Flush any partial bulk when test finishes
    if (m_conns_manager->finished()) {
        flush_bulk();
    }

    // ... rest of existing code ...
}
```

**In `protocol.h`** (abstract_protocol class):

Add virtual methods:
```cpp
virtual bool has_partial_bulk() { return false; }
virtual void finalize_partial_bulk() {}
```

**In `protocol.cpp`** (skip_header_protocol class):

Implement the methods:
```cpp
bool skip_header_protocol::has_partial_bulk() {
    return m_accumulated_commands > 0;
}

void skip_header_protocol::finalize_partial_bulk() {
    if (m_accumulated_commands == 0) {
        return;  // No partial bulk
    }

    // Update header in-place with actual batch_count and payload_size
    // (same as done for the last command in a full bulk)
    update_header_in_place(m_accumulated_commands, m_bulk_payload_size);

    // Reset for next bulk
    m_accumulated_commands = 0;
    m_bulk_payload_size = 0;
}
```

**Why this works**:
- When test finishes, `fill_pipeline()` stops generating new commands
- `flush_bulk()` is called to finalize any partial bulk
- The partial bulk is sent to Redis with correct header
- All commands are transmitted before the connection closes
- Statistics remain accurate (each command counted individually)

### Example: Partial Bulk Scenario

**Scenario**: `bulk_size = 6`, test ends after 14 commands

```
Commands 1-6:   Complete bulk, sent immediately
Commands 7-12:  Complete bulk, sent immediately
Commands 13-14: Partial bulk (2 commands)
                - Command 13: Write header + command 1
                - Command 14: Write command 2
                - Test finishes, fill_pipeline() stops
                - flush_bulk() called
                - Header updated: batch_count=2, payload_size=(sum of 2 commands)
                - Partial bulk sent to Redis
```

### Statistics Impact with Partial Bulks

**With bulk_size = 6, 14 commands total**:
- 14 commands sent (each with its own request)
- 14 responses received (each matched to its request)
- `inc_reqs_processed()` called 14 times
- `handle_response()` called 14 times
- Statistics show 14 operations (correct)

**Result**: Partial bulks are handled correctly without affecting statistics accuracy

### Why This Works

1. **Minimal changes**: Only the protocol layer changes (how commands are buffered)
2. **Preserved semantics**: One command = one request = one response
3. **Accurate statistics**: Each command counted individually (as before)
4. **Backward compatible**: When `bulk_size=1`, behavior is identical to current implementation
5. **Efficient transmission**: Multiple commands sent with single header (optimization only)
6. **Partial bulk handling**: Flush mechanism ensures all commands are sent, even partial bulks

### Statistics Impact

**With bulk_size = 6**:
- 6 commands sent (each with its own request)
- 6 responses received (each matched to its request)
- `inc_reqs_processed()` called 6 times (once per response)
- `handle_response()` called 6 times (once per response)
- Statistics show 6 operations (exactly as if bulk_size=1)

**Result**: Statistics are identical whether using `bulk_size=1` or `bulk_size=6`
- Same number of operations counted
- Same latency measurements (per command)
- Same throughput calculations

