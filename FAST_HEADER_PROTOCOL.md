# Fast Header Protocol for memtier_benchmark

## Overview

The fast header protocol is an optimization for DMC (Data Mesh Connector) that allows clients to bypass full RESP3 protocol parsing by providing pre-calculated routing information via a custom 10-byte header.

## Header Format

Each command sent with the fast header protocol is prefixed with a 10-byte header:

```
Byte 0:      Designator (0x80) - Command designator that doesn't collide with RESP/Redis type designators
Bytes 1-4:   Length (uint32_t, big-endian) - Size of RESP3 payload (0..0x7FFFFFFF)
Byte 5:      NCMD (uint8_t) - Number of commands in message (1..127, high bit reserved)
Bytes 6-7:   Slot (uint16_t, big-endian) - Redis cluster slot (0-16383, 0xFFFF = no slot)
Bytes 8-9:   Client IDX (uint16_t, big-endian) - Client correlation ID for tracking request-response pairs
```

Total: 10 bytes

## Implementation Details

### Slot Calculation

The slot is calculated using CRC16 hash of the key, then masked to 0-16383 range:
```
slot = CRC16(key) & 0x3FFF
```

This matches Redis cluster's slot calculation algorithm.

### Protocol Flow

1. Client calculates the slot from the key using CRC16
2. Client prepares the RESP3 payload (normal Redis command)
3. Client calculates the payload size
4. Client prepends the 10-byte header with designator 0x80
5. Client sends: [10-byte header][RESP3 payload]
6. DMC receives the header, extracts slot and payload size
7. DMC bypasses RESP3 parsing and forwards payload directly to the appropriate shard

### Supported Commands

Currently implemented:
- `SET` - with optional expiry and offset (SETEX, SETRANGE)
- `GET` - with optional offset (GETRANGE)

## Usage

### Command Line

```bash
./memtier_benchmark -P redis-fast-header -s <host> -p <port> [other options]
```

### Example

```bash
# Benchmark with fast header protocol
./memtier_benchmark \
  -P redis-fast-header \
  -s localhost \
  -p 6379 \
  -t 4 \
  -c 50 \
  -n 100000 \
  -d 256 \
  --ratio 1:1
```

## Performance Benefits

The fast header protocol provides performance benefits by:

1. **Eliminating RESP3 parsing overhead** - DMC doesn't need to parse the full RESP3 protocol
2. **Direct slot routing** - Slot is pre-calculated, no need for key extraction and hashing
3. **Reduced latency** - Faster request processing in DMC
4. **Improved throughput** - More requests can be processed per second

## Implementation Files

### Modified Files

- `memtier_benchmark.h` - Added `PROTOCOL_REDIS_FAST_HEADER` enum value
- `memtier_benchmark.cpp` - Added protocol parsing and help text
- `protocol.cpp` - Added `fast_header_protocol` class implementation
- `Makefile.am` - No changes needed (fast_header_protocol is in protocol.cpp)

### Key Classes

**fast_header_protocol** - Extends `redis_protocol` class
- Overrides `write_command_set()` and `write_command_get()`
- Prepends 10-byte header before each command
- Maintains client_idx counter for correlation

## Testing

To verify the implementation:

```bash
# Check that the protocol is available
./memtier_benchmark --help | grep fast-header

# Run a simple benchmark
./memtier_benchmark -P redis-fast-header -s localhost -p 6379 -t 1 -c 1 -n 100
```

## Compatibility

- Works with Redis Enterprise DMC that supports fast header optimization
- Backward compatible with standard Redis (headers will be treated as part of the command)
- Requires DMC to be configured to recognize and handle fast headers

## Future Enhancements

- Support for additional commands (MGET, MSET, etc.)
- Batch command support (multiple commands in single header)
- Configurable request ID strategy
- Performance metrics collection

