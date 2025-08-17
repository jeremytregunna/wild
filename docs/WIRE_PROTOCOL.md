# WILD Wire Protocol Specification

This document describes the binary wire protocol used for communication between WILD clients and servers.

## Protocol Overview

WILD uses a high-performance binary protocol optimized for minimal latency and maximum throughput. The protocol is connection-oriented and requires authentication before any database operations.

### Protocol Characteristics
- **Binary format**: No text parsing overhead
- **Fixed-size headers**: Predictable parsing performance
- **Authentication-first**: Security enforced at protocol level
- **Zero-copy design**: Direct memory access patterns
- **Endianness**: Little-endian (x86-64 native)

## Connection Flow

```
Client                           Server
  |                               |
  |-- TCP Connect ---------------->|
  |                               |
  |-- Auth Request --------------->|
  |<-- Auth Response --------------|
  |                               |
  |-- Database Operations ------->|
  |<-- Responses ------------------|
  |                               |
  |-- TCP Close ------------------>|
```

## Message Format

All messages follow this structure:

```
┌─────────────────┬──────────────────────┐
│   Message       │   Variable-Length    │
│   Header        │   Data Payload       │
│   (24 bytes)    │   (0-1MB)           │
└─────────────────┴──────────────────────┘
```

### Message Header (24 bytes)

```c
struct WireMessage {
    uint32_t message_type;    // Message type identifier
    uint64_t key;            // Database key (0 for non-data operations)
    uint32_t data_length;    // Length of following data in bytes
    uint32_t status;         // Status/error code
    uint32_t reserved;       // Reserved for future use (must be 0)
}
```

**Field Descriptions:**
- `message_type`: Operation type (see Message Types below)
- `key`: 64-bit database key for data operations
- `data_length`: Size of variable data following header
- `status`: Success/error status code
- `reserved`: Must be zero, reserved for protocol extensions

## Message Types

### Authentication Messages

#### Auth Request (Type 1)
Client initiates authentication with shared secret.

```
Header:
  message_type: 1 (auth_request)
  key: 0
  data_length: <secret_length>
  status: 0 (success)
  reserved: 0

Data:
  <shared_secret_bytes>
```

#### Auth Response (Type 2)
Server responds with authentication result.

```
Header:
  message_type: 2 (auth_response)
  key: 0
  data_length: 0
  status: 0 (success) or 7 (auth_failed)
  reserved: 0

Data: (none)
```

### Database Operations

#### Read Request (Type 3)
Client requests value for a key.

```
Header:
  message_type: 3 (read_request)
  key: <64-bit_key>
  data_length: 0
  status: 0 (success)
  reserved: 0

Data: (none)
```

#### Read Response (Type 4)
Server returns value or not-found status.

```
Header:
  message_type: 4 (read_response)
  key: <64-bit_key>
  data_length: <value_length> (0 if not found)
  status: 0 (success) or 1 (not_found)
  reserved: 0

Data:
  <value_bytes> (if found)
```

#### Write Request (Type 5)
Client stores key-value pair.

```
Header:
  message_type: 5 (write_request)
  key: <64-bit_key>
  data_length: <value_length>
  status: 0 (success)
  reserved: 0

Data:
  <value_bytes>
```

#### Write Response (Type 6)
Server confirms write operation.

```
Header:
  message_type: 6 (write_response)
  key: <64-bit_key>
  data_length: 0
  status: 0 (success) or 2 (error_internal)
  reserved: 0

Data: (none)
```

#### Delete Request (Type 7)
Client deletes a key.

```
Header:
  message_type: 7 (delete_request)
  key: <64-bit_key>
  data_length: 0
  status: 0 (success)
  reserved: 0

Data: (none)
```

#### Delete Response (Type 8)
Server confirms deletion.

```
Header:
  message_type: 8 (delete_response)
  key: <64-bit_key>
  data_length: 0
  status: 0 (success) or 1 (not_found)
  reserved: 0

Data: (none)
```

#### Error Response (Type 9)
Server reports error condition.

```
Header:
  message_type: 9 (error_response)
  key: <64-bit_key>
  data_length: 0
  status: <error_code>
  reserved: 0

Data: (none)
```

## Status Codes

| Code | Name | Description |
|------|------|-------------|
| 0 | `success` | Operation completed successfully |
| 1 | `not_found` | Key does not exist |
| 2 | `error_internal` | Server internal error |
| 3 | `error_invalid_key` | Invalid key format |
| 4 | `error_data_too_large` | Value exceeds maximum size (52 bytes) |
| 5 | `error_connection` | Connection-level error |
| 6 | `error_auth_required` | Authentication required |
| 7 | `error_auth_failed` | Authentication failed |

## Protocol Constraints

### Size Limits
- **Maximum value size**: 52 bytes (cache-line storage constraint)
- **Maximum message size**: 1MB (designed for future batch operations)
- **Key size**: Always 8 bytes (64-bit)
- **Current limitation**: Single operation per message (batch support planned)

### Authentication
- **Required**: All connections must authenticate before operations
- **Method**: Shared secret comparison with timing-attack resistance
- **Scope**: Per-connection (authenticate once per TCP connection)

### Performance Characteristics
- **Header parsing**: Fixed 24-byte structure for O(1) parsing
- **Zero-copy**: Direct memory mapping where possible
- **Cache-aligned**: Optimized for CPU cache-line access patterns

## Error Handling

### Connection Errors
- Authentication failure → immediate connection termination
- Invalid message format → error response + connection termination
- Data too large → error response, connection remains open

### Database Errors
- Key not found → `not_found` status in response
- Internal errors → `error_internal` status in response
- Invalid operations → appropriate error code in response

## Security Considerations

### Authentication Security
- **Constant-time comparison**: Prevents timing-based attacks
- **Connection-scoped**: Authentication state tied to TCP connection
- **Secret transmission**: Shared secret sent in plaintext (use TLS if needed)

### Protocol Security
- **No command injection**: Binary protocol eliminates injection risks
- **Fixed parsing**: Predictable parsing prevents buffer overflows
- **Size validation**: All lengths validated before memory operations

## Implementation Notes

### Endianness
All multi-byte integers are little-endian (x86-64 native format).

### Alignment
Message headers are naturally aligned for optimal CPU access.

### Error Recovery
Clients should handle connection drops gracefully and re-authenticate on reconnection.

### Performance Tips
- **Connection pooling**: Reuse authenticated connections
- **Batch operations**: Pipeline multiple operations for throughput
- **Buffer management**: Pre-allocate buffers to avoid allocation overhead

## Example Session

```
# TCP Connection established

# Authentication
C→S: [1, 0, 8, 0, 0] "mysecret"
S→C: [2, 0, 0, 0, 0]

# Write operation
C→S: [5, 12345, 4, 0, 0] "test"
S→C: [6, 12345, 0, 0, 0]

# Read operation  
C→S: [3, 12345, 0, 0, 0]
S→C: [4, 12345, 4, 0, 0] "test"

# Delete operation
C→S: [7, 12345, 0, 0, 0] 
S→C: [8, 12345, 0, 0, 0]

# TCP Connection closed
```

## Client Implementation Guidelines

### Connection Management
1. Establish TCP connection
2. Send authentication request immediately
3. Wait for authentication response
4. Begin database operations
5. Handle connection errors with reconnection + re-authentication

### Performance Optimization
- Use connection pooling for concurrent operations
- Pipeline requests when latency > throughput matters
- Pre-allocate message buffers
- Implement proper timeout handling

### Error Handling
- Always check status codes in responses
- Implement exponential backoff for connection failures
- Handle authentication failures with credential refresh

---

## Current Limitations and Future Enhancements

### Single Operation Protocol
The current wire protocol supports only single operations per message:
- One key-value pair per write request
- One key per read/delete request
- Maximum efficiency: 52 bytes data + 24 bytes header = 76 bytes per operation

### Planned Batch Operations
WILD's core already supports efficient batch operations (`readBatch`, `writeBatch`, `deleteBatch`) with hardware-optimized sizing. Future wire protocol enhancements will add:

- **Batch message types**: `batch_write_request`, `batch_read_request`, `batch_delete_request`
- **Multi-operation payloads**: Pack thousands of operations into single 1MB messages
- **Massive efficiency gains**: 
  - Current: 1,000 ops = 76KB + 1,000 round trips
  - Planned: 1,000 ops = ~60KB + 1 round trip

### Performance Impact
Batch operations would provide:
- **25% bandwidth reduction** (packed format vs individual messages)
- **1000× fewer round trips** (single request vs per-operation requests)  
- **Optimal hardware utilization** using `getOptimalBatchSize()` for core-based scaling

The 1MB protocol limit exists specifically to support these future batch operations while maintaining the current single-operation compatibility.

---

This protocol is designed for maximum performance while maintaining security and reliability. For additional details on specific implementations, see the [Client API documentation](API.md).