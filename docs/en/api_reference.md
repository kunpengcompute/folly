# API Reference

## Folly v1.1.0: IOBuf TLS Memory Pool APIs

This document describes the IOBuf thread-local storage (TLS) memory pool APIs and lifecycle rules introduced in Folly v1.1.0. By default, this capability does not change the original allocation behavior. The pooled allocation path is entered only after `enableMemoryPool()` is called.

### API Overview

|Name|Type|Description|
|--|--|--|
|IOBuf::enableMemoryPool|Configuration interface|Enables the IOBuf TLS memory pool.|
|IOBuf::setBlockSize|Configuration interface|Sets the total size of subsequent pooled blocks. The default is 8 KB.|
|IOBuf::create|Extension of an existing interface|Selects the pooled or original allocation path based on the switch and capacity.|
|IOBuf::createFromPoolShared|Internal interface|Splits slices from the shared block of the current thread and constructs IOBuf objects.|
|ioBufBlockAllocate|Internal interface|Prefers to obtain free blocks from the cache of the current thread.|
|ioBufBlockRelease|Internal interface|Returns free blocks to the TLS cache or releases them to the system.|
|share_block|Internal interface|Obtains the current shared block that can accommodate the specified capacity.|

### IOBuf::enableMemoryPool

#### IOBuf::enableMemoryPool Function Description

Enables the in-process IOBuf pooled creation path. After it is enabled, `IOBuf::create()` preferentially splits slices from the IoBufBlock of the current thread when the capacity requirement is met. If the capacity requirement is not met, the original Folly allocation implementation is used.

#### IOBuf::enableMemoryPool Function Definition

```cpp
static void folly::IOBuf::enableMemoryPool();
```

#### IOBuf::enableMemoryPool Constraints

- It is recommended to call this API once before the service starts and worker threads are created.
- When this API is not called, the original createCombined() or createSeparate() behavior is retained.
- After this API is enabled, the fallback paths for large-capacity requests and failed pooling are still retained.

### IOBuf::setBlockSize

#### IOBuf::setBlockSize Function Description

Sets the block size for subsequently created IoBufBlock objects. The default value is 8 KB, and the actual splittable space is calculated after deducting the block header metadata.

#### IOBuf::setBlockSize Function Definition

```cpp
static void folly::IOBuf::setBlockSize(std::size_t size);
```

#### IOBuf::setBlockSize Function Parameter Description

|Parameter|Description|Input/Output|
|--|--|--|
|size|Total number of bytes for a newly created pooled block.|Input|

This configuration affects subsequently created data blocks and should not be modified frequently during request processing. It is recommended to complete the setting before calling `enableMemoryPool()` and to evaluate memory usage based on the request size distribution.

### IOBuf::create Routing

#### IOBuf::create Function Definition

```cpp
static std::unique_ptr<folly::IOBuf>
folly::IOBuf::create(std::size_t capacity);
```

#### IOBuf::create Routing Rules

```text
IOBuf::create(capacity)
├── Memory pool not enabled
│   └── Original createCombined/createSeparate path
├── capacity exceeds the pooled block data area capacity
│   └── Original createCombined/createSeparate path
└── capacity can be accommodated by the pooled block
    └── createFromPoolShared(capacity)
```

Pooling changes only how the underlying data area is obtained. It does not pool the IOBuf objects themselves or split large requests into multiple pooled blocks.

### Internal Data Structures and APIs

#### IoBufBlock

IoBufBlock is a self-describing pooled data block.

```text
IoBufBlock
├── magic: verifies the block type and validity
├── capacity: data area capacity
├── ref_count: reference count held by TLS and all IOBuf objects
├── share_count: number of IOBuf objects referencing this block
├── data_len: length of data already sliced
└── payloadBegin(): start of the data area
```

The default block size is 8 KB. The data area advances continuously by `data_len`, ensuring that slices within the same block do not overlap.

#### TLSBlockCache

Each thread maintains an independent TLSBlockCache.

```text
TLSBlockCache
├── blocks[8]: array of free blocks
├── count: current number of free blocks
└── current_share: the current block used to split slices
```

Free blocks are reused in LIFO order. The `data_len` of `current_share` is advanced only by its owning thread, avoiding the need for a shared lock when allocating slices.

#### Data Block Allocation and Reclamation

##### Function Definition

```cpp
IoBufBlock* ioBufBlockAllocate();
void ioBufBlockRelease(IoBufBlock* block);
IoBufBlock* share_block(std::size_t minCapacity);

static std::unique_ptr<folly::IOBuf>
folly::IOBuf::createFromPoolShared(std::size_t capacity);
```

##### Processing Flow

1. `ioBufBlockAllocate()` first retrieves a free block from the `blocks[]` of the current thread. If the cache is empty, it requests a block from the system.
2. `share_block()` checks the remaining space in `current_share`. If the remaining space is insufficient, it releases the TLS-held block and switches to a new block.
3. `createFromPoolShared()` splits a slice starting from `data_len` and increments both `ref_count` and `share_count`.
4. When IOBuf is destroyed, the corresponding counters are decremented. When the last reference is released, the data block is added to the TLS cache of the thread performing the release.
5. When the TLS cache is full, `ioBufBlockRelease()` releases the data block to the system.

```text
IoBufBlock
[Block Header][slice 1][slice 2][Remaining Space]
       ↑ IOBuf 1 ↑ IOBuf 2
```

### Pooling Flags and Lifecycle

#### `flagsAndSharedInfo_` Reuse

The memory pool does not add new data members to IOBuf; instead, it reuses `flagsAndSharedInfo_`.

```text
Normal IOBuf
└── flagsAndSharedInfo_ = flags + SharedInfo*

Pooled IOBuf
└── flagsAndSharedInfo_ = kFlagPoolIOBuf + IoBufBlock*
```

This design keeps `sizeof(IOBuf)` unchanged, avoiding ABI breakage. Pooled IOBuf objects obtain the data block through `block()`, while `sharedInfo()` returns a null pointer. The block pointer cannot be interpreted as `SharedInfo`.

#### Reference Counting Rules

- `ref_count` tracks TLS-held references and all IOBuf references. The data block can be returned to the pool or freed only after `ref_count` reaches zero.
- `share_count` tracks only IOBuf references, and is used to distinguish TLS ownership from the actual data usage state.
- `cloneOne()` and `cloneOneAsValue()` must increment both counts at the same time.
- `decrementRefcount()` decrements both counts at the same time when a pooled IOBuf is destroyed.
- When `reserveSlow()` moves the original slice out, it releases only the block reference held by the current IOBuf, not the entire shared block.
- `isManagedOne()` should identify pooled IOBuf objects as managed objects.
- `isSharedOne()` returns the shared state of a pooled object according to the current usage constraints.

#### Thread Exit and Cross-Thread Release

When a thread exits, `current_share` can release only the reference held by TLS itself. Data blocks still referenced by other IOBuf objects must remain alive. When an IOBuf is destructed across threads, the data block may enter the TLS cache of the thread that performs the final release. This is an accepted form of cross-thread migration in the current design, and cache usage across threads should be monitored based on the actual workload.

### Configuration and Fallback Principles

- By default, each thread caches a maximum of eight free blocks, with a default block size of 8 KB.
- Only data blocks are pooled, and IOBuf objects are not pooled.
- When the memory pool is disabled, the requested capacity is too large, or pooled creation fails, the original Folly path is used a fallback.
- It is recommended to call `setBlockSize()` only once during the startup phase.
- The block size and per-thread cache limit should be tuned based on the request distribution, number of worker threads, and overall memory budget.

## Folly v1.0.0: io_uring Hybrid Read/Write APIs

> ![note](./public_sys-resources/icon-note.gif)**Note:**
>This section describes the hybrid `io_uring`  read/write APIs provided in Folly v1.0.0, separately from the IOBuf TLS memory pool introduced in Folly v1.1.0.

### Folly v1.0.0：io_uring API Overview

|Name|Description|
|--|--|
|newSocket|Supports the same synchronous connection functionality as `AsyncSocket`.|
|writeChain|Stores write requests into the write queue in order, and sends them through the open-source `send`.|
|PollWriteSqe|Receives socket-writable notifications through `io_uring`.|

### AsyncIoUringSocket::writeChain

#### Function Description

It handles write operations in `AsyncIoUringSocket`. The optimized version no longer uses `io_uring WriteSqe` to send data. Instead, it queues write requests in order and continuously calls `send` to avoid potential latency introduced by `io_uring` when write order must be preserved.

#### Function Definition

```cpp
void AsyncIoUringSocket::writeChain(
    WriteCallback* callback,
    std::unique_ptr<IOBuf>&& buf,
    WriteFlags flags);
```

#### Parameter Description

|Parameter|Description|Input/Output|
|--|--|--|
|callback|Write completion callback|Input|
|buf|IOBuf chain to be sent|Input|
|flags|Write operation flags|Input|

This function does not return a value. When `send` cannot continue sending data due to insufficient buffer space, it works with `PollWriteSqe` to monitor the writable event of the socket file descriptor and resumes sending after the socket becomes writable.

## Change History

|Release|Date|Description|
|:---|:---|:---|
|01|2026-09-30|This is the first official release.|
