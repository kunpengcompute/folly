# Release Notes

## Version Requirements

### Product Versions

|Item|Version|
|--|--|
|Product name|Kunpeng BoostKit|
|Product version|26.2.RC1|
|Software name|Folly performance optimization patch|
|Package version|v1.1.0|

### OS, Compiler, and CPU

|OS|Compiler|CPU|
|--|--|--|
|Debian 12, openEuler, or other Linux systems|Clang 16|Kunpeng 950 or other supported processors|

### Virus Scan Result

Virus scanning is not involved because no software package is released.

## Important Notes

For details, see [Quick Start](./quick_start.md).

## Change Description

### v1.1.0

#### New Features

|Feature|Description|
|--|--|
|IOBuf TLS memory pool|Each thread maintains a TLSBlockCache, reuses IoBufBlock, and continuously splits slices from `current_share` to reduce `malloc`/`free` for small IOBuf data areas.|
|Configurable size of pooled blocks|Adds `IOBuf::setBlockSize()`. The default block size is 8 KB. It is recommended to configure the block size only during service startup.|
|Explicit enablement API|Adds `IOBuf::enableMemoryPool()`. When this API is not called, the original Folly allocation path is retained.|
|Capacity-based routing and fallback|Small-capacity requests preferentially use pooled blocks. Large-capacity requests, requests when the memory pool is not enabled, or requests for which pooling fails fall back to `createCombined` or `createSeparate`.|
|ABI-compatible design|Reuses `flagsAndSharedInfo_` to store the pooling flag and `IoBufBlock` pointer, without adding data members to IOBuf or changing `sizeof(IOBuf)`.|
|Complete lifecycle management|Uses `ref_count` and `share_count` to manage TLS-held references and IOBuf references, respectively, covering cloning, reserve, destruction, thread exit, and cross-thread release.|

#### Default Configuration

|Configuration Item|Default Value|Description|
|--|--|--|
|Size of pooled blocks|8 KB|Includes the block header, so the actual data area is slightly smaller.|
|Maximum number of free blocks per thread|8|When the cache is full, excess blocks are released to the system.|
|Enablement status|Disabled|Enabled after calling `enableMemoryPool()`.|

#### Compatibility and Risks

- The memory pool only pools data blocks. IOBuf objects still use the original creation and destruction methods.
- When the memory pool is not enabled or the capacity is not suitable for pooling, the original path is automatically used as a fallback, leaving the original large-request path unaffected.
- `setBlockSize()` is a global configuration and should be set before creating worker threads to avoid configuration contention during runtime.
- When a thread exits, only the references held by TLS are released. Data blocks still referenced by IOBuf objects must remain alive.
- When an IOBuf is destructed across threads, data blocks may enter the TLS cache of the thread that performs the final release. The memory distribution across threads should be monitored.
- The block size and cache limit affect resident memory usage and should be tuned based on the number of threads and request size distribution.

### v1.0.0

#### New Features

|Feature|Description|
|--|--|
|Folly `io_uring` hybrid read/write optimization|Read operations use `io_uring` multishot to reduce system calls. Write operations use the open-source `send` to maintain sequential write order and use `PollWriteSqe` to monitor socket writability.|

## Related Documentation

### v1.1.0 Documentation

|Document|Description|Delivery Form|
|---|---|---|
|[APT Reference](./api_reference.md)|Provides API usage instructions for the Folly v1.1.0 optimization features.|Open-source repository|
|[Quick Start](./quick_start.md)|Provides entry-level guidance for compiling the optimized Folly version|Open-source repository.|
|[Release Notes](./release_notes.md)|Provides release information for the Folly optimization patch versions.|Open-source repository|

### v1.0.0 Documentation

The current documentation already incorporates the previous documentation.

## Change History

|Release|Date|Description|
|:---|:---|:---|
|01|2026-09-30|This is the first official release.|

### Obtaining Documentation

Visit the [open-source repository](https://gitcode.com/boostkit/folly) to view or download related documents.
