# Folly Performance Optimization Patches

English|[简体中文](./README.md)

## What's New

- [2026.09.30]: Released a patch for Folly v1.1.0, adding a thread-local storage (TLS) memory pool for IOBuf. Thread-local data block reuse and contiguous slice splitting reduce `malloc/free` operations in high-queries per second (QPS) scenarios.
- [2026.06.30]: Released a patch repository for Folly v1.0.0, optimizing io_uring for the Folly asynchronous network I/O framework to improve network I/O stability and baseline I/O performance.

## Project Introduction

Folly is a high-performance C++11/14/17 component library open-sourced by Meta, designed specifically for large-scale, highly concurrent, low-latency server-side application scenarios.

This project optimizes the Folly network I/O and buffer allocation paths. Folly v1.1.0 introduces the IOBuf TLS memory pool, which pools only data blocks, not the IOBuf objects themselves. Folly v1.0.0 adopts the io_uring hybrid mode: read operations use multishot to reduce system calls, while write operations use the open-source `send` to ensure the ordering and latency stability of consecutive writes.

The core design of the IOBuf TLS memory pool is as follows:

- Each thread uses `TLSBlockCache` to cache idle `IoBufBlock` objects, with a default maximum of 8.
- The default data block size is 8 KB, and non-overlapping slices are split contiguously from `current_share`.
- Small-capacity `IOBuf::create()` calls preferentially reuse pool blocks; for large capacities, when the pool is not enabled, or when pooling fails, the original Folly path is used as a fallback.
- `flagsAndSharedInfo_` is reused to store the pool flag and block pointer without changing `sizeof(IOBuf)`.
- `ref_count` and `share_count` are used to manage the block lifecycle and IOBuf references, respectively, supporting clone, reserve, and cross-thread release.

## Directory Structure

```text
fbthrift/
├── docs/                           # Documentation directory
│   ├── en/                         # English documents
│   │   ├── api_reference.md        # API reference sheet
│   │   ├── quick_start.md          # Menu for beginners
│   │   └── release_notes.md        # Release notes
│   │
│   ├── zh/                         # Chinese documents
│   │   ├── api_reference.md        # API reference sheet
│   │   ├── quick_start.md          # Menu for beginners
│   │   └── release_notes.md        # Release notes
│   └── LICENSE
├── LICENSE
├── iouring.patch                   # Folly v1.1.0 optimization patch file
└── README.md                       # Project introduction
```

## Release Notes

For version updates of the Folly optimization patches, see [Release Notes](docs/en/release_notes.md).

## Quick Start

For quick start instructions on Folly, see [Quick Start](docs/en/quick_start.md).

## Learning Documents

| Document | Content |
| --------- | --------- |
| [Quick Start](docs/en/quick_start.md) | Provides guidance on compiling, enabling, and verifying io_uring and the IOBuf TLS memory pool. |
| [Release Notes](docs/en/release_notes.md) | Provides Folly v1.1.0 version information, compatibility constraints, and feature updates. |
| [API Reference](docs/en/api_reference.md) | Provides version-specific descriptions of the TLS memory pool and io_uring related interfaces. |

## Disclaimer

This repository participates in the open-source release of Folly software and provides performance optimizations for asynchronous network I/O and IOBuf buffer allocation paths. The code follows the design and coding style of open-source software and retains the original allocation fallback path. Any vulnerabilities and security issues in the software are addressed by the corresponding upstream community according to its vulnerability and security response mechanisms. Please closely follow the notices and version updates released by the upstream community.

## License

Folly is licensed under the Apache-2.0 license. For details, see [LICENSE](LICENSE).

The documents of this project are licensed under CC-BY 4.0. For details, see [LICENSE](docs/LICENSE).

## Contribution Statement

We welcome your contributions to the community. If you have any questions/suggestions or want to provide feedback on feature requirements and bug reports, you can [submit issues](https://gitcode.com/boostkit/community/blob/master/docs/contributor/issue-submit.md). For details, see the [contribution guideline](https://gitcode.com/boostkit/community/blob/master/docs/contributor/contributing.md). You are also welcome to share insights in the [Discussions](https://gitcode.com/boostkit/community/discussions). Thank you for your support.

## Acknowledgments

Thank you to everyone in the community for your PRs. We warmly welcome contributions to the Folly patch repository!
