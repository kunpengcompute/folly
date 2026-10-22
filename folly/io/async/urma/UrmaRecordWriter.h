/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0. */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <vector>
#include <folly/io/async/AsyncTransport.h>

namespace folly {
// Optional capability for transports and write-transparent wrappers. Wrappers
// must explicitly forward records; callers must not unwrap TLS or transforms.
class UrmaRecordWriter {
 public:
  virtual ~UrmaRecordWriter() = default;
  virtual bool supportsUrmaRecords() const = 0;
  virtual void writeChainWithRecords(
      AsyncTransport::WriteCallback*,
      std::unique_ptr<IOBuf>&&,
      std::vector<size_t>,
      WriteFlags = WriteFlags::NONE) noexcept = 0;
};
} // namespace folly
#endif
