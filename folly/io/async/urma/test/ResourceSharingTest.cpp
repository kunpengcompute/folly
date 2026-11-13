/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0. */
#include "TestUtil.h"
struct TestListener : UrmaIoContext::Listener {
  unsigned wakes{0}, completions{0}, receives{0};
  bool blockData{false}, retainData{false};
  std::unique_ptr<IOBuf> held;
  bool receiveReady(const urma_detail::PacketHeader& h, const urma_cr_t&,
                    const uint8_t*, size_t) override {
    return !blockData || h.kind != urma_detail::PacketKind::Data;
  }
  uint64_t lastUser{0};
  unsigned failures{0};
  bool allowFailure{false};
  std::function<void()> onFailure, onCompletion;
  void receive(
      const urma_detail::PacketHeader& h,
      const urma_cr_t&,
      std::unique_ptr<IOBuf> data) override {
    ++receives;
    if (retainData && h.kind == urma_detail::PacketKind::Data) held = std::move(data);
  }
  void completed(const urma_cr_t& c, bool) override {
    ++completions;
    lastUser = c.user_ctx;
    if (onCompletion)
      onCompletion();
  }
  void writable() override { ++wakes; }
  void failed(const char*) override {
    if (allowFailure) {
      ++failures;
      if (onFailure)
        onFailure();
      return;
    }
    throw std::runtime_error("unexpected owner failure");
  }
};
struct Echo : AsyncTransport::ReadCallback, AsyncTransport::WriteCallback {
  AsyncTransport::UniquePtr socket;
  std::string expected;
  size_t received{0}, completed{0};
  bool echo;
  bool allowWriteError{false};
  bool closeOnRead{false};
  bool retainRead{false};
  bool allowReadError{false};
  size_t readErrors{0}, eofs{0};
  std::unique_ptr<IOBuf> retained;
  explicit Echo(AsyncTransport::UniquePtr s, bool e)
      : socket(std::move(s)), echo(e) {
    socket->setReadCB(this);
  }
  ~Echo() {
    allowWriteError =
        true; // Destruction may cancel writes while a test unwinds.
    socket->setReadCB(nullptr);
    socket->closeNow();
  }
  void getReadBuffer(void**, size_t*) override { std::abort(); }
  bool isBufferMovable() noexcept override { return true; }
  void readDataAvailable(size_t) noexcept override { std::abort(); }
  void readBufferAvailable(std::unique_ptr<IOBuf> b) noexcept override {
    if (echo) {
      socket->writeChain(this, std::move(b));
      return;
    }
    auto* node = b.get();
    do {
      require(
          received + node->length() <= expected.size(),
          "unexpected response length");
      require(
          !std::memcmp(
              expected.data() + received, node->data(), node->length()),
          "response mismatch");
      received += node->length();
      node = node->next();
    } while (node != b.get());
    if (retainRead)
      retained = b->clone();
    if (closeOnRead && received == expected.size())
      socket->closeNow();
  }
  void readEOF() noexcept override { ++eofs; }
  void readErr(const AsyncSocketException& e) noexcept override {
    if (allowReadError) {
      ++readErrors;
      return;
    }
    std::cerr << e.what() << std::endl;
    std::abort();
  }
  void writeSuccess() noexcept override { ++completed; }
  void writeErr(size_t, const AsyncSocketException&) noexcept override {
    if (!allowWriteError)
      std::abort();
  }
};
struct Acceptor : AsyncServerSocket::AcceptCallback {
  EventBase& evb;
  std::vector<std::unique_ptr<Echo>> connections;
  explicit Acceptor(EventBase& e) : evb(e) {}
  void connectionAccepted(
      NetworkSocket fd, const SocketAddress&, AcceptInfo) noexcept override {
    connections.emplace_back(new Echo(
        AsyncTransport::UniquePtr(new UrmaAsyncSocket(&evb, fd.toFd())), true));
  }
  void acceptError(exception_wrapper) noexcept override { std::abort(); }
  void acceptStarted() noexcept override {}
  void acceptStopped() noexcept override {}
};
void elasticCases(bool pressureTimeout = false) {
  UrmaConfig c;
  c.tx_depth = 2;
  c.rx_depth = 32;
  c.rx_target = 8;
  c.max_pending_bytes = 16384;
  auto& p = c.registered_pool;
  p.zero_copy = false; // Explicit source IOBufs make region selection deterministic.
  p.pool_size = p.region_bytes = 256 * 1024;
  p.pool_max_size = 1024 * 1024;
  p.tls_block_size = 16384;
  p.classes = {{4096}, {16384}};


  p.retry_ms = 2;
  p.idle_ms = 2000;
  UrmaInit::init(c);
  {
    EventBase evb, second;
    auto& owner = UrmaIoContext::get(&evb);
    const auto initialId = UrmaInit::initialSegment().regionId();
    std::vector<UrmaPoolBuffer> occupied;
    pauseRegistration = true;
    auto unpause = makeGuard([&] { pauseRegistration = false; });
    while (auto b = UrmaInit::poolAlloc(4096))
      occupied.push_back(std::move(b));
    auto& preparing = UrmaIoContext::get(&second);
    TestListener listener;
    auto id = preparing.attach(&listener);
    require(!preparing.prepare() && preparing.snapshot().posted == 0,
            "new IO owner did not wait for exhausted registered memory");
    pauseRegistration = false;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (!preparing.prepare() && std::chrono::steady_clock::now() < deadline)
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    require(preparing.prepare() && preparing.snapshot().posted == 8 &&
            UrmaInit::poolStats().registered_bytes > p.pool_size,
            "new IO owner did not complete preposts after growth");
    preparing.detach(id);
    UrmaIoContext::release(&second);
    occupied.clear();
    Acceptor acceptor(evb);
    SocketAddress address;
    auto server = listenOnLoopback(evb, acceptor, address, 8);
    auto spin = [&](auto done) {
      auto end = std::chrono::steady_clock::now() + std::chrono::seconds(3);
      while (!done() && std::chrono::steady_clock::now() < end) evb.loopOnce(EVLOOP_NONBLOCK);
      require(done(), "elastic transport did not progress");
    };
    {
      Echo client(UrmaAsyncSocket::newSocket(&evb, address), false);
      spin([&] { return acceptor.connections.size() == 1 &&
          static_cast<UrmaAsyncSocket*>(client.socket.get())->isUrmaReady(); });
      auto& paused = *acceptor.connections.front();
      paused.echo = false;
      paused.expected.assign(18000, 'b');
      paused.socket->setReadCB(nullptr);
      for (size_t i = 0; i < 6; ++i) {
        client.socket->write(&client, paused.expected.data() + i * 3000, 3000);
        spin([&] { return client.completed == i + 1; });
      }
      for (int i = 0; i < 32; ++i) evb.loopOnce(EVLOOP_NONBLOCK);
      require(owner.snapshot().posted == c.rx_target && !paused.received &&
              !paused.readErrors, "legal receive pressure closed or exceeded the byte budget");
      client.socket->shutdownWrite();
      for (int i = 0; i < 32; ++i) evb.loopOnce(EVLOOP_NONBLOCK);
      require(!paused.eofs, "TCP FIN bypassed deferred receive bytes");
      if (pressureTimeout) {
        client.allowReadError = true;
        auto end = std::chrono::steady_clock::now() + std::chrono::seconds(32);
        while (!paused.socket->error() && std::chrono::steady_clock::now() < end) {
          evb.loopOnce(EVLOOP_NONBLOCK);
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        require(paused.socket->error() && !paused.received,
                "receive pressure exceeded its bounded wait");
        spin([&] { return client.readErrors == 1; });
        require(!client.eofs, "pressure abort was reported as a clean EOF");
        for (int i = 0; i < 32; ++i) evb.loopOnce(EVLOOP_NONBLOCK);
      } else {
        paused.socket->setReadCB(&paused);
        spin([&] { return paused.received == 18000 && paused.eofs == 1; });
      }
      require(owner.snapshot().posted == c.rx_target && !paused.readErrors,
              "receive budget did not recover after resume/timeout");
      client.socket->closeNow();
      acceptor.connections.clear();
      for (int i = 0; i < 32; ++i) evb.loopOnce(EVLOOP_NONBLOCK);
    }
    for (bool cancelAck : {false, true}) {
      Echo client(UrmaAsyncSocket::newSocket(&evb, address), false);
      spin([&] { return acceptor.connections.size() == 1 &&
          static_cast<UrmaAsyncSocket*>(client.socket.get())->isUrmaReady(); });
      UrmaPoolBuffer source;
      deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
      while (!source && std::chrono::steady_clock::now() < deadline) {
        auto b = UrmaInit::poolAlloc(8192);
        if (b && b.regionId() != initialId) source = std::move(b);
        else if (b) occupied.push_back(std::move(b));
        else std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      require(bool(source), "no grown source region");
      occupied.clear();
      client.expected.assign(8192, 'g');
      std::memcpy(source.data(), client.expected.data(), 8192);
      const auto regionId = source.regionId();
      const auto pulls = checkedPulls, announcements = checkedSegments;
      holdSegmentReady = true;
      client.socket->writeChain(&client, UrmaInit::takePoolBuffer(source, 8192));
      spin([&] { return !heldSegmentReady.empty(); });
      for (int i = 0; i < 16; ++i) evb.loopOnce(EVLOOP_NONBLOCK);
      require(checkedSegments == announcements + 1 && checkedPulls == pulls &&
              !client.received && !client.completed,
              "PULL posted before peer imported/acknowledged its segment");
      holdSegmentReady = false;
      if (cancelAck) {
        acceptor.connections.front()->allowReadError = true;
        client.allowWriteError = true;
        client.socket->closeNow();
      }
      completeHeld(heldSegmentReady);
      if (!cancelAck) {
        spin([&] { return client.received == 8192 && client.completed == 1; });
        const auto stats = static_cast<UrmaAsyncSocket*>(client.socket.get())->getUrmaStats();
        require(stats.directReadBytes == 8192 && stats.copiedReadBytes == 0,
                "grown registered source took a staging copy");
      } else {
        for (int i = 0; i < 32; ++i) evb.loopOnce(EVLOOP_NONBLOCK);
        spin([&] { return acceptor.connections.front()->readErrors == 1; });
        require(checkedPulls == pulls && !acceptor.connections.front()->eofs,
                "cancelled segment wait exposed a READ source or clean EOF");
      }
      UrmaInit::retirePoolBuffer(source);
      source = {};
      if (!cancelAck) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        bool held = false;
        for (const auto& r : UrmaInit::poolRegions()) if (r.id == regionId) held = true;
        require(held, "advertised region released while connection remained open");
      }
      client.socket->closeNow();
      acceptor.connections.clear();
      for (int i = 0; i < 32; ++i) evb.loopOnce(EVLOOP_NONBLOCK);
    }
    server->stopAccepting();
    server.reset();
    UrmaIoContext::release(&evb);
  }
  UrmaInit::fini();
  require(!UrmaInit::isInitialized() && !registrations &&
          segmentImports == segmentUnimports && jfsCreated == jfsDeleted &&
          jfrCreated == jfrDeleted, "elastic cancellation leaked DMA/import/queue resources");
  std::cout << "PASS IO preparation after growth, SEGMENT acknowledgement gate, zero-copy grown READ, leases and cancellation\n";
}
void revocationCases(bool abandon = false) {
  for (int scenario : {0, 1, 2}) {
    UrmaConfig c;
    c.tx_depth = 2; c.rx_depth = 16; c.rx_target = 8;
    auto& p = c.registered_pool;
    p.zero_copy = false;
    p.pool_size = 256 * 1024; p.region_bytes = 16384;
    p.pool_max_size = 512 * 1024;
    p.classes = {{4096}, {16384}};

    p.idle_ms = scenario == 2 ? 30 : 2000; p.retry_ms = 2;
    UrmaInit::init(c);
    EventBase evb;
    auto& owner = UrmaIoContext::get(&evb);
    Acceptor acceptor(evb);
    SocketAddress address;
    auto server = listenOnLoopback(evb, acceptor, address, 8);
    const char* stage = "connect";
    auto spin = [&](auto done) {
      auto end = std::chrono::steady_clock::now() + std::chrono::seconds(4);
      while (!done() && std::chrono::steady_clock::now() < end) {
        evb.loopOnce(EVLOOP_NONBLOCK);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      if (!done()) std::cerr << "revocation scenario=" << scenario << " stage=" << stage << '\n';
      require(done(), "segment revocation did not progress");
    };
    std::vector<std::unique_ptr<Echo>> clients;
    for (int i = 0; i < 2; ++i)
      clients.emplace_back(new Echo(UrmaAsyncSocket::newSocket(&evb, address), false));
    spin([&] {
      return acceptor.connections.size() == 2 &&
          static_cast<UrmaAsyncSocket*>(clients[0]->socket.get())->isUrmaReady() &&
          static_cast<UrmaAsyncSocket*>(clients[1]->socket.get())->isUrmaReady();
    });
    // Fill initial storage, then request exactly one whole growth region. RX
    // and READ targets use the returned initial blocks, not the chosen region.
    std::vector<UrmaPoolBuffer> occupied;
    const auto initial = UrmaInit::initialSegment().regionId();
    pauseRegistration = true;
    while (auto b = UrmaInit::poolAlloc(16384))
      occupied.push_back(std::move(b));
    pauseRegistration = false;
    UrmaPoolBuffer source;
    stage = "growth";
    spin([&] {
      if (!source) source = UrmaInit::poolAlloc(16384);
      return bool(source);
    });
    require(source.regionId() != initial && source.segment()->seg.len == 16384,
            "revocation test source not an isolated growth region");
    const auto id = source.regionId();
    const auto va = source.segment()->seg.ubva.va;
    auto present = [&] {
      for (const auto& r : UrmaInit::poolRegions()) if (r.id == id) return true;
      return false;
    };
    occupied.clear();
    std::memset(source.data(), 'v', 8192);
    for (size_t i = 0; i < clients.size(); ++i) {
      acceptor.connections[i]->echo = false;
      acceptor.connections[i]->expected.assign(8192, 'v');
      clients[i]->socket->writeChain(clients[i].get(), UrmaInit::takePoolBuffer(source, 8192));
    }
    stage = "parallel readers";
    spin([&] { return clients[0]->completed == 1 && clients[1]->completed == 1 &&
        acceptor.connections[0]->received == 8192 && acceptor.connections[1]->received == 8192; });
    const auto before = checkedRevokes;
    const auto ackBefore = checkedRevoked;
    const auto failures = rejectedUnimports.load();
    holdSegmentRevoked = true;
    blockedUnimport = va;
    if (scenario != 2) UrmaInit::retirePoolBuffer(source);
    // Allocation still owns the region: neither explicit retirement nor
    // automatic shrink may revoke it until the last block reference returns.
    for (int i = 0; i < 8; ++i) evb.loopOnce(EVLOOP_NONBLOCK);
    require(checkedRevokes == before, "revoke raced a live block/export");
    source = {};
    stage = "both peers revoke, unimport retry";
    spin([&] { return checkedRevokes >= before + 2 && rejectedUnimports > failures; });
    require(checkedRevoked == ackBefore && present(),
            "failed/shared unimport was acknowledged or storage unregistered");
    blockedUnimport = 0;
    stage = "unimport completes before ACK";
    spin([&] { return heldSegmentRevoked.size() >= 2; });
    require(present(), "unregister preceded peer revocation acknowledgements");
    if (abandon) {
      clients.clear(); acceptor.connections.clear();
      heldSegmentRevoked.clear(); holdSegmentRevoked = false;
      server->stopAccepting(); server.reset();
      // Simulate losing the owner before confirmation: local queue deletion
      // is not proof that every remote peer has released its imported segment.
      UrmaIoContext::release(&evb); UrmaInit::fini();
      require(UrmaInit::isInitialized() && present() && UrmaInit::pendingCleanupCount(),
              "abandoned revoke released unconfirmed registered storage");
      std::cout << "PASS abandoned revocation quarantines unconfirmed segment\n";
      return;
    }
    if (scenario == 1) {
      clients.clear(); acceptor.connections.clear();
      for (int i = 0; i < 8; ++i) evb.loopOnce(EVLOOP_NONBLOCK);
      require(present(), "close discarded an unconfirmed revocation lease");
    }
    holdSegmentRevoked = false;
    auto first = heldSegmentRevoked.front();
    signalQueue(first.first, first.second);
    for (int i = 0; i < 8; ++i) evb.loopOnce(EVLOOP_NONBLOCK);
    require(present(), "one connection ACK released another peer's lease");
    for (size_t i = 1; i < heldSegmentRevoked.size(); ++i)
      signalQueue(heldSegmentRevoked[i].first, heldSegmentRevoked[i].second);
    heldSegmentRevoked.clear();
    stage = "all peer ACKs release region";
    spin([&] { return !present(); });
    if (scenario != 1) {
      // Original connections stay usable after shrinking, including DATA.
      for (size_t i = 0; i < clients.size(); ++i) {
        acceptor.connections[i]->expected.append(100, 'z');
        clients[i]->socket->write(clients[i].get(), std::string(100, 'z').data(), 100);
      }
      spin([&] { return clients[0]->completed == 2 && clients[1]->completed == 2 &&
          acceptor.connections[0]->received == 8292 && acceptor.connections[1]->received == 8292; });
    }
    clients.clear(); acceptor.connections.clear();
    stage = "closed protocol cleanup";
    spin([&] { return owner.snapshot().retired == 0 && owner.snapshot().inflight == 0; });
    server->stopAccepting(); server.reset();
    UrmaIoContext::release(&evb); UrmaInit::fini();
    require(!UrmaInit::isInitialized() && !registrations && segmentImports == segmentUnimports,
            "revocation leaked registration/import references");
  }
  std::cout << "PASS shared-import revocation, failed unimport retry, ACK barrier, close race and automatic idle shrink\n";
}
void closeReadCases(bool unknown) {
  // READ CQE after close, FREE RNR before/after close, unsubmitted READ,
  // and a PULL waiting for admission all retain distinct release obligations.
  for (int mode = unknown ? 6 : 1; mode <= (unknown ? 6 : 5); ++mode) {
    std::cout << "CLOSE READ " << mode << std::endl;
    UrmaConfig c;
    c.tx_depth = 4;
    c.rx_depth = 32;
    c.rx_target = 8;
    auto& p = c.registered_pool;
    p.zero_copy = p.elastic = false;
    p.pool_size = p.pool_max_size = 256 * 1024;
    p.classes = {{4096}, {16384}};

    UrmaInit::init(c);
    const auto frees = checkedFrees;
    {
      EventBase evb;
      auto& owner = UrmaIoContext::get(&evb);
      auto spin = [&](auto done) {
        auto end = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (!done() && std::chrono::steady_clock::now() < end)
          evb.loopOnce(EVLOOP_NONBLOCK);
        require(done(), "closed READ/FREE did not finish");
      };
      Acceptor acceptor(evb);
      SocketAddress address;
      auto server = listenOnLoopback(evb, acceptor, address, 8);
      auto sender = std::make_unique<Echo>(
          AsyncTransport::UniquePtr(new UrmaAsyncSocket(&evb, address)), false);
      sender->allowReadError = sender->allowWriteError = unknown;
      spin([&] { return acceptor.connections.size() == 1 &&
          static_cast<UrmaAsyncSocket*>(sender->socket.get())->isUrmaReady() &&
          static_cast<UrmaAsyncSocket*>(acceptor.connections[0]->socket.get())->isUrmaReady(); });
      auto& receiver = *acceptor.connections[0];
      receiver.echo = false;
      receiver.allowReadError = unknown;
      receiver.expected.assign(8192, 'c');
      auto budget = mode == 5 ? holdReadSlots(owner) : std::vector<std::shared_ptr<void>>{};
      const auto posts = readPostCalls, accepted = acceptedReadWrs;
      holdReads = mode == 1;
      holdFreeCompletions = mode == 2 || mode == 3;
      rejectFrees = holdFreeCompletions ? 1 : 0;
      blockReadPosts = mode == 4 || unknown;
      unknownReadBoundary = unknown;
      sender->socket->write(sender.get(), receiver.expected.data(), receiver.expected.size());
      if (unknown) {
        spin([&] { return receiver.readErrors == 1; });
        require(checkedFrees == frees, "unknown READ acceptance sent premature FREE");
      } else {
        if (mode == 1) spin([&] { return heldReads.size() == 1; });
        else if (mode == 2 || mode == 3)
          spin([&] { return heldFreeCompletions.size() == 1; });
        else if (mode == 4) spin([&] { return readPostCalls > posts; });
        else spin([&] { return owner.snapshot().waiting != 0; });
        if (mode == 3) {
          signalQueue(heldFreeCompletions[0].first, heldFreeCompletions[0].second);
          heldFreeCompletions.clear();
          owner.poll(); // RNR handled while live; close before the retry timer.
        }
        require(checkedFrees == frees, "FREE unexpectedly completed before close");
        const auto delivered = receiver.received;
        receiver.socket->closeNow();
        if (mode == 1) {
          owner.poll();
          require(checkedFrees == frees && owner.snapshot().retired,
                  "closed READ lost completion routing before its terminal CQE");
          signalQueue(heldReads[0].first, heldReads[0].second);
          heldReads.clear();
        }
        completeHeld(heldFreeCompletions);
        holdReads = holdFreeCompletions = blockReadPosts = false;
        budget.clear();
        spin([&] { return sender->completed == 1 && !owner.snapshot().retired &&
            !owner.snapshot().inflight; });
        require(checkedFrees == frees + 1 && receiver.received == delivered,
                "closed reader lost FREE or called the application again");
        require(mode < 4 || acceptedReadWrs == accepted,
                "cancelled READ reached the SDK");
        (void)holdReadSlots(owner); // Every READ/FREE responsibility was released.
      }
      holdReads = holdFreeCompletions = blockReadPosts = unknownReadBoundary = false;
      sender.reset();
      acceptor.connections.clear();
      server->stopAccepting();
      server.reset();
      UrmaIoContext::release(&evb);
    }
    UrmaInit::fini();
    if (unknown)
      require(UrmaInit::isInitialized() && UrmaInit::pendingCleanupCount(),
              "unknown READ source was not quarantined");
    else
      require(!UrmaInit::isInitialized() && !registrations &&
                  importsCreated == importsDeleted && segmentImports == segmentUnimports &&
                  jfsCreated == jfsDeleted && jfrCreated == jfrDeleted,
              "closed READ/FREE resources leaked");
  }
}
void rnrGapCases() {
  UrmaConfig config;
  config.rx_depth = config.rx_target = 4;
  config.tx_depth = 16;
  UrmaInit::init(config);
  {
    EventBase evb;
    auto& io = UrmaIoContext::get(&evb);
    Acceptor acceptor(evb);
    SocketAddress address;
    auto server = listenOnLoopback(evb, acceptor, address, 8);
    auto spin = [&](auto done) {
      auto end = std::chrono::steady_clock::now() + std::chrono::seconds(3);
      while (!done() && std::chrono::steady_clock::now() < end)
        evb.loopOnce(EVLOOP_NONBLOCK);
      require(done(), "RNR sequence gap did not recover");
    };
    for (int mode = 0; mode < 3; ++mode) {
      const size_t size = mode ? 8192 : 1000;
      const auto accepted = acceptor.connections.size();
      Echo client(UrmaAsyncSocket::newSocket(&evb, address), false);
      spin([&] { return acceptor.connections.size() == accepted + 1 &&
          static_cast<UrmaAsyncSocket*>(client.socket.get())->isUrmaReady(); });
      auto& peer = *acceptor.connections.back();
      peer.echo = false;
      rejectSequence = 1;
      for (int i = 0; i < 5; ++i) {
        std::string message(size, char('a' + i));
        peer.expected += message;
        client.socket->writeChain(&client, IOBuf::copyBuffer(message));
      }
      // Four later records consume every hardware RX slot before the first
      // record's software retry. None may reach the application out of order.
      io.poll();
      require(!peer.received, "sequence gap bypassed ordered delivery");
      rejectSequence = 0;
      if (mode == 2) {
        client.allowReadError = client.allowWriteError = true;
        const auto frees = checkedFrees;
        peer.socket->closeNow();
        spin([&] { return checkedFrees == frees + 4 && !io.snapshot().retired &&
            !io.snapshot().inflight; });
        require(!peer.received, "close delivered deferred PULL data");
      } else {
        spin([&] { return client.completed == 5 && peer.received == 5 * size; });
      }
      client.socket->closeNow();
      peer.socket->closeNow();
      spin([&] { return !io.snapshot().inflight && !io.snapshot().retired; });
    }
    acceptor.connections.clear();
    server->stopAccepting();
    UrmaIoContext::release(&evb);
  }
  UrmaInit::fini();
  require(!UrmaInit::isInitialized() && !registrations &&
              jfsCreated == jfsDeleted && jfrCreated == jfrDeleted &&
              importsCreated == importsDeleted && segmentImports == segmentUnimports,
          "RNR recovery leaked resources");
}
void readPressureCases() {
  UrmaConfig c;
  c.rx_depth = c.rx_target = 4;
  c.tx_depth = 8;
  c.read_window = c.offer_window = 2;
  c.max_pending_bytes = 32768;
  c.registered_pool.zero_copy = c.registered_pool.elastic = false;
  c.registered_pool.pool_size = c.registered_pool.pool_max_size = 256 * 1024;
  UrmaInit::init(c);
  {
    EventBase first, second;
    auto& a = UrmaIoContext::get(&first);
    auto& b = UrmaIoContext::get(&second);
    auto pump = [&] { first.loopOnce(EVLOOP_NONBLOCK); second.loopOnce(EVLOOP_NONBLOCK); };
    auto spin = [&](auto done, int seconds = 3) {
      const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
      while (!done() && std::chrono::steady_clock::now() < deadline) {
        pump(); std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      require(done(), "bidirectional READ pressure did not progress");
    };
    Acceptor acceptor(second);
    SocketAddress address;
    auto server = listenOnLoopback(second, acceptor, address, 8);
    auto client = std::make_unique<Echo>(UrmaAsyncSocket::newSocket(&first, address), false);
    spin([&] { return acceptor.connections.size() == 1 &&
        static_cast<UrmaAsyncSocket*>(client->socket.get())->isUrmaReady() &&
        static_cast<UrmaAsyncSocket*>(acceptor.connections[0]->socket.get())->isUrmaReady(); });
    auto& peer = *acceptor.connections[0];
    peer.echo = false;
    client->allowReadError = client->allowWriteError = true;
    peer.allowReadError = peer.allowWriteError = true;
    client->expected = peer.expected = std::string(8192, 'p');
    auto source = [&] {
      auto data = UrmaInit::createPoolBuffer(8192);
      require(bool(data), "registered pressure source unavailable");
      std::memset(data->writableData(), 'p', 8192); data->append(8192);
      return data;
    };
    holdPullReceives = true;
    client->socket->writeChain(client.get(), source());
    peer.socket->writeChain(&peer, source());
    spin([&] { return heldPullReceives.size() == 2 &&
        !a.snapshot().inflight && !b.snapshot().inflight; });
    std::vector<UrmaPoolBuffer> pressure;
    while (auto block = UrmaInit::poolAlloc(4096)) pressure.push_back(std::move(block));
    const auto accepted = acceptedReadWrs;
    const auto frees = checkedFrees;
    holdPullReceives = false; completeHeld(heldPullReceives);
    spin([&] { return a.snapshot().waiting && b.snapshot().waiting; });
    require(acceptedReadWrs == accepted && !client->completed && !peer.completed &&
                !client->received && !peer.received &&
                client->socket->getAppBytesBuffered() <= c.max_pending_bytes &&
                peer.socket->getAppBytesBuffered() <= c.max_pending_bytes,
            "exhausted READ submitted DMA or exceeded pending limits");
    // The actual production READ/FREE deadline applies in both directions.
    spin([&] { return client->readErrors == 1 && peer.readErrors == 1; }, 32);
    spin([&] { return !a.snapshot().retired && !b.snapshot().retired &&
        !a.snapshot().inflight && !b.snapshot().inflight; });
    require(acceptedReadWrs == accepted && checkedFrees == frees + 2 &&
                UrmaInit::poolStats().exported_bytes == 0 &&
                !client->socket->getAppBytesBuffered() && !peer.socket->getAppBytesBuffered(),
            "timeout lost cancellation FREE or retained pending data");
    client.reset(); acceptor.connections.clear(); pressure.clear();
    spin([&] { return a.snapshot().posted == c.rx_target && b.snapshot().posted == c.rx_target; });
    // Reuse the same owners after resources return: a fresh bidirectional READ.
    client = std::make_unique<Echo>(UrmaAsyncSocket::newSocket(&first, address), false);
    spin([&] { return acceptor.connections.size() == 1 &&
        static_cast<UrmaAsyncSocket*>(client->socket.get())->isUrmaReady(); });
    auto& recovered = *acceptor.connections[0]; recovered.echo = false;
    client->expected = recovered.expected = std::string(8192, 'p');
    client->socket->writeChain(client.get(), source());
    recovered.socket->writeChain(&recovered, source());
    spin([&] { return client->completed == 1 && recovered.completed == 1 &&
        client->received == 8192 && recovered.received == 8192; });
    client->allowReadError = recovered.allowReadError = true;
    client.reset(); acceptor.connections.clear();
    spin([&] { return !a.snapshot().retired && !b.snapshot().retired; });
    server->stopAccepting(); server.reset();
    UrmaIoContext::release(&first); UrmaIoContext::release(&second);
  }
  UrmaInit::fini();
  require(!UrmaInit::isInitialized() && !registrations &&
              importsCreated == importsDeleted && segmentImports == segmentUnimports,
          "bidirectional exhaustion left pool or import resources");
  std::cout << "PASS bidirectional READ exhaustion, bounded timeout, cancellation FREE and recovery\n";
}
void bufferPressureCases() {
  UrmaConfig c;
  c.rx_depth = 8;
  c.rx_target = 2;
  c.tx_depth = 1024;
  c.registered_pool.zero_copy = false;
  c.registered_pool.elastic = false;
  c.registered_pool.pool_size = c.registered_pool.pool_max_size = 65536;
  c.registered_pool.tls_block_size = 4096;
  UrmaInit::init(c);
  {
    EventBase first, second;
    auto& a = UrmaIoContext::get(&first);
    auto& b = UrmaIoContext::get(&second);
    require(UrmaInit::poolStats().allocated_bytes == 6 * 4096,
            "idle owner allocated TX-depth data storage");
    TestListener sender, receiver;
    receiver.retainData = true;
    const auto id = a.attach(&sender), target = b.attach(&receiver);
    auto peer = a.importPeer(b.endpoint());
    auto slot = a.reserve(id);
    auto* packet = a.buffer(slot, 25);
    urma_detail::encodeHeader(packet, {urma_detail::PacketKind::Data, target, 1});
    packet[24] = 73;
    urma_sge_t sg{};
    sg.addr = reinterpret_cast<uint64_t>(packet);
    sg.len = 25;
    sg.tseg = a.segment(slot);
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_SEND;
    wr.tjetty = UrmaIoContext::handle(peer);
    wr.send.src = {&sg, 1};
    std::vector<UrmaPoolBuffer> pressure;
    while (auto block = UrmaInit::poolAlloc(4096)) pressure.push_back(std::move(block));
    require(a.post(slot, id, wr, {}) == URMA_SUCCESS, "pressure DATA post");
    b.poll(); // Keep TX occupied until its CQE is polled on a.
    require(receiver.held && receiver.held->data()[0] == 73 && b.snapshot().posted == 1,
            "held RX was reused while replacement allocation failed");
    receiver.held.reset();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (b.snapshot().posted != 2 && std::chrono::steady_clock::now() < deadline)
      second.loopOnce(EVLOOP_NONBLOCK);
    require(b.snapshot().posted == 2, "last RX reference did not restore preposting");
    auto control = a.reserve(id, true);
    auto* emergency = a.buffer(control, 24, true);
    require(emergency, "exhausted pool could not send FREE");
    auto blocked = a.reserve(id, true);
    require(!a.buffer(blocked, 24, true), "emergency block borrowed by two WRs");
    a.cancel(control);
    require(a.buffer(blocked, 24, true) == emergency, "cancel lost emergency block");
    urma_detail::encodeHeader(emergency, {urma_detail::PacketKind::Free, target, 1});
    sg.addr = reinterpret_cast<uint64_t>(emergency);
    sg.len = 24;
    sg.tseg = a.segment(blocked);
    wr.opcode = URMA_OPC_SEND_IMM;
    require(a.post(blocked, id, wr, {}) == URMA_SUCCESS, "emergency FREE post");
    control = a.reserve(id, true);
    require(!a.buffer(control, 24, true), "in-flight FREE storage reused before CQE");
    a.cancel(control);
    a.poll(); b.poll();
    require(sender.completions == 2 && receiver.receives == 2,
            "emergency completion did not preserve DATA/FREE");
    pressure.clear();
    a.detach(id); b.detach(target);
  }
  UrmaInit::fini();
  require(!UrmaInit::isInitialized() && !registrations,
          "pressure case retained registered storage");
  std::cout << "PASS on-demand TX, emergency FREE, held RX and refill recovery\n";
}
int main(int argc, char** argv) {
  const bool readError = argc == 2 && !std::strcmp(argv[1], "--read-error");
  const bool pressureTimeout = argc == 2 && !std::strcmp(argv[1], "--rx-timeout");
  const bool revokeAbort = argc == 2 && !std::strcmp(argv[1], "--revoke-abort");
  const bool closeRead = argc == 2 && !std::strcmp(argv[1], "--close-read");
  const bool closeUnknown = argc == 2 && !std::strcmp(argv[1], "--close-read-unknown");
  const bool readPressure = argc == 2 && !std::strcmp(argv[1], "--read-pressure");
  const bool rnrGap = argc == 2 && !std::strcmp(argv[1], "--rnr-gap");
  if (argc != 1 && !readError && !pressureTimeout && !revokeAbort &&
      !closeRead && !closeUnknown && !rnrGap && !readPressure)
    return 2;
  try {
    if (readPressure) { readPressureCases(); return 0; }
    if (rnrGap) { rnrGapCases(); return 0; }
    if (closeRead || closeUnknown) { closeReadCases(closeUnknown); return 0; }
    if (pressureTimeout) { elasticCases(true); return 0; }
    if (revokeAbort) { revocationCases(true); return 0; }
    UrmaConfig config;
    require(config.rx_target == 256, "default RX prepost target");
    config.rx_depth = 128;
    config.rx_target = 4;
    config.tx_depth = 2;
    UrmaInit::init(config);
    for (const char* step : {"event", "cq", "jfr", "jfs"}) {
      EventBase evb;
      createFailure = step;
      bool failed = false;
      try {
        UrmaIoContext::get(&evb);
      } catch (const std::runtime_error&) {
        failed = true;
      }
      require(
          failed && registrations == 1 && jfsCreated == jfsDeleted &&
              jfrCreated == jfrDeleted,
          "partial owner creation leaked resources");
    }
    for (bool cancel : {false, true}) {
      EventBase evb;
      receiveAgain = true;
      auto& pending = UrmaIoContext::get(&evb);
      require(pending.snapshot().posted == 0, "partial initial post not retained");
      TestListener listener;
      auto id = pending.attach(&listener);
      if (!cancel) {
        require(pending.prepare() && pending.snapshot().posted == 4,
                "asynchronous initial receive retry failed");
      }
      pending.detach(id);
      if (cancel) require(pending.snapshot().jfr == 0, "cancelled preparation retained hardware");
      UrmaIoContext::release(&evb);
      require(jfsCreated == jfsDeleted && jfrCreated == jfrDeleted,
              "preparation retry/cancel leaked queues");
    }
    {
      EventBase first, second;
      auto& a = UrmaIoContext::get(&first);
      auto& b = UrmaIoContext::get(&second);
      require(&a == &UrmaIoContext::get(&first) && &a != &b, "owner identity");
      require(a.endpoint().id != b.endpoint().id, "owners share local queues");
      require(
          a.snapshot().rxDepth == 128 && a.snapshot().rxTarget == 4 &&
              a.snapshot().posted == 4,
          "hardware depth and posted target conflated");
      require(
          a.reserve(UINT64_MAX) == UrmaIoContext::kNoSlot,
          "foreign connection reserved SQ");
      TestListener listener;
      auto count = jfsCreated;
      for (int i = 0; i < 1000; ++i) {
        auto id = a.attach(&listener);
        a.detach(id);
      }
      require(jfsCreated == count, "connect/close recreated owner");
      auto peer = a.importPeer(b.endpoint());
      auto same = a.importPeer(b.endpoint());
      require(peer == same, "same endpoint imported twice");
      auto otherId = b.endpoint();
      otherId.eid.raw[0] = 1;
      auto other = a.importPeer(otherId);
      require(peer != other, "numeric ID aliases EID");
      auto thirdId = b.endpoint();
      thirdId.uasid = 1;
      auto third = a.importPeer(thirdId);
      require(peer != third, "numeric ID aliases UASID");
      blockUnimport = true;
      peer.reset();
      same.reset();
      require(
          !a.importPeer(b.endpoint()),
          "endpoint reimported before deferred retirement");
      blockUnimport = false;
      require(
          urma_detail::waitForCleanups(std::chrono::milliseconds(500)),
          "peer cleanup did not recover");
      peer = a.importPeer(b.endpoint());
      require(bool(peer), "retired endpoint could not be reimported");
      auto id = a.attach(&listener);
      auto s1 = a.reserve(id), s2 = a.reserve(id);
      require(
          a.reserve(id) == UrmaIoContext::kNoSlot, "SQ capacity not enforced");
      a.cancel(s1);
      a.cancel(s2);
      a.poll();
      require(listener.wakes == 1, "waiter not deduplicated/woken");
      s1 = a.reserve(id);
      s2 = a.reserve(id);
      require(
          a.reserve(id) == UrmaIoContext::kNoSlot, "SQ unexpectedly available");
      a.detach(id);
      require(
          a.snapshot().waiting == 0, "closed connection left a stale waiter");
      a.cancel(s1);
      a.cancel(s2);
      id = a.attach(&listener);
      // Reuse owner-owned contexts beyond the old 16-bit generation range;
      // the original full-width user value must still reach the listener.
      uint64_t completionAddress = 0;
      TestListener receiver;
      auto receiverId = b.attach(&receiver);
      for (uint64_t i = 1; i <= 65538; ++i) {
        auto index = a.reserve(id);
        require(
            index != UrmaIoContext::kNoSlot,
            "unexpected sequential SQ exhaustion");
        auto* packet = a.buffer(index, 25);
        urma_detail::encodeHeader(
            packet, {urma_detail::PacketKind::Data, receiverId, i});
        packet[24] = 0;
        urma_sge_t sg{};
        sg.addr = reinterpret_cast<uint64_t>(packet);
        sg.len = 25;
        sg.tseg = a.segment(index);
        urma_jfs_wr_t send{};
        send.opcode = URMA_OPC_SEND;
        send.tjetty = UrmaIoContext::handle(peer);
        send.send.src.sge = &sg;
        send.send.src.num_sge = 1;
        send.user_ctx = UINT64_MAX;
        require(
            a.post(index, id, send, {}) == URMA_SUCCESS,
            "sequential WR submit");
        if (!completionAddress)
          completionAddress = lastCompletionContext;
        require(
            completionAddress && lastCompletionContext == completionAddress,
            "sequential WRs did not reuse their stable completion context");
        if (i == 65538)
          a.detach(id); // The terminal CQE must not call a dead socket.
        a.poll();
        b.poll();
      }
      require(
          listener.completions == 65537 && listener.lastUser == UINT64_MAX,
          "late CQE callback or truncated user context");
      // Deferred DATA retains buffers, while every RX slot remains available
      // for controls even when the deferred queue exceeds the prepost count.
      id = a.attach(&listener);
      receiver.blockData = true;
      const auto delivered = receiver.receives;
      auto sendPacket = [&](urma_detail::PacketKind kind, uint64_t sequence) {
        auto index = a.reserve(id);
        auto* packet = a.buffer(index, 25);
        urma_detail::encodeHeader(packet, {kind, receiverId, sequence});
        packet[24] = 7;
        urma_sge_t sg{};
        sg.addr = reinterpret_cast<uint64_t>(packet);
        sg.len = kind == urma_detail::PacketKind::Data ? 25 : 24;
        sg.tseg = a.segment(index);
        urma_jfs_wr_t wr{};
        wr.opcode = kind == urma_detail::PacketKind::Data ? URMA_OPC_SEND : URMA_OPC_SEND_IMM;
        wr.tjetty = UrmaIoContext::handle(peer);
        wr.send.src.sge = &sg;
        wr.send.src.num_sge = 1;
        require(a.post(index, id, wr, {}) == URMA_SUCCESS, "pressure send");
        a.poll(); b.poll();
      };
      for (int i = 0; i < 8; ++i) sendPacket(urma_detail::PacketKind::Data, i + 1);
      require(b.snapshot().posted == 4 && receiver.receives == delivered &&
              b.hasDeferredReceives(receiverId), "deferred RX overwritten or delivered");
      sendPacket(urma_detail::PacketKind::Free, 1);
      require(receiver.receives == delivered + 1 && b.snapshot().posted == 4,
              "FREE blocked by legal DATA pressure");
      receiver.blockData = false;
      b.poll();
      require(receiver.receives == delivered + 9 && b.snapshot().posted == 4 &&
              !b.hasDeferredReceives(receiverId), "deferred RX did not recover exactly once");
      receiver.blockData = true;
      sendPacket(urma_detail::PacketKind::Data, 4);
      b.detach(receiverId);
      b.poll();
      require(b.snapshot().posted == 4, "closed deferred RX was not reclaimed");
      a.detach(id);

      id = a.attach(&listener);
      require(a.initialReadSegment() != nullptr, "initial READ segment");
      std::vector<std::shared_ptr<void>> responsibilities;
      for (int i = 0; i < 1024; ++i) {
        responsibilities.push_back(a.retainRead());
        require(
            bool(responsibilities.back()), "owner READ count budget too small");
      }
      require(!a.retainRead(), "owner FREE responsibility count unbounded");
      responsibilities.clear();
      require(
          bool(a.retainRead()), "owner READ responsibility did not recover");
      for (size_t i = 0; i < 65536; ++i) {
        responsibilities.push_back(a.retainReadSource());
        require(bool(responsibilities.back()), "source responsibility limit too small");
      }
      require(!b.retainReadSource(), "source metadata bound was only per owner");
      responsibilities.pop_back();
      require(bool(b.retainReadSource()), "returned source responsibility did not recover");
      responsibilities.clear();
      auto slot = a.reserve(id);
      unsigned releasedHold = 0;
      auto hold = std::shared_ptr<int>(new int, [&](int* p) {
        ++releasedHold;
        delete p;
      });
      urma_jfs_wr_t wr{};
      wr.opcode = URMA_OPC_SEND;
      unknownAcceptance = true;
      listener.allowFailure = true;
      listener.onFailure = [&] {
        a.detach(id);
        bool rejected = false;
        try {
          UrmaIoContext::release(&first);
        } catch (const std::logic_error&) {
          rejected = true;
        }
        require(
            rejected, "owner destroyed while a failure callback was active");
      };
      require(
          a.post(slot, id, wr, std::move(hold)) == URMA_EAGAIN &&
              listener.failures == 1,
          "ambiguous submission did not fail owner");
      a.detach(id);
      require(
          releasedHold == 0,
          "ambiguous WR released DMA references before teardown");
      UrmaIoContext::release(&first);
      require(
          releasedHold == 1,
          "successful JFS teardown did not release WR references");
    }
    require(
        jfsCreated == jfsDeleted && jfrCreated == jfrDeleted &&
            registrations == 1,
        "owner teardown leaked");
    {
      EventBase evb;
      auto& owner = UrmaIoContext::get(&evb);
      TestListener listener;
      auto id = owner.attach(&listener);
      unsigned released = 0;
      uint8_t source = 42, destination = 0;
      urma_sge_t src{}, dst{};
      src.addr = reinterpret_cast<uintptr_t>(&source);
      src.len = 1;
      dst.addr = reinterpret_cast<uintptr_t>(&destination);
      dst.len = 1;
      urma_jfs_wr_t wr{};
      wr.opcode = URMA_OPC_READ;
      wr.user_ctx = UINT64_MAX;
      wr.rw.src.sge = &src;
      wr.rw.src.num_sge = 1;
      wr.rw.dst.sge = &dst;
      wr.rw.dst.num_sge = 1;
      auto submit = [&] {
        auto index = owner.reserve(id);
        require(index != UrmaIoContext::kNoSlot, "reentrant SQ reuse failed");
        auto hold = std::shared_ptr<void>(new int, [&](void* p) {
          ++released;
          delete static_cast<int*>(p);
        });
        require(
            owner.post(index, id, wr, std::move(hold)) == URMA_SUCCESS,
            "reentrant READ submission failed");
      };
      listener.onCompletion = [&] {
        require(
            listener.lastUser == UINT64_MAX && destination == source &&
                released + 1 == listener.completions,
            "completion released its DMA hold before the callback finished");
        owner.poll(); // Nested polling must not recurse into this callback.
        if (listener.completions < 600)
          submit(); // Reuse the context while the previous hold is still live.
        else
          owner.detach(id);
        require(
            released + 1 == listener.completions,
            "slot reuse/close released the previous completion's DMA hold");
      };
      submit();
      owner.poll();
      require(
          listener.completions == 600 && released == 600 &&
              owner.snapshot().inflight == 0,
          "one poll did not drain completions beyond the old 256 CQE budget");
      UrmaIoContext::release(&evb);
    }
    // Preserve event ownership and interrupt failure behavior when replacing
    // PR22's per-connection queues with the shared owner.
    for (int fault : {0, 1, 2, 3, 4}) {
      EventBase evb;
      auto& owner = UrmaIoContext::get(&evb);
      TestListener listener;
      listener.allowFailure = true;
      auto id = owner.attach(&listener);
      auto* cq = receivers.at(owner.endpoint().id)->jfr_cfg.jfc;
      unsigned released = 0;
      uint8_t source = 42, destination = 0;
      urma_sge_t src{}, dst{};
      src.addr = reinterpret_cast<uintptr_t>(&source);
      src.len = 1;
      dst.addr = reinterpret_cast<uintptr_t>(&destination);
      dst.len = 1;
      urma_jfs_wr_t wr{};
      wr.opcode = URMA_OPC_READ;
      wr.rw.src = {&src, 1};
      wr.rw.dst = {&dst, 1};
      auto hold = std::shared_ptr<void>(new int, [&](void* p) {
        ++released;
        delete static_cast<int*>(p);
      });
      holdReads = fault == 4;
      require(owner.post(owner.reserve(id), id, wr, std::move(hold)) ==
                  URMA_SUCCESS, "interrupt READ submission failed");
      holdReads = false;
      failEventWait = fault == 1;
      failCqPoll = fault == 2;
      failCqRearm = fault == 3;
      if (fault == 4) {
        require(heldReads.size() == 1, "rearm race setup failed");
        onRearm = [&] {
          signalQueue(heldReads.front().first, heldReads.front().second);
          heldReads.clear();
        };
        uint64_t one = 1;
        require(::write(cq->jfc_cfg.jfce->fd, &one, sizeof(one)) == sizeof(one),
                "empty interrupt setup failed");
      }
      for (int i = 0; i < 5; ++i) evb.loopOnce(EVLOOP_NONBLOCK);
      const bool failed = fault > 0 && fault < 4;
      const bool completed = fault == 0 || fault >= 3;
      require(listener.failures == unsigned(failed) &&
                  listener.completions == unsigned(completed) &&
                  released == unsigned(completed),
              "event failure lost or prematurely released a DMA hold");
      if (completed) require(destination == source, "interrupt READ data");
      owner.detach(id);
      UrmaIoContext::release(&evb);
      require(released == 1 && eventsRetrieved == eventsAcknowledged,
              "event cleanup failed to acknowledge or release exactly once");
    }
    for (int invalidKind : {0, 1, 2}) {
      EventBase evb;
      auto& owner = UrmaIoContext::get(&evb);
      TestListener listener;
      listener.allowFailure = true;
      auto id = owner.attach(&listener);
      auto* cq = receivers.at(owner.endpoint().id)->jfr_cfg.jfc;
      uint8_t source = 1, destination = 0;
      urma_sge_t src{}, dst{};
      src.addr = reinterpret_cast<uintptr_t>(&source);
      src.len = 1;
      dst.addr = reinterpret_cast<uintptr_t>(&destination);
      dst.len = 1;
      urma_jfs_wr_t wr{};
      wr.opcode = URMA_OPC_READ;
      wr.rw.src.sge = &src;
      wr.rw.src.num_sge = 1;
      wr.rw.dst.sge = &dst;
      wr.rw.dst.num_sge = 1;
      require(
          owner.post(owner.reserve(id), id, wr, {}) == URMA_SUCCESS,
          "invalid CQE setup failed");
      auto invalid = ready[cq].front();
      if (invalidKind == 2)
        owner.poll(); // A duplicate completion for an already idle slot.
      else {
        ready[cq].clear();
        invalid.user_ctx = invalidKind ? invalid.user_ctx + 1 : UINT64_MAX;
      }
      signalQueue(cq, invalid);
      owner.poll();
      require(
          listener.failures == 1 &&
              listener.completions == unsigned(invalidKind == 2),
          "invalid completion pointer was dereferenced/dispatched");
      owner.detach(id);
      UrmaIoContext::release(&evb);
    }
    // The owner tests above intentionally use a two-entry SQ. The transport
    // test needs room to prove ten independent READs outstanding together.
    require(
        urma_detail::waitForCleanups(std::chrono::seconds(2)),
        "owner test cleanup pending");
    UrmaInit::fini();
    require(!UrmaInit::isInitialized(), "owner test context still live");
    config.tx_depth = 32;
    config.rx_target = 32;
    registrations = 0;
    UrmaInit::init(config);
    // A completion callback admits two independent READs in one owner turn.
    // Exercise a partially accepted chain, including close and ambiguous post.
    for (int fault = 0; fault < 5; ++fault) {
      EventBase evb;
      auto& owner = UrmaIoContext::get(&evb);
      TestListener listener;
      listener.allowFailure = true;
      auto id = owner.attach(&listener);
      listener.onFailure = [&] { owner.detach(id); };
      std::array<uint8_t, 3> source{{40, 41, 42}}, destination{};
      std::array<unsigned, 3> released{};
      auto submit = [&](size_t i) {
        urma_sge_t src{}, dst{};
        src.addr = reinterpret_cast<uintptr_t>(&source[i]);
        src.len = 1;
        dst.addr = reinterpret_cast<uintptr_t>(&destination[i]);
        dst.len = 1;
        urma_jfs_wr_t wr{};
        wr.opcode = URMA_OPC_READ;
        wr.user_ctx = i;
        wr.rw.src = {&src, 1};
        wr.rw.dst = {&dst, 1};
        auto hold = std::shared_ptr<void>(new int, [&, i](void* p) {
          ++released[i];
          delete static_cast<int*>(p);
        });
        require(
            owner.post(owner.reserve(id), id, wr, std::move(hold)) ==
                URMA_SUCCESS,
            "READ batch admission failed");
      };
      listener.onCompletion = [&] {
        if (listener.lastUser == 0) {
          holdReads = true;
          rejectReadAfter = fault == 1 ? 0 : 1;
          rejectReadStatus = fault == 3 ? URMA_EINVAL : URMA_EAGAIN;
          unknownReadBoundary = fault == 4;
          submit(1);
          submit(2);
        } else {
          require(
              destination[listener.lastUser] == source[listener.lastUser],
              "READ completion routed to the wrong buffer");
        }
      };
      const auto acceptedBefore = acceptedReadWrs;
      maxReadBatch = 0;
      submit(0);
      owner.poll();
      const size_t prefix = fault == 1 ? 0 : 1;
      require(
          maxReadBatch == 2 && heldReads.size() == prefix &&
              acceptedReadWrs == acceptedBefore + 1 + prefix,
          "partial READ chain boundary was not exercised");
      require(
          released[0] == 1 && released[1] == 0 &&
              released[2] == unsigned(fault == 3),
          "partial post released accepted or ambiguous DMA resources");
      if (fault < 2) {
        blockReadPosts = true;
        owner.poll();
        require(
            heldReads.size() == prefix && owner.snapshot().inflight == 2,
            "rejected suffix lost its slot or duplicated accepted prefix");
        blockReadPosts = false;
        owner.poll();
        require(
            heldReads.size() == 2 && acceptedReadWrs == acceptedBefore + 3,
            "READ suffix was not retried exactly once");
        completeHeldReverse(heldReads);
        owner.poll();
        require(
            listener.completions == 3 && !listener.failures &&
                !owner.snapshot().inflight && released[1] == 1 &&
                released[2] == 1,
            "reversed batch completions leaked holds or slots");
        owner.detach(id);
      } else if (fault == 2) {
        owner.detach(id);
        require(
            released[1] == 0 && released[2] == 1 &&
                owner.snapshot().inflight == 1,
            "detach did not distinguish queued READ from active DMA");
        completeHeld(heldReads);
        owner.poll();
        require(
            listener.completions == 1 && released[1] == 1 &&
                !owner.snapshot().inflight,
            "late CQE called a detached listener or leaked its hold");
      } else {
        require(
            listener.failures == 1 &&
                owner.snapshot().inflight == (fault == 3 ? 1 : 2),
            "fatal batch failure lost uncertain DMA responsibility");
      }
      UrmaIoContext::release(&evb);
      require(
          released[1] == 1 && released[2] == 1,
          "READ batch teardown did not release every hold once");
      heldReads.clear();
      holdReads = unknownReadBoundary = blockReadPosts = false;
      rejectReadAfter = -1;
      rejectReadStatus = URMA_EAGAIN;
    }
    {
      EventBase evb;
      Acceptor acceptor(evb);
      SocketAddress address;
      auto server = listenOnLoopback(evb, acceptor, address, 128);
      // Publish server identity only after validating the client's HELLO.
      // Exercise bytewise HELLO and a split ACK on the real TCP channel.
      int raw = ::socket(AF_INET, SOCK_STREAM, 0);
      require(raw >= 0, "raw handshake socket");
      int noDelay = 1;
      require(
          !::setsockopt(
              raw, IPPROTO_TCP, TCP_NODELAY, &noDelay, sizeof(noDelay)),
          "raw handshake TCP_NODELAY");
      sockaddr_in destination{};
      destination.sin_family = AF_INET;
      destination.sin_port = htons(address.getPort());
      destination.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
      require(
          !::connect(
              raw,
              reinterpret_cast<sockaddr*>(&destination),
              sizeof(destination)),
          "raw handshake connect");
      for (int i = 0; i < 1000 && acceptor.connections.empty(); ++i)
        evb.loopOnce(EVLOOP_NONBLOCK);
      require(acceptor.connections.size() == 1, "raw handshake accept");
      uint8_t reply[urma_detail::kHelloSize];
      require(
          ::recv(raw, reply, sizeof(reply), MSG_DONTWAIT) < 0 &&
              errno == EAGAIN,
          "server exposed its routing ID before validating peer HELLO");
      urma_detail::Hello hello;
      const auto& endpoint = UrmaIoContext::get(&evb).endpoint();
      hello.bufferSize = 4096;
      hello.posted = 4;
      hello.jfr = endpoint.id;
      hello.tpType = URMA_CTP;
      hello.socket = UrmaIoContext::allocateSocketId();
      std::array<uint8_t, urma_detail::kHelloSize> encoded;
      urma_detail::encodeHello(encoded.data(), hello);
      for (size_t i = 0; i < encoded.size(); ++i) {
        require(
            ::send(raw, &encoded[i], 1, MSG_NOSIGNAL) == 1, "short HELLO send");
        evb.loopOnce(EVLOOP_NONBLOCK);
      }
      size_t got = 0;
      for (int i = 0; i < 10000 && got != sizeof(reply); ++i) {
        evb.loopOnce(EVLOOP_NONBLOCK);
        auto n = ::recv(raw, reply + got, sizeof(reply) - got, MSG_DONTWAIT);
        if (n > 0)
          got += n;
      }
      require(
          got == sizeof(reply) && urma_detail::decodeHello(reply, got, hello),
          "fragmented HELLO did not complete");
      uint8_t ack[4] = {0, 0, 0, 1};
      require(::send(raw, ack, 2, MSG_NOSIGNAL) == 2, "first ACK fragment");
      evb.loopOnce(EVLOOP_NONBLOCK);
      auto* accepted = static_cast<UrmaAsyncSocket*>(
          acceptor.connections.front()->socket.get());
      require(!accepted->isUrmaReady(), "OPEN before complete TCP ACK");
      require(::send(raw, ack + 2, 2, MSG_NOSIGNAL) == 2, "last ACK fragment");
      for (int i = 0; i < 10000 && !accepted->isUrmaReady(); ++i)
        evb.loopOnce(EVLOOP_NONBLOCK);
      require(accepted->isUrmaReady(), "split TCP ACK not accepted");
      ::close(raw);
      acceptor.connections.clear();
      std::vector<std::unique_ptr<Echo>> clients;
      for (int i = 0; i < 96; ++i)
        clients.emplace_back(
            new Echo(UrmaAsyncSocket::newSocket(&evb, address), false));
      const char* stage = "handshake";
      auto spin = [&](auto done) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (!done() && std::chrono::steady_clock::now() < deadline)
          evb.loopOnce(EVLOOP_NONBLOCK);
        if (!done()) {
          std::cerr << "stalled stage: " << stage << std::endl;
          for (size_t i = 0; i < clients.size(); ++i)
            if (clients[i]->received != clients[i]->expected.size() ||
                !clients[i]->completed)
              std::cerr << "client " << i << " bytes=" << clients[i]->received
                        << " completed=" << clients[i]->completed << std::endl;
          auto snapshot = UrmaIoContext::get(&evb).snapshot();
          std::cerr << "inflight=" << snapshot.inflight
                    << " wait=" << snapshot.waiting
                    << " heldReads=" << heldReads.size() << std::endl;
        }
        require(done(), "mock shared transport did not progress");
      };
      spin([&] {
        if (acceptor.connections.size() != clients.size())
          return false;
        for (auto& c : clients)
          if (!static_cast<UrmaAsyncSocket*>(c->socket.get())->isUrmaReady())
            return false;
        return true;
      });
      auto& owner = UrmaIoContext::get(&evb);
      auto count = jfsCreated;
      require(
          owner.snapshot().connections == 192 && owner.snapshot().imports == 1,
          "sockets did not share owner/import");
      for (size_t size : {size_t(1024), size_t(8192), size_t(1024)}) {
        stage = size == 8192 ? "shared READ" : "shared DATA";
        postAgain = true;
        receiveAgain = true;
        for (size_t i = 0; i < clients.size(); ++i) {
          auto& c = *clients[i];
          c.expected.assign(size, char('a' + i));
          c.received = c.completed = 0;
          c.socket->write(&c, c.expected.data(), size);
        }
        spin([&] {
          for (auto& c : clients)
            if (c->received != c->expected.size() || c->completed != 1)
              return false;
          return true;
        });
        for (int i = 0; i < 32; ++i)
          evb.loopOnce(EVLOOP_NONBLOCK);
        require(
            registrations == 1,
            "idle READ registrations grew with connections");
      }
      for (auto& c : clients)
        require(
            static_cast<UrmaAsyncSocket*>(c->socket.get())
                    ->getUrmaStats()
                    .reads > 0,
            "READ not actually completed per connection");
      require(
          segmentImports == 1 && checkedPulls && checkedFrees,
          "HELLO segment imported per message/connection or final controls absent");
      auto& ordered = *clients[1];
      for (bool oneWrite : {false, true}) {
        stage = "ten simultaneous READ messages, reverse completions";
        ordered.expected.clear();
        ordered.received = ordered.completed = 0;
        holdReads = true;
        const auto parallelPullsBefore = checkedPulls;
        const auto parallelStatsBefore =
            static_cast<UrmaAsyncSocket*>(ordered.socket.get())->getUrmaStats();
        std::vector<std::unique_ptr<IOBuf>> sharedSources;
        IOBufQueue fragmented;
        maxReadBatch = 0;
        for (int i = 0; i < 10; ++i) {
          std::string message(8192, char('A' + i));
          ordered.expected += message;
          sharedSources.push_back(IOBuf::copyBuffer(message));
          if (oneWrite)
            fragmented.append(sharedSources.back()->clone());
          else
            ordered.socket->writeChain(&ordered, sharedSources.back()->clone());
        }
        if (oneWrite)
          ordered.socket->writeChain(&ordered, fragmented.move());
        spin([&] { return heldReads.size() == 10; });
        require(
            maxReadBatch > 1,
            "discontiguous READs were not posted as a WR chain");
        const auto parallelStats =
            static_cast<UrmaAsyncSocket*>(ordered.socket.get())->getUrmaStats();
        require(
            parallelStats.directReadBytes -
                        parallelStatsBefore.directReadBytes ==
                    81920 &&
                parallelStats.copiedReadBytes ==
                    parallelStatsBefore.copiedReadBytes,
            "registered READ source took a staging copy");
        require(
            checkedPulls == parallelPullsBefore + 10 && !ordered.completed &&
                !ordered.received,
            "READs serialized or data delivered before completion");
        // Deliver 9..1 first. Their FREEs may complete the associated source,
        // but both application bytes and write callbacks must remain ordered.
        for (size_t i = heldReads.size(); i > 1; --i)
          signalQueue(heldReads[i - 1].first, heldReads[i - 1].second);
        for (int i = 0; i < 32; ++i)
          evb.loopOnce(EVLOOP_NONBLOCK);
        require(
            !ordered.received && !ordered.completed,
            "later READ bypassed the first message");
        auto firstRead = heldReads.front();
        heldReads.clear();
        holdReads = false;
        signalQueue(firstRead.first, firstRead.second);
        spin([&] {
          return ordered.received == ordered.expected.size() &&
              ordered.completed == (oneWrite ? 1 : 10);
        });
        sharedSources.clear();
      }

      stage = "registered SEND chain exceeds one WR SGE count";
      ordered.expected.clear();
      ordered.received = ordered.completed = 0;
      IOBufQueue chain;
      for (int i = 0; i < 4; ++i) {
        std::string part(900, char('a' + i));
        ordered.expected += part;
        chain.append(IOBuf::copyBuffer(part));
      }
      const auto sendStatsBefore =
          static_cast<UrmaAsyncSocket*>(ordered.socket.get())->getUrmaStats();
      ordered.socket->writeChain(&ordered, chain.move());
      spin([&] { return ordered.received == 3600 && ordered.completed == 1; });
      const auto sendStats =
          static_cast<UrmaAsyncSocket*>(ordered.socket.get())->getUrmaStats();
      require(
          sendStats.copiedSendBytes - sendStatsBefore.copiedSendBytes == 3600 &&
              sendStats.directSendBytes == sendStatsBefore.directSendBytes,
          "fragmented SEND must copy-coalesce instead of splitting at the SGE limit");

      stage = "single registered SEND uses exclusive headroom";
      ordered.expected.assign(1000, 'h');
      ordered.received = ordered.completed = 0;
      auto single = IOBuf::create(1024);
      single->advance(urma_detail::kHeaderSize);
      std::memcpy(single->writableData(), ordered.expected.data(), 1000);
      single->append(1000);
      const auto directBefore = static_cast<UrmaAsyncSocket*>(ordered.socket.get())->getUrmaStats();
      ordered.socket->writeChain(&ordered, std::move(single));
      require(lastDataSges == 1, "exclusive SEND headroom did not remove the header SGE");
      spin([&] { return ordered.received == 1000 && ordered.completed == 1; });
      require(static_cast<UrmaAsyncSocket*>(ordered.socket.get())->getUrmaStats().directSendBytes
                  == directBefore.directSendBytes + 1000, "in-place SEND copied payload");

      ordered.expected.assign(5000, 'r');
      ordered.received = ordered.completed = 0;
      stage = "reordered DATA";
      reorderData = true;
      static_cast<UrmaAsyncSocket*>(ordered.socket.get())
          ->writeChainWithRecords(
              &ordered, IOBuf::copyBuffer(ordered.expected), {2000, 3000});
      spin([&] { return ordered.received == 5000 && ordered.completed == 1; });
      require(
          !reorderData && heldData.empty(),
          "out-of-order DATA was not exercised");

      // FREE before local PULL CQE must not finish the write; duplicate FREE
      // must not consume a receive sequence or release a later READ source.
      ordered.expected.assign(8192, 'v');
      ordered.received = ordered.completed = 0;
      ordered.retainRead = true;
      stage = "FREE before PULL CQE";
      holdPullCompletion = duplicateFree = true;
      ordered.socket->write(
          &ordered, ordered.expected.data(), ordered.expected.size());
      spin([&] {
        return ordered.received == 8192 && heldPullCompletions.size() == 1;
      });
      require(
          ordered.completed == 0,
          "FREE completed a write before its local PULL CQE");
      stage = "released PULL CQEs";
      holdPullCompletion = false;
      completeHeldReverse(heldPullCompletions);
      spin([&] { return ordered.completed == 1; });
      for (int i = 0; i < 32; ++i)
        evb.loopOnce(EVLOOP_NONBLOCK);
      duplicateFree = false;
      ordered.retainRead = false;
      auto retained = std::move(ordered.retained);
      stage = "retained READ destination";
      ordered.expected.assign(8192, 'w');
      ordered.received = ordered.completed = 0;
      ordered.socket->write(
          &ordered, ordered.expected.data(), ordered.expected.size());
      spin([&] { return ordered.received == 8192 && ordered.completed == 1; });
      require(
          retained && retained->length() == 8192,
          "READ IOBuf ownership missing");
      for (size_t i = 0; i < retained->length(); ++i)
        require(
            retained->data()[i] == 'v',
            "subsequent READ overwrote an application-held IOBuf");
      // Last release on a different thread is allowed; owner allocation
      // reclaims it.
      std::thread consumer(
          [data = std::move(retained)]() mutable { data.reset(); });
      consumer.join();

      // Resource pressure must leave a valid PULL queued, not fail the stream
      // or start READ before its destination and active budget are available.
      {
        stage = "READ count wait";
        std::vector<std::shared_ptr<void>> pressure;
        const size_t count = 1024;
        for (size_t i = 0; i < count; ++i) {
          pressure.push_back(
              owner.retainRead());
          require(bool(pressure.back()), "could not occupy READ budget");
        }
        ordered.expected.assign(8192, 'b');
        ordered.received = ordered.completed = 0;
        const auto readsBefore = owner.snapshot().reads;
        ordered.socket->write(
            &ordered, ordered.expected.data(), ordered.expected.size());
        spin([&] { return owner.snapshot().waiting != 0; });
        require(
            ordered.socket->good() && !ordered.completed && !ordered.received &&
                owner.snapshot().reads == readsBefore,
            "READ budget pressure closed the stream or submitted READ");
        pressure.clear();
        spin(
            [&] { return ordered.received == 8192 && ordered.completed == 1; });
        for (int i = 0; i < 32; ++i)
          evb.loopOnce(EVLOOP_NONBLOCK);
        require(owner.snapshot().waiting == 0, "READ budget waiter leaked");
      }

      stage = "READ destination wait";
      ordered.expected.assign(8192, 'm');
      ordered.received = ordered.completed = 0;
      const auto pullsBefore = checkedPulls;
      const auto readsBefore = owner.snapshot().reads;
      ordered.socket->write(
          &ordered, ordered.expected.data(), ordered.expected.size());
      require(checkedPulls > pullsBefore, "source did not publish PULL");
      std::vector<UrmaInit::Buffer> pressure;
      for (size_t bytes : {size_t(1024 * 1024), size_t(4096)}) {
        while (true) {
          auto buffer = UrmaInit::allocateBuffer(bytes);
          if (!buffer.memory)
            break;
          pressure.push_back(std::move(buffer));
        }
      }
      spin([&] { return owner.snapshot().waiting != 0; });
      require(
          ordered.socket->good() && !ordered.completed && !ordered.received &&
              owner.snapshot().reads == readsBefore,
          "destination pressure closed the stream or submitted READ");
      auto budget = holdReadSlots(owner);
      budget.clear();
      // No CQE is needed to wake a buffer returned by an application thread.
      std::thread releaser(
          [buffers = std::move(pressure)]() mutable { buffers.clear(); });
      releaser.join();
      spin([&] { return ordered.received == 8192 && ordered.completed == 1; });
      for (int i = 0; i < 32; ++i)
        evb.loopOnce(EVLOOP_NONBLOCK);
      require(owner.snapshot().waiting == 0, "destination waiter leaked");

      if (readError) {
        stage = "READ failure must not skip an ordered record";
        auto& failingSender = *clients[2];
        auto& failingReceiver = *acceptor.connections[2];
        failingSender.allowWriteError = failingSender.allowReadError = true;
        failingReceiver.allowWriteError = failingReceiver.allowReadError = true;
        failingReceiver.echo = false;
        failingReceiver.expected.assign(3 * 8192, 'e');
        holdReads = true;
        for (int i = 0; i < 3; ++i)
          failingSender.socket->write(
              &failingSender, failingReceiver.expected.data() + i * 8192, 8192);
        spin([&] { return heldReads.size() == 3; });
        for (size_t i = heldReads.size(); i > 1; --i)
          signalQueue(heldReads[i - 1].first, heldReads[i - 1].second);
        // Later records are complete but cannot be delivered across the hole.
        for (int i = 0; i < 32; ++i)
          evb.loopOnce(EVLOOP_NONBLOCK);
        require(
            !failingReceiver.received, "later READ escaped ordered delivery");
        auto failed = heldReads.front();
        failed.second.status = URMA_CR_LOC_ACCESS_ERR;
        heldReads.clear();
        holdReads = false;
        signalQueue(failed.first, failed.second);
        spin([&] { return failingReceiver.readErrors == 1; });
        require(
            !failingReceiver.received && !failingReceiver.socket->good(),
            "failed READ was skipped instead of terminating the stream");
      }

      stage = "paused and closing readers";
      auto& paused = *clients.back();
      auto& closingInCallback = *clients.front();
      paused.expected = "delayed consumer";
      paused.received = paused.completed = 0;
      paused.socket->setReadCB(nullptr);
      closingInCallback.expected.assign(8192, 'z');
      closingInCallback.received = closingInCallback.completed = 0;
      closingInCallback.closeOnRead = true;
      closingInCallback.allowWriteError = true;
      paused.socket->write(
          &paused, paused.expected.data(), paused.expected.size());
      closingInCallback.socket->write(
          &closingInCallback,
          closingInCallback.expected.data(),
          closingInCallback.expected.size());
      spin([&] {
        return closingInCallback.received ==
            closingInCallback.expected.size() &&
            paused.completed == 1;
      });
      require(paused.received == 0, "paused reader was called");
      paused.socket->setReadCB(&paused);
      spin([&] { return paused.received == paused.expected.size(); });
      clients.erase(clients.begin());
      for (int i = 0; i < 32; ++i)
        evb.loopOnce(EVLOOP_NONBLOCK);
      stage = "surviving peer";
      auto& survivor = *clients.back();
      survivor.expected = "still alive";
      survivor.received = survivor.completed = 0;
      survivor.socket->write(
          &survivor, survivor.expected.data(), survivor.expected.size());
      spin([&] {
        return survivor.received == survivor.expected.size() &&
            survivor.completed == 1;
      });
      require(jfsCreated == count, "socket close recreated queues");
      // Closing an published PULL must retain its source until the late FREE.
      // The receiver consumes without echoing, so it can finish after close.
      auto& closing = *clients.back();
      auto& receiver = *acceptor.connections.back();
      receiver.echo = false;
      receiver.expected.assign(3 * 8192, 'q');
      receiver.received = 0;
      closing.expected = receiver.expected;
      closing.allowWriteError = true;
      receiver.allowReadError = true;
      stage = "late FREE";
      holdFree = true;
      for (int i = 0; i < 3; ++i)
        closing.socket->write(
            &closing, closing.expected.data() + i * 8192, 8192);
      spin([&] {
        return receiver.received == receiver.expected.size() &&
            heldFrees.size() == 3;
      });
      const auto retiredBeforeClosing = owner.snapshot().retired;
      closing.socket->closeNow();
      const auto retiredWithClosing = owner.snapshot().retired;
      require(
          retiredWithClosing == retiredBeforeClosing + 1,
          "closed READ source was not quarantined");
      holdFree = false;
      signalQueue(heldFrees.back().first, heldFrees.back().second);
      heldFrees.pop_back();
      for (int i = 0; i < 32; ++i)
        evb.loopOnce(EVLOOP_NONBLOCK);
      require(
          owner.snapshot().retired == retiredWithClosing,
          "one late FREE released other unconfirmed READ sources");
      completeHeldReverse(heldFrees);
      spin([&] {
        return owner.snapshot().retired + 1 == retiredWithClosing &&
            receiver.received == receiver.expected.size() && receiver.readErrors == 1;
      });
      clients.clear();
      acceptor.connections.clear();
      server->stopAccepting();
      server.reset();
      UrmaIoContext::release(&evb);
    }
    UrmaInit::fini();
    if (readError) {
      // Run separately: an errored READ never sends FREE. The source must
      // remain quarantined; a local CQE/close is not a remote DMA barrier.
      require(
          UrmaInit::isInitialized() && registrations &&
              UrmaInit::pendingCleanupCount(),
          "unconfirmed remote READ source was prematurely released");
      std::cout
          << "PASS failed ordered READ is not skipped; unconfirmed source retained\n";
    } else {
      require(
          !UrmaInit::isInitialized() && !registrations &&
              importsCreated == importsDeleted && segmentImports == segmentUnimports &&
              jfsCreated == jfsDeleted && jfrCreated == jfrDeleted,
          "final resource imbalance");
      bufferPressureCases();
      elasticCases();
      revocationCases();
    }
    std::cout
        << "PASS shared owner rollback, full endpoint identity, SQ wakeups, "
           "direct completion reuse, CQ drain, READ chains with per-WR CQEs, "
           "partial submission, reverse completion and DMA lifetime, "
           "actual multi-socket SEND/READ "
           "and close\n";
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
