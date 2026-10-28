/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0. */
// Test-only SDK interposer. A failure is injected BEFORE the real SDK call:
// the live device handle is deliberately retained and must be retried.
#include <folly/io/async/urma/UrmaInit.h>
#include <dlfcn.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <thread>
#include <unordered_map>
namespace {
// The Folly cleanup worker may still run during this interposer's static
// destruction. Keep test bookkeeping alive until process termination, too.
std::mutex& mutex = *new std::mutex;
std::unordered_map<void*, const char*>& handles =
    *new std::unordered_map<void*, const char*>;
std::unordered_map<void*, unsigned>& failures =
    *new std::unordered_map<void*, unsigned>;
std::atomic<unsigned> injected{0}, created{0}, destroyed{0};
std::atomic<bool> resume{false};
bool teardown{false}; // Protected by mutex; strict ordering for one connection.
// Fixed storage avoids allocator recursion in the free interposer. Registered
// DMA backing allocations must not be freed before successful deregistration.
std::atomic<void*> registeredMemory[1024]{};
void require(bool ok, const char* what) {
  if (!ok) { std::fprintf(stderr, "CLEANUP_ASSERT %s\n", what); std::abort(); }
}
template<class T> T next(const char* name) {
  auto p = reinterpret_cast<T>(dlsym(RTLD_NEXT, name));
  require(bool(p), name); return p;
}
const char* mode() { auto p = std::getenv("CLEANUP_FAULT"); return p ? p : ""; }
void track(void* p, const char* kind) {
  if (!p) return;
  std::lock_guard<std::mutex> lock(mutex);
  require(handles.emplace(p, kind).second, "duplicate live handle");
  ++created;
}
bool fail(void* p, const char* kind) {
  std::lock_guard<std::mutex> lock(mutex);
  require(handles.count(p), "cleanup of dead or unknown handle");
  if (!std::strcmp(kind,"jetty")) teardown=true;
  const bool single=!std::getenv("CLEANUP_CONNECTIONS") ||
      !std::strcmp(std::getenv("CLEANUP_CONNECTIONS"),"1");
  if (teardown && single) {
    const char* order[]={"jetty","peer","import","source","destination","jfr","jfc","jfce","io"};
    for (const char* earlier:order) {
      if (!std::strcmp(earlier,kind)) break;
      for (const auto& entry:handles)
        require(std::strcmp(entry.second,earlier)!=0,"cleanup advanced past a live prerequisite");
    }
  }
  if (std::strcmp(mode(), kind) || resume) return false;
  unsigned& count = failures[p];
  if (std::getenv("CLEANUP_PERSIST") || count < 3) {
    ++count; ++injected; return true;
  }
  return false;
}
void untrack(void* p) {
  std::lock_guard<std::mutex> lock(mutex);
  require(handles.erase(p) == 1, "double release"); ++destroyed;
}
}
#define CREATE(NAME, TYPE, CFG, KIND) \
extern "C" TYPE* urma_create_##NAME(urma_context_t* c, CFG* cfg) { \
  auto* p = next<decltype(&urma_create_##NAME)>("urma_create_" #NAME)(c, cfg); \
  track(p, KIND); return p; }
#define DELETE(NAME, TYPE, KIND) \
extern "C" urma_status_t urma_delete_##NAME(TYPE* p) { \
  if (fail(p, KIND)) return URMA_EAGAIN; \
  auto r = next<decltype(&urma_delete_##NAME)>("urma_delete_" #NAME)(p); \
  if (r == URMA_SUCCESS) untrack(p); return r; }
CREATE(jetty, urma_jetty_t, urma_jetty_cfg_t, "jetty")
CREATE(jfr, urma_jfr_t, urma_jfr_cfg_t, "jfr")
CREATE(jfc, urma_jfc_t, urma_jfc_cfg_t, "jfc")
DELETE(jetty, urma_jetty_t, "jetty")
DELETE(jfr, urma_jfr_t, "jfr")
DELETE(jfc, urma_jfc_t, "jfc")
DELETE(jfce, urma_jfce_t, "jfce")
extern "C" urma_jfce_t* urma_create_jfce(urma_context_t* c) {
  auto* p = next<decltype(&urma_create_jfce)>("urma_create_jfce")(c);
  track(p, "jfce"); return p;
}
extern "C" urma_target_jetty_t* urma_import_jetty(urma_context_t* c, urma_rjetty_t* j, urma_token_t* t) {
  auto* p = next<decltype(&urma_import_jetty)>("urma_import_jetty")(c,j,t);
  track(p, "peer"); return p;
}
extern "C" urma_status_t urma_unimport_jetty(urma_target_jetty_t* p) {
  if (fail(p, "peer")) return URMA_EAGAIN;
  auto r = next<decltype(&urma_unimport_jetty)>("urma_unimport_jetty")(p);
  if (r == URMA_SUCCESS) untrack(p); return r;
}
extern "C" urma_target_seg_t* urma_register_seg(urma_context_t* c, urma_seg_cfg_t* cfg) {
  auto* p = next<decltype(&urma_register_seg)>("urma_register_seg")(c,cfg);
  const auto depth = std::getenv("TEST_DEPTH") ? std::strtoul(std::getenv("TEST_DEPTH"),nullptr,10) : 32;
  track(p, cfg->flag.bs.access == URMA_ACCESS_READ ? "source" :
      cfg->len == (depth + 1) * 4096 ? "io" : "destination");
  if (p) {
    bool saved=false;
    for (auto& slot:registeredMemory) {
      void* empty=nullptr;
      if (slot.compare_exchange_strong(empty,reinterpret_cast<void*>(cfg->va))) { saved=true; break; }
    }
    require(saved,"registered memory tracker full");
  }
  return p;
}
extern "C" urma_status_t urma_unregister_seg(urma_target_seg_t* p) {
  const char* kind;
  { std::lock_guard<std::mutex> lock(mutex); require(handles.count(p), "unknown segment"); kind = handles.at(p); }
  if (fail(p,kind)) return URMA_EAGAIN;
  void* memory=reinterpret_cast<void*>(p->seg.ubva.va);
  auto r = next<decltype(&urma_unregister_seg)>("urma_unregister_seg")(p);
  if (r == URMA_SUCCESS) {
    bool found=false;
    for (auto& slot:registeredMemory) {
      void* expected=memory;
      if (slot.compare_exchange_strong(expected,nullptr)) { found=true; break; }
    }
    require(found,"unregistered memory unknown"); untrack(p);
  }
  return r;
}
extern "C" urma_target_seg_t* urma_import_seg(urma_context_t* c, urma_seg_t* s, urma_token_t* t, uint64_t a, urma_import_seg_flag_t f) {
  auto* p = next<decltype(&urma_import_seg)>("urma_import_seg")(c,s,t,a,f);
  track(p, "import"); return p;
}
extern "C" urma_status_t urma_unimport_seg(urma_target_seg_t* p) {
  if (fail(p,"import")) return URMA_EAGAIN;
  auto r = next<decltype(&urma_unimport_seg)>("urma_unimport_seg")(p);
  if (r == URMA_SUCCESS) untrack(p); return r;
}
extern "C" urma_status_t urma_delete_context(urma_context_t* p) {
  // fini must never reach the SDK while a failed resource still owns context.
  { std::lock_guard<std::mutex> lock(mutex); require(handles.empty(), "early context deletion"); }
  return next<decltype(&urma_delete_context)>("urma_delete_context")(p);
}
extern "C" void urma_cleanup_test_finish() {
  using folly::UrmaInit;
  if (std::getenv("CLEANUP_PERSIST")) {
    require(injected && UrmaInit::pendingCleanupCount(), "persistent failure not retained");
    require(UrmaInit::isInitialized(), "context lost during pending cleanup");
    UrmaInit::fini();
    require(UrmaInit::isInitialized(), "fini released pending context");
    { std::lock_guard<std::mutex> lock(mutex);
      bool found=false; for(auto& pair:handles) found |= !std::strcmp(pair.second,mode());
      require(found, "failed handle no longer owned"); }
    std::fprintf(stderr,"CLEANUP_RETAINED pending=%zu injected=%u\n",UrmaInit::pendingCleanupCount(),injected.load());
    if (std::getenv("CLEANUP_LEAVE_PENDING")) {
      std::fprintf(stderr,"CLEANUP_EXPECTED_RETAINED at process exit\n");
      return;
    }
  }
  resume = true;
  for (int i=0; i<1000 && UrmaInit::pendingCleanupCount(); ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  require(UrmaInit::pendingCleanupCount()==0,"cleanup did not recover without EventBase");
  UrmaInit::fini();
  require(!UrmaInit::isInitialized(),"context not released");
  require(created==destroyed,"unbalanced device handles");
  if (*mode()) require(injected>=3,"fault not exercised three times");
  std::fprintf(stderr,"CLEANUP_PASS created=%u destroyed=%u injected=%u pending=0 initialized=0\n",created.load(),destroyed.load(),injected.load());
}
// This Linux-only hardware probe is built for the glibc-based URMA test hosts.
extern "C" void __libc_free(void*);
extern "C" void free(void* p) noexcept {
  if (p) for (auto& slot:registeredMemory)
    require(slot.load()!=p,"free of still-registered DMA memory");
  __libc_free(p);
}
