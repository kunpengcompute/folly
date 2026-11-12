/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0. */
// Keep the transfer checks identical to ReadTest. Run teardown assertions only
// after its Run/EventBase and every socket/callback have been destroyed.
#define main readTestMain
#include "ReadTest.cpp"
#undef main
#include <dlfcn.h>
int main(int argc, char** argv) {
  const int result = readTestMain(argc, argv);
  auto finish = reinterpret_cast<void (*)()>(dlsym(RTLD_DEFAULT, "urma_cleanup_test_finish"));
  if (!finish) return 3;
  finish();
  return result;
}
