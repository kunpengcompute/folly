# URMA transport uses the installed SDK, never an in-tree simulation backend.
option(FOLLY_USE_URMA "Build the URMA hardware transport" OFF)
option(URMA_USE_REAL_LIB "Legacy alias for FOLLY_USE_URMA" OFF)
set(URMA_ROOT "" CACHE PATH "URMA SDK install prefix or header directory")

if(FOLLY_USE_URMA OR URMA_USE_REAL_LIB)
  find_path(URMA_INCLUDE_DIR NAMES urma_api.h
    HINTS "${URMA_ROOT}"
    PATH_SUFFIXES include include/ub/umdk/urma ub/umdk/urma)
  find_library(URMA_LIBRARY NAMES urma
    HINTS "${URMA_ROOT}"
    PATH_SUFFIXES lib lib64 build/lib)
  if(NOT URMA_INCLUDE_DIR OR NOT URMA_LIBRARY)
    message(FATAL_ERROR
      "URMA was requested but the SDK headers or liburma were not found. "
      "Set URMA_ROOT or URMA_INCLUDE_DIR and URMA_LIBRARY explicitly. "
      "URMA will not be silently disabled or replaced with a stub.")
  endif()
  target_compile_definitions(folly_deps INTERFACE FOLLY_HAVE_URMA)
  target_include_directories(folly_deps SYSTEM INTERFACE "${URMA_INCLUDE_DIR}")
  target_link_libraries(folly_deps INTERFACE "${URMA_LIBRARY}")
  message(STATUS "URMA hardware transport: ${URMA_LIBRARY}")
else()
  message(STATUS "URMA transport disabled (enable with -DFOLLY_USE_URMA=ON)")
endif()
