cmake_minimum_required(VERSION 3.12)
project(meta_service)

set(CMAKE_CXX_STANDARD 17)

# Set default build type to Release if not specified
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Add debug symbols for Release mode (optional, remove if not needed)
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g")

# Add debug symbols and disable optimizations for Debug mode
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g -O0")

# ------------------------------
# Go Build Configuration
# ------------------------------
set(GO_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/go)
set(GO_LIB_NAME libredis_wrapper.so)
set(GO_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR})

if(CMAKE_BUILD_TYPE MATCHES Debug)
    set(GO_BUILD_FLAGS "-gcflags=all=-N -l")
else()
    set(GO_BUILD_FLAGS "")
endif()

add_custom_command(
    OUTPUT ${GO_BUILD_DIR}/${GO_LIB_NAME}
    COMMAND go build -buildmode=c-shared ${GO_BUILD_FLAGS} -o ${GO_BUILD_DIR}/${GO_LIB_NAME} ${GO_SOURCE_DIR}/redis_wrapper.go
    WORKING_DIRECTORY ${GO_SOURCE_DIR}
    COMMENT "Building Go Redis wrapper library..."
    DEPENDS ${GO_SOURCE_DIR}/redis_wrapper.go
)

add_custom_target(redis_wrapper ALL
    DEPENDS ${GO_BUILD_DIR}/${GO_LIB_NAME}
)

# ------------------------------
# Dependencies Configuration
# ------------------------------
find_package(GTest REQUIRED)
find_path(ETCD_CPP_INCLUDE_DIR etcd/Client.hpp
    PATHS /usr/local/include /usr/include)
find_library(ETCD_CPP_LIBRARY etcd-cpp-api
    PATHS /usr/local/lib /usr/lib)
if (NOT ETCD_CPP_INCLUDE_DIR OR NOT ETCD_CPP_LIBRARY)
    message(FATAL_ERROR "etcd-cpp-api not found, please check installation path")
endif()
find_package(spdlog REQUIRED)
find_package(Threads REQUIRED)
find_package(cpprestsdk REQUIRED NAMES cpprest cpprestsdk)
find_package(nlohmann_json 3.2.0 REQUIRED)

# ------------------------------
# Include Directories
# ------------------------------
include_directories(
    /usr/local/include
    /usr/include
    include
    ${ETCD_CPP_INCLUDE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}
    ${GTEST_INCLUDE_DIRS}
)

# ------------------------------
# Main Executable
# ------------------------------
add_executable(meta_service
    src/MetaService.cpp
    src/common.cpp
    src/main.cpp
)
add_dependencies(meta_service redis_wrapper)

# ------------------------------
# Test Configuration
# ------------------------------
add_executable(redis_wrapper_tests
    tests/redis_wrapper_test.cpp
)
add_dependencies(redis_wrapper_tests redis_wrapper)
target_link_libraries(redis_wrapper_tests
    PRIVATE
    GTest::GTest
    GTest::Main
    ${CMAKE_THREAD_LIBS_INIT}
    spdlog::spdlog
    nlohmann_json::nlohmann_json
    ${GO_BUILD_DIR}/${GO_LIB_NAME}
    dl
)

# ------------------------------
# Link Libraries for Main Executable
# ------------------------------
target_link_libraries(meta_service
    PRIVATE
    cpprest
    etcd-cpp-api
    spdlog::spdlog
    nlohmann_json::nlohmann_json
    pthread
    dl
    ${CMAKE_THREAD_LIBS_INIT}
    ${ETCD_CPP_LIBRARY}
    ${GO_BUILD_DIR}/${GO_LIB_NAME}
)

# ------------------------------
# Installation
# ------------------------------
install(TARGETS meta_service DESTINATION bin)
install(FILES ${GO_BUILD_DIR}/${GO_LIB_NAME} DESTINATION bin)

# ------------------------------
# Testing Configuration
# ------------------------------
enable_testing()
add_test(
    NAME redis_wrapper_tests
    COMMAND $<TARGET_FILE:redis_wrapper_tests>
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/bin
)