cmake_minimum_required(VERSION 3.12)
project(meta_service)

set(CMAKE_CXX_STANDARD 17)

# Set default build type to Release if not specified
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Add debug symbols for Release mode (optional)
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g")

# Debug mode flags
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
    message(FATAL_ERROR "etcd-cpp-api not found")
endif()
find_package(spdlog REQUIRED)
find_package(Threads REQUIRED)
find_package(cpprestsdk REQUIRED)
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
# MetaServiceClient SDK Library
# ------------------------------
add_library(MetaServiceClient SHARED
    src/MetaServiceClient.cpp
    src/metaserviceclient_capi.cpp  # C API wrapper
)
add_dependencies(MetaServiceClient redis_wrapper)
target_include_directories(MetaServiceClient PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include  # Where MetaServiceClient.h lives
)
target_link_libraries(MetaServiceClient
    PRIVATE
    ${GO_BUILD_DIR}/${GO_LIB_NAME}
    spdlog::spdlog
    nlohmann_json::nlohmann_json
    pthread
    dl
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
target_link_libraries(meta_service
    PRIVATE
    MetaServiceClient
    cpprest
    etcd-cpp-api
    spdlog::spdlog
    nlohmann_json::nlohmann_json
    pthread
    dl
    ${ETCD_CPP_LIBRARY}
    ${GO_BUILD_DIR}/${GO_LIB_NAME}
)

# ------------------------------
# Test Configuration
# ------------------------------
add_executable(metaservice_basic_test
    tests/metaservice_basic_test.cpp
)
add_dependencies(metaservice_basic_test redis_wrapper)
target_link_libraries(metaservice_basic_test
    PRIVATE
    MetaServiceClient
    GTest::GTest
    GTest::Main
    Threads::Threads
    spdlog::spdlog
    nlohmann_json::nlohmann_json
    ${GO_BUILD_DIR}/${GO_LIB_NAME}
    dl
)

# ------------------------------
# Installation
# ------------------------------
install(TARGETS meta_service MetaServiceClient DESTINATION lib)
install(FILES
    ${GO_BUILD_DIR}/${GO_LIB_NAME}
    DESTINATION lib
)
install(FILES
    include/MetaServiceClient.h  # SDK header
    DESTINATION include
)

# ------------------------------
# Testing Configuration
# ------------------------------
enable_testing()
add_test(
    NAME metaservice_basic_test
    COMMAND $<TARGET_FILE:metaservice_basic_test>
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)