cmake_minimum_required(VERSION 3.16)
project(cbdp)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")
include("${CMAKE_SOURCE_DIR}/cmake/azure-storage-lite.cmake")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wconversion -Werror -fsanitize=address")

find_package(CURL REQUIRED)

add_executable(coordinator
        coordinator.cpp
        CurlEasyPtr.cpp
        AzureBlobClient.cpp AzureBlobClient.h)
target_link_libraries(coordinator PUBLIC CURL::libcurl azure-storage-lite)

add_executable(worker
        worker.cpp
        CurlEasyPtr.cpp
        AzureBlobClient.cpp AzureBlobClient.h)
target_link_libraries(worker PUBLIC CURL::libcurl azure-storage-lite)

