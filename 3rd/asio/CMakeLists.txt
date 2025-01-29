cmake_minimum_required(VERSION 3.30)

project(asio)

add_definitions(-DSEPARATE_COMPILATION=1)
add_definitions(-DSTANDALONE=1)

set(CMAKE_BUILD_TYPE Debug)
# set(CMAKE_BUILD_TYPE Release)
set(CMAKE_CXX_STANDARD 20)
set(LIBRARY_OUTPUT_PATH ${CMAKE_CURRENT_LIST_DIR}/../../bin)

# STANDALONE build asio
add_definitions(-DASIO_SEPARATE_COMPILATION)
add_definitions(-DASIO_STANDALONE)

include_directories(${CMAKE_CURRENT_LIST_DIR}/include)

add_library(asio STATIC ${CMAKE_CURRENT_LIST_DIR}/src/asio.cpp)
