cmake_minimum_required(VERSION 3.30)

project(redisclient)

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_STANDARD 20)
set(ROOT_PATH ${CMAKE_CURRENT_LIST_DIR})
set(LIBRARY_OUTPUT_PATH ${ROOT_PATH}/bin)
set(EXECUTABLE_OUTPUT_PATH ${ROOT_PATH}/bin)
set(ASIO_ROOT ${ROOT_PATH}/3rd/asio)

link_directories(${LIBRARY_OUTPUT_PATH})
link_libraries(asio)

include_directories(${ROOT_PATH})
include_directories(${ASIO_ROOT}/include)
include_directories(${ROOT_PATH}/include)

file(GLOB SRC_LIST ${CMAKE_CURRENT_LIST_DIR}/src/*.cpp)

add_library(${PROJECT_NAME} STATIC ${SRC_LIST})
