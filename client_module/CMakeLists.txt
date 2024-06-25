cmake_minimum_required(VERSION 3.16)
project(libcatcher)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
add_library(catcher SHARED catcher.cpp)
target_link_libraries(catcher dl)
set_target_properties(catcher PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_library(catcher_original SHARED catcher_original.cpp)
target_link_libraries(catcher_original dl)
set_target_properties(catcher_original PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_library(catcher_filter SHARED catcher_filter.cpp)
target_link_libraries(catcher_filter dl)
set_target_properties(catcher_filter PROPERTIES POSITION_INDEPENDENT_CODE ON)
add_executable(main main.cpp)

