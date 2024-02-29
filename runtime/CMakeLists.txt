cmake_minimum_required(VERSION 3.5.0)
project(libVNF)

set(PROJECT_DESCRIPTION "libvnf is a library to easily build custom scalable high performance Virtual Network Functions (VNFs)")
# Release type
set(CMAKE_BUILD_TYPE Debug)

# Project version
set(PROJECT_VERSION_MAJOR 0)
set(PROJECT_VERSION_MINOR 9)
set(PROJECT_VERSION_PATCH 1)
set(PROJECT_VERSION "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH}")

# Installation Paths.
set(LIBRARY_INSTALL_DIR /usr/lib)
set(LIBRARY_INSTALL_DIR2 /usr/local/lib)
set(HEADERS_INSTALL_DIR /usr/include/libvnf)
set(HEADERS_INSTALL_DIR2 /usr/local/include/libvnf)

# Flags
set(CMAKE_CXX_STANDARD 11)
if (${CMAKE_BUILD_TYPE} STREQUAL Debug)
	message(STATUS "Building in Debug mode.")
	add_definitions(-DDEBUG=true)
	set(CMAKE_CXX_FLAGS "-Wno-deprecated-declarations -Wno-pointer-arith -lnuma -lrt -DLIBVNF_STACK=1 -g -ljsoncpp -O0")
else ()
	message(STATUS "Building in Release mode.")
	set(CMAKE_CXX_FLAGS "-Wno-deprecated-declarations -Wno-pointer-arith -lnuma -lrt -DLIBVNF_STACK=1 -ljsoncpp -O3")
endif()

# Include
# include_directories(include)
# include_directories(/home/kailian)
include_directories(/home/kailian/DB4NFV/include)
include_directories(/home/kailian/DB4NFV/runtime/include/)
include_directories(/home/kailian/DB4NFV/target/cxxbridge/)

# Sources and Headers
file(GLOB_RECURSE SOURCES "src/core.cpp")

# Library
add_library(vnf-kernel-dynamic SHARED ${SOURCES})
# add_library(vnf-kernel-static STATIC ${SOURCES} ${HEADERS})

# Install
# install(TARGETS vnf-kernel-dynamic vnf-kernel-static DESTINATION ${LIBRARY_INSTALL_DIR})
# install(TARGETS vnf-kernel-dynamic vnf-kernel-static DESTINATION ${LIBRARY_INSTALL_DIR2})
install(TARGETS vnf-kernel-dynamic DESTINATION ${LIBRARY_INSTALL_DIR})
install(TARGETS vnf-kernel-dynamic DESTINATION ${LIBRARY_INSTALL_DIR2})
install(DIRECTORY include/ DESTINATION ${HEADERS_INSTALL_DIR})
install(DIRECTORY include/ DESTINATION ${HEADERS_INSTALL_DIR2})

# Installation target 
set(STACK_TARGET vnf-kernel-dynamic)