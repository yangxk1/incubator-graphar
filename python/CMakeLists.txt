# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Require CMake 3.15+ (matching scikit-build-core) Use new versions of all
# policies up to CMake 3.27
cmake_minimum_required(VERSION 3.15)

# Scikit-build-core sets these values for you, or you can just hard-code the
# name and version.
project(
  ${SKBUILD_PROJECT_NAME}
  VERSION ${SKBUILD_PROJECT_VERSION}
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

# -----------------------------------------------------------------------------
# 1. Build Arrow from Source (Static)
# -----------------------------------------------------------------------------
# Add the cmake directory to the module path to find apache-arrow.cmake
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../cmake")

# Include the helper script provided
include(apache-arrow)

# Trigger the download and build of Arrow (Static Libraries)
# Ensure your apache-arrow.cmake sets -DARROW_POSITION_INDEPENDENT_CODE=ON
build_arrow()

# -----------------------------------------------------------------------------
# 2. Locate GraphAr C++ Sources
# -----------------------------------------------------------------------------
# When building from a source checkout, the C++ sources live in ../cpp.
# When building from an sdist, CI vendors the C++ sources into ./cpp.
set(GRAPHAR_CPP_SOURCE_DIR "")
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/../cpp/CMakeLists.txt")
  set(GRAPHAR_CPP_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../cpp")
elseif(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/cpp/CMakeLists.txt")
  set(GRAPHAR_CPP_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/cpp")
else()
  message(
    FATAL_ERROR
      "GraphAr C++ sources not found. Expected ../cpp (source checkout) or ./cpp (vendored in sdist).")
endif()

# -----------------------------------------------------------------------------
# 3. Add GraphAr Subdirectory
# -----------------------------------------------------------------------------
# Note: You might need to ensure GraphAr's CMakeLists.txt knows how to use
# the static Arrow targets we just built, or at least doesn't fail on find_package(Arrow).
add_subdirectory(${GRAPHAR_CPP_SOURCE_DIR} ${CMAKE_BINARY_DIR}/graphar)

# -----------------------------------------------------------------------------
# 4. Find Dependencies
# -----------------------------------------------------------------------------
find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)
find_package(pybind11 CONFIG REQUIRED)

# We need OpenSSL because Arrow is statically linked and likely depends on it.
# Arrow's CMake config usually handles this, but since we are manually linking
# static libs, we might need to expose OpenSSL symbols to the final shared object.
find_package(OpenSSL REQUIRED)

# NOTE: We DO NOT find_package(Arrow/Parquet) here anymore, 
# because we are using the targets defined by build_arrow().

# -----------------------------------------------------------------------------
# 5. Build Python Extension Module
# -----------------------------------------------------------------------------
python_add_library(_core MODULE src/bindings/types_binding.cc
                                   src/bindings/cli_binding.cc
                                   src/bindings/graph_info_binding.cc
                                   src/bindings/high_level_binding.cc
                                   src/bindings/_core_module.cc WITH_SOABI)
                                   
# Link against the STATIC targets defined in apache-arrow.cmake
target_link_libraries(_core PRIVATE 
    pybind11::headers 
    graphar 
    gar_arrow_static
    gar_parquet_static
    gar_dataset_static
    gar_acero_static
    gar_arrow_bundled_dependencies_static # Contains jemalloc, mimalloc, etc.
    OpenSSL::SSL 
    OpenSSL::Crypto
)

target_include_directories(_core PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_include_directories(_core PRIVATE ${GRAPHAR_CPP_SOURCE_DIR}/src)
target_include_directories(_core PRIVATE ${GRAPHAR_CPP_SOURCE_DIR}/thirdparty)

# This is passing in the version as a define just as an example
target_compile_definitions(_core PRIVATE VERSION_INFO=${PROJECT_VERSION})

# -----------------------------------------------------------------------------
# 6. Installation & RPATH
# -----------------------------------------------------------------------------
# The install directory is the output (wheel) directory
# Use platform-appropriate rpath so the Python extension can find the
# packaged libgraphar at runtime inside the wheel.
if(APPLE)
  # macOS uses @loader_path to find libraries relative to the module
  set_target_properties(_core PROPERTIES INSTALL_RPATH "@loader_path")
else()
  # On Linux and other Unix, use $ORIGIN (escaped so CMake preserves the $)
  set_target_properties(_core PROPERTIES INSTALL_RPATH "\$ORIGIN")
endif()

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src/cli/ DESTINATION cli)
if(TARGET graphar)
  install(TARGETS graphar DESTINATION graphar)
endif()
install(TARGETS _core DESTINATION graphar)