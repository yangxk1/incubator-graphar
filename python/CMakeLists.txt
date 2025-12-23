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

# Options synced with cpp/CMakeLists.txt to control Arrow handling
option(USE_STATIC_ARROW "Link arrow static library" OFF)
option(BUILD_ARROW_FROM_SOURCE "Build Arrow from source" OFF)

# Build C++ core first; it will honor the above options
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../cpp ${CMAKE_BINARY_DIR}/graphar)

# Find the module development requirements (requires FindPython from 3.17 or
# scikit-build-core's built-in backport)
find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)
find_package(pybind11 CONFIG REQUIRED)

# Find Arrow only when not building from source (cpp subdir handles source build)
if(NOT BUILD_ARROW_FROM_SOURCE)
  find_package(Arrow REQUIRED)
  find_package(ArrowDataset REQUIRED)
  if (${Arrow_VERSION} VERSION_GREATER_EQUAL "12.0.0")
    find_package(ArrowAcero REQUIRED)
  endif()
  find_package(Parquet REQUIRED)
  # Check if ORC is enabled.
  if (NOT ${ARROW_ORC})
      message(WARNING "apache-arrow is built without ORC extension, ORC related functionalities will be disabled.")
  else()
      add_definitions(-DARROW_ORC) # Add macro, otherwise inconsistent in build phase on ubuntu.
  endif()
else()
  # In source-build mode, cpp has already enabled ORC; keep macro consistent here.
  add_definitions(-DARROW_ORC)
endif()
# Add a library using FindPython's tooling (pybind11 also provides a helper like
# this)
python_add_library(_core MODULE src/bindings/types_binding.cc
                                   src/bindings/cli_binding.cc
                                   src/bindings/graph_info_binding.cc
                                   src/bindings/high_level_binding.cc
                                   src/bindings/_core_module.cc WITH_SOABI)
# Link libraries depending on Arrow mode
if(BUILD_ARROW_FROM_SOURCE)
  # Arrow headers from the cpp external build (needed for GraphAr public headers)
  if(DEFINED GAR_ARROW_INCLUDE_DIR)
    target_include_directories(_core SYSTEM BEFORE PRIVATE ${GAR_ARROW_INCLUDE_DIR})
  endif()
  # graphar already bundles Arrow deps in this mode; link only graphar and pybind11
  target_link_libraries(_core PRIVATE pybind11::headers graphar)
else()
  if(APPLE)
    if(USE_STATIC_ARROW)
      target_link_libraries(_core PRIVATE pybind11::headers graphar -Wl,-force_load
                            Arrow::arrow_static
                            Parquet::parquet_static
                            ArrowDataset::arrow_dataset_static)
      if (TARGET ArrowAcero::arrow_acero_static)
        target_link_libraries(_core PRIVATE -Wl,-force_load ArrowAcero::arrow_acero_static)
      endif()
    else()
      target_link_libraries(_core PRIVATE pybind11::headers graphar
                            Arrow::arrow_shared
                            Parquet::parquet_shared
                            ArrowDataset::arrow_dataset_shared)
      if (TARGET ArrowAcero::arrow_acero_shared)
        target_link_libraries(_core PRIVATE ArrowAcero::arrow_acero_shared)
      endif()
    endif()
  else()
    if(USE_STATIC_ARROW)
      target_link_libraries(_core PRIVATE pybind11::headers graphar -Wl,--exclude-libs,ALL -Wl,--whole-archive
                            Arrow::arrow_static
                            Parquet::parquet_static
                            ArrowDataset::arrow_dataset_static -Wl,--no-whole-archive)
      if (TARGET ArrowAcero::arrow_acero_static)
        target_link_libraries(_core PRIVATE -Wl,--exclude-libs,ALL -Wl,--whole-archive ArrowAcero::arrow_acero_static -Wl,--no-whole-archive)
      endif()
    else()
      target_link_libraries(_core PRIVATE pybind11::headers graphar
                            Arrow::arrow_shared
                            Parquet::parquet_shared
                            ArrowDataset::arrow_dataset_shared)
      if (TARGET ArrowAcero::arrow_acero_shared)
        target_link_libraries(_core PRIVATE ArrowAcero::arrow_acero_shared)
      endif()
    endif()
  endif()
endif()
target_include_directories(_core PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_include_directories(_core PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../cpp/src)
target_include_directories(_core PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../cpp/thirdparty)

# This is passing in the version as a define just as an example
target_compile_definitions(_core PRIVATE VERSION_INFO=${PROJECT_VERSION})

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
install(TARGETS graphar DESTINATION graphar)
install(TARGETS _core DESTINATION graphar)