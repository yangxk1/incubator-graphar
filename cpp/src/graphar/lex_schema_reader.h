/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <string>
#include <memory>

#include "graphar/graph_info.h"
#include "graphar/result.h"

namespace graphar {

/**
 * @brief LexSchemaReader is a class to parse LDBC schema files and convert them
 * to GraphInfo, VertexInfo, and EdgeInfo objects.
 */
class LexSchemaReader {
 public:
  /**
   * @brief Loads graph schema from LDBC schema YAML file.
   * 
   * @param input The YAML content string of LDBC schema.
   * @param relative_path The relative path to access vertex/edge data.
   * @return A Result object containing the GraphInfo instance, or a Status
   * object indicating an error.
   */
  static Result<std::shared_ptr<GraphInfo>> Load(
      const std::string& input, const std::string& relative_path = "./");

  /**
   * @brief Loads graph schema from LDBC schema YAML file.
   * 
   * @param path The path of the LDBC schema YAML file.
   * @return A Result object containing the GraphInfo instance, or a Status
   * object indicating an error.
   */
  static Result<std::shared_ptr<GraphInfo>> LoadFromFile(
      const std::string& path);
};

} // namespace graphar