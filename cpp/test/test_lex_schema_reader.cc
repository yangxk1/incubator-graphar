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

#include <iostream>
#include <memory>

#include "graphar/lex_schema_reader.h"
#include "graphar/graph_info.h"
#include "graphar/status.h"

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

namespace graphar {

TEST_CASE("LexSchemaReaderTest") {
  SECTION("LoadFromFile") {
    // File path is relative to the build directory (cpp/build)
    std::string file_path = "/Users/yangxk/code/yangxk/incubator-graphar/testing/ldbc/ldbc_schema.yaml";
    
    std::cout << "Attempting to load file: " << file_path << std::endl;
    
    auto result = LexSchemaReader::LoadFromFile(file_path);
    if (!result.status().ok()) {
      std::cout << "Load failed with error: " << result.status().message() << std::endl;
      std::cerr << "Load failed with error: " << result.status().message() << std::endl;
      FAIL("Load failed: " + result.status().message());
    }
    
    auto graph_info = result.value();
    REQUIRE(graph_info != nullptr);
    
    // Check graph name
    REQUIRE(graph_info->GetName() == "SNB_demo_graph");
    
    // Check vertex infos
    REQUIRE(graph_info->VertexInfoNum() == 2);
    
    // Check organisation vertex
    auto org_vertex = graph_info->GetVertexInfo("organisation");
    REQUIRE(org_vertex != nullptr);
    REQUIRE(org_vertex->GetType() == "organisation");
    REQUIRE(org_vertex->GetChunkSize() == 128);
    REQUIRE(org_vertex->GetPrefix() == "vertex/organisation/");
    REQUIRE(org_vertex->PropertyGroupNum() == 2);
    
    // Check person vertex
    auto person_vertex = graph_info->GetVertexInfo("person");
    REQUIRE(person_vertex != nullptr);
    REQUIRE(person_vertex->GetType() == "person");
    REQUIRE(person_vertex->GetChunkSize() == 128);
    REQUIRE(person_vertex->GetPrefix() == "vertex/person/");
    REQUIRE(person_vertex->PropertyGroupNum() == 6);
    
    // Check edge infos
    REQUIRE(graph_info->EdgeInfoNum() == 2);
    
    // Check knows edge
    auto knows_edge = graph_info->GetEdgeInfo("person", "knows", "person");
    REQUIRE(knows_edge != nullptr);
    REQUIRE(knows_edge->GetEdgeType() == "knows");
    REQUIRE(knows_edge->GetSrcType() == "person");
    REQUIRE(knows_edge->GetDstType() == "person");
    REQUIRE(knows_edge->GetChunkSize() == 256);
    REQUIRE(knows_edge->GetSrcChunkSize() == 128);
    REQUIRE(knows_edge->GetDstChunkSize() == 128);
    REQUIRE(knows_edge->HasAdjacentListType(AdjListType::ordered_by_dest));
    REQUIRE(knows_edge->HasAdjacentListType(AdjListType::ordered_by_source));
    
    // Check workAt edge
    auto workat_edge = graph_info->GetEdgeInfo("person", "workAt", "organisation");
    REQUIRE(workat_edge != nullptr);
    REQUIRE(workat_edge->GetEdgeType() == "workAt");
    REQUIRE(workat_edge->GetSrcType() == "person");
    REQUIRE(workat_edge->GetDstType() == "organisation");
    REQUIRE(workat_edge->GetChunkSize() == 256);
    REQUIRE(workat_edge->GetSrcChunkSize() == 128);
    REQUIRE(workat_edge->GetDstChunkSize() == 128);
    REQUIRE(workat_edge->HasAdjacentListType(AdjListType::ordered_by_dest));
    REQUIRE(workat_edge->HasAdjacentListType(AdjListType::ordered_by_source));
  }
}

}  // namespace graphar