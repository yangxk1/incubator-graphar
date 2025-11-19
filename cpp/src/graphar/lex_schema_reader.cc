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

#include "graphar/lex_schema_reader.h"
#include "graphar/yaml.h"
#include "graphar/filesystem.h"
#include "graphar/types.h"

#include "mini-yaml/yaml/Yaml.hpp"

#include <iostream>
#include <vector>
#include <unordered_map>

namespace graphar {

namespace {

std::string PathToDirectory(const std::string& path) {
  if (path.rfind("s3://", 0) == 0) {
    int t = path.find_last_of('?');
    std::string prefix = path.substr(0, t);
    std::string suffix = path.substr(t);
    const size_t last_slash_idx = prefix.rfind('/');
    if (std::string::npos != last_slash_idx) {
      return prefix.substr(0, last_slash_idx + 1) + suffix;
    }
  } else {
    const size_t last_slash_idx = path.rfind('/');
    if (std::string::npos != last_slash_idx) {
      return path.substr(0, last_slash_idx + 1);  // +1 to include the slash
    }
  }
  return path;
}

Result<Property> ParseProperty(::Yaml::Node property_node) {
  std::string name = property_node["name"].As<std::string>();
  std::string storage_type = property_node["storageType"].As<std::string>();
  bool is_primary = property_node["isPrimary"].As<bool>();
  
  // Default values
  bool is_nullable = true;
  Cardinality cardinality = Cardinality::SINGLE;
  
  // Parse data type
  std::shared_ptr<DataType> type = DataType::TypeNameToDataType(storage_type);
  if (!type) {
    return Status::Invalid("Unsupported data type: ", storage_type);
  }

  // Handle list types
  if (storage_type.find("list<") == 0) {
    cardinality = Cardinality::LIST;
  }

  return Property(name, type, is_primary, is_nullable, cardinality);
}

Result<std::shared_ptr<PropertyGroup>> ParsePropertyGroup(
    ::Yaml::Node pg_node,
    const std::unordered_map<std::string, Property>& property_map) {
  // Get file format
  std::string file_format = pg_node["fileFormat"].As<std::string>();
  FileType file_type = StringToFileType(file_format);
  
  // Get properties
  std::vector<Property> properties;
  ::Yaml::Node properties_node = pg_node["properties"];
  if (!properties_node.IsSequence()) {
    return Status::YamlError("Property groups 'properties' should be a sequence.");
  }
  
  for (::Yaml::Iterator it = properties_node.Begin(); it != properties_node.End(); it++) {
    std::string property_name = (*it).second.As<std::string>();
    auto prop_it = property_map.find(property_name);
    if (prop_it == property_map.end()) {
      return Status::YamlError("Property '", property_name, "' not found in properties definition.");
    }
    properties.push_back(prop_it->second);
  }
  
  // Get prefix if exists
  std::string prefix = "";
  if (!pg_node["pathPrefix"].IsNone()) {
    prefix = pg_node["pathPrefix"].As<std::string>();
  }
  
  return std::make_shared<PropertyGroup>(properties, file_type, prefix);
}

Result<std::shared_ptr<VertexInfo>> ParseVertexInfo(::Yaml::Node vertex_node) {
  // Get type label
  std::string type_label = vertex_node["typeLabel"].As<std::string>();
  
  // Get chunk size
  IdType chunk_size = static_cast<IdType>(vertex_node["chunkSize"].As<int64_t>());
  
  // Get prefix
  std::string prefix = vertex_node["pathPrefix"].As<std::string>();
  
  // Parse properties first to build a map
  std::unordered_map<std::string, Property> property_map;
  std::vector<Property> all_properties;
  ::Yaml::Node properties_node = vertex_node["properties"];
  if (!properties_node.IsSequence()) {
    return Status::YamlError("Vertex 'properties' should be a sequence.");
  }
  
  for (::Yaml::Iterator it = properties_node.Begin(); it != properties_node.End(); it++) {
    GAR_ASSIGN_OR_RAISE(Property property, ParseProperty((*it).second));
    property_map[property.name] = property;
    all_properties.push_back(property);
  }
  
  // Parse property groups
  PropertyGroupVector property_groups;
  ::Yaml::Node pg_node = vertex_node["propertyGroups"];
  if (!pg_node.IsSequence()) {
    return Status::YamlError("Vertex 'propertyGroups' should be a sequence.");
  }
  
  for (::Yaml::Iterator it = pg_node.Begin(); it != pg_node.End(); it++) {
    GAR_ASSIGN_OR_RAISE(auto property_group, 
                        ParsePropertyGroup((*it).second, property_map));
    property_groups.push_back(property_group);
  }
  
  // Create VertexInfo
  return CreateVertexInfo(type_label, chunk_size, property_groups, {}, prefix);
}

Result<std::shared_ptr<AdjacentList>> ParseAdjacentList(::Yaml::Node adj_list_node) {
  // Parse ordered flag
  bool ordered = adj_list_node["ordered"].As<bool>();
  
  // Parse alignedBy
  std::string aligned_by = adj_list_node["alignedBy"].As<std::string>();
  
  // Determine AdjListType
  AdjListType adj_list_type = AdjListType::unordered_by_source;
  if (ordered && aligned_by == "source") {
    adj_list_type = AdjListType::ordered_by_source;
  } else if (!ordered && aligned_by == "source") {
    adj_list_type = AdjListType::unordered_by_source;
  } else if (ordered && aligned_by == "destination") {
    adj_list_type = AdjListType::ordered_by_dest;
  } else if (!ordered && aligned_by == "destination") {
    adj_list_type = AdjListType::unordered_by_dest;
  }
  
  // Parse file format
  std::string file_format = adj_list_node["fileFormat"].As<std::string>();
  FileType file_type = StringToFileType(file_format);
  
  // Get prefix if exists
  std::string prefix = "";
  if (!adj_list_node["pathPrefix"].IsNone()) {
    prefix = adj_list_node["pathPrefix"].As<std::string>();
  }
  
  return CreateAdjacentList(adj_list_type, file_type, prefix);
}

Result<std::shared_ptr<EdgeInfo>> ParseEdgeInfo(::Yaml::Node edge_node) {
  // Get basic info
  std::string edge_type = edge_node["typeLabel"].As<std::string>();
  std::string src_type = edge_node["srcType"].As<std::string>();
  std::string dst_type = edge_node["dstType"].As<std::string>();
  
  // Get chunk sizes
  IdType chunk_size = static_cast<IdType>(edge_node["chunkSize"].As<int64_t>());
  IdType src_chunk_size = static_cast<IdType>(edge_node["srcChunkSize"].As<int64_t>());
  IdType dst_chunk_size = static_cast<IdType>(edge_node["dstChunkSize"].As<int64_t>());
  
  // Default directed as true
  bool directed = true;
  
  // Get prefix
  std::string prefix = edge_node["pathPrefix"].As<std::string>();
  
  // Parse properties first to build a map
  std::unordered_map<std::string, Property> property_map;
  std::vector<Property> all_properties;
  ::Yaml::Node properties_node = edge_node["properties"];
  if (!properties_node.IsSequence()) {
    return Status::YamlError("Edge 'properties' should be a sequence.");
  }
  
  for (::Yaml::Iterator it = properties_node.Begin(); it != properties_node.End(); it++) {
    GAR_ASSIGN_OR_RAISE(Property property, ParseProperty((*it).second));
    property_map[property.name] = property;
    all_properties.push_back(property);
  }
  
  // Parse property groups
  PropertyGroupVector property_groups;
  ::Yaml::Node pg_node = edge_node["propertyGroups"];
  if (!pg_node.IsNone() && pg_node.IsSequence()) {
    for (::Yaml::Iterator it = pg_node.Begin(); it != pg_node.End(); it++) {
      GAR_ASSIGN_OR_RAISE(auto property_group,
                          ParsePropertyGroup((*it).second, property_map));
      property_groups.push_back(property_group);
    }
  }
  
  // Parse adjacency lists
  AdjacentListVector adjacent_lists;
  ::Yaml::Node adj_lists_node = edge_node["adjLists"];
  if (!adj_lists_node.IsSequence()) {
    return Status::YamlError("Edge 'adjLists' should be a sequence.");
  }
  
  for (::Yaml::Iterator it = adj_lists_node.Begin(); it != adj_lists_node.End(); it++) {
    GAR_ASSIGN_OR_RAISE(auto adjacent_list, ParseAdjacentList((*it).second));
    adjacent_lists.push_back(adjacent_list);
  }
  
  // Create EdgeInfo
  return CreateEdgeInfo(src_type, edge_type, dst_type, chunk_size, 
                        src_chunk_size, dst_chunk_size, directed,
                        adjacent_lists, property_groups, prefix);
}

} // namespace

Result<std::shared_ptr<GraphInfo>> LexSchemaReader::Load(
    const std::string& input, const std::string& relative_path) {
  // Parse YAML
  GAR_ASSIGN_OR_RAISE(auto yaml, Yaml::Load(input));
  
  // Get graph name
  ::Yaml::Node graph_node = (*yaml)["graph"];
  std::string graph_name = graph_node["name"].As<std::string>();
  
  // Get prefix
  std::string prefix = graph_node["pathPrefix"].As<std::string>();
  
  // Parse vertex infos
  VertexInfoVector vertex_infos;
  ::Yaml::Node node_types = (*yaml)["nodeTypes"];
  if (!node_types.IsSequence()) {
    return Status::YamlError("'nodeTypes' should be a sequence.");
  }
  
  for (::Yaml::Iterator it = node_types.Begin(); it != node_types.End(); it++) {
    GAR_ASSIGN_OR_RAISE(auto vertex_info, ParseVertexInfo((*it).second));
    vertex_infos.push_back(vertex_info);
  }
  
  // Parse edge infos
  EdgeInfoVector edge_infos;
  ::Yaml::Node edge_types = (*yaml)["edgeTypes"];
  if (!edge_types.IsSequence()) {
    return Status::YamlError("'edgeTypes' should be a sequence.");
  }
  
  for (::Yaml::Iterator it = edge_types.Begin(); it != edge_types.End(); it++) {
    GAR_ASSIGN_OR_RAISE(auto edge_info, ParseEdgeInfo((*it).second));
    edge_infos.push_back(edge_info);
  }
  
  // Create GraphInfo
  return CreateGraphInfo(graph_name, vertex_infos, edge_infos, {}, prefix);
}

Result<std::shared_ptr<GraphInfo>> LexSchemaReader::LoadFromFile(
    const std::string& path) {
  std::string no_url_path;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(path, &no_url_path));
  GAR_ASSIGN_OR_RAISE(auto yaml_content,
                      fs->ReadFileToValue<std::string>(no_url_path));
                      
  std::string relative_path = PathToDirectory(path);
  return Load(yaml_content, relative_path);
}

} // namespace graphar