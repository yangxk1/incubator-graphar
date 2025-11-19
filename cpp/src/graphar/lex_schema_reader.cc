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
  std::cout << "Parsing property..." << std::endl;
  std::string name = property_node["name"].As<std::string>();
  std::string storage_type = property_node["storageType"].As<std::string>();
  bool is_primary = property_node["isPrimary"].As<bool>();
  
  std::cout << "Property name: " << name << ", type: " << storage_type << ", primary: " << is_primary << std::endl;
  
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
  std::cout << "Parsing property group..." << std::endl;
  // Get file format
  std::string file_format = pg_node["fileFormat"].As<std::string>();
  FileType file_type = StringToFileType(file_format);
  
  std::cout << "File format: " << file_format << std::endl;
  
  // Get properties
  std::vector<Property> properties;
  ::Yaml::Node properties_node = pg_node["properties"];
  if (!properties_node.IsSequence()) {
    return Status::YamlError("Property groups 'properties' should be a sequence.");
  }
  
  std::cout << "Properties count: " << properties_node.Size() << std::endl;
  
  for (::Yaml::Iterator it = properties_node.Begin(); it != properties_node.End(); it++) {
    std::string property_name = (*it).second.As<std::string>();
    std::cout << "Processing property: " << property_name << std::endl;
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
  
  std::cout << "Property group prefix: " << prefix << std::endl;
  
  return std::make_shared<PropertyGroup>(properties, file_type, prefix);
}

Result<std::shared_ptr<VertexInfo>> ParseVertexInfo(::Yaml::Node vertex_node) {
  std::cout << "Parsing vertex info..." << std::endl;
  // Get type label
  std::string type_label = vertex_node["typeLabel"].As<std::string>();
  
  // Get chunk size
  IdType chunk_size = static_cast<IdType>(vertex_node["chunkSize"].As<int64_t>());
  
  // Get prefix
  std::string prefix = vertex_node["pathPrefix"].As<std::string>();
  
  std::cout << "Vertex type: " << type_label << ", chunk size: " << chunk_size << ", prefix: " << prefix << std::endl;
  
  // Parse properties first to build a map
  std::unordered_map<std::string, Property> property_map;
  std::vector<Property> all_properties;
  ::Yaml::Node properties_node = vertex_node["properties"];
  if (!properties_node.IsSequence()) {
    return Status::YamlError("Vertex 'properties' should be a sequence.");
  }
  
  std::cout << "Vertex properties count: " << properties_node.Size() << std::endl;
  
  for (::Yaml::Iterator it = properties_node.Begin(); it != properties_node.End(); it++) {
    std::cout << "Parsing vertex property..." << std::endl;
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
  
  std::cout << "Vertex property groups count: " << pg_node.Size() << std::endl;
  
  for (::Yaml::Iterator it = pg_node.Begin(); it != pg_node.End(); it++) {
    std::cout << "Parsing vertex property group..." << std::endl;
    GAR_ASSIGN_OR_RAISE(auto property_group, 
                        ParsePropertyGroup((*it).second, property_map));
    property_groups.push_back(property_group);
  }
  
  // Create VertexInfo
  std::cout << "Creating vertex info for: " << type_label << std::endl;
  return CreateVertexInfo(type_label, chunk_size, property_groups, {}, prefix);
}

Result<std::shared_ptr<AdjacentList>> ParseAdjacentList(::Yaml::Node adj_list_node) {
  std::cout << "Parsing adjacent list..." << std::endl;
  // Parse ordered flag
  bool ordered = adj_list_node["ordered"].As<bool>();
  
  // Parse alignedBy
  std::string aligned_by = adj_list_node["alignedBy"].As<std::string>();
  
  std::cout << "Ordered: " << ordered << ", aligned by: " << aligned_by << std::endl;
  
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
  
  std::cout << "File format: " << file_format << std::endl;
  
  // Get prefix if exists
  std::string prefix = "";
  if (!adj_list_node["pathPrefix"].IsNone()) {
    prefix = adj_list_node["pathPrefix"].As<std::string>();
  }
  
  std::cout << "Adjacent list prefix: " << prefix << std::endl;
  
  return CreateAdjacentList(adj_list_type, file_type, prefix);
}

Result<std::shared_ptr<EdgeInfo>> ParseEdgeInfo(::Yaml::Node edge_node) {
  std::cout << "Parsing edge info..." << std::endl;
  // Get basic info
  std::string edge_type = edge_node["typeLabel"].As<std::string>();
  std::string src_type = edge_node["srcType"].As<std::string>();
  std::string dst_type = edge_node["dstType"].As<std::string>();
  
  std::cout << "Edge type: " << edge_type << ", src: " << src_type << ", dst: " << dst_type << std::endl;
  
  // Get chunk sizes
  IdType chunk_size = static_cast<IdType>(edge_node["chunkSize"].As<int64_t>());
  IdType src_chunk_size = static_cast<IdType>(edge_node["srcChunkSize"].As<int64_t>());
  IdType dst_chunk_size = static_cast<IdType>(edge_node["dstChunkSize"].As<int64_t>());
  
  // Default directed as true
  bool directed = true;
  
  // Get prefix
  std::string prefix = edge_node["pathPrefix"].As<std::string>();
  
  std::cout << "Chunk size: " << chunk_size << ", src chunk size: " << src_chunk_size << ", dst chunk size: " << dst_chunk_size << std::endl;
  std::cout << "Prefix: " << prefix << std::endl;
  
  // Parse properties first to build a map
  std::unordered_map<std::string, Property> property_map;
  std::vector<Property> all_properties;
  ::Yaml::Node properties_node = edge_node["properties"];
  if (!properties_node.IsSequence()) {
    return Status::YamlError("Edge 'properties' should be a sequence.");
  }
  
  std::cout << "Edge properties count: " << properties_node.Size() << std::endl;
  
  for (::Yaml::Iterator it = properties_node.Begin(); it != properties_node.End(); it++) {
    std::cout << "Parsing edge property..." << std::endl;
    GAR_ASSIGN_OR_RAISE(Property property, ParseProperty((*it).second));
    property_map[property.name] = property;
    all_properties.push_back(property);
  }
  
  // Parse property groups
  PropertyGroupVector property_groups;
  ::Yaml::Node pg_node = edge_node["propertyGroups"];
  if (!pg_node.IsNone() && pg_node.IsSequence()) {
    std::cout << "Edge property groups count: " << pg_node.Size() << std::endl;
    for (::Yaml::Iterator it = pg_node.Begin(); it != pg_node.End(); it++) {
      std::cout << "Parsing edge property group..." << std::endl;
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
  
  std::cout << "Edge adj lists count: " << adj_lists_node.Size() << std::endl;
  
  for (::Yaml::Iterator it = adj_lists_node.Begin(); it != adj_lists_node.End(); it++) {
    std::cout << "Parsing edge adjacent list..." << std::endl;
    GAR_ASSIGN_OR_RAISE(auto adjacent_list, ParseAdjacentList((*it).second));
    adjacent_lists.push_back(adjacent_list);
  }
  
  // Create EdgeInfo
  std::cout << "Creating edge info for: " << edge_type << std::endl;
  return CreateEdgeInfo(src_type, edge_type, dst_type, chunk_size, 
                        src_chunk_size, dst_chunk_size, directed,
                        adjacent_lists, property_groups, prefix);
}

} // namespace

Result<std::shared_ptr<GraphInfo>> LexSchemaReader::Load(
    const std::string& input, const std::string& relative_path) {
  std::cout << "Loading from input string..." << std::endl;
  // Parse YAML
  auto yaml_reasult = Yaml::Load(input);
  if (yaml_reasult.has_error()) {
    std::cout << "Error: " << yaml_reasult.error().message() << std::endl;
  }
  auto root = yaml_reasult.value();
  auto yaml = (*root)["graphStorageSchema"];
  // Get graph name
  ::Yaml::Node graph_node = (yaml)["graph"];
  std::string graph_name = graph_node["name"].As<std::string>();
  
  std::cout << "Graph name: " << graph_name << std::endl;
  
  // Get prefix
  std::string prefix = graph_node["pathPrefix"].As<std::string>();
  
  std::cout << "Graph prefix: " << prefix << std::endl;
  
  // Parse vertex infos
  VertexInfoVector vertex_infos;
  ::Yaml::Node node_types = (yaml)["nodeTypes"];
  if (!node_types.IsSequence()) {
    return Status::YamlError("'nodeTypes' should be a sequence.");
  }
  
  std::cout << "Node types count: " << node_types.Size() << std::endl;
  
  for (::Yaml::Iterator it = node_types.Begin(); it != node_types.End(); it++) {
    std::cout << "Parsing node type..." << std::endl;
    GAR_ASSIGN_OR_RAISE(auto vertex_info, ParseVertexInfo((*it).second));
    vertex_infos.push_back(vertex_info);
  }
  
  // Parse edge infos
  EdgeInfoVector edge_infos;
  ::Yaml::Node edge_types = (yaml)["edgeTypes"];
  if (!edge_types.IsSequence()) {
    return Status::YamlError("'edgeTypes' should be a sequence.");
  }
  
  std::cout << "Edge types count: " << edge_types.Size() << std::endl;
  
  for (::Yaml::Iterator it = edge_types.Begin(); it != edge_types.End(); it++) {
    std::cout << "Parsing edge type..." << std::endl;
    GAR_ASSIGN_OR_RAISE(auto edge_info, ParseEdgeInfo((*it).second));
    edge_infos.push_back(edge_info);
  }
  
  // Create GraphInfo
  std::cout << "Creating graph info..." << std::endl;
  return CreateGraphInfo(graph_name, vertex_infos, edge_infos, {}, prefix);
}

Result<std::shared_ptr<GraphInfo>> LexSchemaReader::LoadFromFile(
    const std::string& path) {
  std::cout << "Loading from file: " << path << std::endl;
  
  std::string no_url_path;
  auto fs_result = FileSystemFromUriOrPath(path, &no_url_path);
  if (!fs_result.status().ok()) {
    std::cerr << "Failed to create filesystem from path: " << fs_result.status().message() << std::endl;
    return fs_result.status();
  }
  GAR_ASSIGN_OR_RAISE(auto fs, fs_result);
  std::cout << "No URL path: " << no_url_path << std::endl;
  
  auto read_result = fs->ReadFileToValue<std::string>(no_url_path);
  if (!read_result.status().ok()) {
    std::cerr << "Failed to read file: " << read_result.status().message() << std::endl;
    return read_result.status();
  }
  GAR_ASSIGN_OR_RAISE(auto yaml_content, read_result);
  std::cout << "YAML content size: " << yaml_content.size() << " bytes" << std::endl;
  std::cout << "YAML content: " << yaml_content.substr(0, std::min(size_t(200), yaml_content.size())) << "..." << std::endl;
                      
  std::string relative_path = PathToDirectory(path);
  std::cout << "Relative path: " << relative_path << std::endl;
  
  return Load(yaml_content, relative_path);
}

} // namespace graphar