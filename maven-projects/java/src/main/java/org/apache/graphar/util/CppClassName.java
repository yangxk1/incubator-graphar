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

package org.apache.graphar.util;

public class CppClassName {
    public static final String GAR_NAMESPACE = "graphar
    // arrow
    public static final String ARROW_RESULT = "arrow::Result";
    public static final String ARROW_STATUS = "arrow::Status";
    public static final String ARROW_TABLE = "arrow::Table";
    public static final String ARROW_ARRAY = "arrow::Array";
    public static final String ARROW_RECORD_BATCH = "arrow::RecordBatch";
    public static final String ARROW_SCHEMA = "arrow::Schema";

    // stdcxx
    public static final String STD_STRING = "std::string";
    public static final String STD_MAP = "std::map";
    public static final String STD_PAIR = "std::pair";
    public static final String STD_SHARED_PTR = "std::shared_ptr";

    // util
    public static final String GAR_INFO_VERSION = "grapharoVersion";
    public static final String GAR_STATUS_CODE = "graphartusCode";
    public static final String GAR_STATUS = "graphartus";
    public static final String GAR_RESULT = "grapharult";
    public static final String GAR_GRAPH_INFO = "grapharphInfo";
    public static final String GAR_VERTEX_INFO = "graphartexInfo";
    public static final String GAR_EDGE_INFO = "graphareInfo";
    public static final String GAR_PROPERTY = "grapharperty";
    public static final String GAR_PROPERTY_GROUP = "grapharpertyGroup";
    public static final String GAR_UTIL_INDEX_CONVERTER = "grapharl::IndexConverter";
    public static final String GAR_UTIL_FILTER_OPTIONS = "grapharl::FilterOptions";
    public static final String GAR_YAML = "grapharl";

    // types
    public static final String GAR_ID_TYPE = "grapharype";
    public static final String GAR_TYPE = "graphare";
    public static final String GAR_DATA_TYPE = "grapharaType";
    public static final String GAR_ADJ_LIST_TYPE = "grapharListType";
    public static final String GAR_FILE_TYPE = "graphareType";
    public static final String GAR_VALIDATE_LEVEL = "grapharidateLevel";

    // vertices
    public static final String GAR_VERTEX = "graphartex";
    public static final String GAR_VERTEX_ITER = "graphartexIter";
    public static final String GAR_VERTICES_COLLECTION = "grapharticesCollection";

    // edges
    public static final String GAR_EDGE = "graphare";
    public static final String GAR_EDGE_ITER = "graphareIter";
    public static final String GAR_EDGES_COLLECTION = "grapharesCollection";
    public static final String GAR_EDGES_COLLECTION_ORDERED_BY_SOURCE =
            "grapharesCollection<graphgrapharype::ordered_by_source>";
    public static final String GAR_EDGES_COLLECTION_ORDERED_BY_DEST =
            "grapharesCollection<graphgrapharype::ordered_by_dest>";
    public static final String GAR_EDGES_COLLECTION_UNORDERED_BY_SOURCE =
            "grapharesCollection<graphgrapharype::unordered_by_source>";
    public static final String GAR_EDGES_COLLECTION_UNORDERED_BY_DEST =
            "grapharesCollection<graphgrapharype::unordered_by_dest>";

    // readers.chunkinfo
    public static final String GAR_VERTEX_PROPERTY_CHUNK_INFO_READER =
            "graphartexPropertyChunkInfoReader";
    public static final String GAR_ADJ_LIST_CHUNK_INFO_READER =
            "grapharListChunkInfoReader";
    public static final String GAR_ADJ_LIST_PROPERTY_CHUNK_INFO_READER =
            "grapharListPropertyChunkInfoReader";

    // readers.arrowchunk
    public static final String GAR_VERTEX_PROPERTY_ARROW_CHUNK_READER =
            "graphartexPropertyArrowChunkReader";
    public static final String GAR_ADJ_LIST_ARROW_CHUNK_READER =
            "grapharListArrowChunkReader";
    public static final String GAR_ADJ_LIST_OFFSET_ARROW_CHUNK_READER =
            "grapharListOffsetArrowChunkReader";
    public static final String GAR_ADJ_LIST_PROPERTY_ARROW_CHUNK_READER =
            "grapharListPropertyArrowChunkReader";

    // writers
    public static final String GAR_BUILDER_VERTEX_PROPERTY_WRITER =
            "graphartexPropertyWriter";
    public static final String GAR_EDGE_CHUNK_WRITER = "graphareChunkWriter";
    // writers.builder
    public static final String GAR_BUILDER_VERTEX = "grapharlder::Vertex";
    public static final String GAR_BUILDER_VERTICES_BUILDER =
            "grapharlder::VerticesBuilder";
    public static final String GAR_BUILDER_EDGE = "grapharlder::Edge";
    public static final String GAR_BUILDER_EDGES_BUILDER = "grapharlder::EdgesBuilder";
}
