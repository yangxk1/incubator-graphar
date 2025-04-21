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

package org.apache.graphar.graphinfo;

import org.apache.graphar.stdcxx.StdMap;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.stdcxx.StdVector;
import org.apache.graphar.types.AdjListType;
import org.apache.graphar.util.InfoVersion;
import org.apache.graphar.util.Result;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class GraphInfoTest {
    public static final String root = System.getenv("GAR_TEST_DATA") + "/java";

    @Test
    public void test1() {
        String graphName = "test_graph";
        String prefix = "test_prefix";
        InfoVersion version = InfoVersion.create(1);
        GraphInfo graphInfo = GraphInfo.create(graphName, version, prefix);
        Assert.assertEquals(graphName, graphInfo.getName().toJavaString());
        Assert.assertEquals(prefix, graphInfo.getPrefix().toJavaString());
        Assert.assertTrue(version.eq(graphInfo.getInfoVersion()));

        // test add vertex and get vertex info
        StdString vertexLabel = StdString.create("test_vertex");
        long vertexChunkSize = 100;
        StdString vertexPrefix = StdString.create("test_vertex_prefix");
        StdString vertexInfoPath = StdString.create("/tmp/test_vertex.vertex.yml");
        StdString unknownLabel = StdString.create("text_not_exist");
        VertexInfo vertexInfo =
                VertexInfo.factory.create(vertexLabel, vertexChunkSize, version, vertexPrefix);
        Assert.assertEquals(0, graphInfo.getVertexInfos().size());
        Assert.assertTrue(graphInfo.addVertex(vertexInfo).ok());
        graphInfo.addVertexInfoPath(vertexInfoPath);
        Assert.assertEquals(1, graphInfo.getVertexInfos().size());
        Result<VertexInfo> maybeVertexInfo = graphInfo.getVertexInfo(vertexLabel);
        Assert.assertFalse(maybeVertexInfo.hasError());
        Assert.assertTrue(vertexLabel.eq(maybeVertexInfo.value().getLabel()));
        Assert.assertTrue(vertexPrefix.eq(maybeVertexInfo.value().getPrefix()));
        Assert.assertTrue(graphInfo.getVertexInfo(unknownLabel).status().isKeyError());
        // existed vertex info can't be added again
        Assert.assertTrue(graphInfo.addVertex(vertexInfo).isInvalid());

        // test add edge and get edge info
        StdString srcLabel = StdString.create("test_vertex");
        StdString edgeLabel = StdString.create("test_edge");
        StdString dstLabel = StdString.create("test_vertex");
        long edgeChunkSize = 1024;
        StdString edgeInfoPath = StdString.create("/tmp/test_edge.edge.yml");
        EdgeInfo edgeInfo =
                EdgeInfo.factory.create(
                        srcLabel,
                        edgeLabel,
                        dstLabel,
                        edgeChunkSize,
                        vertexChunkSize,
                        vertexChunkSize,
                        true,
                        version);
        Assert.assertEquals(0, graphInfo.getEdgeInfos().size());
        Assert.assertTrue(graphInfo.addEdge(edgeInfo).ok());
        graphInfo.addEdgeInfoPath(edgeInfoPath);
        Assert.assertEquals(1, graphInfo.getEdgeInfos().size());
        Result<EdgeInfo> maybeEdgeInfo = graphInfo.getEdgeInfo(srcLabel, edgeLabel, dstLabel);
        Assert.assertFalse(maybeEdgeInfo.hasError());
        Assert.assertTrue(srcLabel.eq(maybeEdgeInfo.value().getSrcLabel()));
        Assert.assertTrue(edgeLabel.eq(maybeEdgeInfo.value().getEdgeLabel()));
        Assert.assertTrue(dstLabel.eq(maybeEdgeInfo.value().getDstLabel()));
        Assert.assertTrue(
                graphInfo
                        .getEdgeInfo(unknownLabel, unknownLabel, unknownLabel)
                        .status()
                        .isKeyError());
        // existed edge info can't be added again
        Assert.assertTrue(graphInfo.addEdge(edgeInfo).isInvalid());

        // test version
        Assert.assertTrue(version.eq(graphInfo.getInfoVersion()));
    }

    @Test
    public void testGraphInfoLoadFromFile() {
        String path = root + "/ldbc_sample/csv/ldbc_sample.graph.yml";
        Result<GraphInfo> graphInfoResult = GraphInfo.load(path);
        Assert.assertFalse(graphInfoResult.hasError());
        GraphInfo graphInfo = graphInfoResult.value();
        //basics
        Assert.assertEquals("ldbc_sample", graphInfo.getName().toJavaString());
        Assert.assertEquals(root + "/ldbc_sample/csv/", graphInfo.getPrefix().toJavaString());
        Assert.assertEquals("gar/v1", graphInfo.getInfoVersion().toJavaString());
        StdMap<StdString, VertexInfo> vertexInfos = graphInfo.getVertexInfos();
        StdMap<StdString, EdgeInfo> edgeInfos = graphInfo.getEdgeInfos();
        Assert.assertEquals(1, vertexInfos.size());
        Assert.assertEquals(1, edgeInfos.size());

        VertexInfo vertexInfo = vertexInfos.get(StdString.create("person"));
        //IsValidate
        Assert.assertTrue(vertexInfo.isValidated());
        //vertex info
        Assert.assertEquals("person", vertexInfo.getLabel().toJavaString());
        Assert.assertEquals(100, vertexInfo.getChunkSize());
        Assert.assertEquals("vertex/person/",vertexInfo.getPrefix().toJavaString());
        Assert.assertEquals("vertex/person/vertex_count",vertexInfo.getVerticesNumFilePath().value().toJavaString());
        Assert.assertEquals("gar/v1", vertexInfo.getVersion());
        Assert.assertEquals(2, vertexInfo.getPropertyGroups().size());
        
        //vertex properties
        Result<PropertyGroup> propertyGroup1_result = vertexInfo.getPropertyGroup(StdString.create("id"));
        Assert.assertFalse(propertyGroup1_result.hasError());
        PropertyGroup propertyGroup1 = propertyGroup1_result.value();
        Assert.assertEquals("id/", propertyGroup1.getPrefix().toJavaString());
        Assert.assertEquals("vertex/person/id/chunk0",vertexInfo.getFilePath(propertyGroup1,0).value().toJavaString());
        Assert.assertEquals("vertex/person/id/chunk4",vertexInfo.getFilePath(propertyGroup1,4).value().toJavaString());
        Assert.assertEquals("csv", propertyGroup1.getFileType().toJavaString());
        Assert.assertTrue(vertexInfo.containProperty(StdString.create("id")));
        Property property1 = propertyGroup1.getProperties().get(0);
        Assert.assertEquals("int64", property1.getType().toString());
        Assert.assertTrue(property1.isPrimary());
        Assert.assertTrue(vertexInfo.containProperty(StdString.create("firstName")));
        Result<PropertyGroup> propertyGroup2_result = vertexInfo.getPropertyGroup(StdString.create("firstName"));
        Assert.assertFalse(propertyGroup2_result.hasError());
        PropertyGroup propertyGroup2 = propertyGroup2_result.value();
        Assert.assertEquals("firstName_lastName_gender/", propertyGroup2.getPrefix().toString());
        Assert.assertEquals("vertex/person/firstName_lastName_gender/chunk0",vertexInfo.getFilePath(propertyGroup2,0).value().toString());
        Assert.assertEquals("vertex/person/firstName_lastName_gender/chunk4",vertexInfo.getFilePath(propertyGroup2,4).value().toString());
        Assert.assertEquals("csv", propertyGroup2.getFileType().toString());
        StdVector<Property> g2Properties = propertyGroup2.getProperties();
        Assert.assertEquals("firstName",g2Properties.get(0).getName().toString());
        Assert.assertEquals("string",g2Properties.get(0).getType().toString());
        Assert.assertFalse(g2Properties.get(0).isPrimary());

        Assert.assertEquals("lastName",g2Properties.get(1).getName().toString());
        Assert.assertEquals("string",g2Properties.get(1).getType().toString());
        Assert.assertFalse(g2Properties.get(1).isPrimary());

        Assert.assertEquals("gender",g2Properties.get(2).getName().toString());
        Assert.assertEquals("string",g2Properties.get(2).getType().toString());
        Assert.assertFalse(g2Properties.get(2).isPrimary());


        EdgeInfo edgeInfo = edgeInfos.get(StdString.create("knows"));
        //isValidated
        Assert.assertTrue(edgeInfo.isValidated());
        //edge info
        Assert.assertEquals("knows", edgeInfo.getEdgeLabel().toJavaString());
        Assert.assertEquals("person", edgeInfo.getSrcLabel().toJavaString());
        Assert.assertEquals("person", edgeInfo.getDstLabel().toJavaString());
        Assert.assertEquals(1024, edgeInfo.getChunkSize());
        Assert.assertEquals(100, edgeInfo.getSrcChunkSize());
        Assert.assertEquals(100, edgeInfo.getDstChunkSize());
        Assert.assertFalse(edgeInfo.isDirected());
        Assert.assertEquals("edge/person_knows_person/", edgeInfo.getPrefix().toJavaString());
        Assert.assertEquals("gar/v1", edgeInfo.getVersion());

        //edge adjlist
        AdjListType ordered_by_source = AdjListType.ordered_by_source;
        AdjListType ordered_by_dest = AdjListType.ordered_by_dest;
        Assert.assertTrue(edgeInfo.containAdjList(ordered_by_source));
        Assert.assertEquals("ordered_by_source/", edgeInfo.getAdjListPathPrefix(ordered_by_source).value().toJavaString());
        Assert.assertEquals("csv", edgeInfo.getFileType(ordered_by_source).value().toString());
        Assert.assertEquals("edge/person_knows_person/ordered_by_source/vertex_count",edgeInfo.getVerticesNumFilePath(ordered_by_source).value().toJavaString());
        Assert.assertEquals("edge/person_knows_person/ordered_by_source/edge_count0", edgeInfo.getEdgesNumFilePath(0, ordered_by_dest).value().toJavaString());
        Assert.assertEquals("edge/person_knows_person/ordered_by_source/adj_list/part0/chunk0", edgeInfo.getAdjListFilePath(0,0,ordered_by_source).value().toJavaString());
        Assert.assertEquals("edge/person_knows_person/ordered_by_source/offset/chunk0", edgeInfo.getAdjListOffsetFilePath(0, ordered_by_source).value().toJavaString());
        Assert.assertEquals("edge/person_knows_person/ordered_by_source/offset/chunk4", edgeInfo.getAdjListOffsetFilePath(4, ordered_by_source).value().toJavaString());
        Assert.assertEquals("edge/person_knows_person/ordered_by_source/offset/", edgeInfo.getOffsetPathPrefix(ordered_by_source).value().toJavaString());
        
        Assert.assertTrue(edgeInfo.containAdjList(ordered_by_dest));
        Assert.assertEquals("ordered_by_dest/", edgeInfo.getAdjListPathPrefix(ordered_by_dest).value().toJavaString());
        Assert.assertEquals("csv", edgeInfo.getFileType(ordered_by_dest).value().toString());
        Assert.assertEquals("edge/person_knows_person/ordered_by_dest/vertex_count",edgeInfo.getVerticesNumFilePath(ordered_by_dest).value().toJavaString());
        Assert.assertEquals("edge/person_knows_person/ordered_by_dest/edge_count0", edgeInfo.getEdgesNumFilePath(0, ordered_by_dest).value().toJavaString());
        Assert.assertEquals("edge/person_knows_person/ordered_by_dest/adj_list/part0/chunk0", edgeInfo.getAdjListFilePath(0,0,ordered_by_source).value().toJavaString());
        Assert.assertEquals("edge/person_knows_person/ordered_by_dest/offset/chunk0", edgeInfo.getAdjListOffsetFilePath(0, ordered_by_dest).value().toJavaString());
        Assert.assertEquals("edge/person_knows_person/ordered_by_dest/offset/chunk4", edgeInfo.getAdjListOffsetFilePath(4, ordered_by_dest).value().toJavaString());
        Assert.assertEquals("edge/person_knows_person/ordered_by_dest/offset/", edgeInfo.getOffsetPathPrefix(ordered_by_dest).value().toJavaString());
        
        //edge properties_group
        Assert.assertEquals(1, edgeInfo.getPropertyGroups(ordered_by_source).value().size());
        PropertyGroup os_propertyGroup = edgeInfo.getPropertyGroups(ordered_by_source).value().get(0);
        Assert.assertEquals("creationDate/",os_propertyGroup.getPrefix().toJavaString());
        Assert.assertEquals("csv",os_propertyGroup.getFileType().toString());
        Property os_property = os_propertyGroup.getProperties().get(0);
        Assert.assertEquals("creationDate",os_property.getName().toJavaString());
        Assert.assertEquals("string",os_property.getType().toString());
        Assert.assertFalse(os_property.isPrimary());
        Assert.assertEquals("edge/person_knows_person/ordered_by_source/creationDate/part0/chunk0", edgeInfo.getPropertyFilePath(os_propertyGroup, ordered_by_source, 0, 0).value().toJavaString());
        Assert.assertEquals("edge/person_knows_person/ordered_by_source/creationDate/part1/chunk2", edgeInfo.getPropertyFilePath(os_propertyGroup, ordered_by_source, 1, 2).value().toJavaString());
        Assert.assertEquals("edge/person_knows_person/ordered_by_source/creationDate/", edgeInfo.getPropertyGroupPathPrefix(os_propertyGroup, ordered_by_source).value().toJavaString());

        Assert.assertEquals(1, edgeInfo.getPropertyGroups(ordered_by_dest).value().size());
        PropertyGroup od_propertyGroup = edgeInfo.getPropertyGroups(ordered_by_dest).value().get(0);
        Assert.assertEquals("creationDate/",od_propertyGroup.getPrefix().toJavaString());
        Assert.assertEquals("csv",od_propertyGroup.getFileType().toString());
        Property od_property = od_propertyGroup.getProperties().get(0);
        Assert.assertEquals("creationDate",od_property.getName().toJavaString());
        Assert.assertEquals("string",od_property.getType().toString());
        Assert.assertFalse(od_property.isPrimary());
        Assert.assertEquals("edge/person_knows_person/ordered_by_dest/creationDate/part0/chunk0", edgeInfo.getPropertyFilePath(os_propertyGroup, ordered_by_dest, 0, 0).value().toJavaString());
        Assert.assertEquals("edge/person_knows_person/ordered_by_dest/creationDate/part1/chunk2", edgeInfo.getPropertyFilePath(os_propertyGroup, ordered_by_dest, 1, 2).value().toJavaString());
        Assert.assertEquals("edge/person_knows_person/ordered_by_dest/creationDate/", edgeInfo.getPropertyGroupPathPrefix(os_propertyGroup, ordered_by_dest).value().toJavaString());


}

    @Ignore(
            "Problem about arrow 12.0.0 with S3, see https://github.com/apache/incubator-graphar/issues/187")
    public void testGraphInfoLoadFromS3() {
        // arrow::fs::Fi
        // nalizeS3 was not called even though S3 was initialized.  This could lead to a
        // segmentation
        // fault at exit
        String path =
                "s3://graphar/ldbc/ldbc.graph.yml"
                        + "?endpoint_override=graphscope.oss-cn-beijing.aliyuncs.com";
        Result<GraphInfo> graphInfoResult = GraphInfo.load(path);
        Assert.assertFalse(graphInfoResult.hasError());
        GraphInfo graphInfo = graphInfoResult.value();
        Assert.assertEquals("ldbc", graphInfo.getName().toJavaString());
        StdMap<StdString, VertexInfo> vertexInfos = graphInfo.getVertexInfos();
        StdMap<StdString, EdgeInfo> edgeInfos = graphInfo.getEdgeInfos();
        Assert.assertEquals(8, vertexInfos.size());
        Assert.assertEquals(23, edgeInfos.size());
    }
}
