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

package org.apache.graphar.info;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.type.FileType;
import org.junit.Assert;
import org.junit.Test;

public class GraphInfoLabelsTest {

    @Test
    public void testGraphInfoWithLabels() {
        // Create property group
        Property property = new Property("id", DataType.INT32, true, false);
        PropertyGroup propertyGroup =
                new PropertyGroup(Collections.singletonList(property), FileType.PARQUET, "test/");

        // Create vertex info
        VertexInfo vertexInfo =
                new VertexInfo(
                        "person",
                        100L,
                        Collections.singletonList(propertyGroup),
                        "vertex/person/",
                        "gar/v1");

        // Create edge info
        AdjacentList adjacentList =
                new AdjacentList(AdjListType.ordered_by_source, FileType.CSV, "adj_list/");
        EdgeInfo edgeInfo =
                new EdgeInfo(
                        "person",
                        "knows",
                        "person",
                        1024L,
                        100L,
                        100L,
                        true,
                        "edge/person_knows_person/",
                        "gar/v1",
                        Collections.singletonList(adjacentList),
                        Collections.singletonList(propertyGroup));

        // Create labels
        List<String> labels = Arrays.asList("SocialNetwork", "LDBC");

        // Create graph info with labels
        GraphInfo graphInfo =
                new GraphInfo(
                        "test_graph",
                        Collections.singletonList(vertexInfo),
                        Collections.singletonList(edgeInfo),
                        labels,
                        "/tmp/test/",
                        "gar/v1");

        // Test getters
        Assert.assertEquals("test_graph", graphInfo.getName());
        Assert.assertEquals(labels, graphInfo.getLabels());
        Assert.assertEquals("/tmp/test/", graphInfo.getPrefix());
        Assert.assertEquals(1, graphInfo.getVertexInfoNum());
        Assert.assertEquals(1, graphInfo.getEdgeInfoNum());
        Assert.assertEquals("gar/v1", graphInfo.getVersion().toString());

        // Test vertex and edge info access
        Assert.assertTrue(graphInfo.hasVertexInfo("person"));
        Assert.assertNotNull(graphInfo.getVertexInfo("person"));
        Assert.assertTrue(graphInfo.hasEdgeInfo("person", "knows", "person"));
        Assert.assertNotNull(graphInfo.getEdgeInfo("person", "knows", "person"));

        // Test validation
        Assert.assertTrue(graphInfo.isValidated());

        // Test dump
        String yaml = graphInfo.dump();
        Assert.assertNotNull(yaml);
        Assert.assertTrue(yaml.contains("labels:"));
        Assert.assertTrue(yaml.contains("SocialNetwork"));
        Assert.assertTrue(yaml.contains("LDBC"));
    }

    @Test
    public void testGraphInfoWithoutLabels() {
        // Create property group
        Property property = new Property("id", DataType.INT32, true, false);
        PropertyGroup propertyGroup =
                new PropertyGroup(Collections.singletonList(property), FileType.PARQUET, "test/");

        // Create vertex info
        VertexInfo vertexInfo =
                new VertexInfo(
                        "person",
                        100L,
                        Collections.singletonList(propertyGroup),
                        "vertex/person/",
                        "gar/v1");

        // Create edge info
        AdjacentList adjacentList =
                new AdjacentList(AdjListType.ordered_by_source, FileType.CSV, "adj_list/");
        EdgeInfo edgeInfo =
                new EdgeInfo(
                        "person",
                        "knows",
                        "person",
                        1024L,
                        100L,
                        100L,
                        true,
                        "edge/person_knows_person/",
                        "gar/v1",
                        Collections.singletonList(adjacentList),
                        Collections.singletonList(propertyGroup));

        // Create graph info without labels (using old constructor)
        GraphInfo graphInfo =
                new GraphInfo(
                        "test_graph",
                        Collections.singletonList(vertexInfo),
                        Collections.singletonList(edgeInfo),
                        "/tmp/test/",
                        "gar/v1");

        // Test that labels list is empty but not null
        Assert.assertEquals("test_graph", graphInfo.getName());
        Assert.assertNotNull(graphInfo.getLabels());
        Assert.assertTrue(graphInfo.getLabels().isEmpty());
        Assert.assertEquals("/tmp/test/", graphInfo.getPrefix());

        // Test validation
        Assert.assertTrue(graphInfo.isValidated());

        // Test dump
        String yaml = graphInfo.dump();
        Assert.assertNotNull(yaml);
    }

    @Test
    public void testGraphInfoWithEmptyLabels() {
        // Create property group
        Property property = new Property("id", DataType.INT32, true, false);
        PropertyGroup propertyGroup =
                new PropertyGroup(Collections.singletonList(property), FileType.PARQUET, "test/");

        // Create vertex info
        VertexInfo vertexInfo =
                new VertexInfo(
                        "person",
                        100L,
                        Collections.singletonList(propertyGroup),
                        "vertex/person/",
                        "gar/v1");

        // Create edge info
        AdjacentList adjacentList =
                new AdjacentList(AdjListType.ordered_by_source, FileType.CSV, "adj_list/");
        EdgeInfo edgeInfo =
                new EdgeInfo(
                        "person",
                        "knows",
                        "person",
                        1024L,
                        100L,
                        100L,
                        true,
                        "edge/person_knows_person/",
                        "gar/v1",
                        Collections.singletonList(adjacentList),
                        Collections.singletonList(propertyGroup));

        // Create graph info with empty labels
        GraphInfo graphInfo =
                new GraphInfo(
                        "test_graph",
                        Collections.singletonList(vertexInfo),
                        Collections.singletonList(edgeInfo),
                        Collections.emptyList(),
                        "/tmp/test/",
                        "gar/v1");

        // Test that labels list is empty but not null
        Assert.assertEquals("test_graph", graphInfo.getName());
        Assert.assertNotNull(graphInfo.getLabels());
        Assert.assertTrue(graphInfo.getLabels().isEmpty());
        Assert.assertEquals("/tmp/test/", graphInfo.getPrefix());

        // Test validation
        Assert.assertTrue(graphInfo.isValidated());
    }

    @Test
    public void testGraphInfoAddVertexWithLabels() {
        // Create property group
        Property property = new Property("id", DataType.INT32, true, false);
        PropertyGroup propertyGroup =
                new PropertyGroup(Collections.singletonList(property), FileType.PARQUET, "test/");

        // Create initial vertex info
        VertexInfo vertexInfo1 =
                new VertexInfo(
                        "person",
                        100L,
                        Collections.singletonList(propertyGroup),
                        "vertex/person/",
                        "gar/v1");

        // Create graph info with labels
        List<String> labels = Arrays.asList("SocialNetwork", "LDBC");
        GraphInfo graphInfo =
                new GraphInfo(
                        "test_graph",
                        Collections.singletonList(vertexInfo1),
                        Collections.emptyList(),
                        labels,
                        "/tmp/test/",
                        "gar/v1");

        // Create new vertex info to add
        VertexInfo vertexInfo2 =
                new VertexInfo(
                        "comment",
                        100L,
                        Collections.singletonList(propertyGroup),
                        "vertex/comment/",
                        "gar/v1");

        // Add vertex
        GraphInfo newGraphInfo = graphInfo.addVertexAsNew(vertexInfo2).orElse(null);
        Assert.assertNotNull(newGraphInfo);
        Assert.assertEquals(2, newGraphInfo.getVertexInfoNum());
        Assert.assertEquals(0, newGraphInfo.getEdgeInfoNum());
        Assert.assertEquals(labels, newGraphInfo.getLabels());
        Assert.assertTrue(newGraphInfo.hasVertexInfo("person"));
        Assert.assertTrue(newGraphInfo.hasVertexInfo("comment"));
    }

    @Test
    public void testGraphInfoAddEdgeWithLabels() {
        // Create property group
        Property property = new Property("id", DataType.INT32, true, false);
        PropertyGroup propertyGroup =
                new PropertyGroup(Collections.singletonList(property), FileType.PARQUET, "test/");

        // Create vertex info
        VertexInfo vertexInfo =
                new VertexInfo(
                        "person",
                        100L,
                        Collections.singletonList(propertyGroup),
                        "vertex/person/",
                        "gar/v1");

        // Create initial edge info
        AdjacentList adjacentList =
                new AdjacentList(AdjListType.ordered_by_source, FileType.CSV, "adj_list/");
        EdgeInfo edgeInfo1 =
                new EdgeInfo(
                        "person",
                        "knows",
                        "person",
                        1024L,
                        100L,
                        100L,
                        true,
                        "edge/person_knows_person/",
                        "gar/v1",
                        Collections.singletonList(adjacentList),
                        Collections.singletonList(propertyGroup));

        // Create graph info with labels
        List<String> labels = Arrays.asList("SocialNetwork", "LDBC");
        GraphInfo graphInfo =
                new GraphInfo(
                        "test_graph",
                        Collections.singletonList(vertexInfo),
                        Collections.singletonList(edgeInfo1),
                        labels,
                        "/tmp/test/",
                        "gar/v1");

        // Create new edge info to add
        EdgeInfo edgeInfo2 =
                new EdgeInfo(
                        "person",
                        "likes",
                        "person",
                        1024L,
                        100L,
                        100L,
                        true,
                        "edge/person_likes_person/",
                        "gar/v1",
                        Collections.singletonList(adjacentList),
                        Collections.singletonList(propertyGroup));

        // Add edge
        GraphInfo newGraphInfo = graphInfo.addEdgeAsNew(edgeInfo2).orElse(null);
        Assert.assertNotNull(newGraphInfo);
        Assert.assertEquals(1, newGraphInfo.getVertexInfoNum());
        Assert.assertEquals(2, newGraphInfo.getEdgeInfoNum());
        Assert.assertEquals(labels, newGraphInfo.getLabels());
        Assert.assertTrue(newGraphInfo.hasEdgeInfo("person", "knows", "person"));
        Assert.assertTrue(newGraphInfo.hasEdgeInfo("person", "likes", "person"));
    }
}
