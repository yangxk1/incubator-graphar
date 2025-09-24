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
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.type.FileType;
import org.junit.Assert;
import org.junit.Test;

public class VertexInfoLabelsTest {

    @Test
    public void testVertexInfoWithLabels() {
        // Create property group
        Property property = new Property("id", DataType.INT32, true, false);
        PropertyGroup propertyGroup =
                new PropertyGroup(Collections.singletonList(property), FileType.PARQUET, "test/");

        // Create labels
        List<String> labels = Arrays.asList("Person", "Employee", "User");

        // Create vertex info with labels
        VertexInfo vertexInfo =
                new VertexInfo(
                        "person",
                        100L,
                        Collections.singletonList(propertyGroup),
                        labels,
                        "vertex/person/",
                        "gar/v1");

        // Test getters
        Assert.assertEquals("person", vertexInfo.getType());
        Assert.assertEquals(100L, vertexInfo.getChunkSize());
        Assert.assertEquals(labels, vertexInfo.getLabels());
        Assert.assertEquals("vertex/person/", vertexInfo.getPrefix());
        Assert.assertEquals("gar/v1", vertexInfo.getVersion().toString());

        // Test property group related methods
        Assert.assertEquals(1, vertexInfo.getPropertyGroupNum());
        Assert.assertTrue(vertexInfo.hasProperty("id"));
        Assert.assertTrue(vertexInfo.isPrimaryKey("id"));
        Assert.assertFalse(vertexInfo.isNullableKey("id"));

        // Test validation
        Assert.assertTrue(vertexInfo.isValidated());

        // Test dump
        String yaml = vertexInfo.dump();
        Assert.assertNotNull(yaml);
        Assert.assertTrue(yaml.contains("labels:"));
        Assert.assertTrue(yaml.contains("Person"));
        Assert.assertTrue(yaml.contains("Employee"));
        Assert.assertTrue(yaml.contains("User"));
    }

    @Test
    public void testVertexInfoWithoutLabels() {
        // Create property group
        Property property = new Property("id", DataType.INT32, true, false);
        PropertyGroup propertyGroup =
                new PropertyGroup(Collections.singletonList(property), FileType.PARQUET, "test/");

        // Create vertex info without labels (using old constructor)
        VertexInfo vertexInfo =
                new VertexInfo(
                        "person",
                        100L,
                        Collections.singletonList(propertyGroup),
                        "vertex/person/",
                        "gar/v1");

        // Test that labels list is empty but not null
        Assert.assertEquals("person", vertexInfo.getType());
        Assert.assertEquals(100L, vertexInfo.getChunkSize());
        Assert.assertNotNull(vertexInfo.getLabels());
        Assert.assertTrue(vertexInfo.getLabels().isEmpty());
        Assert.assertEquals("vertex/person/", vertexInfo.getPrefix());

        // Test validation
        Assert.assertTrue(vertexInfo.isValidated());

        // Test dump
        String yaml = vertexInfo.dump();
        Assert.assertNotNull(yaml);
    }

    @Test
    public void testVertexInfoWithEmptyLabels() {
        // Create property group
        Property property = new Property("id", DataType.INT32, true, false);
        PropertyGroup propertyGroup =
                new PropertyGroup(Collections.singletonList(property), FileType.PARQUET, "test/");

        // Create vertex info with empty labels
        VertexInfo vertexInfo =
                new VertexInfo(
                        "person",
                        100L,
                        Collections.singletonList(propertyGroup),
                        Collections.emptyList(),
                        "vertex/person/",
                        "gar/v1");

        // Test that labels list is empty but not null
        Assert.assertEquals("person", vertexInfo.getType());
        Assert.assertEquals(100L, vertexInfo.getChunkSize());
        Assert.assertNotNull(vertexInfo.getLabels());
        Assert.assertTrue(vertexInfo.getLabels().isEmpty());
        Assert.assertEquals("vertex/person/", vertexInfo.getPrefix());

        // Test validation
        Assert.assertTrue(vertexInfo.isValidated());
    }
}
