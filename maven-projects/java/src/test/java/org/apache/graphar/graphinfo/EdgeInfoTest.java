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

import java.io.File;

import com.alibaba.fastffi.CXXReference;
import org.apache.graphar.stdcxx.StdSharedPtr;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.stdcxx.StdVector;
import org.apache.graphar.types.AdjListType;
import org.apache.graphar.types.DataType;
import org.apache.graphar.types.FileType;
import org.apache.graphar.types.Type;
import org.apache.graphar.util.GrapharStaticFunctions;
import org.apache.graphar.util.InfoVersion;
import org.apache.graphar.util.Result;
import org.junit.Assert;
import org.junit.Test;

public class EdgeInfoTest {
    @Test
    public void test1() {
        StdString srcLabel = StdString.create("person");
        StdString edgeLabel = StdString.create("knows");
        StdString dstLabel = StdString.create("person");
        long chunkSize = 100;
        long srcChunkSize = 100;
        long dstChunkSize = 100;
        boolean directed = true;
        StdVector.Factory<AdjacentList> adjancyListVecFactory = StdVector.getStdVectorFactory("std::vector<graphar::AdjacentList>");
        StdVector<AdjacentList> adjacentListStdVector = adjancyListVecFactory.create();
        StdVector.Factory<PropertyGroup> propertyGroupVecFactory = StdVector.getStdVectorFactory("std::vector<graphar::PropertyGroup>");
        StdVector<PropertyGroup> propertyGroupStdVector = propertyGroupVecFactory.create();
        InfoVersion infoVersion = InfoVersion.create(1);
        StdString prefix = StdString.create("");
        EdgeInfo edgeInfo =
                EdgeInfo.factory.create(
                        srcLabel,
                        edgeLabel,
                        dstLabel,
                        chunkSize,
                        srcChunkSize,
                        dstChunkSize,
                        directed,
                        adjacentListStdVector,
                        propertyGroupStdVector,
                        prefix,
                        infoVersion);
        Assert.assertEquals(srcLabel.toJavaString(), edgeInfo.getSrcLabel().toJavaString());
        Assert.assertEquals(edgeLabel.toJavaString(), edgeInfo.getEdgeLabel().toJavaString());
        Assert.assertEquals(dstLabel.toJavaString(), edgeInfo.getDstLabel().toJavaString());
        Assert.assertEquals(chunkSize, edgeInfo.getChunkSize());
        Assert.assertEquals(srcChunkSize, edgeInfo.getSrcChunkSize());
        Assert.assertEquals(dstChunkSize, edgeInfo.getDstChunkSize());
        Assert.assertEquals(directed, edgeInfo.isDirected());
        Assert.assertEquals(
                srcLabel.toJavaString()
                        + "_"
                        + edgeLabel.toJavaString()
                        + "_"
                        + dstLabel.toJavaString()
                        + "/",
                edgeInfo.getPrefix().toJavaString());
        Assert.assertTrue(infoVersion.eq(edgeInfo.getVersion()));

        // test add adjList
        AdjListType adjListType = AdjListType.ordered_by_source;
        FileType fileType = FileType.PARQUET;
        StdSharedPtr<AdjacentList> adjacentList = GrapharStaticFunctions.INSTANCE.createAdjacentList(adjListType, fileType);
        Assert.assertTrue(edgeInfo.addAdjacentList(adjacentList).status().ok());
        Assert.assertTrue(edgeInfo.hasAdjacentListType(adjListType));
        // same adj list type can not be added twice
        StdSharedPtr<AdjacentList> adjacentListTwice = GrapharStaticFunctions.INSTANCE.createAdjacentList(adjListType, fileType);
        Assert.assertTrue(edgeInfo.addAdjacentList(adjacentListTwice).hasError());
        FileType fileTypeResult = edgeInfo.getAdjacentList(adjListType).get().getFileType();
        Assert.assertEquals(fileType, fileTypeResult);
        StdString prefixOfAdjListType =
                StdString.create(AdjListType.adjListType2String(adjListType) + "/");
        Result<StdString> adjListPathPrefix = edgeInfo.getAdjListPathPrefix(adjListType);
        Assert.assertFalse(adjListPathPrefix.hasError());
        Assert.assertEquals(
                edgeInfo.getPrefix().toJavaString()
                        + prefixOfAdjListType.toJavaString()
                        + "adj_list/",
                adjListPathPrefix.value().toJavaString());
        Result<StdString> adjListFilePath = edgeInfo.getAdjListFilePath(0, 0, adjListType);
        Assert.assertFalse(adjListFilePath.hasError());
        Assert.assertEquals(
                adjListPathPrefix.value().toJavaString() + "part0/chunk0",
                adjListFilePath.value().toJavaString());
        Result<StdString> adjListOffsetPathPrefix = edgeInfo.getOffsetPathPrefix(adjListType);
        Assert.assertFalse(adjListOffsetPathPrefix.hasError());
        Assert.assertEquals(
                edgeInfo.getPrefix().toJavaString()
                        + prefixOfAdjListType.toJavaString()
                        + "offset/",
                adjListOffsetPathPrefix.value().toJavaString());
        Result<StdString> adjListOffsetFilePath = edgeInfo.getAdjListOffsetFilePath(0, adjListType);
        Assert.assertFalse(adjListOffsetFilePath.hasError());
        Assert.assertEquals(
                adjListOffsetPathPrefix.value().toJavaString() + "chunk0",
                adjListOffsetFilePath.value().toJavaString());

        // adj list type not exist
        AdjListType adjListTypeNotExist = AdjListType.ordered_by_dest;
        Assert.assertFalse(edgeInfo.hasAdjacentListType(adjListTypeNotExist));
        Assert.assertNull(edgeInfo.getAdjacentList(adjListTypeNotExist).get());
        Assert.assertTrue(
                edgeInfo.getAdjListFilePath(0, 0, adjListTypeNotExist).status().isKeyError());
        Assert.assertTrue(edgeInfo.getAdjListPathPrefix(adjListTypeNotExist).status().isKeyError());
        Assert.assertTrue(
                edgeInfo.getAdjListOffsetFilePath(0, adjListTypeNotExist).status().isKeyError());
        Assert.assertTrue(edgeInfo.getOffsetPathPrefix(adjListTypeNotExist).status().isKeyError());

        // test add property group
        Property property = Property.factory.create();
        property.setName(StdString.create("creationDate"));
        property.setType(GrapharStaticFunctions.INSTANCE.stringType());
        property.setPrimary(false);
        StdVector.Factory<Property> propertyVecFactory =
                StdVector.getStdVectorFactory("std::vector<graphar::Property>");
        StdVector<Property> propertyStdVector = propertyVecFactory.create();
        propertyStdVector.push_back(property);
        PropertyGroup propertyGroup = PropertyGroup.factory.create(propertyStdVector, fileType);
        StdVector<PropertyGroup> propertyGroups = edgeInfo.getPropertyGroups();
        Assert.assertEquals(0, propertyGroups.size());
        Assert.assertTrue(edgeInfo.addPropertyGroup(propertyGroup).status().ok());
        Assert.assertTrue(edgeInfo.hasPropertyGroup(propertyGroup));
        propertyGroups = edgeInfo.getPropertyGroups();
        Assert.assertEquals(1, propertyGroups.size());
        Result<PropertyGroup> propertyGroupResult =
                edgeInfo.getPropertyGroup(property.getName());
        Assert.assertFalse(propertyGroupResult.hasError());
        Assert.assertTrue(propertyGroup.eq(propertyGroupResult.value()));
        Result<DataType> dataTypeResult = edgeInfo.getPropertyType(property.getName());
        Assert.assertFalse(dataTypeResult.hasError());
        Assert.assertTrue(property.getType().get().eq(dataTypeResult.value()));
        boolean isPrimaryResult = edgeInfo.isPrimaryKey(property.getName());
        Assert.assertEquals(property.isPrimary(), isPrimaryResult);
        Result<StdString> propertyPathPrefix =
                edgeInfo.getPropertyGroupPathPrefix(propertyGroup, adjListType);
        Assert.assertFalse(propertyPathPrefix.hasError());
        Assert.assertEquals(
                edgeInfo.getPrefix().toJavaString()
                        + prefixOfAdjListType.toJavaString()
                        + propertyGroup.getPrefix().toJavaString(),
                propertyPathPrefix.value().toJavaString());
        Result<StdString> propertyFilePath =
                edgeInfo.getPropertyFilePath(propertyGroup, adjListType, 0, 0);
        Assert.assertFalse(propertyFilePath.hasError());
        Assert.assertEquals(
                propertyPathPrefix.value().toJavaString() + "part0/chunk0",
                propertyFilePath.value().toJavaString());

        // test property not exist
        StdString propertyNotExist = StdString.create("p_not_exist");
        Assert.assertTrue(
                edgeInfo.getPropertyGroup(propertyNotExist).status().isKeyError());
        Assert.assertTrue(edgeInfo.getPropertyType(propertyNotExist).status().isKeyError());

        // test property group not exist
        PropertyGroup propertyGroupNotExist = PropertyGroup.factory.create();
        Assert.assertTrue(
                edgeInfo.getPropertyFilePath(propertyGroupNotExist, adjListType, 0, 0)
                        .status()
                        .isKeyError());
        Assert.assertTrue(
                edgeInfo.getPropertyGroupPathPrefix(propertyGroupNotExist, adjListType)
                        .status()
                        .isKeyError());

        // test adj list not exist
        Assert.assertTrue(
                edgeInfo.getPropertyFilePath(propertyGroup, adjListTypeNotExist, 0, 0)
                        .status()
                        .isKeyError());
        Assert.assertTrue(
                edgeInfo.getPropertyGroupPathPrefix(propertyGroup, adjListTypeNotExist)
                        .status()
                        .isKeyError());
        Assert.assertTrue(
                edgeInfo.getEdgesNumFilePath(0, adjListTypeNotExist).status().isKeyError());
        Assert.assertTrue(
                edgeInfo.getVerticesNumFilePath(adjListTypeNotExist).status().isKeyError());

        // edge count file path
        Result<StdString> maybePath = edgeInfo.getEdgesNumFilePath(0, adjListType);
        Assert.assertFalse(maybePath.hasError());
        Assert.assertEquals(
                edgeInfo.getPrefix().toJavaString()
                        + prefixOfAdjListType.toJavaString()
                        + "edge_count0",
                maybePath.value().toJavaString());

        // vertex count file path
        Result<StdString> maybePath2 = edgeInfo.getVerticesNumFilePath(adjListType);
        Assert.assertFalse(maybePath2.hasError());
        Assert.assertEquals(
                edgeInfo.getPrefix().toJavaString()
                        + prefixOfAdjListType.toJavaString()
                        + "vertex_count",
                maybePath2.value().toJavaString());

        // test save
        StdString savePath = StdString.create("/tmp/gar-java-edge-tmp-file");
        Assert.assertTrue(edgeInfo.save(savePath).ok());
        File tempFile = new File(savePath.toJavaString());
        Assert.assertTrue(tempFile.exists());
    }
}
