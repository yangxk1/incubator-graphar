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

import static org.apache.graphar.util.CppClassName.*;
import static org.apache.graphar.util.CppHeaderName.GAR_ARROW_CHUNK_READER_H;
import static org.apache.graphar.util.CppHeaderName.GAR_CHUNK_INFO_READER_H;
import static org.apache.graphar.util.CppHeaderName.GAR_GRAPH_H;
import static org.apache.graphar.util.CppHeaderName.GAR_GRAPH_INFO_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;

@FFIGen
@FFITypeAlias(GAR_RESULT)
@CXXHead(GAR_GRAPH_INFO_H)
@CXXHead(GAR_GRAPH_H)
@CXXHead(GAR_CHUNK_INFO_READER_H)
@CXXHead(GAR_ARROW_CHUNK_READER_H)
@CXXTemplate(cxx = "bool", java = "java.lang.Boolean")
@CXXTemplate(cxx = "long", java = "java.lang.Long")
@CXXTemplate(cxx = "int64_t", java = "java.lang.Long")
public interface Result<T> extends CXXPointer {

    @CXXReference
    T value();

    @CXXValue
    Status status();

    @FFINameAlias("has_error")
    boolean hasError();
}
