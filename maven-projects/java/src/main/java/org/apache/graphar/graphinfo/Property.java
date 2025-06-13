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

import static org.apache.graphar.util.CppClassName.GAR_PROPERTY;
import static org.apache.graphar.util.CppHeaderName.GAR_GRAPH_INFO_H;

import com.alibaba.fastffi.*;
import org.apache.graphar.stdcxx.StdString;

/** Property is a class to store the property information for a group. */
@FFIGen
@FFITypeAlias(GAR_PROPERTY)
@CXXHead(GAR_GRAPH_INFO_H)
public interface Property extends CXXPointer {
    Factory factory = FFITypeFactory.getFactory(Property.class);

    @FFIGetter
    @FFINameAlias("name")
    @CXXValue
    StdString getName();

    @FFISetter
    @FFINameAlias("name")
    void setName(@CXXValue StdString name);

    @FFIGetter
    @FFINameAlias("is_primary")
    boolean isPrimary();

    @FFISetter
    @FFINameAlias("is_primary")
    void setPrimary(boolean nullable);

    @FFIGetter
    @FFINameAlias("is_nullable")
    boolean isNullable();

    @FFISetter
    @FFINameAlias("is_nullable")
    void setNullable(boolean nullable);

    @FFIFactory
    interface Factory {

        @CXXValue
        Property create(@CXXReference StdString name);
    }
}
