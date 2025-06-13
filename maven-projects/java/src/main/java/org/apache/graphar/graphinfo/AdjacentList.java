package org.apache.graphar.graphinfo;

import static org.apache.graphar.util.CppClassName.GAR_ADJACENT_LIST;
import static org.apache.graphar.util.CppHeaderName.GAR_GRAPH_INFO_H;

import com.alibaba.fastffi.*;
import org.apache.graphar.stdcxx.StdString;

@FFIGen
@FFITypeAlias(GAR_ADJACENT_LIST)
@CXXHead(GAR_GRAPH_INFO_H)
public interface AdjacentList extends FFIPointer {

    @FFINameAlias("GetPrefix")
    @FFIConst
    @CXXReference
    StdString getPrefix();

    @FFINameAlias("IsValidated")
    boolean isValidated();
}
