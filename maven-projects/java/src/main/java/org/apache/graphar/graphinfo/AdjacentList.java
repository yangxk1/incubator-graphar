package org.apache.graphar.graphinfo;

import static org.apache.graphar.util.CppClassName.GAR_ADJACENT_LIST;
import static org.apache.graphar.util.CppHeaderName.GAR_GRAPH_INFO_H;

import com.alibaba.fastffi.*;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.types.AdjListType;
import org.apache.graphar.types.FileType;

@FFIGen
@FFITypeAlias(GAR_ADJACENT_LIST)
@CXXHead(GAR_GRAPH_INFO_H)
public interface AdjacentList extends FFIPointer {

    AdjacentList.Factory factory = FFITypeFactory.getFactory(AdjacentList.class);

    static AdjacentList create(AdjListType type, FileType file_type, String prefix) {
        StdString stdPrefix = StdString.create(prefix);
        AdjacentList res = factory.create(type, file_type, stdPrefix);
        stdPrefix.delete();
        return res;
    }

    static AdjacentList create(AdjListType type, FileType file_type) {
        return factory.create(type, file_type);
    }

    @FFINameAlias("GetType")
    @CXXValue
    @FFIConst
    AdjListType getType();

    @FFINameAlias("GetFileType")
    @CXXValue
    @FFIConst
    FileType getFileType();

    @FFINameAlias("GetPrefix")
    @CXXValue
    @FFIConst
    @CXXReference
    StdString getPrefix();

    @FFINameAlias("isValidated")
    boolean isValidated();

    @FFIFactory
    interface Factory {
        AdjacentList create(
                AdjListType adjListType, FileType fileType, @CXXReference StdString prefix);

        AdjacentList create(AdjListType adjListType, FileType fileType);
    }
}
