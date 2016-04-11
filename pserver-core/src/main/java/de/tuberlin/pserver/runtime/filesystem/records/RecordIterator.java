package de.tuberlin.pserver.runtime.filesystem.records;

import de.tuberlin.pserver.runtime.filesystem.AbstractFileIterationContext;
import de.tuberlin.pserver.runtime.filesystem.distributed.DistributedFileIterator;
import de.tuberlin.pserver.types.matrix.typeinfo.MatrixTypeInfo;
import de.tuberlin.pserver.types.typeinfo.properties.FileFormat;

import java.io.InputStream;
import java.util.Iterator;
import java.util.Optional;

public interface RecordIterator extends Iterator<Record> {

    // ---------------------------------------------------
    // Factory Methods.
    // ---------------------------------------------------

    static RecordIterator create(MatrixTypeInfo matrixTypeInfo, AbstractFileIterationContext ic) {
        switch(matrixTypeInfo.input().fileFormat()) {
            case SVM_FORMAT: {
                return new SVMRecordIterator(matrixTypeInfo, ic);
            }
            default:
                throw new UnsupportedOperationException("unknown file format");
        }
    }
}
