package de.tuberlin.pserver.runtime.filesystem.records;

import de.tuberlin.pserver.runtime.filesystem.AbstractFileIterationContext;
import de.tuberlin.pserver.runtime.filesystem.distributed.DistributedFileIterationContext;
import de.tuberlin.pserver.types.matrix.typeinfo.MatrixTypeInfo;

public class SVMRecordIterator implements RecordIterator {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final MatrixTypeInfo matrixTypeInfo;

    private final AbstractFileIterationContext iterationContext;

    private final SVMRecordParser recordParser;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SVMRecordIterator(MatrixTypeInfo matrixTypeInfo, AbstractFileIterationContext iterationContext) {
        this.matrixTypeInfo     = matrixTypeInfo;
        this.iterationContext   = iterationContext;
        this.recordParser       = new SVMRecordParser(iterationContext);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public boolean hasNext() { return matrixTypeInfo.rows() > iterationContext.row; }

    @Override
    public Record next() {
        Record record = recordParser.parseNextRow(iterationContext.row);
        ++iterationContext.row;
        return record;
    }
}
