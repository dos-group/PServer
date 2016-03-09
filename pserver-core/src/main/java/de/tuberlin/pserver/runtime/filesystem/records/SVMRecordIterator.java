package de.tuberlin.pserver.runtime.filesystem.records;

import de.tuberlin.pserver.runtime.filesystem.AbstractFileIterationContext;
import de.tuberlin.pserver.runtime.filesystem.distributed.DistributedFileIterationContext;
import de.tuberlin.pserver.types.matrix.typeinfo.MatrixTypeInfo;

public class SVMRecordIterator implements RecordIterator {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final MatrixTypeInfo matrixTypeInfo;

    private final DistributedFileIterationContext iterationContext;

    private final SVMRecordParser recordParser;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SVMRecordIterator(MatrixTypeInfo matrixTypeInfo, AbstractFileIterationContext ic) {
        this.matrixTypeInfo     = matrixTypeInfo;
        this.iterationContext   = (DistributedFileIterationContext)ic;
        this.recordParser       = new SVMRecordParser(ic);
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
