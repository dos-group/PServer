package de.tuberlin.pserver.runtime.filesystem.records;

import de.tuberlin.pserver.runtime.filesystem.AbstractFileIterationContext;
import de.tuberlin.pserver.types.matrix.typeinfo.MatrixTypeInfo;

public class SVMRecordIterator implements RecordIterator {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final MatrixTypeInfo matrixTypeInfo;

    private final SVMRecordParser recordParser;

    private int currentRow;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SVMRecordIterator(MatrixTypeInfo matrixTypeInfo, AbstractFileIterationContext ic) {
        this.matrixTypeInfo = matrixTypeInfo;
        this.recordParser   = new SVMRecordParser(ic);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public boolean hasNext() { return matrixTypeInfo.rows() > currentRow; }

    @Override
    public Record next() {
        Record record = recordParser.parseNextRow(currentRow);
        ++currentRow;
        return record;
    }
}
