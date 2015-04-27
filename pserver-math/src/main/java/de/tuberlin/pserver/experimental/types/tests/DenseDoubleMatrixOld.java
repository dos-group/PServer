package de.tuberlin.pserver.experimental.types.tests;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.UnsafeOp;
import de.tuberlin.pserver.experimental.memory.Types;

public class DenseDoubleMatrixOld extends DenseMatrixOld {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int size = Types.DOUBLE_TYPE_INFO.size();

    private final byte[] pdata;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DenseDoubleMatrixOld(final int rows, final int cols, final BlockLayout layout) {
        super(rows, cols, Types.DOUBLE_TYPE_INFO, Preconditions.checkNotNull(layout));
        pdata = buffer.getRawData();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public final double get(final long row, final long col) {
        //final long offset = getOffset(row, col);
        return UnsafeOp.unsafe.getDouble(pdata, (row * cols + col) * size + UnsafeOp.BYTE_ARRAY_OFFSET);
    }

    public void set(final long row, final long col, final double value) {
        //final long offset = getOffset(row, col);
        UnsafeOp.unsafe.putDouble(pdata, (row * cols + col) * size + UnsafeOp.BYTE_ARRAY_OFFSET, value);
    }
}
