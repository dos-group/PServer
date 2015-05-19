package de.tuberlin.pserver.experimental.exp1.types.matrices;

public class DenseDoubleMatrix extends DenseMatrix {
/*
    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DenseDoubleMatrix(final int rows, final int cols, final BlockLayout layout) {
        super(rows, cols, Types.DOUBLE_TYPE_INFO, Preconditions.checkNotNull(layout), false);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public double get(final int row, final int col) {
        final long offset = getOffset(row, col);
        return UnsafeOp.unsafe.getDouble(buffer.getRawData(), offset);
    }

    public void set(final int row, final int col, final double value) {
        final long offset = getOffset(row, col);
        UnsafeOp.unsafe.putDouble(buffer.getRawData(), offset, value);
    }

    public double[] toDoubleArray() {
        return UnsafeOp.primitiveArrayTypeCast(getBuffer().getRawData(), byte[].class, double[].class);
    }*/
}
