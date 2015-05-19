package de.tuberlin.pserver.experimental.old;

// TODO:
public class DMatrixView implements DMatrix {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int rowOffset;

    private final int colOffset;

    private final int viewRows;

    private final int viewCols;

    private final DMatrix delegate;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DMatrixView(final int rowOffset,
                       final int colOffset,
                       final int viewRows,
                       final int viewCols,
                       final DMatrix delegate) {

        this.rowOffset = rowOffset;
        this.colOffset = colOffset;
        this.viewRows  = viewRows;
        this.viewCols  = viewCols;
        this.delegate  = delegate;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public long numRows() {
        return 0;
    }

    @Override
    public long numCols() {
        return 0;
    }

    @Override
    public double get(final long row, final long col) {
        return 0;
    }

    @Override
    public void set(final long row, final long col, final double value) {}

    @Override
    public double[] toArray() {
        return new double[0];
    }

    @Override
    public void setArray(final double[] data) {}

    @Override
    public RowIterator rowIterator() {
        return null;
    }

    @Override
    public RowIterator rowIterator(final int startRow, final int endRow) {
        return null;
    }

    // ---------------------------------------------------
    // Matrix Operation Delegates.
    // ---------------------------------------------------

    @Override
    public DMatrix add(final DMatrix B) {
        return null;
    }

    @Override
    public DMatrix sub(final DMatrix B) {
        return null;
    }

    @Override
    public DVector mul(final DVector x, final DVector y) {
        return null;
    }

    @Override
    public DMatrix scale(final double alpha) {
        return null;
    }

    @Override
    public DMatrix transpose() {
        return null;
    }

    @Override
    public DMatrix transpose(final DMatrix B) {
        return null;
    }

    @Override
    public boolean invert() {
        return false;
    }
}
