package de.tuberlin.pserver.math;

import com.google.common.base.Preconditions;

/**
 * Created by fsander on 05.06.15.
 */
public abstract class AbstractMatrix implements Matrix {

    protected Object owner;

    protected final long rows;

    protected final long cols;

    public AbstractMatrix(long rows, long cols) {
        this.rows = rows;
        this.cols = cols;
    }

    @Override  public void setOwner(Object owner) { this.owner = owner; }

    @Override  public Object getOwner() { return owner; }

    @Override  public long numRows() { return rows; }

    @Override  public long numCols() { return cols; }

    @Override  public RowIterator rowIterator() { return new RowIterator(this); }

    @Override  public RowIterator rowIterator(int startRow, int endRow) { return new RowIterator(this, startRow, endRow); }

    @Override
    public double aggregate(DoubleDoubleFunction combiner, DoubleFunction mapper) {
        return aggregateRows(new VectorFunction() {
            @Override
            public double apply(Vector v) {
                return v.aggregate(combiner, mapper);
            }
        }).aggregate(combiner, Functions.IDENTITY);
    }

    @Override
    public Vector aggregateRows(final VectorFunction f) {
        Vector r = new DVector(numRows());
        long n = numRows();
        for (int row = 0; row < n; row++) {
            r.set(row, f.apply(viewRow(row)));
        }
        return r;
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static final class RowIterator implements Matrix.RowIterator {

        private AbstractMatrix self;

        private int currentRow;

        private final int startRow;

        private final long endRow;

        // ---------------------------------------------------

        public RowIterator(final AbstractMatrix v) { this(v, 0, Utils.toInt(Preconditions.checkNotNull(v).numRows()) - 1); }
        public RowIterator(final AbstractMatrix v, final int startRow, final int endRow) {
            this.self = v;
            Preconditions.checkArgument(startRow >= 0 && startRow < self.numRows());
            Preconditions.checkArgument(endRow > startRow && endRow < self.numRows());
            this.startRow = startRow * Utils.toInt(self.cols);
            this.endRow = endRow * self.cols;
            this.currentRow = this.startRow;
            reset();
        }

        // ---------------------------------------------------

        @Override
        public boolean hasNextRow() { return currentRow < endRow; }

        @Override
        public void nextRow() { currentRow++; }

        @Override
        public double getValueOfColumn(final int col) { return self.get(currentRow, col); }

        @Override
        public void reset() { currentRow = startRow; }

        @Override
        public long numRows() { return self.rows; }

        @Override
        public long numCols() { return self.cols; }
    }



}
