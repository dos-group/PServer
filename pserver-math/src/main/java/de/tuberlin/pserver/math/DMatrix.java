package de.tuberlin.pserver.math;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicDoubleArray;
import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import de.tuberlin.pserver.math.delegates.MathLibFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class DMatrix implements Matrix, Serializable {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public enum MemoryLayout {

        ROW_LAYOUT,

        COLUMN_LAYOUT
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private static final class RowIterator implements Matrix.RowIterator {

        private DMatrix self;

        private final long end;

        private int globalRowIndex;

        private final int startRow;

        // ---------------------------------------------------

        public RowIterator(final DMatrix v) { this(v, 0, (int)Preconditions.checkNotNull(v).numRows() - 1); }
        public RowIterator(final DMatrix v, final int startRow, final int endRow) {
            this.self = v;
            Preconditions.checkArgument(startRow >= 0 && startRow < self.numRows());
            Preconditions.checkArgument(endRow > startRow && endRow < self.numRows());
            this.startRow = startRow * (int)self.cols;
            this.end = endRow * self.cols;
            this.globalRowIndex = this.startRow - (int)-self.cols;
            reset();
        }

        // ---------------------------------------------------

        @Override
        public boolean hasNextRow() { return globalRowIndex < end /*|| globalRowIndex < self.rows * self.cols*/; }

        @Override
        public void nextRow() { globalRowIndex += self.cols; }

        @Override
        public double getValueOfColumn(final int col) { return self.data[globalRowIndex + col]; }

        @Override
        public void reset() { globalRowIndex = startRow - (int)self.cols; }

        @Override
        public long numRows() { return self.rows; }

        @Override
        public long numCols() { return self.cols; }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(DMatrix.class);


    private Object owner;

    // ---------------------------------------------------

    private static final LibraryMatrixOps<Matrix, Vector> matrixOpDelegate =
            MathLibFactory.delegateDMatrixOpsTo(MathLibFactory.DMathLibrary.EJML_LIBRARY);

    private final long rows;

    private final long cols;

    private double[] data;

    private final MemoryLayout layout;

    private AtomicDoubleArray atomicWrapper;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DMatrix(final DMatrix mtx) { this(mtx.rows, mtx.cols, mtx.data, mtx.layout); }
    public DMatrix(final long rows, final long cols) { this(rows, cols, null, MemoryLayout.ROW_LAYOUT); }
    public DMatrix(final long rows, final long cols, final double[] data) { this(rows, cols, data, MemoryLayout.ROW_LAYOUT); }
    public DMatrix(final long rows, final long cols, final double[] data, final MemoryLayout layout) {
        this.rows = rows;
        this.cols = cols;
        this.layout = Preconditions.checkNotNull(layout);
        this.data = (data == null) ? new double[(int)(rows * cols)] : Preconditions.checkNotNull(data);
        //Preconditions.checkState(this.data.length == rows * cols);
        atomicWrapper = new AtomicDoubleArray(data);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void setOwner(final Object owner) { this.owner = owner; }

    @Override
    public Object getOwner() { return owner; }

    // ---------------------------------------------------

    @Override
    public long numRows() { return rows; }

    @Override
    public long numCols() { return cols; }

    @Override
    public double get(final long row, final long col) { return data[getPos(row, col)]; }

    @Override
    public void set(long row, long col, double value) { data[getPos(row, col)] = value; }

    @Override
    public double atomicGet(long row, long col) { return atomicWrapper.get(getPos(row, col)); }

    @Override
    public void atomicSet(long row, long col, double value) { atomicWrapper.set(getPos(row, col), value); }

    @Override
    public double[] toArray() {
        return data;
    }

    @Override
    public void setArray(final double[] data) {
        Preconditions.checkState(data.length == rows * cols);
        this.data = data;
    }

    @Override
    public RowIterator rowIterator() { return new RowIterator(this); }

    @Override
    public RowIterator rowIterator(final int startRow, final int endRow) { return new RowIterator(this, startRow, endRow); }

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
    // Matrix Operation Delegates.
    // ---------------------------------------------------

    @Override public Matrix add(final Matrix B) { return matrixOpDelegate.add(B, this); }

    @Override public Matrix sub(final Matrix B) { return matrixOpDelegate.sub(B, this); }

    @Override public Matrix mul(final Matrix B) { return matrixOpDelegate.mul(this, B); }

    @Override public Vector mul(final Vector v) { return matrixOpDelegate.mul(this, v); }

    @Override public void mul(final Vector x, final Vector y) { matrixOpDelegate.mul(this, x, y); }

    @Override public Matrix scale(final double alpha) { return matrixOpDelegate.scale(alpha, this); }

    @Override public Matrix transpose() { return matrixOpDelegate.transpose(this); }

    @Override public void transpose(final Matrix B) { matrixOpDelegate.transpose(this, B); }

    @Override public boolean invert() { return matrixOpDelegate.invert(this); }

    // ---------------------------------------------------

    @Override
    public Matrix assign(final Matrix v) {
        Preconditions.checkState(v.numRows() * v.numCols() == rows * cols);
        System.arraycopy(v.toArray(), 0, data, 0, (int)(rows * cols));
        return this;
    }

    @Override
    public Matrix assign(final double v) {
        for (int i = 0; i < rows * cols; ++i)
            data[i] = v;
        return this;
    }

    @Override
    public Vector viewRow(final long row) {
        Vector r = new DVector(numCols());
        for (int i = 0; i < numCols(); ++i)
            r.set(i, data[getPos(row, i)]);
        return r;
    }

    @Override
    public Vector viewColumn(final long col) {
        Vector r = new DVector(numRows());
        for (int i = 0; i < numRows(); ++i)
            r.set(i, data[getPos(i, col)]);
        return r;
    }

    @Override
    public Matrix assignRow(final long row, final Vector v) {
        Preconditions.checkNotNull(numCols() == v.size());
        for (int i = 0; i < v.size(); ++i)
            data[getPos(row, i)] = v.get(i);
        return this;
    }

    @Override
    public Matrix assignColumn(final long col, final Vector v) {
        Preconditions.checkNotNull(numRows() == v.size());
        for (int i = 0; i < v.size(); ++i)
            data[getPos(i, col)] = v.get(i);
        return this;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private int getPos(final long row, final long col) {
        switch (layout) {
            case ROW_LAYOUT: return (int)(row * cols + col);
            case COLUMN_LAYOUT: return (int)(col * rows + row);
        }
        throw new IllegalStateException();
    }
}
