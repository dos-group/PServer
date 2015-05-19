package de.tuberlin.pserver.math;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import de.tuberlin.pserver.math.delegates.MathLibFactory;

import org.apache.commons.lang3.NotImplementedException;

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
    // Fields.
    // ---------------------------------------------------

    private static final LibraryMatrixOps<Matrix, Vector> matrixOpDelegate =
            MathLibFactory.delegateDMatrixOpsTo(MathLibFactory.DMathLibrary.EJML_LIBRARY);

    private final long rows;

    private final long cols;

    private double[] data;

    private final MemoryLayout layout;

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
        Preconditions.checkState(data.length == rows * cols);
    }

    // ---------------------------------------------------
    // Public Methods.
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
    public double[] toArray() {
        return data;
    }

    @Override
    public void setArray(final double[] data) {
        Preconditions.checkState(data.length == rows * cols);
        this.data = data;
    }

    @Override
    public RowIterator rowIterator() { throw new NotImplementedException(""); }

    @Override
    public RowIterator rowIterator(int startRow, int endRow) { throw new NotImplementedException(""); }

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

    @Override public Vector mul(final Vector x, final Vector y) { return matrixOpDelegate.mul(this, x, y); }

    @Override public Matrix scale(final double alpha) { return matrixOpDelegate.scale(alpha, this); }

    @Override public Matrix transpose() { return matrixOpDelegate.transpose(this); }

    @Override public Matrix transpose(final Matrix B) { return matrixOpDelegate.transpose(B, this); }

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
