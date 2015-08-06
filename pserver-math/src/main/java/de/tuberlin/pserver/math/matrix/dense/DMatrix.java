package de.tuberlin.pserver.math.matrix.dense;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import de.tuberlin.pserver.math.delegates.MathLibFactory;
import de.tuberlin.pserver.math.matrix.AbstractMatrix;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.utils.*;
import de.tuberlin.pserver.math.vector.dense.DVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DMatrix extends AbstractMatrix implements Serializable {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private static final class RowIterator implements Matrix.RowIterator {

        private DMatrix self;

        private int globalRowIndex;

        private final int end;

        private final int start;

        private final int numRows;

        private int currentRowIndex;

        private Random rand;

        // ---------------------------------------------------

        public RowIterator(final DMatrix v) { this(v, 0, (int)Preconditions.checkNotNull(v).numRows() - 1); }
        public RowIterator(final DMatrix v, final int startRow, final int endRow) {
            this.self = v;
            Preconditions.checkArgument(startRow >= 0 && startRow < self.numRows());
//            Preconditions.checkArgument(endRow > startRow && endRow < self.numRows());
            this.start = startRow * (int)self.cols;
            this.end = endRow * (int)self.cols;
            this.globalRowIndex = this.start - (int)-self.cols;
            this.numRows = endRow - startRow;
            this.rand = new Random();
            reset();
        }

        // ---------------------------------------------------

        @Override
        public boolean hasNextRow() { return globalRowIndex < end /*|| globalRowIndex < self.rows * self.cols*/; }

        @Override
        public void nextRow() { globalRowIndex += self.cols; currentRowIndex = globalRowIndex; }

        @Override
        public void nextRandomRow() {
            globalRowIndex += self.cols;
            currentRowIndex = start +  (rand.nextInt(numRows) * (int)self.cols);
        }

        @Override
        public double getValueOfColumn(final int col) { return self.data[currentRowIndex + col]; }

        @Override
        public de.tuberlin.pserver.math.vector.Vector getAsVector() { return getAsVector(0, (int)self.cols); }

        @Override
        public de.tuberlin.pserver.math.vector.Vector getAsVector(int from, int size) {
            final double v[] = new double[size];
            System.arraycopy(self.data, currentRowIndex + from, v, 0, size);
            return new DVector(size, v);
        }

        @Override
        public void reset() { globalRowIndex = start - (int)self.cols; }

        @Override
        public long numRows() { return self.rows; }

        @Override
        public long numCols() { return self.cols; }

        @Override
        public int getCurrentRowNum() { return currentRowIndex; }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(DMatrix.class);

    private Object owner;

    // ---------------------------------------------------

    private static final LibraryMatrixOps<Matrix, de.tuberlin.pserver.math.vector.Vector> matrixOpDelegate =
            MathLibFactory.delegateDMatrixOpsTo(MathLibFactory.DMathLibrary.EJML_LIBRARY);

    private double[] data;

    private Lock lock;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    // Copy Constructor.
    public DMatrix(final de.tuberlin.pserver.math.vector.Vector m) {
        super(m.layout() == de.tuberlin.pserver.math.vector.Vector.Layout.COLUMN_LAYOUT ?
                1 : m.length(),
              m.layout() == de.tuberlin.pserver.math.vector.Vector.Layout.COLUMN_LAYOUT ?
                m.length() : 1,
              m.layout() == de.tuberlin.pserver.math.vector.Vector.Layout.COLUMN_LAYOUT ?
                Matrix.Layout.COLUMN_LAYOUT : Matrix.Layout.ROW_LAYOUT);

        final double[] md = m.toArray();
        this.data = new double[md.length];
        System.arraycopy(md, 0, this.data, 0, md.length);
    }

    // Copy Constructor.
    public DMatrix(final DMatrix m) {
        super(m.rows, m.cols, m.layout);
        this.data = new double[m.data.length];
        System.arraycopy(m.data, 0, this.data, 0, m.data.length);
    }

    public DMatrix(final long rows, final long cols) { this(rows, cols, null, Matrix.Layout.ROW_LAYOUT); }
    public DMatrix(final long rows, final long cols, final double[] data) { this(rows, cols, data, Matrix.Layout.ROW_LAYOUT); }
    public DMatrix(final long rows, final long cols, final double[] data, final Layout layout) {
        super(rows, cols, layout);
        this.data = (data == null) ? new double[(int)(rows * cols)] : Preconditions.checkNotNull(data);
        //Preconditions.checkState(this.data.length == rows * cols);
        this.lock = new ReentrantLock();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public double get(final long row, final long col) { return data[Utils.getPos(row, col, this)]; }

    @Override
    public void set(long row, long col, double value) { data[Utils.getPos(row, col, this)] = value; }

    @Override
    public double atomicGet(long row, long col) { throw new UnsupportedOperationException(); }

    @Override
    public void atomicSet(long row, long col, double value) { throw new UnsupportedOperationException(); }

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
    public double aggregate(DoubleFunction2Arg combiner, DoubleFunction1Arg mapper) {
        return aggregateRows(new VectorFunction() {
            @Override
            public double apply(de.tuberlin.pserver.math.vector.Vector v) {
                return v.aggregate(combiner, mapper);
            }
        }).aggregate(combiner, Functions.IDENTITY);
    }

    @Override
    public de.tuberlin.pserver.math.vector.Vector aggregateRows(final VectorFunction f) {
        de.tuberlin.pserver.math.vector.Vector r = new DVector(numRows());
        long n = numRows();
        for (int row = 0; row < n; row++) {
            r.set(row, f.apply(rowAsVector(row)));
        }
        return r;
    }

    // ---------------------------------------------------
    // Matrix Operation Delegates.
    // ---------------------------------------------------

    @Override public Matrix axpy(final double alpha, final Matrix B) { return null; }

    @Override public Matrix add(final Matrix B) { return matrixOpDelegate.add(B, this); }

    @Override public Matrix sub(final Matrix B) { return matrixOpDelegate.sub(B, this); }

    @Override public Matrix mul(final Matrix B) { return matrixOpDelegate.mul(this, B); }

    @Override public de.tuberlin.pserver.math.vector.Vector mul(final de.tuberlin.pserver.math.vector.Vector v) { return matrixOpDelegate.mul(this, v); }

    @Override public void mul(final de.tuberlin.pserver.math.vector.Vector x, final de.tuberlin.pserver.math.vector.Vector y) { matrixOpDelegate.mul(this, x, y); }

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
    public de.tuberlin.pserver.math.vector.Vector rowAsVector() {
        return rowAsVector(0, 0, numCols());
    }

    @Override
    public de.tuberlin.pserver.math.vector.Vector rowAsVector(final long row) {
        return rowAsVector(row, 0, numCols());
    }

    @Override
    public de.tuberlin.pserver.math.vector.Vector rowAsVector(final long row, final long from, final long to) { // TODO: Optimize with respect to the layout with array copy.
        de.tuberlin.pserver.math.vector.Vector r = new DVector(to - from);
        for (long i = from; i < to; ++i)
            r.set(i, data[getPos(row, i)]);
        return r;
    }

    @Override
    public de.tuberlin.pserver.math.vector.Vector colAsVector() {
        return colAsVector(0, 0, rows);
    }

    @Override
    public de.tuberlin.pserver.math.vector.Vector colAsVector(final long col) {
        return colAsVector(col, 0, rows);
    }

    @Override
    public de.tuberlin.pserver.math.vector.Vector colAsVector(final long col, final long from, final long to) {
        double[] result = new double[(int)(to - from)];
        if(layout == Layout.COLUMN_LAYOUT) {
            System.arraycopy(data, (int)(col * rows + from), result, 0, result.length);
        }
        else {
            for (int i = 0; i < result.length; i++) {
                int row = (int)from+i;
                result[i] = data[(int)(row * cols + col)];
            }
        }
        return new DVector(result.length, result);
    }

    @Override
    public Matrix assignRow(final long row, final de.tuberlin.pserver.math.vector.Vector v) {
        Preconditions.checkNotNull(numCols() == v.length());
        for (int i = 0; i < v.length(); ++i)
            data[Utils.getPos(row, i, this)] = v.get(i);
        return this;
    }

    @Override
    public Matrix assignColumn(final long col, final de.tuberlin.pserver.math.vector.Vector v) {
        Preconditions.checkArgument(numRows() == v.length());
        System.arraycopy(v.toArray(), 0, data, Utils.getPos(0, col, this), (int)v.length());
        return this;
    }

    @Override
    public Matrix copy() {
        return new DMatrix(this);
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
