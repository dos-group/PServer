package de.tuberlin.pserver.math;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicDoubleArray;
import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import de.tuberlin.pserver.math.delegates.MathLibFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DMatrix extends AbstractMatrix implements Matrix, Serializable {

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
        public Vector getAsVector() { return getAsVector(0, (int)self.cols); }

        @Override
        public Vector getAsVector(int from, int size) {
            final double v[] = new double[size];
            System.arraycopy(self.data, globalRowIndex + from, v, 0, size);
            return new DVector(size, v);
        }

        @Override
        public void reset() { globalRowIndex = startRow - (int)self.cols; }

        @Override
        public long numRows() { return self.rows; }

        @Override
        public long numCols() { return self.cols; }
    }

    // ---------------------------------------------------

    private static final class RandomRowIterator implements Matrix.RowIterator {

        private DMatrix self;

        private final int start;

        private final int end;

        private final int numRows;

        private int globalRowIndex;


        private int randRowIndex;

        private Random rand;

        // ---------------------------------------------------

        public RandomRowIterator(final DMatrix v) { this(v, 0, (int)Preconditions.checkNotNull(v).numRows() - 1); }
        public RandomRowIterator(final DMatrix v, final int startRow, final int endRow) {
            this.self = v;
            Preconditions.checkArgument(startRow >= 0 && startRow < self.numRows());
            Preconditions.checkArgument(endRow > startRow && endRow < self.numRows());
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
        public void nextRow() {
            globalRowIndex += self.cols;
            randRowIndex = start + (int)((rand.nextDouble() * numRows) * self.cols);
        }

        @Override
        public double getValueOfColumn(final int col) { return self.data[randRowIndex + col]; }

        @Override
        public Vector getAsVector() { return getAsVector(0, (int)self.cols); }

        @Override
        public Vector getAsVector(int from, int size) {
            final double v[] = new double[size];
            System.arraycopy(v, randRowIndex + from, v, 0, size);
            return new DVector(size, v);
        }

        @Override
        public void reset() { globalRowIndex = start - (int)self.cols; }

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

    private double[] data;

    private AtomicDoubleArray atomicWrapper;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DMatrix(final DMatrix mtx) { this(mtx.rows, mtx.cols, mtx.data, mtx.layout); }
    public DMatrix(final long rows, final long cols) { this(rows, cols, null, MemoryLayout.ROW_LAYOUT); }
    public DMatrix(final long rows, final long cols, final double[] data) { this(rows, cols, data, MemoryLayout.ROW_LAYOUT); }
    public DMatrix(final long rows, final long cols, final double[] data, final MemoryLayout layout) {
        super(rows, cols, layout);
        this.data = (data == null) ? new double[(int)(rows * cols)] : Preconditions.checkNotNull(data);
        //Preconditions.checkState(this.data.length == rows * cols);
        this.atomicWrapper = new AtomicDoubleArray(data);
    }

    public static DMatrix fromSMatrix(SMatrix mat, MemoryLayout targetLayout) {
        return new DMatrix(mat.rows, mat.cols, mat.toArray(targetLayout), targetLayout);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public double get(final long row, final long col) { return data[Utils.getPos(row, col, this)]; }

    @Override
    public void set(long row, long col, double value) { data[Utils.getPos(row, col, this)] = value; }

    @Override
    public double atomicGet(long row, long col) { return atomicWrapper.get(Utils.getPos(row, col, this)); }

    @Override
    public void atomicSet(long row, long col, double value) { atomicWrapper.set(Utils.getPos(row, col, this), value); }

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
    public Matrix.RowIterator randomRowIterator() { return new RandomRowIterator(this); }

    @Override
    public Matrix.RowIterator randomRowIterator(int startRow, int endRow) { return new RandomRowIterator(this, startRow, endRow); }

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

    @Override public Matrix axpy(final double alpha, final Matrix B) { return null; }

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
            r.set(i, data[Utils.getPos(row, i, this)]);
        return r;
    }

    @Override
    public Vector viewColumn(final long col) {
        Vector r = new DVector(numRows());
        for (int i = 0; i < numRows(); ++i)
            r.set(i, data[Utils.getPos(i, col, this)]);
        return r;
    }

    @Override
    public Matrix assignRow(final long row, final Vector v) {
        Preconditions.checkNotNull(numCols() == v.size());
        for (int i = 0; i < v.size(); ++i)
            data[Utils.getPos(row, i, this)] = v.get(i);
        return this;
    }

    @Override
    public Matrix assignColumn(final long col, final Vector v) {
        Preconditions.checkNotNull(numRows() == v.size());
        for (int i = 0; i < v.size(); ++i)
            data[Utils.getPos(i, col, this)] = v.get(i);
        return this;
    }

}
