package de.tuberlin.pserver.math;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.stuff.*;

import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

public abstract class AbstractMatrix implements Matrix {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected Object owner;

    protected final long rows;

    protected final long cols;

    protected Lock lock;

    protected final Layout layout;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractMatrix(long rows, long cols, Layout layout) {
        this.rows = rows;
        this.cols = cols;
        this.layout = Preconditions.checkNotNull(layout);
        Preconditions.checkArgument(java.util.Arrays.asList(Layout.values()).contains(layout), "Unknown MemoryLayout: " + layout.toString());
        this.lock = new ReentrantLock();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override  public long sizeOf() { return rows * cols * Double.BYTES; }

    @Override  public void setOwner(Object owner) { this.owner = owner; }

    @Override  public Object getOwner() { return owner; }

    @Override  public long numRows() { return rows; }

    @Override  public long numCols() { return cols; }

    @Override  public Layout getLayout() { return layout; }

    @Override  public abstract RowIterator rowIterator();

    @Override  public abstract RowIterator rowIterator(int startRow, int endRow);

    @Override
    public double aggregate(DoubleBinaryOperator combiner, DoubleUnaryOperator mapper) {
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
            r.set(row, f.apply(rowAsVector(row)));
        }
        return r;
    }

    @Override
    public void lock() { lock.lock(); }

    @Override
    public void unlock() { lock.unlock(); }

    // ---------------------------------------------------
    // Operations. Default Implementations.
    // ---------------------------------------------------


    @Override
    public Matrix add(Matrix B) {
        return add(B, this);
    }

    @Override
    public Matrix add(Matrix B, Matrix C) {
        // TODO: range check
        return this.applyOnElements((x, y) -> x + y, B, C);
    }

    @Override
    public Matrix sub(Matrix B) {
        return sub(B, this);
    }

    @Override
    public Matrix sub(Matrix B, Matrix C) {
        // TODO: range check
        return this.applyOnElements((x, y) -> x - y, B, C);
    }

    @Override
    public Matrix mul(Matrix B) {
        return this.applyOnElements((x, y) -> x * y, B);
    }

    @Override
    public Matrix mul(Matrix B, Matrix C) {
        return null;
    }

    @Override
    public Vector mul(Vector b, Vector c) {
        return null;
    }

    @Override
    public Matrix scale(double a) {
        return null;
    }

    @Override
    public Matrix scale(double a, Matrix B) {
        return null;
    }

    @Override
    public Matrix transpose() {
        return null;
    }

    @Override
    public Matrix transpose(Matrix B) {
        return null;
    }

    @Override
    public Matrix applyOnElements(final DoubleUnaryOperator f) {
        for (int i = 0; i < numRows(); ++i) {
            for (int j = 0; j < numCols(); ++j) {
                this.set(i, j, f.applyAsDouble(this.get(i, j)));
            }
        }
        return this;
    }

    @Override
    public Matrix applyOnElements(final DoubleUnaryOperator f, final Matrix target) {
        // TODO: shape check
        for (int i = 0; i < target.numRows(); ++i) {
            for (int j = 0; j < target.numCols(); ++j) {
                target.set(i, j, f.applyAsDouble(this.get(i, j)));
            }
        }
        return target;
    }

    @Override
    public Matrix applyOnElements(final DoubleBinaryOperator f, final Matrix B) {
        // TODO: shape check
        for (int i = 0; i < numRows(); ++i) {
            for (int j = 0; j < numCols(); ++j) {
                this.set(i, j, f.applyAsDouble(this.get(i, j), B.get(i, j)));
            }
        }
        return this;
    }

    @Override
    public Matrix applyOnElements(DoubleBinaryOperator f, Matrix B, Matrix C) {
        // TODO: shape check
        for (int i = 0; i < numRows(); ++i) {
            for (int j = 0; j < numCols(); ++j) {
                C.set(i, j, f.applyAsDouble(this.get(i, j), B.get(i, j)));
            }
        }
        return C;
    }

    @Override
    public Matrix addVectorToRows(final Vector v) {
        final Matrix res = copy();
        Preconditions.checkArgument(v.length() == numCols());
        for (int i = 0; i < res.numRows(); ++i) {
            for (int j = 0; j < res.numCols(); ++j) {
                res.set(i, j, this.get(i, j) + v.get(j));
            }
        }
        return res;
    }

    @Override
    public Matrix addVectorToCols(final Vector v) {
        final Matrix res = copy();
        Preconditions.checkArgument(v.length() == numRows());
        for (int j = 0; j < res.numCols(); ++j) {
            for (int i = 0; i < res.numRows(); ++i) {
                res.set(i, j, this.get(i, j) + v.get(i));
            }
        }
        return res;
    }

    @Override
    public Matrix setDiagonalsToZero() {
        long diag = 0;
        while(diag < rows && diag < cols) {
            this.set(diag, diag, 0.);
            diag++;
        }
        return this;
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static abstract class AbstractRowIterator implements Matrix.RowIterator {

        protected AbstractMatrix target;

        protected int currentRow;

        protected final int startRow;

        protected final int endRow;

        protected int numFetched;

        protected Random rand = new Random();

        public AbstractRowIterator(final AbstractMatrix mat) { this(mat, 0, Utils.toInt(Preconditions.checkNotNull(mat).numRows()) - 1); }
        public AbstractRowIterator(final AbstractMatrix mat, final int startRow, final int endRow) {
            this.target = mat;
            Preconditions.checkArgument(startRow >= 0 && startRow < target.numRows());
            Preconditions.checkArgument(endRow > startRow && endRow < target.numRows());
            this.startRow = startRow;
            this.endRow = endRow;
            reset();
        }

        @Override
        public boolean hasNextRow() {
            return numFetched < (endRow - startRow + 1);
        }

        @Override
        public void nextRow() {
            numFetched++;
            currentRow++;
        }

        @Override
        public void nextRandomRow() {
            numFetched++;
            currentRow = startRow + rand.nextInt(endRow + 1);
        }

        @Override
        public double getValueOfColumn(final int col) { return target.get(currentRow, col); }

        protected Vector getAsVector(int from, int size, Vector result) {
            Preconditions.checkArgument(from + size <= target.numCols());
            Preconditions.checkArgument(result.length() == size);
            for(int i = from; i - from < size; i++) {
                result.set(i, target.get(currentRow, i));
            }
            return result;
        }

        @Override
        public abstract Vector getAsVector();

        @Override
        public abstract Vector getAsVector(int from, int size);

        @Override
        public void reset() {
            currentRow = startRow - 1;
            numFetched = 0;
        }

        @Override
        public long numRows() { return target.rows; }

        @Override
        public long numCols() { return target.cols; }

    }
}
