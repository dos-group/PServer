package de.tuberlin.pserver.math.matrix;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.utils.*;
import de.tuberlin.pserver.math.vector.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.tuberlin.pserver.math.vector.dense.DVector;

import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

public abstract class AbstractMatrix implements Matrix {

    private final Logger LOG = LoggerFactory.getLogger(AbstractMatrix.class);

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

    @Override  public long sizeOf() {
        // TODO: depends on specialization
        return rows * cols * Double.BYTES;
    }

    @Override  public void setOwner(Object owner) { this.owner = owner; }

    @Override  public Object getOwner() { return owner; }

    @Override  public long numRows() { return rows; }

    @Override  public long numCols() { return cols; }

    @Override  public Layout getLayout() { return layout; }

    @Override  public abstract RowIterator rowIterator();

    @Override  public abstract RowIterator rowIterator(int startRow, int endRow);

    @Override
    public double aggregate(DoubleBinaryOperator combiner, DoubleUnaryOperator mapper) {
        //return aggregateRows(v -> v.aggregate(combiner, mapper)).aggregate(combiner, Functions.IDENTITY);
        // for merge:
        return aggregateRows(v -> v.aggregate(combiner, mapper)).aggregate(combiner, x -> x);
    }

    @Override
    public Vector aggregateRows(final VectorFunction f) {
        // TODO: we do not create new Vectors/Matrices in our API.
        // TODO: Especially not of DVector inside of AbstractMatrix.
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

    protected abstract Matrix newInstance(long rows, long cols);

    @Override
    public Matrix add(Matrix B) {
        return add(B, newInstance(this.rows, this.cols));
    }

    @Override
    public Matrix add(Matrix B, Matrix C) {
        Utils.checkShapeEqual(this, B, C);
        return this.applyOnElements(B, (x, y) -> x + y, C);
    }

    @Override
    public Matrix sub(Matrix B) {
        return sub(B, newInstance(this.rows, this.cols));
    }

    @Override
    public Matrix sub(Matrix B, Matrix C) {
        Utils.checkShapeEqual(this, B, C);
        return this.applyOnElements(B, (x, y) -> x - y, C);
    }

    @Override
    public Matrix mul(Matrix B) {
        return mul(B, newInstance(this.rows, B.numCols()));
    }

    @Override
    public Matrix mul(Matrix B, Matrix C) {
        Utils.checkShapeMatrixMatrixMult(this, B, C);
        for (int row = 0; row < C.numRows(); row++) {
            for (int col = 0; col < C.numCols(); col++) {
                C.set(row, col, this.rowAsVector(row).dot(B.colAsVector(col)));
            }
        }
        return C;
    }

    @Override
    public Vector mul(Vector b, Vector c) {
        Utils.checkShapeMatrixVectorMult(this, b, c);
        for (int i = 0; i < c.length(); i++) {
            c.set(i, this.rowAsVector(i).dot(b));
        }
        return c;
    }

    @Override
    public Matrix scale(double a) {
        return scale(a, newInstance(rows, cols));
    }

    @Override
    public Matrix scale(double a, Matrix B) {
        Utils.checkShapeEqual(this, B);
        return applyOnElements(x -> a * x, B);
    }

    @Override
    public Matrix transpose() {
        return transpose(newInstance(cols, rows));
    }

    @Override
    public Matrix transpose(Matrix B) {
        Utils.checkShapeTranspose(this, B);
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                B.set(col, row, this.get(row, col));
            }
        }
        return B;
    }

    @Override
    public Matrix invert() {
        return invert(newInstance(cols, rows));
    }

    @Override
    public Matrix invert(Matrix B) {
        LOG.warn("invert() has not been overridden by effective subclass. This implementation does nothing.");
        return B;
    }

    @Override
    public Matrix applyOnElements(final DoubleUnaryOperator f) {
        return applyOnElements(f, newInstance(rows, cols));
    }

    @Override
    public Matrix applyOnElements(final DoubleUnaryOperator f, final Matrix B) {
        Utils.checkShapeEqual(this, B);
        for (int i = 0; i < B.numRows(); ++i) {
            for (int j = 0; j < B.numCols(); ++j) {
                B.set(i, j, f.applyAsDouble(this.get(i, j)));
            }
        }
        return B;
    }

    @Override
    public Matrix applyOnElements(final Matrix B, final DoubleBinaryOperator f) {
        return applyOnElements(B, f, newInstance(rows, cols));
    }

    @Override
    public Matrix applyOnElements(Matrix B, DoubleBinaryOperator f, Matrix C) {
        Utils.checkShapeEqual(this, B, C);
        for (int i = 0; i < numRows(); ++i) {
            for (int j = 0; j < numCols(); ++j) {
                C.set(i, j, f.applyAsDouble(this.get(i, j), B.get(i, j)));
            }
        }
        return C;
    }

    @Override
    public Matrix applyOnElements(final MatrixElementUnaryOperator f) {
        return applyOnElements(f, newInstance(rows, cols));
    }

    @Override
    public Matrix applyOnElements(final MatrixElementUnaryOperator f, final Matrix B) {
        Utils.checkShapeEqual(this, B);
        for (int i = 0; i < B.numRows(); ++i) {
            for (int j = 0; j < B.numCols(); ++j) {
                B.set(i, j, f.apply(i, j, this.get(i, j)));
            }
        }
        return B;
    }

    @Override
    public Matrix addVectorToRows(final Vector v) {
        return addVectorToRows(v, newInstance(rows, cols));
    }

    @Override
    public Matrix addVectorToRows(Vector v, Matrix B) {
        Utils.checkApplyVectorToRows(this, v, B);
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                B.set(row, col, this.get(row, col) + v.get(col));
            }
        }
        return B;
    }

    @Override
    public Matrix addVectorToCols(final Vector v) {
        return addVectorToCols(v, newInstance(rows, cols));
    }

    @Override
    public Matrix addVectorToCols(Vector v, Matrix B) {
        Utils.checkApplyVectorToCols(this, v, B);
        for (int col = 0; col < cols; col++) {
            for (int row = 0; row < rows; row++) {
                B.set(row, col, this.get(row, col) + v.get(row));
            }
        }
        return B;
    }

    @Override
    public Matrix setDiagonalsToZero() {
        return setDiagonalsToZero(this.copy());
    }

    @Override
    public Matrix setDiagonalsToZero(Matrix B) {
        long diag = 0;
        while(diag < rows && diag < cols) {
            B.set(diag, diag, 0.);
            diag++;
        }
        return B;
    }

    @Override
    public Matrix applyOnNonZeroElements(MatrixElementUnaryOperator f) {
        return applyOnNonZeroElements(f, newInstance(rows, cols));
    }

    @Override
    public Matrix applyOnNonZeroElements(MatrixElementUnaryOperator f, Matrix B) {
        Utils.checkShapeEqual(this, B);
        for (int row = 0; row < numRows(); ++row) {
            for (int col = 0; col < numCols(); ++col) {
                double oldVal = get(row, col);
                if(oldVal != 0.0) {
                    double newVal = f.apply(row, col, oldVal);
                    if (newVal != oldVal) {
                        B.set(row, col, newVal);
                    }
                }
            }
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

        @Override
        public int getCurrentRowNum() { return currentRow; }
    }
}
