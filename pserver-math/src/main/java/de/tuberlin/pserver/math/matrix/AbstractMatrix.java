package de.tuberlin.pserver.math.matrix;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.utils.MatrixAggregation;
import de.tuberlin.pserver.math.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        this.lock = new ReentrantLock(true);
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

    @Override  public long rows() { return rows; }

    @Override  public long cols() { return cols; }

    @Override  public Layout layout() { return layout; }

    @Override  public abstract RowIterator rowIterator();

    @Override  public abstract RowIterator rowIterator(int startRow, int endRow);

    @Override
    public double aggregate(DoubleBinaryOperator combiner, DoubleUnaryOperator mapper, final Matrix result) {
        // return aggregateRows(v -> v.aggregate(combiner, mapper), this).aggregate(combiner, Functions.IDENTITY);
        return 0;
    }

    @Override
    public Matrix aggregateRows(final MatrixAggregation f) {
        return aggregateRows(f, newInstance(rows, 1));
    }

    @Override
    public Matrix aggregateRows(final MatrixAggregation f, Matrix result) {
        Preconditions.checkArgument(result.rows() == rows && result.cols() == 1);
        for (int row = 0; row < rows; row++) {
            result.set(row, 0, f.apply(getRow(row)));
        }
        return result;
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
        return mul(B, newInstance(this.rows, B.cols()));
    }

    @Override
    public Matrix mul(Matrix B, Matrix C) {
        Utils.checkShapeMatrixMatrixMult(this, B, C);
        for (int row = 0; row < C.rows(); row++) {
            for (int col = 0; col < C.cols(); col++) {
                C.set(row, col, this.getRow(row).dot(B.getCol(col)));
            }
        }
        return C;
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
        for (int i = 0; i < B.rows(); ++i) {
            for (int j = 0; j < B.cols(); ++j) {
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
        for (int i = 0; i < rows(); ++i) {
            for (int j = 0; j < cols(); ++j) {
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
        for (int i = 0; i < B.rows(); ++i) {
            for (int j = 0; j < B.cols(); ++j) {
                B.set(i, j, f.apply(i, j, this.get(i, j)));
            }
        }
        return B;
    }

    @Override
    public Matrix addVectorToRows(final Matrix v) {
        return addVectorToRows(v, newInstance(rows, cols));
    }

    @Override
    public Matrix addVectorToRows(Matrix v, Matrix B) {
        Utils.checkApplyVectorToRows(this, v, B);
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                B.set(row, col, this.get(row, col) + v.get(col));
            }
        }
        return B;
    }

    @Override
    public Matrix addVectorToCols(final Matrix v) {
        return addVectorToCols(v, newInstance(rows, cols));
    }

    @Override
    public Matrix addVectorToCols(Matrix v, Matrix B) {
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
        for (int row = 0; row < rows(); ++row) {
            for (int col = 0; col < cols(); ++col) {
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

    @Override
    public Matrix copy(long rows, long cols) {
        Matrix result = newInstance(rows, cols);
        for (int row = 0; row < Math.min(this.rows, rows); row++) {
            for (int col = 0; col < Math.min(this.cols, cols); col++) {
                result.set(row, col, this.get(row, col));
            }
        }
        return result;
    }

    @Override
    public double sum() {
        double sum = 0;
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                sum += this.get(row, col);
            }
        }
        return sum;
    }

    @Override
    public double norm(int p) {
        double norm = 0;
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                norm += Math.pow(this.get(row, col), p);
            }
        }
        return Math.pow(norm, 1./p);
    }

    @Override
    public double dot(Matrix B) {
        double result = 0;
        if(this.layout == Layout.ROW_LAYOUT) {
            Preconditions.checkArgument(rows == 1);
            Preconditions.checkArgument(B.layout() == Layout.ROW_LAYOUT);
            Preconditions.checkArgument(B.rows() == 1);
            Preconditions.checkArgument(cols == B.cols());
            for (int col = 0; col < cols; col++) {
                result += this.get(col) * B.get(col);
            }
        }
        else if(this.layout == Layout.COLUMN_LAYOUT) {
            Preconditions.checkArgument(cols == 1);
            Preconditions.checkArgument(B.layout() == Layout.COLUMN_LAYOUT);
            Preconditions.checkArgument(B.cols() == 1);
            Preconditions.checkArgument(rows == B.rows());
            for (int row = 0; row < rows; row++) {
                result += this.get(row) * B.get(row);
            }
        }
        else {
            throw new IllegalStateException("Unknown layout: " + layout.name());
        }
        return result;
    }

    @Override
    public Matrix concat(Matrix B) {
        if(this.layout == Layout.ROW_LAYOUT) {
            Preconditions.checkArgument(cols == B.cols());
            return concat(B, newInstance(rows + B.rows(), cols));
        }
        else if(this.layout == Layout.COLUMN_LAYOUT) {
            Preconditions.checkArgument(rows == B.rows());
            return concat(B, newInstance(rows, B.cols() + cols));
        }
        else {
            throw new IllegalStateException("Unknown layout: " + layout.name());
        }
    }

    @Override
    public Matrix concat(Matrix B, Matrix C) {
        if(this.layout == Layout.ROW_LAYOUT) {
            Preconditions.checkArgument(cols == B.cols());
            Preconditions.checkArgument(C.rows() == rows + B.rows() && C.cols() == cols);
            for (int row = 0; row < C.rows(); row++) {
                for (int col = 0; col < cols; col++) {
                    double val = row < rows ? this.get(row, col) : B.get(row, col);
                    C.set(row, col, val);
                }
            }
        }
        else if(this.layout == Layout.COLUMN_LAYOUT) {
            Preconditions.checkArgument(rows == B.rows());
            Preconditions.checkArgument(C.rows() == rows && C.cols() == cols + B.cols());
            for (int row = 0; row < C.rows(); row++) {
                for (int col = 0; col < cols; col++) {
                    double val = col < cols ? this.get(row, col) : B.get(row, col);
                    C.set(row, col, val);
                }
            }
        }
        else {
            throw new IllegalStateException("Unknown layout: " + layout.name());
        }
        return C;
    }

    @Override
    public Matrix assign(long rowOffset, long colOffset, Matrix m) {
        for (long row = rowOffset; row < Math.min(m.rows(), rows); row++) {
            for (long col = colOffset; col < Math.min(m.cols(), cols); col++) {
                set(row, col, m.get(row - rowOffset, col - colOffset));
            }
        }
        return this;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Dense64Matrix["+rows+"|"+cols+"]: ");
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                stringBuilder.append("("+row+","+col+","+get(row,col)+") ");
            }
        }
        return stringBuilder.toString();
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

        public AbstractRowIterator(final AbstractMatrix mat) { this(mat, 0, Utils.toInt(Preconditions.checkNotNull(mat).rows()) - 1); }
        public AbstractRowIterator(final AbstractMatrix mat, final int startRow, final int endRow) {
            this.target = mat;
            Preconditions.checkArgument(startRow >= 0 && startRow < target.rows());
            Preconditions.checkArgument(endRow > startRow && endRow < target.rows());
            this.startRow = startRow;
            this.endRow = endRow;
            reset();
        }

        @Override
        public boolean hasNext() {
            return numFetched < (endRow - startRow + 1);
        }

        @Override
        public void next() {
            numFetched++;
            currentRow++;
        }

        @Override
        public void nextRandom() {
            numFetched++;
            currentRow = startRow + rand.nextInt(endRow + 1);
        }

        @Override
        public double value(final long col) { return target.get(currentRow, col); }

        protected Matrix get(int from, int size, Matrix result) {
            Preconditions.checkArgument(from + size <= target.cols());
            Preconditions.checkArgument(result.rows() == 1 && result.cols() == size);
            for(int i = from; i - from < size; i++) {
                result.set(0, i, target.get(currentRow, i));
            }
            return result;
        }

        @Override
        public abstract Matrix get();

        @Override
        public abstract Matrix get(int from, int size);

        @Override
        public void reset() {
            currentRow = startRow - 1;
            numFetched = 0;
        }

        @Override
        public int size() {
            return endRow - startRow + 1;
        }

        @Override
        public int rowNum() { return currentRow; }
    }
}
