package de.tuberlin.pserver.math.matrix.dense;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix64F;
import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;
import de.tuberlin.pserver.math.utils.Utils;

import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DenseMatrix64F implements Matrix64F {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    //private static final Logger LOG = LoggerFactory.getLogger(DMatrix.class);

    private double[] data;

    private final long rows;

    private final long cols;

    //private final Layout layout;

    private final Lock lock;

    private Object owner;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    // Copy Constructor.
    public DenseMatrix64F(final DenseMatrix64F m) {
        this(m.rows(), m.cols(), null);
        System.arraycopy(m.data, 0, this.data, 0, m.data.length);
    }

    public DenseMatrix64F(final long rows, final long cols) { this(rows, cols, null); }
    public DenseMatrix64F(final long rows, final long cols, final double[] data) {
        this.rows = rows;
        this.cols = cols;
        //this.layout = Preconditions.checkNotNull(layout);
        //Preconditions.checkArgument(java.util.Arrays.asList(Layout.values()).contains(layout), "Unknown MemoryLayout: " + layout.toString());
        this.lock = new ReentrantLock(true);
        this.data = (data == null) ? new double[(int)(rows * cols)] : Preconditions.checkNotNull(data);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public long rows() {
        return rows;
    }

    @Override
    public long cols() {
        return cols;
    }

    @Override
    public long sizeOf() {
        return rows * cols * Double.BYTES;
    }

    //@Override
    //public Layout layout() {
    //    return layout;
    //}

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void setOwner(final Object owner) {
        this.owner = owner;
    }

    @Override
    public Object getOwner() {
        return owner;
    }

    @Override
    public Matrix64F copy() {
        return new DenseMatrix64F(this);
    }

    @Override
    public Matrix64F copy(final long rows, final long cols) {
        DenseMatrix64F result = (DenseMatrix64F)newInstance(rows, cols);
        for (int row = 0; row < Math.min(this.rows, rows); row++) {
            for (int col = 0; col < Math.min(this.cols, cols); col++) {
                result.set(row, col, this.get(row, col));
            }
        }
        return result;
    }

    //@Override
    protected Matrix64F newInstance(long rows, long cols) {
        return new DenseMatrix64F(rows, cols);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DenseMatrix64F["+rows+"|"+cols+"]: ");
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                stringBuilder.append("("+row+","+col+","+get(row,col)+") ");
            }
        }
        return stringBuilder.toString();
    }

    // ---------------------------------------------------
    // SETTER.
    // ---------------------------------------------------

    @Override
    public void set(final long row, final long col, final Double value) {
        try {
            data[Utils.getPos(row, col, this)] = value;
        } catch(ArrayIndexOutOfBoundsException e) {
            if(row < rows && col < cols) {
                throw new IllegalStateException(String.format("Attempt to set a valid position (%d, %d) " +
                        "in matrix of shape (%d, %d) yielded ArrayIndexOutOfBounds: %d",
                        row, col, rows, cols, Utils.getPos(row, col, this)), e);
            }
            throw new IllegalStateException(String.format("Attempt to set a invalid position (%d, %d) " +
                    "in matrix of shape (%d, %d) yielded ArrayIndexOutOfBounds: %d",
                    row, col, rows, cols, Utils.getPos(row, col, this)), e);
        }
    }

    @Override
    public Matrix64F setDiagonalsToZero() {
        return setDiagonalsToZero(this.copy());
    }

    @Override
    public Matrix64F setDiagonalsToZero(final Matrix<Double> B) {
        long diag = 0;
        while(diag < rows && diag < cols) {
            B.set(diag, diag, 0.);
            diag++;
        }
        return (Matrix64F)B;
    }

    @Override
    public void setArray(final Object data) {
        final double[] mData = (double[])data;
        Preconditions.checkState(mData.length == rows * cols);
        this.data = mData;
    }

    // ---------------------------------------------------
    // GETTER.
    // ---------------------------------------------------

    @Override
    public Double get(final long index) {
        return data[(int)index];
    }

    @Override
    public Double get(final long row, final long col) {
        return data[Utils.getPos(row, col, this)];
    }

    @Override
    public Matrix64F getRow(final long row) {
        return getRow(row, 0, cols());
    }

    @Override
    public Matrix64F getRow(final long row, final long from, final long to) {
        // TODO: Optimize with respect to the layout with array copy.
        Matrix64F r = new DenseMatrix64F(1, to - from);
        for (long i = from; i < to; ++i)
            r.set(0, i, data[Utils.getPos(row, i, this)]);
        return r;
    }

    @Override
    public Matrix64F getCol(final long col) {
        return getCol(col, 0, rows);
    }

    @Override
    public Matrix64F getCol(final long col, final long from, final long to) {
        double[] result = new double[(int)(to - from)];
        //if(layout == Layout.COLUMN_LAYOUT) {
        //    System.arraycopy(data, (int)(col * rows + from), result, 0, result.length);
        //}
        //else {
            for (int i = 0; i < result.length; i++) {
                int row = (int)from + i;
                result[i] = data[(int)(row * cols + col)];
            }
        //}
        return new DenseMatrix64F(result.length, 1, result);
    }

    @Override
    public double[] toArray() {
        return data;
    }

    // ---------------------------------------------------
    // APPLY ON ELEMENTS.
    // ---------------------------------------------------

    @Override
    public Matrix64F applyOnElements(final UnaryOperator<Double> f) {
        return applyOnElements(f, newInstance(rows, cols));
    }

    @Override
    public Matrix64F applyOnElements(final UnaryOperator<Double> f, final Matrix<Double> B) {
        Utils.checkShapeEqual(this, B);
        for (int i = 0; i < B.rows(); ++i) {
            for (int j = 0; j < B.cols(); ++j) {
                B.set(i, j, f.apply(this.get(i, j)));
            }
        }
        return (Matrix64F)B;
    }

    @Override
    public Matrix64F applyOnElements(final Matrix<Double> B, final BinaryOperator<Double> f) {
        return applyOnElements(B, f, newInstance(rows, cols));
    }

    @Override
    public Matrix64F applyOnElements(final Matrix<Double> B, final BinaryOperator<Double> f, final Matrix<Double> C) {
        Utils.checkShapeEqual(this, B, C);
        for (int i = 0; i < rows(); ++i) {
            for (int j = 0; j < cols(); ++j) {
                C.set(i, j, f.apply(this.get(i, j), B.get(i, j)));
            }
        }
        return (Matrix64F)C;
    }

    @Override
    public Matrix64F applyOnElements(final MatrixElementUnaryOperator<Double> f) {
        return applyOnElements(f, newInstance(rows, cols));
    }

    @Override
    public Matrix64F applyOnElements(final MatrixElementUnaryOperator<Double> f, final Matrix<Double> B) {
        Utils.checkShapeEqual(this, B);
        for (int i = 0; i < B.rows(); ++i) {
            for (int j = 0; j < B.cols(); ++j) {
                B.set(i, j, f.apply(i, j, this.get(i, j)));
            }
        }
        return (Matrix64F)B;
    }

    @Override
    public Matrix64F applyOnNonZeroElements(final MatrixElementUnaryOperator<Double> f) {
        return applyOnNonZeroElements(f, newInstance(rows, cols));
    }

    @Override
    public Matrix64F applyOnNonZeroElements(final MatrixElementUnaryOperator<Double> f, final Matrix<Double> B) {
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

    // ---------------------------------------------------
    // ASSIGN.
    // ---------------------------------------------------

    @Override
    public Matrix64F assign(final Matrix<Double> v) {
        Preconditions.checkState(v.rows() * v.cols() == rows * cols);
        System.arraycopy(v.toArray(), 0, data, 0, (int)(rows * cols));
        return this;
    }

    @Override
    public Matrix64F assign(final Double v) {
        for (int i = 0; i < rows * cols; ++i)
            data[i] = v;
        return this;
    }

    @Override
    public Matrix64F assignRow(final long row, final Matrix<Double> v) {
        Preconditions.checkNotNull(cols == v.cols());
        for (int i = 0; i < cols; ++i)
            data[Utils.getPos(row, i, this)] = v.get(i);
        return this;
    }

    @Override
    public Matrix64F assignColumn(final long col, final Matrix<Double> v) {
        double[] vData = (double[])v.toArray();
        Preconditions.checkArgument(rows == vData.length);
        //if(layout == Layout.COLUMN_LAYOUT) {
        //    System.arraycopy(v.toArray(), 0, data, (int)(col * rows), vData.length);
        //}
        //else {
            for (int row = 0; row < rows; row++) {
                data[(int)(row * cols + col)] = vData[row];
            }
        //}
        return this;
    }

    @Override
    public Matrix64F assign(final long rowOffset, final long colOffset, final Matrix<Double> m) {
        //if (layout == Layout.ROW_LAYOUT && m.layout() == layout && cols == m.cols()) {
            System.arraycopy(m.toArray(), 0, data, (int) (rowOffset * cols + colOffset), ((double[])m.toArray()).length);
        //}
        //else if(layout == Layout.COLUMN_LAYOUT && m.layout() == layout && rows == m.rows()) {
        //    System.arraycopy(m.toArray(), 0, data, (int) (colOffset * rows + rowOffset), ((double[])m.toArray()).length);
        //}
        return null; //assign(rowOffset, colOffset, m);
    }

    // ---------------------------------------------------
    // AGGREGATION.
    // ---------------------------------------------------

    @Override
    public Double aggregate(final BinaryOperator<Double> combiner, final UnaryOperator<Double> mapper, final Matrix<Double> result) {
        // return aggregateRows(v -> v.aggregate(combiner, mapper), this).aggregate(combiner, Functions.IDENTITY);
        return 0.;
    }

    @Override
    public Matrix64F aggregateRows(final MatrixAggregation<Double> f) {
        return aggregateRows(f, newInstance(rows, 1));
    }

    @Override
    public Matrix64F aggregateRows(final MatrixAggregation<Double> f, final Matrix<Double> result) {
        Preconditions.checkArgument(result.rows() == rows && result.cols() == 1);
        for (int row = 0; row < rows; row++) {
            result.set(row, 0, f.apply(getRow(row)));
        }
        return (Matrix64F)result;
    }

    @Override
    public Double sum() {
        double sum = 0;
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                sum += this.get(row, col);
            }
        }
        return sum;
    }

    // ---------------------------------------------------
    // ARITHMETIC.
    // ---------------------------------------------------

    @Override
    public Matrix64F add(final Matrix<Double> B) {
        return add(B, newInstance(this.rows, this.cols));
    }

    @Override
    public Matrix64F add(final Matrix<Double> B, final Matrix<Double> C) {
        Utils.checkShapeEqual(this, B, C);
        return this.applyOnElements(B, (x, y) -> x + y, C);
    }

    @Override
    public Matrix64F addVectorToRows(final Matrix<Double> v) {
        return addVectorToRows(v, newInstance(rows, cols));
    }

    @Override
    public Matrix64F addVectorToRows(final Matrix<Double> v, final Matrix<Double> B) {
        Utils.checkApplyVectorToRows(this, v, B);
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                B.set(row, col, this.get(row, col) + v.get(col));
            }
        }
        return (Matrix64F)B;
    }

    @Override
    public Matrix64F addVectorToCols(final Matrix<Double> v) {
        return addVectorToCols(v, newInstance(rows, cols));
    }

    @Override
    public Matrix64F addVectorToCols(final Matrix<Double> v, final Matrix<Double> B) {
        Utils.checkApplyVectorToCols(this, v, B);
        for (int col = 0; col < cols; col++) {
            for (int row = 0; row < rows; row++) {
                B.set(row, col, this.get(row, col) + v.get(row));
            }
        }
        return (Matrix64F)B;
    }

    // ----------------------------------------

    @Override
    public Matrix64F sub(final Matrix<Double> B) {
        return sub(B, newInstance(this.rows, this.cols));
    }

    @Override
    public Matrix64F sub(final Matrix<Double> B, final Matrix<Double> C) {
        Utils.checkShapeEqual(this, B, C);
        return this.applyOnElements(B, (x, y) -> x - y, C);
    }

    // ----------------------------------------

    @Override
    public Matrix64F mul(final Matrix<Double> B) {
        return mul(B, newInstance(this.rows, B.cols()));
    }

    @Override
    public Matrix64F mul(final Matrix<Double> B, final Matrix<Double> C) {
        Utils.checkShapeMatrixMatrixMult(this, B, C);
        for (int row = 0; row < C.rows(); row++) {
            for (int col = 0; col < C.cols(); col++) {
                C.set(row, col, this.getRow(row).dot(B.getCol(col)));
            }
        }
        return (Matrix64F)C;
    }

    // ----------------------------------------

    @Override
    public Matrix64F scale(final Double a) {
        return scale(a, newInstance(rows, cols));
    }

    @Override
    public Matrix64F scale(final Double a, final Matrix<Double> B) {
        Utils.checkShapeEqual(this, B);
        return applyOnElements(x -> a * x, B);
    }

    // ----------------------------------------

    @Override
    public Matrix64F transpose() {
        return transpose(newInstance(cols, rows));
    }

    @Override
    public Matrix64F transpose(final Matrix<Double> B) {
        Utils.checkShapeTranspose(this, B);
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                B.set(col, row, this.get(row, col));
            }
        }
        return (Matrix64F)B;
    }

    // ----------------------------------------

    @Override
    public Matrix64F invert() {
        return invert(newInstance(cols, rows));
    }

    @Override
    public Matrix64F invert(final Matrix<Double> B) {
        //LOG.warn("invert() has not been overridden by effective subclass. This implementation does nothing.");
        return (Matrix64F)B;
    }

    // ----------------------------------------

    @Override
    public Double norm(final int p) {
        double norm = 0;
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                norm += Math.pow(this.get(row, col), p);
            }
        }
        return Math.pow(norm, 1./p);
    }

    // ----------------------------------------

    @Override
    public Double dot(final Matrix<Double> B) {
        double result = 0;
        //if(this.layout == Layout.ROW_LAYOUT) {
            //Preconditions.checkArgument(rows == 1);
            //Preconditions.checkArgument(B.layout() == Layout.ROW_LAYOUT);
            //Preconditions.checkArgument(cols == B.cols());
            //Preconditions.checkArgument(B.rows() == cols());
            for (int i = 0; i < rows * cols; i++) {
                result += this.get(i) * B.get(i);
            }
        //}
        //else if(this.layout == Layout.COLUMN_LAYOUT) {
        //    Preconditions.checkArgument(cols == 1);
        //    Preconditions.checkArgument(B.layout() == Layout.COLUMN_LAYOUT);
        //    Preconditions.checkArgument(B.cols() == 1);
        //    Preconditions.checkArgument(rows == B.rows());
        //    for (int row = 0; row < rows; row++) {
        //        result += this.get(row) * B.get(row);
        //    }
        //}
        //else {
        //    throw new IllegalStateException("Unknown layout: " + layout.name());
        //}
        return result;
    }

    // ---------------------------------------------------
    // SLICING.
    // ---------------------------------------------------

    @Override
    public Matrix64F subMatrix(final long rowOffset, final long colOffset, final long rows, final long cols) {
        //if (layout == Layout.ROW_LAYOUT) {
            final int length = (int)(rows * cols);
            final double[] subData = new double[length];
            try {
                System.arraycopy(data, (int) (rowOffset * cols + colOffset), subData, 0, length);
            }
            catch(ArrayIndexOutOfBoundsException e) {
                throw new RuntimeException(String.format("subMatrix(%d, %d, %d, %d) caused ArrayIndexOfBoundsException", rowOffset, colOffset, rows, cols), e);
            }
            return new DenseMatrix64F(rows, cols, subData);
        //} else
        //    throw new UnsupportedOperationException();
    }

    @Override
    public Matrix64F concat(final Matrix<Double> B) {
        //if(this.layout == Layout.ROW_LAYOUT) {
            Preconditions.checkArgument(cols == B.cols());
            return concat(B, newInstance(rows + B.rows(), cols));
        //}
        //else if(this.layout == Layout.COLUMN_LAYOUT) {
        //    Preconditions.checkArgument(rows == B.rows());
        //    return concat(B, newInstance(rows, B.cols() + cols));
        //}
        //else {
        //    throw new IllegalStateException("Unknown layout: " + layout.name());
        //}
    }

    @Override
    public Matrix64F concat(final Matrix<Double> B, final Matrix<Double> C) {
        //if(this.layout == Layout.ROW_LAYOUT) {
            Preconditions.checkArgument(cols == B.cols());
            Preconditions.checkArgument(C.rows() == rows + B.rows() && C.cols() == cols);
            for (int row = 0; row < C.rows(); row++) {
                for (int col = 0; col < cols; col++) {
                    double val = row < rows ? this.get(row, col) : B.get(row, col);
                    C.set(row, col, val);
                }
            }
        //}
        /*else if(this.layout == Layout.COLUMN_LAYOUT) {
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
        }*/
        return (Matrix64F)C;
    }

    // ---------------------------------------------------
    // ROW ITERATOR.
    // ---------------------------------------------------

    @Override
    public RowIterator rowIterator() {
        return new RowIterator(this);
    }

    @Override
    public RowIterator rowIterator(final long startRow, final long endRow) {
        return new RowIterator(this, startRow, endRow);
    }

    // ---------------------------------------------------

    public static final class RowIterator implements Matrix64F.RowIterator {

        private DenseMatrix64F self;

        private final long end;

        private final long start;

        private long currentRow;

        private final long rowsToFetch;

        private long rowsFetched;

        private Random rand;

        // ---------------------------------------------------

        public RowIterator(final DenseMatrix64F v) { this(v, 0, (int)Preconditions.checkNotNull(v).rows()); }
        public RowIterator(final DenseMatrix64F v, final long startRow, final long endRow) {
            this.self = v;
            Preconditions.checkArgument(startRow >= 0 && startRow < self.rows());
            Preconditions.checkArgument(endRow >= startRow && endRow <= self.rows());
            this.start = startRow;
            this.end = endRow;
            this.rowsToFetch = endRow - startRow;
            this.rand = new Random();
            reset();
        }

        // ---------------------------------------------------

        @Override
        public boolean hasNext() {
            return rowsFetched < rowsToFetch;
        }

        @Override
        public void next() {
            // the generic case is just currentRow++, but the reset method only sets rowsFetched = 0
            if(rowsFetched == 0) {
                currentRow = 0;
            }
            else {
                currentRow++;
            }
            rowsFetched++;
            // can overflow if nextRandom and next is called alternatingly
            if(currentRow >= end) {
                currentRow = start;
            }
        }

        @Override
        public void nextRandom() {
            rowsFetched++;
            currentRow = start + rand.nextInt((int)end);
        }

        @Override
        public Double value(final long col) {
            return self.data[Utils.getPos(currentRow, col, self)];
        }

        @Override
        public DenseMatrix64F get() {
            return get(0, (int) self.cols);
        }

        @Override
        public DenseMatrix64F get(final long from, final long size) {
            final double v[] = new double[(int)size];
            //if(self.layout == Layout.ROW_LAYOUT) {
                try {
                    System.arraycopy(self.data, Utils.getPos(currentRow, from, self), v, 0, (int)size);
                }
                catch(ArrayIndexOutOfBoundsException e) {
                    System.out.println("failed copy from: " + Utils.getPos(currentRow, from, self) + "; currentRow: " + currentRow + "; from: " + from + ";  length: " + size + "; array.length: " + self.data.length);
                    throw e;
                }

            //}
            //else {
            //    for (long i = from; i < size; i++) {
            //        v[(int)(i - from)] = self.data[Utils.getPos(currentRow, i, self)];
            //    }
            //}
            return new DenseMatrix64F(1, size, v);
        }

        @Override
        public void reset() {
            rowsFetched = 0;
        }

        @Override
        public long size() {
            return rowsToFetch;
        }

        @Override
        public long rowNum() { return currentRow; }
    }
}
