package de.tuberlin.pserver.math.matrix.sparse;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix.Layout;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.utils.*;
import gnu.trove.map.hash.TLongFloatHashMap;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class SparseMatrix32F implements Matrix32F {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private TLongFloatHashMap data;

    private final long rows;

    private final long cols;

    private final Layout layout;

    private final Lock lock;

    private Object owner;

    private boolean sorted;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SparseMatrix32F(final long rows, final long cols, final Layout layout) {
        this.rows = rows;
        this.cols = cols;
        this.layout = Preconditions.checkNotNull(layout);
        this.data = new TLongFloatHashMap();
        this.sorted = false;
        Preconditions.checkArgument(java.util.Arrays.asList(Layout.values()).contains(layout), "Unknown MemoryLayout: " + layout.toString());
        this.lock = new ReentrantLock(true);
    }

    public SparseMatrix32F(final long rows, final long cols) {
        this(rows, cols, Layout.ROW_LAYOUT);
    }

    // Copy Constructor.
    public SparseMatrix32F(final SparseMatrix32F m) {
        this(m.rows(), m.cols(), m.layout());
        this.data = new TLongFloatHashMap(m.data);
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
        return data.keys().length * Float.BYTES;
    }

    @Override
    public Layout layout() {
        return layout;
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void setOwner(Object owner) {
        this.owner = owner;
    }

    @Override
    public Object getOwner() {
        return owner;
    }

    @Override
    public Matrix32F copy() {
        return new SparseMatrix32F(this);
    }

    @Override
    public Matrix32F copy(long rows, long cols) {
        return null;
    }

    // ---------------------------------------------------
    // SETTER.
    // ---------------------------------------------------

    @Override
    public void set(final long row, final long col, final Float value) {
        Preconditions.checkArgument(row < rows(), String.format("Row index %d is out of bounds for Matrix of size(%d, %d)", row, rows(), cols()));
        Preconditions.checkArgument(col < cols(), String.format("Column index %d is out of bounds for Matrix of size(%d, %d)", col, rows(), cols()));

        int pos = Utils.getPos(row, col, this);
        if (value == data.getNoEntryValue()) {
            if (data.containsKey(pos)) {
                data.remove(pos);
            }
        } else {
            data.put(pos, value);
            sorted = false;
        }
    }

    @Override
    public Matrix32F setDiagonalsToZero() {
        return null;
    }

    @Override
    public Matrix32F setDiagonalsToZero(Matrix B) {
        return null;
    }

    @Override
    public void setArray(Object data) {

    }

    // ---------------------------------------------------
    // GETTER.
    // ---------------------------------------------------

    @Override
     public Float get(final long index) {
        Preconditions.checkArgument(index < rows() * cols());
        return data.get(index);
    }

    @Override
    public Float get(final long row, final long col) {
        Preconditions.checkArgument(row < rows(), String.format("Row index %d is out of bounds for Matrix of size(%d, %d)", row, rows(), cols()));
        Preconditions.checkArgument(col < cols(), String.format("Column index %d is out of bounds for Matrix of size(%d, %d)", col, rows(), cols()));
        return data.get(Utils.getPos(row, col, this));
    }

    @Override
    public Matrix32F getRow(long row) {
        return getRow(row, 0, this.cols());
    }

    @Override
    public Matrix32F getRow(long row, long from, long to) {
        Matrix32F result = new SparseMatrix32F(1, to - from);

        for (long i = from; i < to; ++i) {
            result.set(0, i, get(row, i));
        }
        return result;
    }

    @Override
    public Matrix32F getCol(long col) {
        return null;
    }

    @Override
    public Matrix32F getCol(long col, long from, long to) {
        return null;
    }

    @Override
    public Object toArray() {
        return new float[0];
    }

    // ---------------------------------------------------
    // APPLY ON ELEMENTS.
    // ---------------------------------------------------

    @Override
    public Matrix32F applyOnElements(UnaryOperator<Float> f) {
        return null;
    }

    @Override
    public Matrix32F applyOnElements(UnaryOperator<Float> f, Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F applyOnElements(Matrix<Float> B, BinaryOperator<Float> f) {
        return null;
    }

    @Override
    public Matrix32F applyOnElements(Matrix<Float> B, BinaryOperator<Float> f, Matrix<Float> C) {
        return null;
    }

    @Override
    public Matrix32F applyOnElements(MatrixElementUnaryOperator<Float> f) {
        return null;
    }

    @Override
    public Matrix32F applyOnElements(MatrixElementUnaryOperator<Float> f, Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F applyOnNonZeroElements(MatrixElementUnaryOperator<Float> f) {
        return null;
    }

    @Override
    public Matrix32F applyOnNonZeroElements(MatrixElementUnaryOperator<Float> f, Matrix<Float> B) {
        return null;
    }

    // ---------------------------------------------------
    // ASSIGN.
    // ---------------------------------------------------

    @Override
    public Matrix32F assign(Matrix<Float> v) {
        return null;
    }

    @Override
    public Matrix32F assign(Float v) {
        return null;
    }

    @Override
    public Matrix32F assignRow(long row, Matrix<Float> v) {
        return null;
    }

    @Override
    public Matrix32F assignColumn(long col, Matrix<Float> v) {
        return null;
    }

    @Override
    public Matrix32F assign(long rowOffset, long colOffset, Matrix<Float> m) {
        return null;
    }

    // ---------------------------------------------------
    // AGGREGATION.
    // ---------------------------------------------------

    @Override
    public Float aggregate(BinaryOperator<Float> combiner, UnaryOperator<Float> mapper, Matrix<Float> result) {
        return 0f;
    }

    @Override
    public Matrix32F aggregateRows(MatrixAggregation<Float> f) {
        return null;
    }

    @Override
    public Matrix32F aggregateRows(MatrixAggregation<Float> f, Matrix<Float> result) {
        return null;
    }

    @Override
    public Float sum() {
        return 0f;
    }

    // ---------------------------------------------------
    // ARITHMETIC.
    // ---------------------------------------------------

    @Override
    public Matrix32F add(Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F add(Matrix<Float> B, Matrix<Float> C) {
        return null;
    }

    @Override
    public Matrix32F addVectorToRows(Matrix<Float> v) {
        return null;
    }

    @Override
    public Matrix32F addVectorToRows(Matrix<Float> v, Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F addVectorToCols(Matrix<Float> v) {
        return null;
    }

    @Override
    public Matrix32F addVectorToCols(Matrix<Float> v, Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F sub(Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F sub(Matrix<Float> B, Matrix<Float> C) {
        return null;
    }

    @Override
    public Matrix32F mul(Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F mul(Matrix<Float> B, Matrix<Float> C) {
        return null;
    }

    @Override
    public Matrix32F scale(Float a) {
        return null;
    }

    @Override
    public Matrix32F scale(Float a, Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F transpose() {
        return null;
    }

    @Override
    public Matrix32F transpose(Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F invert() {
        return null;
    }

    @Override
    public Matrix32F invert(Matrix<Float> B) {
        return null;
    }

    @Override
    public Float norm(int p) {
        return 0f;
    }

    @Override
    public Float dot(Matrix<Float> B) {
        Utils.checkDotProduct(this, B);

        float result = 0.0f;
        for (long entry : data.keys()) {
            result += this.get(entry) * B.get(entry);
        }
        return result;
    }

    // ---------------------------------------------------
    // SLICING.
    // ---------------------------------------------------

    @Override
    public Matrix32F subMatrix(long rowOffset, long colOffset, long rows, long cols) {
        return null;
    }

    @Override
    public Matrix32F concat(Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F concat(Matrix<Float> B, Matrix<Float> C) {
        return null;
    }

    // ---------------------------------------------------
    // ROW ITERATOR.
    // ---------------------------------------------------

    @Override
    public RowIterator rowIterator() {
        return new RowIterator(this);
    }

    @Override
    public RowIterator rowIterator(long startRow, long endRow) {
        return new RowIterator(this, startRow, endRow);
    }


    public static class RowIterator implements Matrix32F.RowIterator {

        protected SparseMatrix32F self;

        protected final long end;

        protected final long start;

        protected long currentRow;

        protected final long rowsToFetch;

        protected long rowsFetched;

        protected RandomDataGenerator rand;

        public RowIterator(final SparseMatrix32F v) {
            this(v, 0, (int) Preconditions.checkNotNull(v).rows());
        }

        public RowIterator(final SparseMatrix32F v, final long startRow, final long endRow) {
            this.self = v;
            Preconditions.checkArgument(startRow >= 0 && startRow < self.rows());
            Preconditions.checkArgument(endRow >= startRow && endRow <= self.rows());
            this.start = startRow;
            this.end = endRow;
            this.rowsToFetch = endRow - startRow;
            this.rand = new RandomDataGenerator();
            reset();
        }

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
            currentRow = rand.nextLong(start, end-1);
        }

        @Override
        public Float value(final long col) {
            return self.get(currentRow, col);
        }

        @Override
        public Matrix32F get() {
            return self.getRow(rowNum());
        }

        @Override
        public Matrix32F get(final long from, final long size) {
            return self.getRow(rowNum(), from, from + size);
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
        public long rowNum() {
            return currentRow;
        }
    }
}
