package de.tuberlin.pserver.math.matrix.sparse;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.*;

public class SparseMatrix32F implements Matrix32F {

    // ---------------------------------------------------
    // Logger.
    // ---------------------------------------------------

    static final Logger logger = LoggerFactory.getLogger(SparseMatrix32F.class);

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public TLongFloatMap data;

    private final long rows;

    private final long cols;

    private final Lock lock;

    private Object owner;

    private boolean areSortedKeysCreated = false;

    public long[] sortedKeys;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SparseMatrix32F(long rows, long cols) {
        this.rows = rows;
        this.cols = cols;
        this.lock = new ReentrantLock(true);
        this.data = new TLongFloatHashMap((int)(rows * cols * 0.1));
    }

    // Copy Constructor
    private SparseMatrix32F(SparseMatrix32F m) {
        this(m.rows(), m.cols());
        m.data.forEachEntry((k, v) -> {
            this.data.put(k, v);
            return true;
        });
    }

    // Copy Constructor
    private SparseMatrix32F(SparseMatrix32F m, long rows, long cols) {
        this(rows, cols);
        m.data.forEachEntry((k, v) -> {
            this.data.put(k, v);
            return true;
        });
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private long findRowFromIndex(long index) {
        return index / this.cols;
    }

    private long findColFromIndex(long index) {
        return index % this.cols;
    }

    public long[] createSortedKeys() {

        if (!areSortedKeysCreated) {

            sortedKeys = Arrays.copyOf(this.data.keys(), this.data.keys().length);

            Arrays.sort(sortedKeys);
        }

        areSortedKeysCreated = true;

        return sortedKeys;
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
        return new SparseMatrix32F(this, rows, cols);
    }

    // ---------------------------------------------------
    // SETTER.
    // ---------------------------------------------------

    @Override
    public void set(final long row, final long col, final Float value) {
        long key = row * cols + col;
        if (value == this.data.getNoEntryValue()) {
            if (this.data.containsKey(key))
                this.data.remove(key);
        }
        else
            this.data.put(key, value);
    }


    public void set(final long index, final Float value) {
        if (value == this.data.getNoEntryValue()) {
            if (this.data.containsKey(index))
                this.data.remove(index);
        }
        else {
            this.data.put(index, value);
        }
    }

    @Override
    public Matrix32F setDiagonalsToZero() {
        return this.setDiagonalsToZero(this.copy());
    }

    @Override
    public Matrix32F setDiagonalsToZero(Matrix m) {
        if (m instanceof SparseMatrix32F)
            return this.setDiagonalsToZero((SparseMatrix32F) m);

        long diag = 0;
        while(diag < rows && diag < cols) {
            m.set(diag, diag, 0F);
            diag++;
        }
        return (Matrix32F) m;
    }

    public Matrix32F setDiagonalsToZero(SparseMatrix32F sm32f) {
        sm32f.data.forEachEntry((k, v) -> {
            if (sm32f.findRowFromIndex(k) == sm32f.findColFromIndex(k))
                sm32f.data.remove(k);
            return true;
        });
        return sm32f;
    }

    @Override
    public void setArray(Object data) {
        float[] mData = (float[]) data;
        checkState(mData.length == this.rows * this.cols);
        logger.info("Warning! 'setArray' is potentially a very expensive operation.");
        this.data = new TLongFloatHashMap();
        for (int i = 0; i < mData.length; i++) {
            if (mData[i] != 0F) this.data.put(i, mData[i]);
        }
    }

    // ---------------------------------------------------
    // GETTER.
    // ---------------------------------------------------

    @Override
     public Float get(final long index) {
        checkArgument(index < rows() * cols());
        Float value = data.get(index);
        return (value == null) ? 0f : value;
    }

    @Override
    public Float get(final long row, final long col) {
        final Float value = data.get((row * cols + col));
        return (value == null) ? 0f : value;
    }

    @Override
    public Matrix32F getRow(long row) {
        SparseMatrix32F result = new SparseMatrix32F(1, cols);
        data.forEachKey(value -> {
            if (value >= row * cols + 0 && value < row * cols + cols)
                result.set(value - (row * cols), this.get(value));
            return true;
        });
        return result;
    }

    @Override
    public Matrix32F getRow(long row, long from, long to) {
        SparseMatrix32F result = new SparseMatrix32F(1, to - from);
        for (long col = from; col < to; col++)
            if (this.data.containsKey(row * cols + col))
                result.set(0, col, this.get(row, col));

        //data.forEachKey(value -> {
        //    if (value >= row * cols + from && value < row * cols + to)
        //        result.set(value - (row * cols), this.get(value));
        //    return true;
        //});

        return result;
    }

    @Override
    public Matrix32F getCol(long col) {
        return this.getCol(col, 0, this.rows);
    }

    @Override
    public Matrix32F getCol(long col, long from, long to) {
        Matrix32F result = new SparseMatrix32F(to - from, 1);
        for (long row = from; row < to; row++)
            if (this.data.containsKey(row * cols + col))
                result.set(row, 0, this.get(row, col));
        return result;
    }

    @Override
    public Object toArray() {
        throw new UnsupportedOperationException("Invalid operation for Sparse Matrices.");
    }

    // ---------------------------------------------------
    // APPLY ON ELEMENTS.
    // ---------------------------------------------------

    @Override
    public Matrix32F applyOnElements(UnaryOperator<Float> f) {
        return this.applyOnElements(f, new SparseMatrix32F(this.rows, this.cols));
    }

    @Override
    public Matrix32F applyOnElements(UnaryOperator<Float> f, Matrix<Float> m) {
        logger.info("Warning! 'applyOnElements' is potentially a very expensive operation.");
        for (int i = 0; i < m.rows(); i++) {
            for (int j = 0; j < m.cols(); j++) {
                m.set(i, j, f.apply(this.get(i, j)));
            }
        }
        return (Matrix32F) m;
    }

    @Override
    public Matrix32F applyOnElements(Matrix<Float> m, BinaryOperator<Float> f) {
        return applyOnElements(m, f, new SparseMatrix32F(this.rows, this.cols));
    }

    @Override
    public Matrix32F applyOnElements(Matrix<Float> m, BinaryOperator<Float> f, Matrix<Float> result) {
        logger.info("Warning! 'applyOnElements' is potentially a very expensive operation.");
        for (int i = 0; i < this.rows(); i++) {
            for (int j = 0; j < this.cols(); j++) {
                result.set(i, j, f.apply(this.get(i, j), m.get(i, j)));
            }
        }
        return (Matrix32F) result;
    }

    @Override
    public Matrix32F applyOnElements(MatrixElementUnaryOperator<Float> f) {
        return this.applyOnElements(f, new SparseMatrix32F(this.rows, this.cols));
    }

    @Override
    public Matrix32F applyOnElements(MatrixElementUnaryOperator<Float> f, Matrix<Float> m) {
        logger.info("Warning! 'applyOnElements' is potentially a very expensive operation.");
        for (int i = 0; i < m.rows(); i++) {
            for (int j = 0; j < m.cols(); j++) {
                m.set(i, j, f.apply(i, j, this.get(i, j)));
            }
        }
        return (Matrix32F) m;
    }

    @Override
    public Matrix32F applyOnNonZeroElements(MatrixElementUnaryOperator<Float> f) {
        return applyOnNonZeroElements(f, new SparseMatrix32F(this.rows, this.cols));
    }

    @Override
    public Matrix32F applyOnNonZeroElements(MatrixElementUnaryOperator<Float> f, Matrix<Float> m) {
        SparseMatrix32F sm32f = (SparseMatrix32F) m;
        this.data.forEachEntry((k, v) -> {
            if (v != 0F) {
                long row = this.findRowFromIndex(k);
                long col = this.findColFromIndex(k);
                sm32f.set(row, col, f.apply(row, col, v));
            }
            return true;
        });
        return sm32f;
    }

    // ---------------------------------------------------
    // ASSIGN.
    // ---------------------------------------------------

    @Override
    public Matrix32F assign(Float f) {
        logger.info("Warning! 'assign(Float)' is potentially a very expensive operation.");
        for (int i = 0; i < rows * cols; i++)
            this.data.put(i, f);
        return this;
    }

    @Override
    public Matrix32F assign(Matrix<Float> m) {
        checkState(m.rows() * m.cols() == this.rows * this.cols);
        logger.info("Warning! 'assign(Float)' is potentially a very expensive operation.");
        for (int row = 0; row < m.rows(); row++)
            for (int col = 0; col < m.cols(); col++)
                this.set(row, col, m.get(row, col));
        return this;
    }

    @Override
    public Matrix32F assignRow(long row, Matrix<Float> m) {
        checkState(m.rows() == 1 && row < this.rows && m.cols() == this.cols);
        for (long col = 0; col < this.cols; col++)
            this.set(row, col, m.get(0, col));
        return this;
    }

    @Override
    public Matrix32F assignColumn(long col, Matrix<Float> m) {
        checkState(m.cols() == 1 && col < this.cols && m.rows() == this.rows);
        for (long row = 0; row < this.rows; row++)
            this.set(row, col, m.get(row, 0));
        return this;
    }

    @Override
    public Matrix32F assign(long rowOffset, long colOffset, Matrix<Float> m) {
        checkState(this.rows > rowOffset + m.rows() && this.cols > colOffset + m.cols());
        logger.info("Warning! 'assign(Float)' is potentially a very expensive operation.");
        for (long col = colOffset; col < m.cols(); col++)
            this.set(rowOffset, col, m.get(rowOffset, col));
        return this;
    }

    // ---------------------------------------------------
    // AGGREGATION.
    // ---------------------------------------------------

    @Override
    public Float aggregate(BinaryOperator<Float> combiner, UnaryOperator<Float> mapper, Matrix<Float> result) {
        throw new NotImplementedException("TODO: Need to implement 'aggregate' method");
    }

    @Override
    public Matrix32F aggregateRows(MatrixAggregation<Float> f) {
        return aggregateRows(f, new SparseMatrix32F(this.rows, 1));
    }

    @Override
    public Matrix32F aggregateRows(MatrixAggregation<Float> f, Matrix<Float> result) {
        checkArgument(result.rows() == this.rows && result.cols() == 1);
        for (int row = 0; row < this.rows; row++) {
            result.set(row, 0, f.apply(this.getRow(row)));
        }
        return (Matrix32F) result;
    }

    @Override
    public Float sum() {
        float sum = 0;
        for (float value : this.data.values())
            sum += value;
        return sum;
    }

    // ---------------------------------------------------
    // ARITHMETIC.
    // ---------------------------------------------------

    @Override
    public Matrix32F add(Matrix<Float> m) {
        return this.add(m, new SparseMatrix32F(this.rows, this.cols));
    }

    @Override
    public Matrix32F add(Matrix<Float> m, Matrix<Float> result) {
        return this.applyOnElements(m, (v1, v2) -> v1 + v2, result);
    }

    @Override
    public Matrix32F addVectorToRows(Matrix<Float> v) {
        return this.addVectorToRows(v, new SparseMatrix32F(this.rows, this.cols));
    }

    @Override
    public Matrix32F addVectorToRows(Matrix<Float> v, Matrix<Float> result) {
        for (int row = 0; row < this.rows; row++) {
            for (int col = 0; col < this.cols; col++) {
                result.set(row, col, this.get(row, col) + v.get(0, col));
            }
        }
        return (Matrix32F) result;
    }

    @Override
    public Matrix32F addVectorToCols(Matrix<Float> v) {
        return this.addVectorToCols(v, new SparseMatrix32F(this.rows, this.cols));
    }

    @Override
    public Matrix32F addVectorToCols(Matrix<Float> v, Matrix<Float> result) {
        for (int col = 0; col < this.cols; col++) {
            for (int row = 0; row < this.rows; row++) {
                result.set(row, col, this.get(row, col) + v.get(row, 0));
            }
        }
        return (Matrix32F) result;
    }

    @Override
    public Matrix32F sub(Matrix<Float> B) {
        return this.sub(B, new SparseMatrix32F(this.rows, this.cols));
    }

    @Override
    public Matrix32F sub(Matrix<Float> m, Matrix<Float> result) {
        return this.applyOnElements(m, (v1, v2) -> v1 - v2, result);
    }

    @Override
    public Matrix32F mul(Matrix<Float> B) {
        return this.mul(B, new SparseMatrix32F(this.rows, this.cols));
    }

    @Override
    public Matrix32F mul(Matrix<Float> m, Matrix<Float> result) {
        SparseMatrix32F sm32f = (SparseMatrix32F) m;
        sm32f.data.forEachEntry((k, v) -> {
            long row = this.findRowFromIndex(k), col = this.findColFromIndex(k);
            result.set(row, col, this.get(k) * v);
            return true;
        });
        return (Matrix32F) result;
    }

    @Override
    public Matrix32F scale(Float a) {
        return this.scale(a, new SparseMatrix32F(this.rows, this.cols));
    }

    @Override
    public Matrix32F scale(Float a, Matrix<Float> result) {
        this.data.forEachEntry((k, v) -> {
            long row = this.findRowFromIndex(k), col = this.findColFromIndex(k);
            result.set(row, col, v * a);
            return true;
        });
        return (Matrix32F) result;
    }

    @Override
    public Matrix32F transpose() {
        return this.transpose(new SparseMatrix32F(this.rows, this.cols));
    }

    @Override
    public Matrix32F transpose(Matrix<Float> result) {
        for (int row = 0; row < this.rows; row++)
            for (int col = 0; col < this.cols; col++)
                result.set(row, col, this.get(col, row));
        return (Matrix32F) result;
    }

    @Override
    public Matrix32F invert() {
        return this.invert(new SparseMatrix32F(this.rows, this.cols));
    }

    @Override
    public Matrix32F invert(Matrix<Float> B) {
        throw new NotImplementedException("TODO: Need to implement 'invert' method");
    }

    @Override
    public Float norm(int p) {
        double norm = 0;
        for (int row = 0; row < this.rows; row++) {
            for (int col = 0; col < this.cols; col++) {
                norm += Math.pow(this.get(row, col), p);
            }
        }
        return (float) Math.pow(norm, 1./p);
    }

    @Override
    public Float dot(Matrix<Float> m) {
        float result = 0.0f;
        for (long key : data.keys()) {
            result += this.get(key) * m.get(key);
        }
        return result;
    }

    // ---------------------------------------------------
    // SLICING.
    // ---------------------------------------------------

    @Override
    public Matrix32F subMatrix(long rowOffset, long colOffset, long rows, long cols) {
        checkArgument(rowOffset < this.rows && this.rows >= rows + rowOffset);
        checkArgument(colOffset < this.cols && this.cols >= cols + colOffset);
        SparseMatrix32F sm32f = new SparseMatrix32F(rows, cols);
        for (long key : this.data.keys()) {
            long row = this.findRowFromIndex(key), col = this.findColFromIndex(key);
            if (rowOffset <= row && row <= rows && colOffset <= col && col <= cols)
                sm32f.set(row - rowOffset, col - colOffset, this.data.get(key));
        }
        return sm32f;
    }

    @Override
    public Matrix32F concat(Matrix<Float> m) {
        checkArgument(this.cols == m.cols());
        return concat(m, new SparseMatrix32F(this.rows + m.rows(), this.cols));
    }

    @Override
    public Matrix32F concat(Matrix<Float> m, Matrix<Float> result) {
        checkArgument(this.cols == m.cols());
        checkArgument(result.rows() >= this.rows + m.rows() && result.cols() == this.cols);
        long bRow = 0;
        for (long row = 0; row < result.rows(); row++) {
            if (row < this.rows)
                result.assignRow(row, this.getRow(row));
            else {
                result.assignRow(row, m.getRow(bRow));
                bRow++;
            }
        }
        return (Matrix32F) result;
    }

    // ---------------------------------------------------
    // ROW ITERATOR.
    // ---------------------------------------------------

    @Override
    public RowIterator rowIterator() {
        return this.rowIterator(0, this.rows);
    }

    @Override
    public RowIterator rowIterator(long startRow, long endRow) {
        return new RowIterator(this, startRow, endRow);
    }

    // ---------------------------------------------------
    // INNER CLASSES.
    // ---------------------------------------------------

    private static final class RowIterator implements Matrix32F.RowIterator {

        protected SparseMatrix32F self;
        protected final long end;
        protected final long start;
        protected long currentRow;
        protected final long rowsToFetch;
        protected long rowsFetched;
        protected RandomDataGenerator rand;

        private RowIterator(final SparseMatrix32F v, final long startRow, final long endRow) {
            this.self = v;
            checkArgument(startRow >= 0 && startRow < self.rows());
            checkArgument(endRow >= startRow && endRow <= self.rows());
            this.start = startRow;
            this.end = endRow;
            this.rowsToFetch = endRow - startRow;
            this.rand = new RandomDataGenerator();
            self.createSortedKeys();
            reset();
        }

        @Override
        public boolean hasNext() {
            return rowsFetched < rowsToFetch;
        }

        @Override
        public void next() {
            // the generic case is just currentRow++, but the reset method only sets rowsFetched = 0
            if (rowsFetched == 0) {
                currentRow = 0;
            } else {
                currentRow++;
            }
            rowsFetched++;
            // can overflow if nextRandom and next is called alternatingly
            if (currentRow >= end) {
                currentRow = start;
            }
        }

        @Override
        public void nextRandom() {
            rowsFetched++;
            currentRow = rand.nextLong(start, end - 1);
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

    // ---------------------------------------------------
    // EXPERIMENTAL!
    // ---------------------------------------------------

    private static int binarySearch(long[] a, int fromIndex, int toIndex, long key) {

        int low = fromIndex;

        int high = toIndex - 1;

        while (low <= high) {

            int mid = (low + high) >>> 1;

            long midVal = a[mid];

            if (midVal < key)

                low = mid + 1;

            else if (midVal > key)

                high = mid - 1;

            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
    }

    public static class SparseMatrix32View implements Matrix32F {

        // ---------------------------------------------------

        private final SparseMatrix32F self;

        private final long[] keys;

        private int[][] rowPointer;

        // ---------------------------------------------------

        private final long rowOffset;

        private final long colOffset;

        private final long viewRows;

        private final long viewCols;

        // ---------------------------------------------------

        public SparseMatrix32View(final SparseMatrix32F self,
                                  final long rowOffset,
                                  final long colOffset,
                                  final long viewRows,
                                  final long viewCols) {

            this.self = self;
            this.keys = self.createSortedKeys();
            this.rowOffset = rowOffset;
            this.colOffset = colOffset;
            this.viewRows  = viewRows;
            this.viewCols  = viewCols;

            computeIndices(rowOffset, colOffset, viewRows, viewCols);
        }

        // ---------------------------------------------------


        private void computeIndices(long rowOffset, long colOffset, long viewRows, long viewCols) {

            rowPointer = new int[(int)viewRows][2];

            if (colOffset == 0 && viewCols == self.cols) {

                int s = 0, o = 0;

                for (long i = rowOffset; i < rowOffset + viewRows; ++i) {

                    final long lastRowIndex = rowOffset * self.cols + self.cols - 1;

                    o = s;

                    final int range = (int) (o + self.cols > keys.length
                            ? keys.length : o + self.cols);

                    s = binarySearch(keys, o, range, lastRowIndex);

                    if (s < 0)
                        s = (s * -1) - 1;

                    rowPointer[(int) (i - rowOffset)] = new int[] {o, s - o};
                }

            } else {

                for (long i = rowOffset; i < rowOffset + viewRows; ++i) {

                    final long startColIndex = i * self.cols + colOffset;

                    final long endColIndex = i * self.cols + colOffset + viewCols - 1;

                    int s = binarySearch(keys, 0, keys.length, startColIndex);

                    if (s < 0)
                        s = (s * -1) - 1;

                    int e = binarySearch(keys, 0, keys.length, endColIndex);

                    if (e < 0)
                        e = (e * -1) - 1;

                    rowPointer[(int) (i - rowOffset)] = new int[] {s, e - s};
                }
            }
        }

        // ---------------------------------------------------

        @Override
        public Matrix32F copy() {
            return null;
        }

        @Override
        public Matrix32F copy(long rows, long cols) {
            return null;
        }

        @Override
        public void set(long row, long col, Float value) {

        }

        @Override
        public Matrix32F setDiagonalsToZero() {
            return null;
        }

        @Override
        public Matrix32F setDiagonalsToZero(Matrix<Float> B) {
            return null;
        }

        @Override
        public void setArray(Object data) {

        }

        @Override
        public Float get(long index) {
            return null;
        }

        @Override
        public Float get(long row, long col) {
            return null;
        }

        @Override
        public Matrix32F getRow(long row) {
            return null;
        }

        @Override
        public Matrix32F getRow(long row, long from, long to) {
            return null;
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
            return null;
        }

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

        @Override
        public Matrix32F assign(Matrix<Float> v) {
            return null;
        }

        @Override
        public Matrix32F assign(Float aFloat) {
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

        @Override
        public Float aggregate(BinaryOperator<Float> combiner, UnaryOperator<Float> mapper, Matrix<Float> result) {
            return null;
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
            return null;
        }

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
            return null;
        }

        @Override
        public Float dot(Matrix<Float> B) {
            return null;
        }

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

        @Override
        public RowIterator rowIterator() {
            return null;
        }

        @Override
        public RowIterator rowIterator(long startRow, long endRow) {
            return null;
        }

        @Override
        public long rows() {
            return 0;
        }

        @Override
        public long cols() {
            return 0;
        }

        @Override
        public long sizeOf() {
            return 0;
        }

        @Override
        public void lock() {
        }

        @Override
        public void unlock() {
        }

        @Override
        public void setOwner(Object owner) {
        }

        @Override
        public Object getOwner() {
            return null;
        }
    }
}
