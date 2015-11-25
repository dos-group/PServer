package de.tuberlin.pserver.math.matrix.sparse;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix64F;
import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;
import de.tuberlin.pserver.math.utils.Utils;
import gnu.trove.map.hash.TLongDoubleHashMap;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.*;

public class SparseMatrix64F implements Matrix64F {

    // ---------------------------------------------------
    // Logger.
    // ---------------------------------------------------

    static final Logger logger = LoggerFactory.getLogger(SparseMatrix32F.class);

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private TLongDoubleHashMap data = new TLongDoubleHashMap();
    private final long rows;
    private final long cols;
    private final Lock lock;
    private Object owner;
    private boolean sorted;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SparseMatrix64F(final long rows, final long cols) {
        this.rows = rows;
        this.cols = cols;
        this.lock = new ReentrantLock(true);
    }

    // Copy Constructor
    private SparseMatrix64F(SparseMatrix64F m) {
        this(m.rows(), m.cols());
        m.data.forEachEntry((k, v) -> {
            this.data.put(k, v);
            return true;
        });
    }

    // Copy Constructor
    private SparseMatrix64F(SparseMatrix64F m, long rows, long cols) {
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

    private void sortData() {
        long[] keys = this.data.keys();
        Arrays.sort(keys);
        TLongDoubleHashMap sortedData = new TLongDoubleHashMap();
        for (int i = 0; i < keys.length; i++) {
            sortedData.put(keys[i], this.data.get(keys[i]));
            this.data.remove(keys[i]);
        }
        this.data = sortedData;
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
        return data.keys().length * Double.BYTES;
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
    public Matrix64F copy() {
        return new SparseMatrix64F(this);
    }

    @Override
    public Matrix64F copy(long rows, long cols) {
        return new SparseMatrix64F(this, rows, cols);
    }

    // ---------------------------------------------------
    // SETTER.
    // ---------------------------------------------------

    @Override
    public void set(long row, long col, Double value) {
        checkArgument(row < this.rows(),
            String.format("Row index %d is out of bounds for Matrix of size(%d, %d)", row, this.rows(), this.cols()));
        checkArgument(col < this.cols(),
            String.format("Column index %d is out of bounds for Matrix of size(%d, %d)", col, this.rows(), this.cols()));
        long key = Utils.getPos(row, col, this);
        if (value == this.data.getNoEntryValue()) {
            if (this.data.containsKey(key))
                this.data.remove(key);
        } else {
            this.data.put(key, value);
            this.sorted = false;
        }
    }

    @Override
    public Matrix64F setDiagonalsToZero() {
        return this.setDiagonalsToZero(this.copy());
    }

    @Override
    public Matrix64F setDiagonalsToZero(Matrix<Double> m) {
        if (m instanceof SparseMatrix64F)
            return this.setDiagonalsToZero((SparseMatrix64F) m);

        long diag = 0;
        while(diag < rows && diag < cols) {
            m.set(diag, diag, 0.0);
            diag++;
        }
        return (Matrix64F) m;
    }

    @Override
    public void setArray(Object data) {
        double[] mData = (double[]) data;
        checkState(mData.length == this.rows * this.cols);
        logger.info("Warning! 'setArray' is potentially a very expensive operation.");
        this.data = new TLongDoubleHashMap();
        for (int i = 0; i < mData.length; i++) {
            if (mData[i] != 0F)
                this.data.put(i, mData[i]);
        }
    }

    // ---------------------------------------------------
    // GETTER.
    // ---------------------------------------------------

    @Override
    public Double get(long index) {
        checkArgument(index < rows() * cols());
        Double value = data.get(Utils.toInt(index));
        return (value == null) ? 0f : value;
    }

    @Override
    public Double get(long row, long col) {
        final Double value = data.get(Utils.getPos(row, col, this));
        return (value == null) ? 0. : value;
    }

    @Override
    public Matrix64F getRow(long row) {
        return this.getRow(row, 0, this.cols);
    }

    @Override
    public Matrix64F getRow(long row, long from, long to) {
        Matrix64F result = new SparseMatrix64F(1, to - from);
        for (long col = from; col < to; col++)
            if (this.data.containsKey(Utils.getPos(row, col, this)))
                result.set(0, col, this.get(row, col));
        return result;
    }

    @Override
    public Matrix64F getCol(long col) {
        return this.getCol(col, 0, this.rows);
    }

    @Override
    public Matrix64F getCol(long col, long from, long to) {
        Matrix64F result = new SparseMatrix64F(to - from, 1);
        for (long row = from; row < to; row++)
            if (this.data.containsKey(Utils.getPos(row, col, this)))
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
    public Matrix64F applyOnElements(UnaryOperator<Double> d) {
        return this.applyOnElements(d, new SparseMatrix64F(this.rows, this.cols));
    }

    @Override
    public Matrix64F applyOnElements(UnaryOperator<Double> d, Matrix<Double> m) {
        Utils.checkShapeEqual(this, m);
        logger.info("Warning! 'applyOnElements' is potentially a very expensive operation.");
        for (int i = 0; i < m.rows(); i++) {
            for (int j = 0; j < m.cols(); j++) {
                m.set(i, j, d.apply(this.get(i, j)));
            }
        }
        return (Matrix64F) m;
    }

    @Override
    public Matrix64F applyOnElements(Matrix<Double> m, BinaryOperator<Double> d) {
        return applyOnElements(m, d, new SparseMatrix64F(this.rows, this.cols));
    }

    @Override
    public Matrix64F applyOnElements(Matrix<Double> m, BinaryOperator<Double> d, Matrix<Double> result) {
        Utils.checkShapeEqual(this, m, result);
        logger.info("Warning! 'applyOnElements' is potentially a very expensive operation.");
        for (int i = 0; i < this.rows(); i++) {
            for (int j = 0; j < this.cols(); j++) {
                result.set(i, j, d.apply(this.get(i, j), m.get(i, j)));
            }
        }
        return (Matrix64F) result;
    }

    @Override
    public Matrix64F applyOnElements(MatrixElementUnaryOperator<Double> d) {
        return this.applyOnElements(d, new SparseMatrix64F(this.rows, this.cols));
    }

    @Override
    public Matrix64F applyOnElements(MatrixElementUnaryOperator<Double> d, Matrix<Double> m) {
        Utils.checkShapeEqual(this, m);
        logger.info("Warning! 'applyOnElements' is potentially a very expensive operation.");
        for (int i = 0; i < m.rows(); i++) {
            for (int j = 0; j < m.cols(); j++) {
                m.set(i, j, d.apply(i, j, this.get(i, j)));
            }
        }
        return (Matrix64F) m;
    }

    @Override
    public Matrix64F applyOnNonZeroElements(MatrixElementUnaryOperator<Double> d) {
        return applyOnNonZeroElements(d, new SparseMatrix64F(this.rows, this.cols));
    }

    @Override
    public Matrix64F applyOnNonZeroElements(MatrixElementUnaryOperator<Double> d, Matrix<Double> m) {
        Utils.checkShapeEqual(this, m);
        SparseMatrix64F sm64f = (SparseMatrix64F) m;
        this.data.forEachEntry((k, v) -> {
            if (v != 0.0) {
                long row = this.findRowFromIndex(k);
                long col = this.findColFromIndex(k);
                sm64f.set(row, col, d.apply(row, col, v));
            }
            return true;
        });
        return sm64f;
    }

    // ---------------------------------------------------
    // ASSIGN.
    // ---------------------------------------------------

    @Override
    public Matrix64F assign(Double d) {
        logger.info("Warning! 'assign(double)' is potentially a very expensive operation.");
        for (int i = 0; i < rows * cols; i++)
            this.data.put(i, d);
        return this;
    }

    @Override
    public Matrix64F assign(Matrix<Double> m) {
        checkState(m.rows() * m.cols() == this.rows * this.cols);
        logger.info("Warning! 'assign(double)' is potentially a very expensive operation.");
        for (int row = 0; row < m.rows(); row++)
            for (int col = 0; col < m.cols(); col++)
                this.set(row, col, m.get(row, col));
        return this;
    }

    @Override
    public Matrix64F assignRow(long row, Matrix<Double> m) {
        checkState(m.rows() == 1 && row < this.rows && m.cols() == this.cols);
        for (long col = 0; col < this.cols; col++)
            this.set(row, col, m.get(0, col));
        return this;
    }

    @Override
    public Matrix64F assignColumn(long col, Matrix<Double> m) {
        checkState(m.cols() == 1 && col < this.cols && m.rows() == this.rows);
        for (long row = 0; row < this.rows; row++)
            this.set(row, col, m.get(row, 0));
        return this;
    }

    @Override
    public Matrix64F assign(long rowOffset, long colOffset, Matrix<Double> m) {
        checkState(this.rows > rowOffset + m.rows() && this.cols > colOffset + m.cols());
        logger.info("Warning! 'assign(double)' is potentially a very expensive operation.");
        for (long col = colOffset; col < m.cols(); col++)
            this.set(rowOffset, col, m.get(rowOffset, col));
        return this;
    }

    // ---------------------------------------------------
    // AGGREGATION.
    // ---------------------------------------------------

    @Override
    public Double aggregate(BinaryOperator<Double> combiner, UnaryOperator<Double> mapper, Matrix<Double> result) {
        throw new NotImplementedException("TODO: Need to implement 'aggregate' method");
    }

    @Override
    public Matrix64F aggregateRows(MatrixAggregation<Double> d) {
        return aggregateRows(d, new SparseMatrix64F(this.rows, 1));
    }

    @Override
    public Matrix64F aggregateRows(MatrixAggregation<Double> d, Matrix<Double> result) {
        checkArgument(result.rows() == this.rows && result.cols() == 1);
        for (int row = 0; row < this.rows; row++) {
            result.set(row, 0, d.apply(this.getRow(row)));
        }
        return (Matrix64F) result;
    }

    @Override
    public Double sum() {
        Double sum = 0.0;
        for (Double value : this.data.values())
            sum += value;
        return sum;
    }

    // ---------------------------------------------------
    // ARITHMETIC.
    // ---------------------------------------------------

    @Override
    public Matrix64F add(Matrix<Double> m) {
        return this.add(m, new SparseMatrix64F(this.rows, this.cols));
    }

    @Override
    public Matrix64F add(Matrix<Double> m, Matrix<Double> result) {
        Utils.checkShapeEqual(this, m, result);
        return this.applyOnElements(m, (v1, v2) -> v1 + v2, result);
    }

    @Override
    public Matrix64F addVectorToRows(Matrix<Double> m) {
        return this.addVectorToRows(m, new SparseMatrix64F(this.rows, this.cols));
    }

    @Override
    public Matrix64F addVectorToRows(Matrix<Double> m, Matrix<Double> result) {
        Utils.checkApplyVectorToRows(this, m, result);
        for (int row = 0; row < this.rows; row++) {
            for (int col = 0; col < this.cols; col++) {
                result.set(row, col, this.get(row, col) + m.get(0, col));
            }
        }
        return (Matrix64F) result;
    }

    @Override
    public Matrix64F addVectorToCols(Matrix<Double> m) {
        return this.addVectorToCols(m, new SparseMatrix64F(this.rows, this.cols));
    }

    @Override
    public Matrix64F addVectorToCols(Matrix<Double> v, Matrix<Double> B) {
        return null;
    }

    @Override
    public Matrix64F sub(Matrix<Double> m) {
        return this.sub(m, new SparseMatrix64F(this.rows, this.cols));
    }

    @Override
    public Matrix64F sub(Matrix<Double> m, Matrix<Double> result) {
        Utils.checkShapeEqual(this, m, result);
        return this.applyOnElements(m, (v1, v2) -> v1 - v2, result);
    }

    @Override
    public Matrix64F mul(Matrix<Double> m) {
        return this.mul(m, new SparseMatrix64F(this.rows, this.cols));
    }

    @Override
    public Matrix64F mul(Matrix<Double> m, Matrix<Double> result) {
        Utils.checkShapeEqual(this, m, result);
        SparseMatrix64F sm64f = (SparseMatrix64F) m;
        sm64f.data.forEachEntry((k, v) -> {
            long row = this.findRowFromIndex(k), col = this.findColFromIndex(k);
            result.set(row, col, this.get(k) * v);
            return true;
        });
        return (Matrix64F) result;
    }

    @Override
    public Matrix64F scale(Double d) {
        return this.scale(d, new SparseMatrix64F(this.rows, this.cols));
    }

    @Override
    public Matrix64F scale(Double d, Matrix<Double> result) {
        this.data.forEachEntry((k, v) -> {
            long row = this.findRowFromIndex(k), col = this.findColFromIndex(k);
            result.set(row, col, v * d);
            return true;
        });
        return (Matrix64F) result;
    }

    @Override
    public Matrix64F transpose() {
        return this.transpose(new SparseMatrix64F(this.rows, this.cols));
    }

    @Override
    public Matrix64F transpose(Matrix<Double> result) {
        Utils.checkShapeTranspose(this, result);
        for (int row = 0; row < this.rows; row++)
            for (int col = 0; col < this.cols; col++)
                result.set(row, col, this.get(col, row));
        return (Matrix64F) result;
    }

    @Override
    public Matrix64F invert() {
        return this.invert(new SparseMatrix64F(this.rows, this.cols));
    }

    @Override
    public Matrix64F invert(Matrix<Double> B) {
        throw new NotImplementedException("TODO: Need to implement 'invert' method");
    }

    @Override
    public Double norm(int p) {
        double norm = 0;
        for (int row = 0; row < this.rows; row++) {
            for (int col = 0; col < this.cols; col++) {
                norm += Math.pow(this.get(row, col), p);
            }
        }
        return (Double) Math.pow(norm, 1./p);
    }

    @Override
    public Double dot(Matrix<Double> m) {
        Utils.checkDotProduct(this, m);
        Double result = 0.0;
        for (long key : data.keys()) {
            result += this.get(key) * m.get(key);
        }
        return result;
    }

    // ---------------------------------------------------
    // SLICING.
    // ---------------------------------------------------

    @Override
    public Matrix64F subMatrix(long rowOffset, long colOffset, long rows, long cols) {
        checkArgument(rowOffset < this.rows && this.rows >= rows + rowOffset);
        checkArgument(colOffset < this.cols && this.cols >= cols + colOffset);
        SparseMatrix64F sm64f = new SparseMatrix64F(rows, cols);
        for (long key : this.data.keys()) {
            long row = this.findRowFromIndex(key), col = this.findColFromIndex(key);
            if (rowOffset <= row && row <= rows && colOffset <= col && col <= cols)
                sm64f.set(row - rowOffset, col - colOffset, this.data.get(key));
        }
        return sm64f;
    }

    @Override
    public Matrix64F concat(Matrix<Double> m) {
        checkArgument(this.cols == m.cols());
        return concat(m, new SparseMatrix64F(this.rows + m.rows(), this.cols));
    }

    @Override
    public Matrix64F concat(Matrix<Double> m, Matrix<Double> result) {
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
        return (Matrix64F) result;
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

    private static class RowIterator implements Matrix64F.RowIterator {

        protected SparseMatrix64F self;
        protected final long end;
        protected final long start;
        protected long currentRow;
        protected final long rowsToFetch;
        protected long rowsFetched;
        protected RandomDataGenerator rand;

        private RowIterator(final SparseMatrix64F v, final long startRow, final long endRow) {
            this.self = v;
            checkArgument(startRow >= 0 && startRow < self.rows());
            checkArgument(endRow >= startRow && endRow <= self.rows());
            this.self.sortData();
            this.start = startRow;
            this.end = endRow;
            this.rowsToFetch = endRow - startRow;
            this.rand = new RandomDataGenerator();
            this.reset();
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
        public Double value(final long col) {
            return self.get(currentRow, col);
        }

        @Override
        public Matrix64F get() {
            return self.getRow(rowNum());
        }

        @Override
        public Matrix64F get(final long from, final long size) {
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
