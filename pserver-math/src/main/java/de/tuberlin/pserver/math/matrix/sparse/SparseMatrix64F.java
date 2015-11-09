package de.tuberlin.pserver.math.matrix.sparse;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.Matrix64F;
import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;
import de.tuberlin.pserver.math.utils.Utils;
import gnu.trove.map.hash.TLongDoubleHashMap;
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
    // Private Utility Methods.
    // ---------------------------------------------------

    private long findRowFromIndex(long index) {
        return index / this.cols;
    }

    private long findColFromIndex(long index) {
        return index % this.cols;
    }

    private void sort() {
        long[] keys = new long[this.data.size()];
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
    public void set(final long row, final long col, final Double value) {
        checkArgument(row < this.rows(),
                String.format("Row index %d is out of bounds for Matrix of size(%d, %d)", row, this.rows(), this.cols()));
        checkArgument(col < this.cols(),
                String.format("Column index %d is out of bounds for Matrix of size(%d, %d)", col, this.rows(), this.cols()));
        int key = Utils.getPos(row, col, this);
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

    // TODO: Discuss casting here, what about Dense Matraces
    @Override
    public Matrix64F setDiagonalsToZero(Matrix m) {
        SparseMatrix64F sm64f = (SparseMatrix64F) m;
        sm64f.data.forEachEntry((k, v) -> {
            if (sm64f.findRowFromIndex(k) == sm64f.findColFromIndex(k))
                sm64f.data.remove(k);
            return true;
        });
        return sm64f;
    }

    // TODO: Confirm that this is correct
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

    // TODO: cannot implement checkArgument otherwise assignRow does not work
    @Override
    public Double get(long row, long col) {
        /*checkArgument(row < rows(),
                String.format("Row index %d is out of bounds for Matrix of size(%d, %d)", row, rows(), cols()));
        checkArgument(col < cols(),
                String.format("Column index %d is out of bounds for Matrix of size(%d, %d)", col, rows(), cols()));*/
        Double value = data.get(Utils.getPos(row, col, this));
        return (value == null) ? 0 : value;
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

    // TODO: Determine if this is a valid method for sparse matrices
    // TODO: Keys will not be preserved
    @Override
    public Object toArray() {
        return this.data.values();
    }

    // ---------------------------------------------------
    // APPLY ON ELEMENTS.
    // ---------------------------------------------------

    @Override
    public Matrix64F applyOnElements(UnaryOperator<Double> d) {
        return this.applyOnElements(d, new SparseMatrix64F(this.rows, this.cols));
    }

    @Override
    public Matrix64F applyOnElements(UnaryOperator<Double> f, Matrix<Double> m) {
        Utils.checkShapeEqual(this, m);
        logger.info("Warning! 'applyOnElements' is potentially a very expensive operation.");
        for (int i = 0; i < m.rows(); i++) {
            for (int j = 0; j < m.cols(); j++) {
                m.set(i, j, f.apply(this.get(i, j)));
            }
        }
        return (Matrix64F) m;
    }

    @Override
    public Matrix64F applyOnElements(Matrix<Double> m, BinaryOperator<Double> f) {
        return applyOnElements(m, f, new SparseMatrix64F(this.rows, this.cols));
    }

    @Override
    public Matrix64F applyOnElements(Matrix<Double> m, BinaryOperator<Double> f, Matrix<Double> result) {
        Utils.checkShapeEqual(this, m, result);
        logger.info("Warning! 'applyOnElements' is potentially a very expensive operation.");
        for (int i = 0; i < this.rows(); i++) {
            for (int j = 0; j < this.cols(); j++) {
                result.set(i, j, f.apply(this.get(i, j), m.get(i, j)));
            }
        }
        return (Matrix64F) result;
    }

    @Override
    public Matrix64F applyOnElements(MatrixElementUnaryOperator<Double> f) {
        return this.applyOnElements(f, new SparseMatrix64F(this.rows, this.cols));
    }

    @Override
    public Matrix64F applyOnElements(MatrixElementUnaryOperator<Double> f, Matrix<Double> m) {
        Utils.checkShapeEqual(this, m);
        logger.info("Warning! 'applyOnElements' is potentially a very expensive operation.");
        for (int i = 0; i < m.rows(); i++) {
            for (int j = 0; j < m.cols(); j++) {
                m.set(i, j, f.apply(i, j, this.get(i, j)));
            }
        }
        return (Matrix64F) m;
    }

    @Override
    public Matrix64F applyOnNonZeroElements(MatrixElementUnaryOperator<Double> f) {
        return applyOnNonZeroElements(f, new SparseMatrix64F(this.rows, this.cols));
    }

    @Override
    public Matrix64F applyOnNonZeroElements(MatrixElementUnaryOperator<Double> f, Matrix<Double> m) {
        Utils.checkShapeEqual(this, m);
        SparseMatrix64F sm64f = (SparseMatrix64F) m;
        this.data.forEachEntry((k, v) -> {
            if (v != 0.0) {
                long row = this.findRowFromIndex(k);
                long col = this.findColFromIndex(k);
                sm64f.set(row, col, f.apply(row, col, v));
            }
            return true;
        });
        return sm64f;
    }

    // ---------------------------------------------------
    // ASSIGN.
    // ---------------------------------------------------

    // TODO: Confirm functionality
    // TODO: Apply to all elements or only to existing elements?
    @Override
    public Matrix64F assign(Double d) {
        logger.info("Warning! 'assign(double)' is potentially a very expensive operation.");
        /*for (long key : this.data.keys())
            this.data.put(key, f);*/
        for (int i = 0; i < rows * cols; i++)
            this.data.put(i, d);
        return this;
    }

    // TODO: Confirm functionality
    // TODO: m could be a Dense Matrix, therefore cant use key/value pairs
    @Override
    public Matrix64F assign(Matrix<Double> m) {
        checkState(m.rows() * m.cols() == this.rows * this.cols);
        logger.info("Warning! 'assign(double)' is potentially a very expensive operation.");
        for (int row = 0; row < m.rows(); row++)
            for (int col = 0; col < m.cols(); col++)
                this.set(row, col, m.get(row, col));
        return this;
    }

    // TODO: Confirm functionality
    // TODO: m could be a Dense Matrix, therefore cant use key/value pairs
    @Override
    public Matrix64F assignRow(long row, Matrix<Double> m) {
        checkState(m.rows() == 1 && row < this.rows && m.cols() == this.cols);
        for (long col = 0; col < this.cols; col++)
            this.set(row, col, m.get(0, col));
        return this;
    }

    // TODO: Confirm functionality
    // TODO: m could be a Dense Matrix, therefore cant use key/value pairs
    @Override
    public Matrix64F assignColumn(long col, Matrix<Double> m) {
        checkState(m.cols() == 1 && col < this.cols && m.rows() == this.rows);
        for (long row = 0; row < this.rows; row++)
            this.set(row, col, m.get(row, 0));
        return this;
    }

    /* TODO: Ask Tobias about functionality, assign only the values in m to this
       TODO: or also remove existing values not in m */
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

    // TODO: Still needs to be implemented
    @Override
    public Double aggregate(BinaryOperator<Double> combiner, UnaryOperator<Double> mapper, Matrix<Double> result) {
        return 0.0;
    }

    @Override
    public Matrix64F aggregateRows(MatrixAggregation<Double> d) {
        return aggregateRows(d, new SparseMatrix64F(this.rows, 1));
    }

    @Override
    public Matrix64F aggregateRows(MatrixAggregation<Double> f, Matrix<Double> result) {
        checkArgument(result.rows() == this.rows && result.cols() == 1);
        for (int row = 0; row < this.rows; row++) {
            result.set(row, 0, f.apply(this.getRow(row)));
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

    // TODO: Need to look at the efficiency of this, can it be done better
    // TODO: what about Dense Matrices
    @Override
    public Matrix64F add(Matrix<Double> m, Matrix<Double> r) {
        Utils.checkShapeEqual(this, m, r);
        return this.applyOnElements(m, (v1, v2) -> v1 + v2, r);
    }

    @Override
    public Matrix64F addVectorToRows(Matrix<Double> v) {
        return this.addVectorToRows(v, new SparseMatrix64F(this.rows, this.cols));
    }

    // TODO: Need to look at the efficiency of this, can it be done better
    @Override
    public Matrix64F addVectorToRows(Matrix<Double> v, Matrix<Double> result) {
        Utils.checkApplyVectorToRows(this, v, result);
        for (int row = 0; row < this.rows; row++) {
            for (int col = 0; col < this.cols; col++) {
                result.set(row, col, this.get(row, col) + v.get(0, col));
            }
        }
        return (Matrix64F) result;
        /*this.data.forEachEntry((k, v) -> {
            long row = this.findRowFromIndex(k), col = this.findColFromIndex(k);
            result.set(row, col, v + m.get(0, col));
            return true;
        });
        return (Matrix64F) result;*/
    }

    @Override
    public Matrix64F addVectorToCols(Matrix<Double> v) {
        return this.addVectorToCols(v, new SparseMatrix64F(this.rows, this.cols));
    }

    // TODO: Need to look at the efficiency of this, can it be done better
    @Override
    public Matrix64F addVectorToCols(Matrix<Double> v, Matrix<Double> result) {
        Utils.checkApplyVectorToCols(this, v, result);
        for (int col = 0; col < this.cols; col++) {
            for (int row = 0; row < this.rows; row++) {
                result.set(row, col, this.get(row, col) + v.get(row, 0));
            }
        }
        return (Matrix64F) result;
    }

    @Override
    public Matrix64F sub(Matrix<Double> B) {
        return this.sub(B, new SparseMatrix64F(this.rows, this.cols));
    }

    // TODO: Need to look at the efficiency of this, can it be done better
    @Override
    public Matrix64F sub(Matrix<Double> m, Matrix<Double> result) {
        Utils.checkShapeEqual(this, m, result);
        return this.applyOnElements(m, (v1, v2) -> v1 - v2, result);
    }

    @Override
    public Matrix64F mul(Matrix<Double> B) {
        return this.mul(B, new SparseMatrix64F(this.rows, this.cols));
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
    public Matrix64F scale(Double a) {
        return this.scale(a, new SparseMatrix64F(this.rows, this.cols));
    }

    @Override
    public Matrix64F scale(Double a, Matrix<Double> result) {
        this.data.forEachEntry((k, v) -> {
            long row = this.findRowFromIndex(k), col = this.findColFromIndex(k);
            result.set(row, col, v * a);
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

    // TODO: This needs to be implemented
    @Override
    public Matrix64F invert(Matrix<Double> B) {
        return null;
    }

    // TODO: This needs to be looked at
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

    // TODO: confirm functionality, ie. just copy over key-value pairs
    // TODO: or ensure every element is the same
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
        /*SparseMatrix64F sm64f = (SparseMatrix64F) m;
        SparseMatrix64F result = (SparseMatrix64F) r;
        result.data.putAll(this.data);
        result.data.putAll(sm64f.data);*/
        return (Matrix64F) result;
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

    // ---------------------------------------------------
    // INNER CLASSES.
    // ---------------------------------------------------

    public static class RowIterator implements Matrix64F.RowIterator {

        protected SparseMatrix64F self;
        protected final long end;
        protected final long start;
        protected long currentRow;
        protected final long rowsToFetch;
        protected long rowsFetched;
        protected RandomDataGenerator rand;

        public RowIterator(final SparseMatrix64F v) {
            this(v, 0, (int) checkNotNull(v).rows());
        }

        public RowIterator(final SparseMatrix64F v, final long startRow, final long endRow) {
            this.self = v;
            checkArgument(startRow >= 0 && startRow < self.rows());
            checkArgument(endRow >= startRow && endRow <= self.rows());
            this.start = startRow;
            this.end = endRow;
            this.rowsToFetch = endRow - startRow;
            this.rand = new RandomDataGenerator();
            this.reset();
        }

        @Override
        public boolean hasNext() {
            return this.rowsFetched < this.rowsToFetch;
        }

        @Override
        public void next() {
            // the generic case is just currentRow++, but the reset method only sets rowsFetched = 0
            if (this.rowsFetched == 0) this.currentRow = 0;
            else this.currentRow++;

            this.rowsFetched++;
            // can overflow if nextRandom and next is called alternatingly
            if (this.currentRow >= this.end) this.currentRow = this.start;
        }

        @Override
        public void nextRandom() {
            this.rowsFetched++;
            this.currentRow = this.rand.nextLong(this.start, this.end - 1);
        }

        @Override
        public Double value(final long col) {
            return this.self.get(this.currentRow, col);
        }

        @Override
        public Matrix64F get() {
            return this.self.getRow(rowNum());
        }

        @Override
        public Matrix64F get(final long from, final long size) {
            return this.self.getRow(rowNum(), from, from + size);
        }

        @Override
        public void reset() {
            this.rowsFetched = 0;
        }

        @Override
        public long size() {
            return this.rowsToFetch;
        }

        @Override
        public long rowNum() {
            return this.currentRow;
        }
    }
}
