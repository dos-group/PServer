package de.tuberlin.pserver.types.matrix.implementation.f32.sparse;


import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.f32.operations.BinaryOperator32;
import de.tuberlin.pserver.types.matrix.implementation.f32.operations.MatrixAggregation32;
import de.tuberlin.pserver.types.matrix.implementation.f32.operations.MatrixElementUnaryOperator32;
import de.tuberlin.pserver.types.matrix.implementation.f32.operations.UnaryOperator32;
import de.tuberlin.pserver.types.matrix.implementation.partitioner.PartitionType;
import de.tuberlin.pserver.types.matrix.metadata.AbstractDistributedMatrixType;
import de.tuberlin.pserver.types.metadata.InternalData;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

public class SparseMatrix32F extends AbstractDistributedMatrixType implements Matrix32F {

    // ---------------------------------------------------
    // Logger.
    // ---------------------------------------------------

    static final Logger logger = LoggerFactory.getLogger(SparseMatrix32F.class);

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public TLongFloatMap data;

    private boolean areSortedKeysCreated = false;

    public long[] sortedKeys;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SparseMatrix32F(long globalRows, long globalCols) {
        this(-1, null, PartitionType.NO_PARTITIONER, globalRows, globalCols, null);
    }

    public SparseMatrix32F(long globalRows, long globalCols, final float[] data) {
        this(-1, null, PartitionType.NO_PARTITIONER, globalRows, globalCols, data);
    }

    // Copy Constructor
    private SparseMatrix32F(SparseMatrix32F m) {
        this(-1, null, PartitionType.NO_PARTITIONER, m.rows(), m.cols(), null);
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

    public SparseMatrix32F(int nodeID, int[] nodes, PartitionType partitionType, long globalRows, long globalCols, final float[] data) {
        super(nodeID, nodes, partitionType, globalRows, globalCols);
        this.data = new TLongFloatHashMap((int)(rows() * cols() * 0.1));
    }

    // ---------------------------------------------------
    // Distributed Type Metadata.
    // ---------------------------------------------------

    @Override public long sizeOf() { return data.size() * Float.BYTES; }

    @Override public long globalSizeOf() { throw new UnsupportedOperationException(); }

    @SuppressWarnings("unchecked")
    @Override public InternalData<TLongFloatMap> internal() { return new InternalData<>(data); };

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private long findRowFromIndex(long index) {
        return index / this.cols();
    }

    private long findColFromIndex(long index) {
        return index % this.cols();
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
    // SETTER.
    // ---------------------------------------------------

    @Override
    public void set(final long row, final long col, final float value) {
        long key = row * cols() + col;
        if (value == this.data.getNoEntryValue()) {
            if (this.data.containsKey(key))
                this.data.remove(key);
        }
        else
            this.data.put(key, value);
    }

    public void set(final long index, final float value) {
        if (value == this.data.getNoEntryValue()) {
            if (this.data.containsKey(index))
                this.data.remove(index);
        }
        else
            this.data.put(index, value);
    }

    @Override
    public Matrix32F setDiagonalsToZero() {
        return this.setDiagonalsToZero(this.copy());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Matrix32F setDiagonalsToZero(Matrix32F m) {
        if (m instanceof SparseMatrix32F)
            return this.setDiagonalsToZero((SparseMatrix32F) m);

        long diag = 0;
        while(diag < rows() && diag < cols()) {
            m.set(diag, diag, 0F);
            diag++;
        }
        return  m;
    }

    public Matrix32F setDiagonalsToZero(SparseMatrix32F sm32f) {
        sm32f.data.forEachEntry((k, v) -> {
            if (sm32f.findRowFromIndex(k) == sm32f.findColFromIndex(k))
                sm32f.data.remove(k);
            return true;
        });
        return sm32f;
    }

    // ---------------------------------------------------
    // GETTER.
    // ---------------------------------------------------

    @Override
    public float get(final long index) {
        checkArgument(index < rows() * cols());
        Float value = data.get(index);
        return (value == null) ? 0f : value;
    }

    @Override
    public float get(final long row, final long col) {
        final Float value = data.get((row * cols() + col));
        return (value == null) ? 0f : value;
    }

    @Override
    public Matrix32F getRow(long row) {
        SparseMatrix32F result = new SparseMatrix32F(1, cols());
        data.forEachKey(k -> {
            if (k >= row * cols() + 0 && k < row * cols() + cols())
                result.set(k - (row * cols()), this.get(k));
            return true;
        });
        return result;
    }

    @Override
    public Matrix32F getRow(long row, long from, long to) {
        SparseMatrix32F result = new SparseMatrix32F(1, to - from);
        //for (long col = from; col < to; col++)
        //    if (this.data.containsKey(row * cols + col))
        //        result.set(0, col, this.get(row, col));

        data.forEachEntry((k, v) -> {
            if (k >= row * cols() + from && k < row * cols() + to)
                result.set(k - (row * cols()), v);
            return true;
        });

        return result;
    }

    @Override
    public Matrix32F getCol(long col) {
        return this.getCol(col, 0, this.rows());
    }

    @Override
    public Matrix32F getCol(long col, long from, long to) {
        Matrix32F result = new SparseMatrix32F(to - from, 1);
        for (long row = from; row < to; row++)
            if (this.data.containsKey(row * cols() + col))
                result.set(row, 0, this.get(row, col));
        return result;
    }

    // ---------------------------------------------------
    // APPLY ON ELEMENTS.
    // ---------------------------------------------------

    @Override
    public Matrix32F applyOnElements(UnaryOperator32 f) {
        return this.applyOnElements(f, new SparseMatrix32F(this.rows(), this.cols()));
    }

    @Override
    public Matrix32F applyOnElements(UnaryOperator32 f, Matrix32F m) {
        logger.info("Warning! 'applyOnElements' is potentially a very expensive operation.");
        for (int i = 0; i < m.rows(); i++) {
            for (int j = 0; j < m.cols(); j++) {
                m.set(i, j, f.apply(this.get(i, j)));
            }
        }
        return  m;
    }

    @Override
    public Matrix32F applyOnElements(Matrix32F m, BinaryOperator32 f) {
        return applyOnElements(m, f, new SparseMatrix32F(this.rows(), this.cols()));
    }

    @Override
    public Matrix32F applyOnElements(Matrix32F m, BinaryOperator32 f, Matrix32F result) {
        logger.info("Warning! 'applyOnElements' is potentially a very expensive operation.");
        for (int i = 0; i < this.rows(); i++) {
            for (int j = 0; j < this.cols(); j++) {
                result.set(i, j, f.apply(this.get(i, j), m.get(i, j)));
            }
        }
        return  result;
    }

    @Override
    public Matrix32F applyOnElements(MatrixElementUnaryOperator32 f) {
        return this.applyOnElements(f, new SparseMatrix32F(this.rows(), this.cols()));
    }

    @Override
    public Matrix32F applyOnElements(MatrixElementUnaryOperator32 f, Matrix32F m) {
        logger.info("Warning! 'applyOnElements' is potentially a very expensive operation.");
        for (int i = 0; i < m.rows(); i++) {
            for (int j = 0; j < m.cols(); j++) {
                m.set(i, j, f.apply(i, j, this.get(i, j)));
            }
        }
        return  m;
    }

    @Override
    public Matrix32F applyOnNonZeroElements(MatrixElementUnaryOperator32 f) {
        return applyOnNonZeroElements(f, new SparseMatrix32F(this.rows(), this.cols()));
    }

    @Override
    public Matrix32F applyOnNonZeroElements(MatrixElementUnaryOperator32 f, Matrix32F m) {
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
    public Matrix32F assign(float f) {
        logger.info("Warning! 'assign(float)' is potentially a very expensive operation.");
        for (int i = 0; i < rows() * cols(); i++)
            this.data.put(i, f);
        return this;
    }

    @Override
    public Matrix32F assign(Matrix32F m) {
        logger.info("Warning! 'assign(float)' is potentially a very expensive operation.");
        for (int row = 0; row < m.rows(); row++)
            for (int col = 0; col < m.cols(); col++)
                this.set(row, col, m.get(row, col));
        return this;
    }

    @Override
    public Matrix32F assignRow(long row, Matrix32F m) {
        for (long col = 0; col < this.cols(); col++)
            this.set(row, col, m.get(0, col));
        return this;
    }

    @Override
    public Matrix32F assignColumn(long col, Matrix32F m) {
        for (long row = 0; row < this.rows(); row++)
            this.set(row, col, m.get(row, 0));
        return this;
    }

    @Override
    public Matrix32F assign(long rowOffset, long colOffset, Matrix32F m) {
        logger.info("Warning! 'assign(float)' is potentially a very expensive operation.");
        for (long col = colOffset; col < m.cols(); col++)
            this.set(rowOffset, col, m.get(rowOffset, col));
        return this;
    }

    // ---------------------------------------------------
    // AGGREGATION.
    // ---------------------------------------------------

    @Override
    public float aggregate(BinaryOperator32 combiner, UnaryOperator32 mapper, Matrix32F result) {
        throw new NotImplementedException("TODO: Need to implement 'aggregate' method");
    }

    @Override
    public Matrix32F aggregateRows(MatrixAggregation32 f) {
        return aggregateRows(f, new SparseMatrix32F(this.rows(), 1));
    }

    @Override
    public Matrix32F aggregateRows(MatrixAggregation32 f, Matrix32F result) {
        checkArgument(result.rows() == this.rows() && result.cols() == 1);
        for (int row = 0; row < this.rows(); row++) {
            result.set(row, 0, f.apply(this.getRow(row)));
        }
        return  result;
    }

    @Override
    public float sum() {
        float sum = 0;
        for (float value : this.data.values())
            sum += value;
        return sum;
    }

    // ---------------------------------------------------
    // ARITHMETIC.
    // ---------------------------------------------------

    @Override
    public Matrix32F add(Matrix32F m) {
        return this.add(m, new SparseMatrix32F(this.rows(), this.cols()));
    }

    @Override
    public Matrix32F add(Matrix32F m, Matrix32F result) {
        return this.applyOnElements(m, (v1, v2) -> v1 + v2, result);
    }

    @Override
    public Matrix32F addVectorToRows(Matrix32F v) {
        return this.addVectorToRows(v, new SparseMatrix32F(this.rows(), this.cols()));
    }

    @Override
    public Matrix32F addVectorToRows(Matrix32F v, Matrix32F result) {
        for (int row = 0; row < this.rows(); row++) {
            for (int col = 0; col < this.cols(); col++) {
                result.set(row, col, this.get(row, col) + v.get(0, col));
            }
        }
        return  result;
    }

    @Override
    public Matrix32F addVectorToCols(Matrix32F v) {
        return this.addVectorToCols(v, new SparseMatrix32F(this.rows(), this.cols()));
    }

    @Override
    public Matrix32F addVectorToCols(Matrix32F v, Matrix32F result) {
        for (int col = 0; col < this.cols(); col++) {
            for (int row = 0; row < this.rows(); row++) {
                result.set(row, col, this.get(row, col) + v.get(row, 0));
            }
        }
        return  result;
    }

    @Override
    public Matrix32F sub(Matrix32F B) {
        return this.sub(B, new SparseMatrix32F(this.rows(), this.cols()));
    }

    @Override
    public Matrix32F sub(Matrix32F m, Matrix32F result) {
        return this.applyOnElements(m, (v1, v2) -> v1 - v2, result);
    }

    @Override
    public Matrix32F mul(Matrix32F B) {
        return this.mul(B, new SparseMatrix32F(this.rows(), this.cols()));
    }

    @Override
    public Matrix32F mul(Matrix32F m, Matrix32F result) {
        SparseMatrix32F sm32f = (SparseMatrix32F) m;
        sm32f.data.forEachEntry((k, v) -> {
            long row = this.findRowFromIndex(k), col = this.findColFromIndex(k);
            result.set(row, col, this.get(k) * v);
            return true;
        });
        return  result;
    }

    @Override
    public Matrix32F scale(float a) {
        return this.scale(a, new SparseMatrix32F(this.rows(), this.cols()));
    }

    @Override
    public Matrix32F scale(float a, Matrix32F result) {
        this.data.forEachEntry((k, v) -> {
            long row = this.findRowFromIndex(k), col = this.findColFromIndex(k);
            result.set(row, col, v * a);
            return true;
        });
        return  result;
    }

    @Override
    public Matrix32F transpose() {
        return this.transpose(new SparseMatrix32F(this.rows(), this.cols()));
    }

    @Override
    public Matrix32F transpose(Matrix32F result) {
        for (int row = 0; row < this.rows(); row++)
            for (int col = 0; col < this.cols(); col++)
                result.set(row, col, this.get(col, row));
        return  result;
    }

    @Override
    public Matrix32F invert() {
        return this.invert(new SparseMatrix32F(this.rows(), this.cols()));
    }

    @Override
    public Matrix32F invert(Matrix32F B) {
        throw new NotImplementedException("TODO: Need to implement 'invert' method");
    }

    @Override
    public float norm(int p) {
        double norm = 0;
        for (int row = 0; row < this.rows(); row++) {
            for (int col = 0; col < this.cols(); col++) {
                norm += Math.pow(this.get(row, col), p);
            }
        }
        return (float) Math.pow(norm, 1./p);
    }

    @Override
    public Matrix32F copy() {
        return null;
    }

    @Override
    public Matrix32F copy(long rows, long cols) {
        return null;
    }

    @Override
    public float dot(Matrix32F m) {
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
        SparseMatrix32F sm32f = new SparseMatrix32F(rows, cols);
        for (long key : this.data.keys()) {
            long row = this.findRowFromIndex(key), col = this.findColFromIndex(key);
            if (rowOffset <= row && row <= rows && colOffset <= col && col <= cols)
                sm32f.set(row - rowOffset, col - colOffset, this.data.get(key));
        }
        return sm32f;
    }

    @Override
    public Matrix32F concat(Matrix32F m) {
        return concat(m, new SparseMatrix32F(this.rows() + m.rows(), this.cols()));
    }

    @Override
    public Matrix32F concat(Matrix32F m, Matrix32F result) {
        long bRow = 0;
        for (long row = 0; row < result.rows(); row++) {
            if (row < this.rows())
                result.assignRow(row, this.getRow(row));
            else {
                result.assignRow(row, m.getRow(bRow));
                bRow++;
            }
        }
        return  result;
    }

    // ---------------------------------------------------
    // ROW ITERATOR.
    // ---------------------------------------------------

    @Override
    public RowIterator rowIterator() {
        return this.rowIterator(0, this.rows());
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
        public float value(final long col) {
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
