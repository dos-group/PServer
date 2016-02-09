package de.tuberlin.pserver.math.matrix32.impl;


import de.tuberlin.pserver.math.matrix32.Matrix32;
import de.tuberlin.pserver.math.matrix32.Matrix32MetaData;
import de.tuberlin.pserver.math.matrix32.operations.BinaryOperator32;
import de.tuberlin.pserver.math.matrix32.operations.MatrixAggregation32;
import de.tuberlin.pserver.math.matrix32.operations.MatrixElementUnaryOperator32;
import de.tuberlin.pserver.math.matrix32.operations.UnaryOperator32;
import de.tuberlin.pserver.math.matrix32.partitioner.PartitionerType;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

public class SparseMatrix32 extends Matrix32MetaData implements Matrix32 {

    // ---------------------------------------------------
    // Logger.
    // ---------------------------------------------------

    static final Logger logger = LoggerFactory.getLogger(SparseMatrix32.class);

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public TLongFloatMap data;

    private boolean areSortedKeysCreated = false;

    public long[] sortedKeys;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SparseMatrix32(PartitionerType type, int nodeID, int[] nodes, long globalRows, long globalCols, final float[] data) {
        super(type, nodeID, nodes, globalRows, globalCols);
        this.data = new TLongFloatHashMap((int)(rows() * cols() * 0.1));
    }

    public SparseMatrix32(long globalRows, long globalCols) {
        this(PartitionerType.NO_PARTITIONER, -1, null, globalRows, globalCols, null);
    }

    public SparseMatrix32(long globalRows, long globalCols, final float[] data) {
        this(PartitionerType.NO_PARTITIONER, -1, null, globalRows, globalCols, data);
    }
    
    // Copy Constructor
    private SparseMatrix32(SparseMatrix32 m) {
        this(PartitionerType.NO_PARTITIONER, -1, null, m.rows(), m.cols(), null);
        m.data.forEachEntry((k, v) -> {
            this.data.put(k, v);
            return true;
        });
    }

    // Copy Constructor
    private SparseMatrix32(SparseMatrix32 m, long rows, long cols) {
        this(rows, cols);
        m.data.forEachEntry((k, v) -> {
            this.data.put(k, v);
            return true;
        });
    }

    // ---------------------------------------------------
    // MetaData Methods.
    // ---------------------------------------------------

    @Override
    public Object toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setArray(Object data) {
        float[] mData = (float[]) data;
        logger.info("Warning! 'setArray' is potentially a very expensive operation.");
        this.data = new TLongFloatHashMap();
        for (int i = 0; i < mData.length; i++) {
            if (mData[i] != 0F) this.data.put(i, mData[i]);
        }
    }

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
    public Matrix32 setDiagonalsToZero() {
        return this.setDiagonalsToZero(this.copy());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Matrix32 setDiagonalsToZero(Matrix32 m) {
        if (m instanceof SparseMatrix32)
            return this.setDiagonalsToZero((SparseMatrix32) m);

        long diag = 0;
        while(diag < rows() && diag < cols()) {
            m.set(diag, diag, 0F);
            diag++;
        }
        return  m;
    }

    public Matrix32 setDiagonalsToZero(SparseMatrix32 sm32f) {
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
    public Matrix32 getRow(long row) {
        SparseMatrix32 result = new SparseMatrix32(1, cols());
        data.forEachKey(k -> {
            if (k >= row * cols() + 0 && k < row * cols() + cols())
                result.set(k - (row * cols()), this.get(k));
            return true;
        });
        return result;
    }

    @Override
    public Matrix32 getRow(long row, long from, long to) {
        SparseMatrix32 result = new SparseMatrix32(1, to - from);
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
    public Matrix32 getCol(long col) {
        return this.getCol(col, 0, this.rows());
    }

    @Override
    public Matrix32 getCol(long col, long from, long to) {
        Matrix32 result = new SparseMatrix32(to - from, 1);
        for (long row = from; row < to; row++)
            if (this.data.containsKey(row * cols() + col))
                result.set(row, 0, this.get(row, col));
        return result;
    }

    // ---------------------------------------------------
    // APPLY ON ELEMENTS.
    // ---------------------------------------------------

    @Override
    public Matrix32 applyOnElements(UnaryOperator32 f) {
        return this.applyOnElements(f, new SparseMatrix32(this.rows(), this.cols()));
    }

    @Override
    public Matrix32 applyOnElements(UnaryOperator32 f, Matrix32 m) {
        logger.info("Warning! 'applyOnElements' is potentially a very expensive operation.");
        for (int i = 0; i < m.rows(); i++) {
            for (int j = 0; j < m.cols(); j++) {
                m.set(i, j, f.apply(this.get(i, j)));
            }
        }
        return  m;
    }

    @Override
    public Matrix32 applyOnElements(Matrix32 m, BinaryOperator32 f) {
        return applyOnElements(m, f, new SparseMatrix32(this.rows(), this.cols()));
    }

    @Override
    public Matrix32 applyOnElements(Matrix32 m, BinaryOperator32 f, Matrix32 result) {
        logger.info("Warning! 'applyOnElements' is potentially a very expensive operation.");
        for (int i = 0; i < this.rows(); i++) {
            for (int j = 0; j < this.cols(); j++) {
                result.set(i, j, f.apply(this.get(i, j), m.get(i, j)));
            }
        }
        return  result;
    }

    @Override
    public Matrix32 applyOnElements(MatrixElementUnaryOperator32 f) {
        return this.applyOnElements(f, new SparseMatrix32(this.rows(), this.cols()));
    }

    @Override
    public Matrix32 applyOnElements(MatrixElementUnaryOperator32 f, Matrix32 m) {
        logger.info("Warning! 'applyOnElements' is potentially a very expensive operation.");
        for (int i = 0; i < m.rows(); i++) {
            for (int j = 0; j < m.cols(); j++) {
                m.set(i, j, f.apply(i, j, this.get(i, j)));
            }
        }
        return  m;
    }

    @Override
    public Matrix32 applyOnNonZeroElements(MatrixElementUnaryOperator32 f) {
        return applyOnNonZeroElements(f, new SparseMatrix32(this.rows(), this.cols()));
    }

    @Override
    public Matrix32 applyOnNonZeroElements(MatrixElementUnaryOperator32 f, Matrix32 m) {
        SparseMatrix32 sm32f = (SparseMatrix32) m;
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
    public Matrix32 assign(float f) {
        logger.info("Warning! 'assign(float)' is potentially a very expensive operation.");
        for (int i = 0; i < rows() * cols(); i++)
            this.data.put(i, f);
        return this;
    }

    @Override
    public Matrix32 assign(Matrix32 m) {
        logger.info("Warning! 'assign(float)' is potentially a very expensive operation.");
        for (int row = 0; row < m.rows(); row++)
            for (int col = 0; col < m.cols(); col++)
                this.set(row, col, m.get(row, col));
        return this;
    }

    @Override
    public Matrix32 assignRow(long row, Matrix32 m) {
        for (long col = 0; col < this.cols(); col++)
            this.set(row, col, m.get(0, col));
        return this;
    }

    @Override
    public Matrix32 assignColumn(long col, Matrix32 m) {
        for (long row = 0; row < this.rows(); row++)
            this.set(row, col, m.get(row, 0));
        return this;
    }

    @Override
    public Matrix32 assign(long rowOffset, long colOffset, Matrix32 m) {
        logger.info("Warning! 'assign(float)' is potentially a very expensive operation.");
        for (long col = colOffset; col < m.cols(); col++)
            this.set(rowOffset, col, m.get(rowOffset, col));
        return this;
    }

    // ---------------------------------------------------
    // AGGREGATION.
    // ---------------------------------------------------

    @Override
    public float aggregate(BinaryOperator32 combiner, UnaryOperator32 mapper, Matrix32 result) {
        throw new NotImplementedException("TODO: Need to implement 'aggregate' method");
    }

    @Override
    public Matrix32 aggregateRows(MatrixAggregation32 f) {
        return aggregateRows(f, new SparseMatrix32(this.rows(), 1));
    }

    @Override
    public Matrix32 aggregateRows(MatrixAggregation32 f, Matrix32 result) {
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
    public Matrix32 add(Matrix32 m) {
        return this.add(m, new SparseMatrix32(this.rows(), this.cols()));
    }

    @Override
    public Matrix32 add(Matrix32 m, Matrix32 result) {
        return this.applyOnElements(m, (v1, v2) -> v1 + v2, result);
    }

    @Override
    public Matrix32 addVectorToRows(Matrix32 v) {
        return this.addVectorToRows(v, new SparseMatrix32(this.rows(), this.cols()));
    }

    @Override
    public Matrix32 addVectorToRows(Matrix32 v, Matrix32 result) {
        for (int row = 0; row < this.rows(); row++) {
            for (int col = 0; col < this.cols(); col++) {
                result.set(row, col, this.get(row, col) + v.get(0, col));
            }
        }
        return  result;
    }

    @Override
    public Matrix32 addVectorToCols(Matrix32 v) {
        return this.addVectorToCols(v, new SparseMatrix32(this.rows(), this.cols()));
    }

    @Override
    public Matrix32 addVectorToCols(Matrix32 v, Matrix32 result) {
        for (int col = 0; col < this.cols(); col++) {
            for (int row = 0; row < this.rows(); row++) {
                result.set(row, col, this.get(row, col) + v.get(row, 0));
            }
        }
        return  result;
    }

    @Override
    public Matrix32 sub(Matrix32 B) {
        return this.sub(B, new SparseMatrix32(this.rows(), this.cols()));
    }

    @Override
    public Matrix32 sub(Matrix32 m, Matrix32 result) {
        return this.applyOnElements(m, (v1, v2) -> v1 - v2, result);
    }

    @Override
    public Matrix32 mul(Matrix32 B) {
        return this.mul(B, new SparseMatrix32(this.rows(), this.cols()));
    }

    @Override
    public Matrix32 mul(Matrix32 m, Matrix32 result) {
        SparseMatrix32 sm32f = (SparseMatrix32) m;
        sm32f.data.forEachEntry((k, v) -> {
            long row = this.findRowFromIndex(k), col = this.findColFromIndex(k);
            result.set(row, col, this.get(k) * v);
            return true;
        });
        return  result;
    }

    @Override
    public Matrix32 scale(float a) {
        return this.scale(a, new SparseMatrix32(this.rows(), this.cols()));
    }

    @Override
    public Matrix32 scale(float a, Matrix32 result) {
        this.data.forEachEntry((k, v) -> {
            long row = this.findRowFromIndex(k), col = this.findColFromIndex(k);
            result.set(row, col, v * a);
            return true;
        });
        return  result;
    }

    @Override
    public Matrix32 transpose() {
        return this.transpose(new SparseMatrix32(this.rows(), this.cols()));
    }

    @Override
    public Matrix32 transpose(Matrix32 result) {
        for (int row = 0; row < this.rows(); row++)
            for (int col = 0; col < this.cols(); col++)
                result.set(row, col, this.get(col, row));
        return  result;
    }

    @Override
    public Matrix32 invert() {
        return this.invert(new SparseMatrix32(this.rows(), this.cols()));
    }

    @Override
    public Matrix32 invert(Matrix32 B) {
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
    public Matrix32 copy() {
        return null;
    }

    @Override
    public Matrix32 copy(long rows, long cols) {
        return null;
    }

    @Override
    public float dot(Matrix32 m) {
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
    public Matrix32 subMatrix(long rowOffset, long colOffset, long rows, long cols) {
        SparseMatrix32 sm32f = new SparseMatrix32(rows, cols);
        for (long key : this.data.keys()) {
            long row = this.findRowFromIndex(key), col = this.findColFromIndex(key);
            if (rowOffset <= row && row <= rows && colOffset <= col && col <= cols)
                sm32f.set(row - rowOffset, col - colOffset, this.data.get(key));
        }
        return sm32f;
    }

    @Override
    public Matrix32 concat(Matrix32 m) {
        return concat(m, new SparseMatrix32(this.rows() + m.rows(), this.cols()));
    }

    @Override
    public Matrix32 concat(Matrix32 m, Matrix32 result) {
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
}
