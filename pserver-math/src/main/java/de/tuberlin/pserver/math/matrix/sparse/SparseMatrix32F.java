package de.tuberlin.pserver.math.matrix.sparse;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;
import de.tuberlin.pserver.math.utils.Utils;
import gnu.trove.map.hash.TLongFloatHashMap;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class SparseMatrix32F implements Matrix32F {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final TLongFloatHashMap data = new TLongFloatHashMap();

    private final long rows;

    private final long cols;

    private final Lock lock;

    private Object owner;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SparseMatrix32F(final long rows, final long cols) {
        this.rows = rows;
        this.cols = cols;
        this.lock = new ReentrantLock(true);
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
        return null;
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
        data.put(Utils.getPos(row, col, this), value);
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
        final Float value = data.get(Utils.toInt(index));
        return (value == null) ? 0f : value;
    }

    @Override
    public Float get(final long row, final long col) {
        final Float value = data.get(Utils.getPos(row, col, this));
        return (value == null) ? 0f : value;
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
        return 0f;
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
        return null;
    }

    @Override
    public RowIterator rowIterator(long startRow, long endRow) {
        return null;
    }
}
