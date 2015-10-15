package de.tuberlin.pserver.math.matrix.sparse;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix.Layout;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix64F;
import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;
import de.tuberlin.pserver.math.utils.Utils;
import gnu.trove.map.hash.TLongDoubleHashMap;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class SparseMatrix64F implements Matrix64F {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final TLongDoubleHashMap data = new TLongDoubleHashMap();

    private final long rows;

    private final long cols;

    private final Layout layout;

    private final Lock lock;

    private Object owner;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SparseMatrix64F(final long rows, final long cols, final Layout layout) {
        this.rows = rows;
        this.cols = cols;
        this.layout = Preconditions.checkNotNull(layout);
        Preconditions.checkArgument(java.util.Arrays.asList(Layout.values()).contains(layout), "Unknown MemoryLayout: " + layout.toString());
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
        return data.keys().length * Double.BYTES;
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
    public Matrix64F copy() {
        return null;
    }

    @Override
    public Matrix64F copy(long rows, long cols) {
        return null;
    }

    // ---------------------------------------------------
    // SETTER.
    // ---------------------------------------------------

    @Override
    public void set(long row, long col, Double value) {
        data.put(Utils.getPos(row, col, this), value);
    }

    @Override
    public Matrix64F setDiagonalsToZero() {
        return null;
    }

    @Override
    public Matrix64F setDiagonalsToZero(Matrix<Double> B) {
        return null;
    }

    @Override
    public void setArray(Object data) {

    }

    // ---------------------------------------------------
    // GETTER.
    // ---------------------------------------------------

    @Override
    public Double get(long index) {
        final Double value = data.get(Utils.toInt(index));
        return (value == null) ? 0. : value;
    }

    @Override
    public Double get(long row, long col) {
        final Double value = data.get(Utils.getPos(row, col, this));
        return (value == null) ? 0. : value;
    }

    @Override
    public Matrix64F getRow(long row) {
        return null;
    }

    @Override
    public Matrix64F getRow(long row, long from, long to) {
        return null;
    }

    @Override
    public Matrix64F getCol(long col) {
        return null;
    }

    @Override
    public Matrix64F getCol(long col, long from, long to) {
        return null;
    }

    @Override
    public Object toArray() {
        return new Double[0];
    }

    // ---------------------------------------------------
    // APPLY ON ELEMENTS.
    // ---------------------------------------------------

    @Override
    public Matrix64F applyOnElements(UnaryOperator<Double> f) {
        return null;
    }

    @Override
    public Matrix64F applyOnElements(UnaryOperator<Double> f, Matrix<Double> B) {
        return null;
    }

    @Override
    public Matrix64F applyOnElements(Matrix<Double> B, BinaryOperator<Double> f) {
        return null;
    }

    @Override
    public Matrix64F applyOnElements(Matrix<Double> B, BinaryOperator<Double> f, Matrix<Double> C) {
        return null;
    }

    @Override
    public Matrix64F applyOnElements(MatrixElementUnaryOperator<Double> f) {
        return null;
    }

    @Override
    public Matrix64F applyOnElements(MatrixElementUnaryOperator<Double> f, Matrix<Double> B) {
        return null;
    }

    @Override
    public Matrix64F applyOnNonZeroElements(MatrixElementUnaryOperator<Double> f) {
        return null;
    }

    @Override
    public Matrix64F applyOnNonZeroElements(MatrixElementUnaryOperator<Double> f, Matrix<Double> B) {
        return null;
    }

    // ---------------------------------------------------
    // ASSIGN.
    // ---------------------------------------------------

    @Override
    public Matrix64F assign(Matrix<Double> v) {
        return null;
    }

    @Override
    public Matrix64F assign(Double v) {
        return null;
    }

    @Override
    public Matrix64F assignRow(long row, Matrix<Double> v) {
        return null;
    }

    @Override
    public Matrix64F assignColumn(long col, Matrix<Double> v) {
        return null;
    }

    @Override
    public Matrix64F assign(long rowOffset, long colOffset, Matrix<Double> m) {
        return null;
    }

    // ---------------------------------------------------
    // AGGREGATION.
    // ---------------------------------------------------

    @Override
    public Double aggregate(BinaryOperator<Double> combiner, UnaryOperator<Double> mapper, Matrix<Double> result) {
        return 0.0;
    }

    @Override
    public Matrix64F aggregateRows(MatrixAggregation<Double> f) {
        return null;
    }

    @Override
    public Matrix64F aggregateRows(MatrixAggregation<Double> f, Matrix<Double> result) {
        return null;
    }

    @Override
    public Double sum() {
        return 0.0;
    }

    // ---------------------------------------------------
    // ARITHMETIC.
    // ---------------------------------------------------

    @Override
    public Matrix64F add(Matrix<Double> B) {
        return null;
    }

    @Override
    public Matrix64F add(Matrix<Double> B, Matrix<Double> C) {
        return null;
    }

    @Override
    public Matrix64F addVectorToRows(Matrix<Double> v) {
        return null;
    }

    @Override
    public Matrix64F addVectorToRows(Matrix<Double> v, Matrix<Double> B) {
        return null;
    }

    @Override
    public Matrix64F addVectorToCols(Matrix<Double> v) {
        return null;
    }

    @Override
    public Matrix64F addVectorToCols(Matrix<Double> v, Matrix<Double> B) {
        return null;
    }

    @Override
    public Matrix64F sub(Matrix<Double> B) {
        return null;
    }

    @Override
    public Matrix64F sub(Matrix<Double> B, Matrix<Double> C) {
        return null;
    }

    @Override
    public Matrix64F mul(Matrix<Double> B) {
        return null;
    }

    @Override
    public Matrix64F mul(Matrix<Double> B, Matrix<Double> C) {
        return null;
    }

    @Override
    public Matrix64F scale(Double a) {
        return null;
    }

    @Override
    public Matrix64F scale(Double a, Matrix<Double> B) {
        return null;
    }

    @Override
    public Matrix64F transpose() {
        return null;
    }

    @Override
    public Matrix64F transpose(Matrix<Double> B) {
        return null;
    }

    @Override
    public Matrix64F invert() {
        return null;
    }

    @Override
    public Matrix64F invert(Matrix<Double> B) {
        return null;
    }

    @Override
    public Double norm(int p) {
        return 0.0;
    }

    @Override
    public Double dot(Matrix<Double> B) {
        return 0.0;
    }

    // ---------------------------------------------------
    // SLICING.
    // ---------------------------------------------------

    @Override
    public Matrix64F subMatrix(long rowOffset, long colOffset, long rows, long cols) {
        return null;
    }

    @Override
    public Matrix64F concat(Matrix<Double> B) {
        return null;
    }

    @Override
    public Matrix64F concat(Matrix<Double> B, Matrix<Double> C) {
        return null;
    }

    // ---------------------------------------------------
    // ROW ITERATOR.
    // ---------------------------------------------------

    @Override
    public RowIterator<Double, Matrix<Double>> rowIterator() {
        return null;
    }

    @Override
    public RowIterator<Double, Matrix<Double>> rowIterator(long startRow, long endRow) {
        return null;
    }
}
