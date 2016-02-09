package de.tuberlin.pserver.math.matrix;


import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;

public class Matrix32FBase implements Matrix32F {

    @Override
    public Matrix32F copy() { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F copy(long rows, long cols) { throw new UnsupportedOperationException(); }

    @Override
    public void set(long row, long col, Float value) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F setDiagonalsToZero() { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F setDiagonalsToZero(Matrix<Float> B) { throw new UnsupportedOperationException(); }

    @Override
    public void setArray(Object data) { throw new UnsupportedOperationException(); }

    @Override
    public Float get(long index) { throw new UnsupportedOperationException(); }

    @Override
    public Float get(long row, long col) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F getRow(long row) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F getRow(long row, long from, long to) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F getCol(long col) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F getCol(long col, long from, long to) { throw new UnsupportedOperationException(); }

    @Override
    public Object toArray() { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F applyOnElements(UnaryOperator<Float> f) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F applyOnElements(UnaryOperator<Float> f, Matrix<Float> B) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F applyOnElements(Matrix<Float> B, BinaryOperator<Float> f) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F applyOnElements(Matrix<Float> B, BinaryOperator<Float> f, Matrix<Float> C) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F applyOnElements(MatrixElementUnaryOperator<Float> f) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F applyOnElements(MatrixElementUnaryOperator<Float> f, Matrix<Float> B) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F applyOnNonZeroElements(MatrixElementUnaryOperator<Float> f) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F applyOnNonZeroElements(MatrixElementUnaryOperator<Float> f, Matrix<Float> B) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F assign(Matrix<Float> v) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F assign(Float aFloat) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F assignRow(long row, Matrix<Float> v) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F assignColumn(long col, Matrix<Float> v) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F assign(long rowOffset, long colOffset, Matrix<Float> m) { throw new UnsupportedOperationException(); }

    @Override
    public Float aggregate(BinaryOperator<Float> combiner, UnaryOperator<Float> mapper, Matrix<Float> result) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F aggregateRows(MatrixAggregation<Float> f) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F aggregateRows(MatrixAggregation<Float> f, Matrix<Float> result) { throw new UnsupportedOperationException(); }

    @Override
    public Float sum() { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F add(Matrix<Float> B) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F add(Matrix<Float> B, Matrix<Float> C) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F addVectorToRows(Matrix<Float> v) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F addVectorToRows(Matrix<Float> v, Matrix<Float> B) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F addVectorToCols(Matrix<Float> v) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F addVectorToCols(Matrix<Float> v, Matrix<Float> B) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F sub(Matrix<Float> B) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F sub(Matrix<Float> B, Matrix<Float> C) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F mul(Matrix<Float> B) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F mul(Matrix<Float> B, Matrix<Float> C) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F scale(Float a) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F scale(Float a, Matrix<Float> B) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F transpose() { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F transpose(Matrix<Float> B) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F invert() { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F invert(Matrix<Float> B) { throw new UnsupportedOperationException(); }

    @Override
    public Float norm(int p) { throw new UnsupportedOperationException(); }

    @Override
    public Float dot(Matrix<Float> B) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F subMatrix(long rowOffset, long colOffset, long rows, long cols) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F concat(Matrix<Float> B) { throw new UnsupportedOperationException(); }

    @Override
    public Matrix32F concat(Matrix<Float> B, Matrix<Float> C) { throw new UnsupportedOperationException(); }

    @Override
    public RowIterator rowIterator() { throw new UnsupportedOperationException(); }

    @Override
    public RowIterator rowIterator(long startRow, long endRow) { return null; }

    @Override
    public long rows() { throw new UnsupportedOperationException(); }

    @Override
    public long cols() { throw new UnsupportedOperationException(); }

    @Override
    public long sizeOf() { throw new UnsupportedOperationException(); }

    @Override
    public void lock() { throw new UnsupportedOperationException(); }

    @Override
    public void unlock() { throw new UnsupportedOperationException(); }

    @Override
    public void setOwner(Object owner) { throw new UnsupportedOperationException(); }

    @Override
    public Object getOwner() { throw new UnsupportedOperationException(); }
}
