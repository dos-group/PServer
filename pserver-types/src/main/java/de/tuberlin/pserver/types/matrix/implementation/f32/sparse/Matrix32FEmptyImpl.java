package de.tuberlin.pserver.types.matrix.implementation.f32.sparse;


import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.f32.operations.BinaryOperator32;
import de.tuberlin.pserver.types.matrix.implementation.f32.operations.MatrixAggregation32;
import de.tuberlin.pserver.types.matrix.implementation.f32.operations.MatrixElementUnaryOperator32;
import de.tuberlin.pserver.types.matrix.implementation.f32.operations.UnaryOperator32;
import de.tuberlin.pserver.types.matrix.metadata.AbstractDistributedMatrixType;
import de.tuberlin.pserver.types.metadata.DistributionScheme;

abstract class Matrix32FEmptyImpl extends AbstractDistributedMatrixType implements Matrix32F {

    public Matrix32FEmptyImpl(int nodeID, int[] nodes, DistributionScheme distributionScheme, long globalRows, long globalCols) {
        super(nodeID, nodes, distributionScheme, globalRows, globalCols);
    }

    @Override
    public float get(long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float get(long row, long col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(long r, long c, float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F copy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F copy(long rows, long cols) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F setDiagonalsToZero() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F setDiagonalsToZero(Matrix32F B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F getRow(long row) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F getRow(long row, long from, long to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F getCol(long col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F getCol(long col, long from, long to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F applyOnElements(UnaryOperator32 f) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F applyOnElements(UnaryOperator32 f, Matrix32F B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F applyOnElements(Matrix32F B, BinaryOperator32 f) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F applyOnElements(Matrix32F B, BinaryOperator32 f, Matrix32F C) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F applyOnElements(MatrixElementUnaryOperator32 f) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F applyOnElements(MatrixElementUnaryOperator32 f, Matrix32F B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F applyOnNonZeroElements(MatrixElementUnaryOperator32 f) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F applyOnNonZeroElements(MatrixElementUnaryOperator32 f, Matrix32F B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F assign(Matrix32F v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F assign(float afloat) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F assignRow(long row, Matrix32F v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F assignColumn(long col, Matrix32F v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F assign(long rowOffset, long colOffset, Matrix32F m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F aggregateRows(MatrixAggregation32 f) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F aggregateRows(MatrixAggregation32 f, Matrix32F result) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F add(Matrix32F B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F add(Matrix32F B, Matrix32F C) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F addVectorToRows(Matrix32F v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F addVectorToRows(Matrix32F v, Matrix32F B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F addVectorToCols(Matrix32F v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F addVectorToCols(Matrix32F v, Matrix32F B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F sub(Matrix32F B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F sub(Matrix32F B, Matrix32F C) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F mul(Matrix32F B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F mul(Matrix32F B, Matrix32F C) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F scale(float a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F scale(float a, Matrix32F B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F transpose() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F transpose(Matrix32F B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F invert() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F invert(Matrix32F B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F subMatrix(long rowOffset, long colOffset, long rows, long cols) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F concat(Matrix32F B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F concat(Matrix32F B, Matrix32F C) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float sum() {
        throw new UnsupportedOperationException();
    }

    @Override
    public float aggregate(BinaryOperator32 combiner, UnaryOperator32 mapper, Matrix32F result) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float dot(Matrix32F B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float norm(int p) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowIterator rowIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowIterator rowIterator(long startRow, long endRow) {
        throw new UnsupportedOperationException();
    }
}
