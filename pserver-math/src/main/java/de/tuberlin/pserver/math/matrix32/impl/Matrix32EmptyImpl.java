package de.tuberlin.pserver.math.matrix32.impl;


import de.tuberlin.pserver.math.matrix32.Matrix32;
import de.tuberlin.pserver.math.matrix32.Matrix32MetaData;
import de.tuberlin.pserver.math.matrix32.operations.BinaryOperator32;
import de.tuberlin.pserver.math.matrix32.operations.MatrixAggregation32;
import de.tuberlin.pserver.math.matrix32.operations.MatrixElementUnaryOperator32;
import de.tuberlin.pserver.math.matrix32.operations.UnaryOperator32;
import de.tuberlin.pserver.math.matrix32.partitioner.PartitionerType;

class Matrix32EmptyImpl extends Matrix32MetaData implements Matrix32 {


    public Matrix32EmptyImpl(Matrix32MetaData m) {
        super(m);
    }

    public Matrix32EmptyImpl(PartitionerType type, int nodeID, int[] nodes, long globalRows, long globalCols) {
        super(type, nodeID, nodes, globalRows, globalCols);
    }

    @Override
    public void setArray(Object data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long rows() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long cols() {
        throw new UnsupportedOperationException();
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
    public Matrix32 copy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 copy(long rows, long cols) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 setDiagonalsToZero() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 setDiagonalsToZero(Matrix32 B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 getRow(long row) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 getRow(long row, long from, long to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 getCol(long col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 getCol(long col, long from, long to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 applyOnElements(UnaryOperator32 f) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 applyOnElements(UnaryOperator32 f, Matrix32 B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 applyOnElements(Matrix32 B, BinaryOperator32 f) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 applyOnElements(Matrix32 B, BinaryOperator32 f, Matrix32 C) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 applyOnElements(MatrixElementUnaryOperator32 f) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 applyOnElements(MatrixElementUnaryOperator32 f, Matrix32 B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 applyOnNonZeroElements(MatrixElementUnaryOperator32 f) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 applyOnNonZeroElements(MatrixElementUnaryOperator32 f, Matrix32 B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 assign(Matrix32 v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 assign(float afloat) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 assignRow(long row, Matrix32 v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 assignColumn(long col, Matrix32 v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 assign(long rowOffset, long colOffset, Matrix32 m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 aggregateRows(MatrixAggregation32 f) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 aggregateRows(MatrixAggregation32 f, Matrix32 result) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 add(Matrix32 B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 add(Matrix32 B, Matrix32 C) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 addVectorToRows(Matrix32 v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 addVectorToRows(Matrix32 v, Matrix32 B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 addVectorToCols(Matrix32 v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 addVectorToCols(Matrix32 v, Matrix32 B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 sub(Matrix32 B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 sub(Matrix32 B, Matrix32 C) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 mul(Matrix32 B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 mul(Matrix32 B, Matrix32 C) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 scale(float a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 scale(float a, Matrix32 B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 transpose() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 transpose(Matrix32 B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 invert() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 invert(Matrix32 B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 subMatrix(long rowOffset, long colOffset, long rows, long cols) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 concat(Matrix32 B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 concat(Matrix32 B, Matrix32 C) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float sum() {
        throw new UnsupportedOperationException();
    }

    @Override
    public float aggregate(BinaryOperator32 combiner, UnaryOperator32 mapper, Matrix32 result) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float dot(Matrix32 B) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float norm(int p) {
        throw new UnsupportedOperationException();
    }
}
