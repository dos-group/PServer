package de.tuberlin.pserver.math.matrix;

import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;

public interface Matrix32F extends Matrix<Float> {

    @Override public Matrix32F copy();

    @Override public Matrix32F copy(long rows, long cols);

    @Override public Matrix32F setDiagonalsToZero();

    @Override public Matrix32F setDiagonalsToZero(Matrix<Float> B);

    @Override public Matrix32F getRow(long row);

    @Override public Matrix32F getRow(long row, long from, long to);

    @Override public Matrix32F getCol(long col);

    @Override public Matrix32F getCol(long col, long from, long to);

    @Override public Matrix32F applyOnElements(UnaryOperator<Float> f);

    @Override public Matrix32F applyOnElements(UnaryOperator<Float> f, Matrix<Float> B);

    @Override public Matrix32F applyOnElements(Matrix<Float> B, BinaryOperator<Float> f);

    @Override public Matrix32F applyOnElements(Matrix<Float> B, BinaryOperator<Float> f, Matrix<Float> C);

    @Override public Matrix32F applyOnElements(MatrixElementUnaryOperator<Float> f);

    @Override public Matrix32F applyOnElements(MatrixElementUnaryOperator<Float> f, Matrix<Float> B);

    @Override public Matrix32F applyOnNonZeroElements(MatrixElementUnaryOperator<Float> f);

    @Override public Matrix32F applyOnNonZeroElements(MatrixElementUnaryOperator<Float> f, Matrix<Float> B);

    @Override public Matrix32F assign(Matrix<Float> v);

    @Override public Matrix32F assign(Float aFloat);

    @Override public Matrix32F assignRow(long row, Matrix<Float> v);

    @Override public Matrix32F assignColumn(long col, Matrix<Float> v);

    @Override public Matrix32F assign(long rowOffset, long colOffset, Matrix<Float> m);

    @Override public Matrix32F aggregateRows(MatrixAggregation<Float> f);

    @Override public Matrix32F aggregateRows(MatrixAggregation<Float> f, Matrix<Float> result);

    @Override public Matrix32F add(Matrix<Float> B);

    @Override public Matrix32F add(Matrix<Float> B, Matrix<Float> C);

    @Override public Matrix32F addVectorToRows(Matrix<Float> v);

    @Override public Matrix32F addVectorToRows(Matrix<Float> v, Matrix<Float> B);

    @Override public Matrix32F addVectorToCols(Matrix<Float> v);

    @Override public Matrix32F addVectorToCols(Matrix<Float> v, Matrix<Float> B);

    @Override public Matrix32F sub(Matrix<Float> B);

    @Override public Matrix32F sub(Matrix<Float> B, Matrix<Float> C);

    @Override public Matrix32F mul(Matrix<Float> B);

    @Override public Matrix32F mul(Matrix<Float> B, Matrix<Float> C);

    @Override public Matrix32F scale(Float a);

    @Override public Matrix32F scale(Float a, Matrix<Float> B);

    @Override public Matrix32F transpose();

    @Override public Matrix32F transpose(Matrix<Float> B);

    @Override public Matrix32F invert();

    @Override public Matrix32F invert(Matrix<Float> B);

    @Override public Matrix32F subMatrix(long rowOffset, long colOffset, long rows, long cols);

    @Override public Matrix32F concat(Matrix<Float> B);

    @Override public Matrix32F concat(Matrix<Float> B, Matrix<Float> C);
}
