package de.tuberlin.pserver.math.matrix;


import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;

public interface Matrix64F extends Matrix<Double> {

    @Override public Matrix64F copy();

    @Override public Matrix64F copy(long rows, long cols);

    @Override public Matrix64F setDiagonalsToZero();

    @Override public Matrix64F setDiagonalsToZero(Matrix<Double> B);

    @Override public Matrix64F getRow(long row);

    @Override public Matrix64F getRow(long row, long from, long to);

    @Override public Matrix64F getCol(long col);

    @Override public Matrix64F getCol(long col, long from, long to);

    @Override public Matrix64F applyOnElements(UnaryOperator<Double> f);

    @Override public Matrix64F applyOnElements(UnaryOperator<Double> f, Matrix<Double> B);

    @Override public Matrix64F applyOnElements(Matrix<Double> B, BinaryOperator<Double> f);

    @Override public Matrix64F applyOnElements(Matrix<Double> B, BinaryOperator<Double> f, Matrix<Double> C);

    @Override public Matrix64F applyOnElements(MatrixElementUnaryOperator<Double> f);

    @Override public Matrix64F applyOnElements(MatrixElementUnaryOperator<Double> f, Matrix<Double> B);

    @Override public Matrix64F applyOnNonZeroElements(MatrixElementUnaryOperator<Double> f);

    @Override public Matrix64F applyOnNonZeroElements(MatrixElementUnaryOperator<Double> f, Matrix<Double> B);

    @Override public Matrix64F assign(Matrix<Double> v);

    @Override public Matrix64F assign(Double aFloat);

    @Override public Matrix64F assignRow(long row, Matrix<Double> v);

    @Override public Matrix64F assignColumn(long col, Matrix<Double> v);

    @Override public Matrix64F assign(long rowOffset, long colOffset, Matrix<Double> m);

    @Override public Matrix64F aggregateRows(MatrixAggregation<Double> f);

    @Override public Matrix64F aggregateRows(MatrixAggregation<Double> f, Matrix<Double> result);

    @Override public Matrix64F add(Matrix<Double> B);

    @Override public Matrix64F add(Matrix<Double> B, Matrix<Double> C);

    @Override public Matrix64F addVectorToRows(Matrix<Double> v);

    @Override public Matrix64F addVectorToRows(Matrix<Double> v, Matrix<Double> B);

    @Override public Matrix64F addVectorToCols(Matrix<Double> v);

    @Override public Matrix64F addVectorToCols(Matrix<Double> v, Matrix<Double> B);

    @Override public Matrix64F sub(Matrix<Double> B);

    @Override public Matrix64F sub(Matrix<Double> B, Matrix<Double> C);

    @Override public Matrix64F mul(Matrix<Double> B);

    @Override public Matrix64F mul(Matrix<Double> B, Matrix<Double> C);

    @Override public Matrix64F scale(Double a);

    @Override public Matrix64F scale(Double a, Matrix<Double> B);

    @Override public Matrix64F transpose();

    @Override public Matrix64F transpose(Matrix<Double> B);

    @Override public Matrix64F invert();

    @Override public Matrix64F invert(Matrix<Double> B);

    @Override public Matrix64F subMatrix(long rowOffset, long colOffset, long rows, long cols);

    @Override public Matrix64F concat(Matrix<Double> B);

    @Override public Matrix64F concat(Matrix<Double> B, Matrix<Double> C);
}
