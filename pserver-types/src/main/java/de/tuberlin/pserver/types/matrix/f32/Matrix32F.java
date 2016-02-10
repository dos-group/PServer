package de.tuberlin.pserver.types.matrix.f32;


import de.tuberlin.pserver.types.matrix.DistributedMatrixType;
import de.tuberlin.pserver.types.matrix.f32.operations.BinaryOperator32;
import de.tuberlin.pserver.types.matrix.f32.operations.MatrixAggregation32;
import de.tuberlin.pserver.types.matrix.f32.operations.MatrixElementUnaryOperator32;
import de.tuberlin.pserver.types.matrix.f32.operations.UnaryOperator32;

public interface Matrix32F extends DistributedMatrixType {

    // ---------------------------------------------------

    float get(final long index);

    float get(final long row, final long col);

    void set(final long r, final long c, final float value);

    // ---------------------------------------------------

    Matrix32F copy();

    Matrix32F copy(long rows, long cols);

    Matrix32F setDiagonalsToZero();

    Matrix32F setDiagonalsToZero(Matrix32F B);

    Matrix32F getRow(long row);

    Matrix32F getRow(long row, long from, long to);

    Matrix32F getCol(long col);

    Matrix32F getCol(long col, long from, long to);

    Matrix32F applyOnElements(UnaryOperator32 f);

    Matrix32F applyOnElements(UnaryOperator32 f, Matrix32F B);

    Matrix32F applyOnElements(Matrix32F B, BinaryOperator32 f);

    Matrix32F applyOnElements(Matrix32F B, BinaryOperator32 f, Matrix32F C);

    Matrix32F applyOnElements(MatrixElementUnaryOperator32 f);

    Matrix32F applyOnElements(MatrixElementUnaryOperator32 f, Matrix32F B);

    Matrix32F applyOnNonZeroElements(MatrixElementUnaryOperator32 f);

    Matrix32F applyOnNonZeroElements(MatrixElementUnaryOperator32 f, Matrix32F B);

    Matrix32F assign(Matrix32F v);

    Matrix32F assign(float afloat);

    Matrix32F assignRow(long row, Matrix32F v);

    Matrix32F assignColumn(long col, Matrix32F v);

    Matrix32F assign(long rowOffset, long colOffset, Matrix32F m);

    Matrix32F aggregateRows(MatrixAggregation32 f);

    Matrix32F aggregateRows(MatrixAggregation32 f, Matrix32F result);

    Matrix32F add(Matrix32F B);

    Matrix32F add(Matrix32F B, Matrix32F C);

    Matrix32F addVectorToRows(Matrix32F v);

    Matrix32F addVectorToRows(Matrix32F v, Matrix32F B);

    Matrix32F addVectorToCols(Matrix32F v);

    Matrix32F addVectorToCols(Matrix32F v, Matrix32F B);

    Matrix32F sub(Matrix32F B);

    Matrix32F sub(Matrix32F B, Matrix32F C);

    Matrix32F mul(Matrix32F B);

    Matrix32F mul(Matrix32F B, Matrix32F C);

    Matrix32F scale(float a);

    Matrix32F scale(float a, Matrix32F B);

    Matrix32F transpose();

    Matrix32F transpose(Matrix32F B);

    Matrix32F invert();

    Matrix32F invert(Matrix32F B);

    Matrix32F subMatrix(long rowOffset, long colOffset, long rows, long cols);

    Matrix32F concat(Matrix32F B);

    Matrix32F concat(Matrix32F B, Matrix32F C);

    // ---------------------------------------------------

    float sum();

    float aggregate(final BinaryOperator32 combiner, final UnaryOperator32 mapper, final Matrix32F result);

    float dot(final Matrix32F B);

    float norm(final int p);

    // ---------------------------------------------------

    RowIterator rowIterator();

    RowIterator rowIterator(final long startRow, final long endRow);

    interface RowIterator {

        boolean hasNext();

        void next();

        void nextRandom();

        float value(final long col);

        Matrix32F get();

        Matrix32F get(final long from, final long size);

        void reset();

        long size();

        long rowNum();
    }
}
