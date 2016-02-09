package de.tuberlin.pserver.math.matrix32;


import de.tuberlin.pserver.math.matrix32.operations.BinaryOperator32;
import de.tuberlin.pserver.math.matrix32.operations.MatrixAggregation32;
import de.tuberlin.pserver.math.matrix32.operations.MatrixElementUnaryOperator32;
import de.tuberlin.pserver.math.matrix32.operations.UnaryOperator32;

public interface Matrix32 {

    // ---------------------------------------------------

    //default long toLong(final float value) { return Float.floatToIntBits(value); }

    //default float fromLong(final long value) { return Float.intBitsToFloat((int)value); }

    // ---------------------------------------------------

    long rows();

    long cols();

    // ---------------------------------------------------

    float get(final long index);

    float get(final long row, final long col);

    void set(final long r, final long c, final float value);

    // ---------------------------------------------------

    Matrix32 copy();

    Matrix32 copy(long rows, long cols);

    Matrix32 setDiagonalsToZero();

    Matrix32 setDiagonalsToZero(Matrix32 B);

    Matrix32 getRow(long row);

    Matrix32 getRow(long row, long from, long to);

    Matrix32 getCol(long col);

    Matrix32 getCol(long col, long from, long to);

    Matrix32 applyOnElements(UnaryOperator32 f);

    Matrix32 applyOnElements(UnaryOperator32 f, Matrix32 B);

    Matrix32 applyOnElements(Matrix32 B, BinaryOperator32 f);

    Matrix32 applyOnElements(Matrix32 B, BinaryOperator32 f, Matrix32 C);

    Matrix32 applyOnElements(MatrixElementUnaryOperator32 f);

    Matrix32 applyOnElements(MatrixElementUnaryOperator32 f, Matrix32 B);

    Matrix32 applyOnNonZeroElements(MatrixElementUnaryOperator32 f);

    Matrix32 applyOnNonZeroElements(MatrixElementUnaryOperator32 f, Matrix32 B);

    Matrix32 assign(Matrix32 v);

    Matrix32 assign(float afloat);

    Matrix32 assignRow(long row, Matrix32 v);

    Matrix32 assignColumn(long col, Matrix32 v);

    Matrix32 assign(long rowOffset, long colOffset, Matrix32 m);

    Matrix32 aggregateRows(MatrixAggregation32 f);

    Matrix32 aggregateRows(MatrixAggregation32 f, Matrix32 result);

    Matrix32 add(Matrix32 B);

    Matrix32 add(Matrix32 B, Matrix32 C);

    Matrix32 addVectorToRows(Matrix32 v);

    Matrix32 addVectorToRows(Matrix32 v, Matrix32 B);

    Matrix32 addVectorToCols(Matrix32 v);

    Matrix32 addVectorToCols(Matrix32 v, Matrix32 B);

    Matrix32 sub(Matrix32 B);

    Matrix32 sub(Matrix32 B, Matrix32 C);

    Matrix32 mul(Matrix32 B);

    Matrix32 mul(Matrix32 B, Matrix32 C);

    Matrix32 scale(float a);

    Matrix32 scale(float a, Matrix32 B);

    Matrix32 transpose();

    Matrix32 transpose(Matrix32 B);

    Matrix32 invert();

    Matrix32 invert(Matrix32 B);

    Matrix32 subMatrix(long rowOffset, long colOffset, long rows, long cols);

    Matrix32 concat(Matrix32 B);

    Matrix32 concat(Matrix32 B, Matrix32 C);

    // ---------------------------------------------------

    float sum();

    float aggregate(final BinaryOperator32 combiner, final UnaryOperator32 mapper, final Matrix32 result);

    float dot(final Matrix32 B);

    float norm(final int p);

    /* RowIterator rowIterator();

     RowIterator rowIterator(final long startRow, final long endRow);

    interface RowIterator extends Matrix.RowIterator<float, Matrix32> {

        boolean hasNext();

        void next();

        void nextRandom();

        float value(final long col);

        Matrix32 get();

        Matrix32 get(final long from, final long size);

        void reset();

        long size();

        long rowNum();
    }*/
}
