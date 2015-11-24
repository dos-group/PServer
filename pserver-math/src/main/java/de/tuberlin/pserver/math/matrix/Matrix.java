package de.tuberlin.pserver.math.matrix;

import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;


public interface Matrix<V extends Number> extends MatrixBase {

    // ---------------------------------------------------
    // TYPE CONVERSION.
    // ---------------------------------------------------

    public long toLong(final V value);
    public V fromLong(final long value);

    // ---------------------------------------------------
    // COPY.
    // ---------------------------------------------------

    public Matrix<V> copy();
    public Matrix<V> copy(final long rows, final long cols);

    // ---------------------------------------------------
    // SETTER.
    // ---------------------------------------------------

    public void set(final long row, final long col, final V value);
    public Matrix<V> setDiagonalsToZero();
    public Matrix<V> setDiagonalsToZero(final Matrix<V> B);
    public void setArray(final Object data);

    // ---------------------------------------------------
    // GETTER.
    // ---------------------------------------------------

    public V get(final long index);
    public V get(final long row, final long col);
    public Matrix<V> getRow(final long row);
    public Matrix<V> getRow(final long row, final long from, final long to);
    public Matrix<V> getCol(final long col);
    public Matrix<V> getCol(final long col, final long from, final long to);
    public Object toArray();

    // ---------------------------------------------------
    // APPLY ON ELEMENTS.
    // ---------------------------------------------------

    public Matrix<V> applyOnElements(final UnaryOperator<V> f);
    public Matrix<V> applyOnElements(final UnaryOperator<V> f, final Matrix<V> B);
    public Matrix<V> applyOnElements(final Matrix<V> B, final BinaryOperator<V> f);
    public Matrix<V> applyOnElements(final Matrix<V> B, final BinaryOperator<V> f, final Matrix<V> C);
    public Matrix<V> applyOnElements(final MatrixElementUnaryOperator<V> f);
    public Matrix<V> applyOnElements(final MatrixElementUnaryOperator<V> f, final Matrix<V> B);
    public Matrix<V> applyOnNonZeroElements(final MatrixElementUnaryOperator<V> f);
    public Matrix<V> applyOnNonZeroElements(final MatrixElementUnaryOperator<V> f, final Matrix<V> B);

    // ---------------------------------------------------
    // ASSIGN.
    // ---------------------------------------------------

    public Matrix<V> assign(final Matrix<V> v);
    public Matrix<V> assign(final V v);
    public Matrix<V> assignRow(final long row, final Matrix<V> v);
    public Matrix<V> assignColumn(final long col, final Matrix<V> v);
    public Matrix<V> assign(final long rowOffset, final long colOffset, final Matrix<V> m);

    // ---------------------------------------------------
    // AGGREGATION.
    // ---------------------------------------------------

    public V aggregate(final BinaryOperator<V> combiner, final UnaryOperator<V> mapper, final Matrix<V> result);
    public Matrix<V> aggregateRows(final MatrixAggregation<V> f);
    public Matrix<V> aggregateRows(final MatrixAggregation<V> f, final Matrix<V> result);
    public V sum();

    // ---------------------------------------------------
    // ARITHMETIC.
    // ---------------------------------------------------

    public Matrix<V> add(final Matrix<V> B);
    public Matrix<V> add(final Matrix<V> B, final Matrix<V> C);
    public Matrix<V> addVectorToRows(final Matrix<V> v);
    public Matrix<V> addVectorToRows(final Matrix<V> v, final Matrix<V> B);
    public Matrix<V> addVectorToCols(final Matrix<V> v);
    public Matrix<V> addVectorToCols(final Matrix<V> v, final Matrix<V> B);

    // ----------------------------------------

    public Matrix<V> sub(final Matrix<V> B);
    public Matrix<V> sub(final Matrix<V> B, final Matrix<V> C);

    // ----------------------------------------

    public Matrix<V> mul(final Matrix<V> B);
    public Matrix<V> mul(final Matrix<V> B, final Matrix<V> C);

    // ----------------------------------------

    public Matrix<V> scale(final V a);
    public Matrix<V> scale(final V a, final Matrix<V> B);

    // ----------------------------------------

    public Matrix<V> transpose();
    public Matrix<V> transpose(final Matrix<V> B);

    // ----------------------------------------

    public Matrix<V> invert();
    public Matrix<V> invert(final Matrix<V> B);

    // ----------------------------------------

    public V norm(final int p);

    // ----------------------------------------

    public V dot(final Matrix<V> B);

    //public Matrix<V> innerProduct(final Matrix<V> B); // TODO!

    // ---------------------------------------------------
    // SLICING.
    // ---------------------------------------------------

    public Matrix<V> subMatrix(final long rowOffset, final long colOffset, final long rows, final long cols);
    public Matrix<V> concat(final Matrix<V> B);
    public Matrix<V> concat(final Matrix<V> B, final Matrix<V> C);

    // ---------------------------------------------------
    // ROW ITERATOR.
    // ---------------------------------------------------

    public RowIterator rowIterator();
    public RowIterator rowIterator(final long startRow, final long endRow);

    interface RowIterator<V extends Number, MAT extends Matrix<V>> {

        boolean hasNext();

        void next();

        void nextRandom();

        V value(final long col);

        MAT get();

        MAT get(final long from, final long size);

        void reset();

        long size();

        long rowNum();
    }
}