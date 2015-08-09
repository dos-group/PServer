package de.tuberlin.pserver.math.vector;

import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.math.exceptions.IncompatibleShapeException;
import de.tuberlin.pserver.math.operations.ApplyOnDoubleElements;

import java.util.Iterator;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

public interface Vector extends SharedObject, ApplyOnDoubleElements<Vector> {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    enum Format {

        SPARSE_VECTOR,

        DENSE_VECTOR
    }

    enum Layout {

        ROW_LAYOUT,

        COLUMN_LAYOUT
    }

    // ---------------------------------------------------
    // Methods.
    // ---------------------------------------------------

    long length();

    Format format();

    Layout layout();

    void set(final long index, final double value);

    double get(final long index);
    
    double atomicGet(final long index);

    void atomicSet(final long index, final double value);

    /**
     * Identical to {@link #mul(double, Vector)} but automatically creates the resulting <code>vector y</code>.
     */
    Vector mul(final double alpha);

    /**
     * Called on vector x. Computes vector-scalar-multiplication y = x * alpha. <br>
     * <strong>Note: x and y have to be of the same shape</strong>
     * @param alpha the scalar to multiply with x
     * @param y the vector to store the result in
     * @return y after computing vector-scalar-multiplication y = x * alpha
     * @throws IncompatibleShapeException If x and y are not of equal length.
     */
    Vector mul(final double alpha, final Vector y);


    Vector div(final double alpha);

    /**
     * Called on Vector x. Computes vector-scalar-division y = x / alpha. <br>
     * <strong>Note: x and y have to be of the same shape</strong>
     * @param alpha the scalar to multiply with x
     * @param y the vector to store the result in
     * @return y after computing vector-scalar-division y = x / alpha
     * @throws IncompatibleShapeException If x and y are not of equal length.
     */
    Vector div(final double alpha, final Vector y);

    /**
     * Identical to {@link #add(Vector, Vector)} but automatically creates the resulting <code>vector z</code>.
     */
    Vector add(final Vector y);

    /**
     * Called on vector x. Computes vector-vector-addition z = x + y. <br>
     * <strong>Note: x and y have to be of the same shape</strong>
     * @param y the vector to add on x
     * @param z the vector to store the result in
     * @return z after computing vector-vector-addition z = x + y
     * @throws IncompatibleShapeException If x and y are not of equal length.
     */
    Vector add(final Vector y, final Vector z);

    /**
     * Identical to {@link #sub(Vector, Vector)} but automatically creates the resulting <code>vector z</code>.
     */
    Vector sub(final Vector y);

    /**
     * Called on vector x. Computes vector-vector-subtraction z = x - y. <br>
     * <strong>Note: x and y have to be of the same shape</strong>
     * @param y the vector to subtract from x
     * @param z the vector to store the result in
     * @return y after computing vector-vector-subtraction z = x - y
     * @throws IncompatibleShapeException If x and y are not of equal length.
     */
    Vector sub(final Vector y, final Vector z);

    /**
     * Identical to {@link #add(double, Vector, Vector)} but automatically creates the resulting <code>vector z</code>.
     */
    Vector add(final double alpha, final Vector y);

    /**
     * Called on vector x. Computes z = alpha * y + x. <br>
     * <strong>Note: x and y have to be of the same shape</strong>
     * @param y the vector to subtract from x
     * @param z the vector to store the result in
     * @return y after computing z = alpha * y + x
     * @throws IncompatibleShapeException If x and y are not of equal length.
     */
    Vector add(final double alpha, final Vector y, Vector z);

    Vector assign(final Vector v);

    Vector assign(final double v);

    Vector viewPart(final long s, final long e);

    Vector like();

    Vector copy();

    Iterator<Element> iterateNonZero();

    double aggregate(DoubleBinaryOperator aggregator, DoubleUnaryOperator map);

    /**
     * Called on vector x. Computes the dot product of x and y.
     * @param y
     * @return dot product of x and y
     */
    double dot(final Vector y);                       // x = x^T * y

    double sum();

    double norm(final double v);

    double maxValue();

    double minValue();

    /**
     * A holder for information about a specific item in the Vector. When using with an Iterator, the implementation
     * may choose to reuse this element, so you may need to make a copy if you want to keep it
     */
    interface Element {

        /** @return the value of this vector element. */
        double get();

        /** @return the index of this vector element. */
        int index();

        /** @param value Set the current element to value. */
        void set(double value);
    }
}
