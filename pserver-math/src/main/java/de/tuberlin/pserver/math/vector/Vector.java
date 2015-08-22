package de.tuberlin.pserver.math.vector;

import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.math.exceptions.IncompatibleShapeException;
import de.tuberlin.pserver.math.operations.ApplyOnDoubleElements;

import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

public interface Vector extends SharedObject, ApplyOnDoubleElements<Vector> {

    // ---------------------------------------------------
    // Inner Interfaces/Classes.
    // ---------------------------------------------------

    interface ElementIterator {

        boolean hasNextElement();

        void nextElement();

        void nextRandomElement();

        double value();

        void reset();

        long length();

        int getCurrentElementNum();
    }

    // ---------------------------------------------------
    // Methods.
    // ---------------------------------------------------

    public long length();

    public Format format();

    public Layout layout();

    public void set(final long index, final double value);

    public double get(final long index);

    public double atomicGet(final long index);

    public void atomicSet(final long index, final double value);

    /**
     * Identical to {@link #mul(double, Vector)} but automatically creates the resulting <code>vector y</code>.
     */
    public Vector mul(final double alpha);

    /**
     * Called on vector x. Computes vector-scalar-multiplication y = x * alpha. <br>
     * <strong>Note: x and y have to be of the same shape</strong>
     * @param alpha the scalar to multiply with x
     * @param y the vector to store the result in
     * @return y after computing vector-scalar-multiplication y = x * alpha
     * @throws IncompatibleShapeException If x and y are not of equal length.
     */
    public Vector mul(final double alpha, final Vector y);


    public Vector div(final double alpha);

    /**
     * Called on Vector x. Computes vector-scalar-division y = x / alpha. <br>
     * <strong>Note: x and y have to be of the same shape</strong>
     * @param alpha the scalar to multiply with x
     * @param y the vector to store the result in
     * @return y after computing vector-scalar-division y = x / alpha
     * @throws IncompatibleShapeException If x and y are not of equal length.
     */
    public Vector div(final double alpha, final Vector y);

    /**
     * Identical to {@link #add(Vector, Vector)} but automatically creates the resulting <code>vector z</code>.
     */
    public Vector add(final Vector y);

    /**
     * Called on vector x. Computes vector-vector-addition z = x + y. <br>
     * <strong>Note: x and y have to be of the same shape</strong>
     * @param y the vector to add on x
     * @param z the vector to store the result in
     * @return z after computing vector-vector-addition z = x + y
     * @throws IncompatibleShapeException If x and y are not of equal length.
     */
    public Vector add(final Vector y, final Vector z);

    /**
     * Identical to {@link #sub(Vector, Vector)} but automatically creates the resulting <code>vector z</code>.
     */
    public Vector sub(final Vector y);

    /**
     * Called on vector x. Computes vector-vector-subtraction z = x - y. <br>
     * <strong>Note: x and y have to be of the same shape</strong>
     * @param y the vector to subtract from x
     * @param z the vector to store the result in
     * @return y after computing vector-vector-subtraction z = x - y
     * @throws IncompatibleShapeException If x and y are not of equal length.
     */
    public Vector sub(final Vector y, final Vector z);

    /**
     * Identical to {@link #add(double, Vector, Vector)} but automatically creates the resulting <code>vector z</code>.
     */
    public Vector add(final double alpha, final Vector y);

    /**
     * Called on vector x. Computes z = alpha * y + x. <br>
     * <strong>Note: x and y have to be of the same shape</strong>
     * @param y the vector to subtract from x
     * @param z the vector to store the result in
     * @return y after computing z = alpha * y + x
     * @throws IncompatibleShapeException If x and y are not of equal length.
     */
    public Vector add(final double alpha, final Vector y, Vector z);

    public Vector assign(final Vector v);

    public Vector assign(final double v);

    public Vector viewPart(final long s, final long e);

    public Vector like();

    public Vector copy();

    public ElementIterator nonZeroElementIterator();

    public ElementIterator elementIterator();

    public ElementIterator elementIterator(final int start, final int end);

    public double aggregate(DoubleBinaryOperator aggregator, DoubleUnaryOperator map);

    /**
     * Called on vector x. Computes the dot product of x and y.
     * @param y
     * @return dot product of x and y
     */
    public double dot(final Vector y);                       // x = x^T * y

    public double sum();

    public double norm(final double v);

    public double maxValue();

    public double minValue();
}
