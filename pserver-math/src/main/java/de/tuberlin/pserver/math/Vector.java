package de.tuberlin.pserver.math;

import java.util.Iterator;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

public interface Vector extends MObject {

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

    Vector mul(final double alpha);                 // x = alpha * x

    Vector div(final double alpha);

    Vector add(final Vector y);                       // x = y + x

    Vector sub(final Vector y);                       // x = y - x

    Vector add(final double alpha, final Vector y);   // x = alpha * y + x

    double dot(final Vector y);                       // x = x^T * y

    double sum();

    double norm(final double v);

    double maxValue();

    double minValue();

    Vector applyOnElements(final DoubleUnaryOperator vf);

    Vector applyOnElements(final Vector v2, final DoubleUnaryOperator vf);

    Vector applyOnElements(final Vector v2, final DoubleBinaryOperator vf);

    Vector assign(final Vector v);

    Vector assign(final double v);

    Vector assign(final DoubleUnaryOperator df);

    Vector assign(final Vector v, DoubleBinaryOperator df);

    Vector viewPart(final long s, final long e);

    Vector like();

    Iterator<Element> iterateNonZero();

    double aggregate(DoubleBinaryOperator aggregator, DoubleUnaryOperator map);

    Vector copy();

    /**
     * A holder for information about a specific item in the Vector. <p/> When using with an Iterator, the implementation
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
