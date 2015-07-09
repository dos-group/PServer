package de.tuberlin.pserver.math;

import java.util.Iterator;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

public interface Vector extends MObject {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public enum Format {

        SPARSE_VECTOR,

        DENSE_VECTOR
    }

    public enum Layout {

        ROW_LAYOUT,

        COLUMN_LAYOUT
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract long length();

    public abstract Format format();

    public abstract Layout layout();

    public abstract void set(final long index, final double value);

    public abstract double get(final long index);
    
    public abstract double atomicGet(final long index);

    public abstract void atomicSet(final long index, final double value);

    public abstract Vector mul(final double alpha);                 // x = alpha * x

    public abstract Vector div(final double alpha);

    public abstract Vector add(final Vector y);                       // x = y + x

    public abstract Vector sub(final Vector y);                       // x = y - x

    public abstract Vector add(final double alpha, final Vector y);   // x = alpha * y + x

    public abstract double dot(final Vector y);                       // x = x^T * y

    public abstract double sum();

    public abstract double norm(final double v);

    public abstract double maxValue();

    public abstract double minValue();

    public abstract Vector applyOnElements(final DoubleUnaryOperator vf);

    public abstract Vector applyOnElements(final Vector v2, final DoubleUnaryOperator vf);

    public abstract Vector applyOnElements(final Vector v2, final DoubleBinaryOperator vf);

    public abstract Vector assign(final Vector v);

    public abstract Vector assign(final double v);

    public abstract Vector assign(final DoubleUnaryOperator df);

    public abstract Vector assign(final Vector v, DoubleBinaryOperator df);

    public abstract Vector viewPart(final long s, final long e);

    public abstract Vector like();

    public abstract Iterator<Element> iterateNonZero();

    public abstract double aggregate(DoubleBinaryOperator aggregator, DoubleUnaryOperator map);

    public abstract Vector copy();

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
