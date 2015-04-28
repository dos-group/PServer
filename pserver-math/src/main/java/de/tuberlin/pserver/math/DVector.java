package de.tuberlin.pserver.math;


import java.io.Serializable;

public interface DVector extends IVectorOps<DVector>, Serializable {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public enum VectorType {

        ROW_VECTOR,

        COLUMN_VECTOR
    }

    // ---------------------------------------------------
    // Abstract Methods.
    // ---------------------------------------------------

    public abstract int size();

    public abstract void set(final int index, final double value);

    public abstract double get(final int index);

    public abstract DVector zero();

    public abstract DVector set(final DVector y);

    public abstract VectorType getVectorType();

    public abstract double[] toArray();

    public abstract void setArray(final double[] data);
}
