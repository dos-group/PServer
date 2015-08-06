package de.tuberlin.pserver.math;


import java.io.Serializable;

public interface SharedObject extends Serializable {

    public abstract void setOwner(final Object owner);

    public abstract Object getOwner();

    public abstract long sizeOf();

    public abstract double[] toArray();

    public abstract void setArray(final double[] data);
}
