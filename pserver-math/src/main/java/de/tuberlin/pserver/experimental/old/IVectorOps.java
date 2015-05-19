package de.tuberlin.pserver.experimental.old;

public interface IVectorOps<TV> {

    public abstract TV scale(final double alpha);             // x = alpha * x

    public abstract TV add(final TV y);                       // x = y + x

    public abstract TV add(final double alpha, final TV y);   // x = alpha * y + x

    public abstract double dot(final TV y);                   // x = x^T * y
}