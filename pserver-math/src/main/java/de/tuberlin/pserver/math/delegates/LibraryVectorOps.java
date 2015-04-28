package de.tuberlin.pserver.math.delegates;


public interface LibraryVectorOps<TV> {

    public abstract TV scale(final TV x, final double alpha);             // x = alpha * x

    public abstract TV add(final TV x, final TV y);                       // x = y + x

    public abstract TV add(final TV x, final double alpha, final TV y);   // x = alpha * y + x

    public abstract double dot(final TV x, final TV y);                   // x = x^T * y
}
