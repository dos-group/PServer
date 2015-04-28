package de.tuberlin.pserver.math;

public interface IMatrixOps<TM, TV> {

    public abstract TM add(final TM B);             // A = B + A

    public abstract TM sub(final TM B);             // A = B - A

    public abstract TV mul(final TV x, final TV y); // y = A * x

    public abstract TM scale(final double alpha);   // A = alpha * A

    public abstract TM transpose();                 // A = A^T

    public abstract TM transpose(final TM B);       // B = A^T

    public abstract boolean invert();               // A = A^-1
}
