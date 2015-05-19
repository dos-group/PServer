package de.tuberlin.pserver.math.delegates;


public interface LibraryVectorOps<TV> {

    public abstract TV mul(final TV x, final double alpha);             // x = alpha * x

    public abstract TV div(final TV x, final double alpha);             // x = alpha * x

    public abstract TV add(final TV x, final TV y);                       // x = y + x

    public abstract TV sub(final TV x, final TV y);                       // x = y - x


    public abstract TV add(final TV x, final double alpha, final TV y);   // x = alpha * y + x

    public abstract double dot(final TV x, final TV y);                   // x = x^T * y

    public abstract double norm(final TV x, final double power);

    public abstract double maxValue(final TV x);

    public abstract double minValue(final TV x);

    public abstract double zSum(final TV x);
}
