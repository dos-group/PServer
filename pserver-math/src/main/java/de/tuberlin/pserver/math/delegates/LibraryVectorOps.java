package de.tuberlin.pserver.math.delegates;


public interface LibraryVectorOps<TV> {

    /**
     * Computes vector-scalar-multiplication y = x * alpha.
     */
    TV mul(final TV x, final double alpha, final TV z);

    /**
     * Computes vector-scalar-division y = x / alpha.
     */
    TV div(final TV x, final double alpha, final TV z);             // x = alpha * x

    /**
     * Computes vector-vector-addition z = x + y.
     */
    TV add(final TV x, final TV y, final TV z);

    /**
     * Computes vector-vector-subtraction z = x - y.
     */
    TV sub(final TV x, final TV y, TV z);

    /**
     * Computes z = alpha * y + x.
     */
    TV add(final TV x, final double alpha, final TV y, final TV z);

    double dot(final TV x, final TV y);

    double norm(final TV x, final double power);

    double maxValue(final TV x);

    double minValue(final TV x);

    double zSum(final TV x);
}
