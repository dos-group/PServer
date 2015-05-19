package de.tuberlin.pserver.math.delegates;

import de.tuberlin.pserver.math.Vector;
import de.tuberlin.pserver.math.Matrix;

public interface LibraryMatrixOps<TM extends Matrix, TV extends Vector> {

    public abstract TM add(final TM B, final TM A);             // A = B + A

    public abstract TM sub(final TM B, final TM A);             // A = B - A

    public abstract TV mul(final TM A, final TV x);

    public abstract TM mul(final TM A, final TM B);

    public abstract TV mul(final TM A, final TV x, final TV y); // y = A * x

    public abstract TM scale(final double alpha, final TM A);   // A = alpha * A

    public abstract TM transpose(final TM A);                   // A = A^T

    public abstract TM transpose(final TM B, final TM A);       // B = A^T

    public abstract boolean invert(final TM A);                 // A = A^-1
}