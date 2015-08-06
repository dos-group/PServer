package de.tuberlin.pserver.math.delegates;

import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.Vector;

public interface LibraryMatrixOps<TM extends Matrix, TV extends Vector> {

    /**
     * Computes Matrix-Matrix-Addition C = A + B and returns C.
     */
    TM add(final TM A, final TM B, final TM C);

    /**
     * Computes Matrix-Matrix-Subtraction C = A - B and returns C.
     */
    TM sub(final TM A, final TM B, final TM C);

    /**
     * Computes Matrix-Matrix-Multiplication C = A * B and returns C.
     */
    TM mul(final TM A, final TM B, final TM C);

    /**
     * Computes Matrix-Vector-Multiplication c = A * b and returns c.
     */
    TV mul(final TM A, final TV b, final TV c);

    /**
     * Computes Matrix-Scalar-Multiplication B = A * a.
     */
    TM scale(final TM A, final double alpha, final TM B);

    /**
     * Computes transpose of A: B = A<sup>T</sup>.
     */
    TM transpose(final TM A, final TM B);

    /**
     * Computes inverse of A: B = A<sup>-1</sup>.
     */
    boolean invert(final TM A, final TM B);
}
