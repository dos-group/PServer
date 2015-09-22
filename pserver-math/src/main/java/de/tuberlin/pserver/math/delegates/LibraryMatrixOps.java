package de.tuberlin.pserver.math.delegates;

import de.tuberlin.pserver.math.matrix.Matrix;

public interface LibraryMatrixOps<T extends Matrix> {

    /**
     * Computes Matrix-Matrix-Addition C = A + B and returns C.
     */
    T add(final T A, final T B, final T C);

    /**
     * Computes Matrix-Matrix-Subtraction C = A - B and returns C.
     */
    T sub(final T A, final T B, final T C);

    /**
     * Computes Matrix-Matrix-Multiplication C = A * B and returns C.
     */
    T mul(final T A, final T B, final T C);

    /**
     * Computes Matrix-Scalar-Multiplication B = A * a.
     */
    T scale(final T A, final double alpha, final T B);

    /**
     * Computes transpose of A: B = A<sup>T</sup>.
     */
    T transpose(final T A, final T B);

    /**
     * Computes inverse of A: B = A<sup>-1</sup>.
     */
    boolean invert(final T A, final T B);
}
