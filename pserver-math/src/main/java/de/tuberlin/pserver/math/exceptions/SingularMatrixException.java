package de.tuberlin.pserver.math.exceptions;

/**
 * Thrown if matrix inversion not be calculated due to singular matrix.
 */
public class SingularMatrixException extends RuntimeException {

    public SingularMatrixException(String message) {
        super(message);
    }
}
