package de.tuberlin.pserver.types.matrix.utils.exceptions;

/**
 * Thrown if matrix inversion not be calculated due to singular matrix.
 */
public class SingularMatrixException extends RuntimeException {

    public SingularMatrixException(String message) {
        super(message);
    }
}
