package de.tuberlin.pserver.math.exceptions;

/**
 * Thrown if a matrix- or vector-operation can not be performed due to incompatible shapes or dimensions of its operands.
 */
public class IncompatibleShapeException extends RuntimeException {

    public IncompatibleShapeException(String message) {
        super(message);
    }

    public IncompatibleShapeException(String message, Object... args) {
        super(String.format(message, args));
    }

}
