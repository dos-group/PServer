package de.tuberlin.pserver.math.matrix32.operations;

@FunctionalInterface
public interface BinaryOperator32 {
    /**
     * Applies this operator to the given operands.
     *
     * @param left the first operand
     * @param right the second operand
     * @return the operator result
     */
    float apply(float left, float right);
}