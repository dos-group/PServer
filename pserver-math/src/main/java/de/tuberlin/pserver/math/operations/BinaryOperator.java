package de.tuberlin.pserver.math.operations;

@FunctionalInterface
public interface BinaryOperator<V extends Number> {
    /**
     * Applies this operator to the given operands.
     *
     * @param left the first operand
     * @param right the second operand
     * @return the operator result
     */
    V apply(V left, V right);
}
