package de.tuberlin.pserver.types.matrix.implementation.f32.operations;


import java.util.Objects;

public interface UnaryOperator32 {

    /**
     * Applies this operator to the given operand.
     *
     * @param operand the operand
     * @return the operator result
     */
    float apply(float operand);

    /**
     * Returns a composed operator that first applies the {@code before}
     * operator to its input, and then applies this operator to the result.
     * If evaluation of either operator throws an exception, it is relayed to
     * the caller of the composed operator.
     *
     * @param before the operator to apply before this operator is applied
     * @return a composed operator that first applies the {@code before}
     * operator and then applies this operator
     * @throws NullPointerException if before is null
     *
     * @see #andThen(UnaryOperator32)
     */
    default UnaryOperator32 compose(UnaryOperator32 before) {
        Objects.requireNonNull(before);
        return (UnaryOperator32)((v) -> apply(before.apply(v)));
    }

    /**
     * Returns a composed operator that first applies this operator to
     * its input, and then applies the {@code after} operator to the result.
     * If evaluation of either operator throws an exception, it is relayed to
     * the caller of the composed operator.
     *
     * @param after the operator to apply after this operator is applied
     * @return a composed operator that first applies this operator and then
     * applies the {@code after} operator
     * @throws NullPointerException if after is null
     *
     * @see #compose(UnaryOperator32)
     */
    default UnaryOperator32 andThen(UnaryOperator32 after) {
        Objects.requireNonNull(after);
        return (UnaryOperator32)((t) -> after.apply(apply(t)));
    }

    /**
     * Returns a unary operator that always returns its input argument.
     *
     * @return a unary operator that always returns its input argument
     */
    static UnaryOperator32 identity() {
        return t -> t;
    }
}
