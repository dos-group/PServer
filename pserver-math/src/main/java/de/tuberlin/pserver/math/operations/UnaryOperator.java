package de.tuberlin.pserver.math.operations;

import java.util.Objects;

@FunctionalInterface
public interface UnaryOperator<V extends Number> {

    /**
     * Applies this operator to the given operand.
     *
     * @param operand the operand
     * @return the operator result
     */
    V apply(V operand);

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
     * @see #andThen(UnaryOperator)
     */
    default UnaryOperator compose(UnaryOperator<V> before) {
        Objects.requireNonNull(before);
        return (UnaryOperator<V>)((V v) -> apply(before.apply(v)));
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
     * @see #compose(UnaryOperator)
     */
    default UnaryOperator andThen(UnaryOperator<V> after) {
        Objects.requireNonNull(after);
        return (UnaryOperator<V>)((V t) -> after.apply(apply(t)));
    }

    /**
     * Returns a unary operator that always returns its input argument.
     *
     * @return a unary operator that always returns its input argument
     */
    static UnaryOperator identity() {
        return t -> t;
    }
}
