package de.tuberlin.pserver.math.operations;


public interface ApplyOnElements<V extends Number> {

    V applyOnElements(final UnaryOperator<V> f);

    V applyOnElements(final UnaryOperator<V> f, final V B);

    V applyOnElements(final V B, final BinaryOperator<V> f);

    V applyOnElements(final V B, final BinaryOperator<V> f, final V C);
}
