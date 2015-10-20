package de.tuberlin.pserver.math.operations;


public interface MatrixElementUnaryOperator<V extends Number> {

    V apply(final long row, final long col, final V element);
}
