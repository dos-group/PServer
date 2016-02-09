package de.tuberlin.pserver.math.matrix32.operations;


public interface MatrixElementUnaryOperator32 {

    float apply(final long row, final long col, final float element);
}
