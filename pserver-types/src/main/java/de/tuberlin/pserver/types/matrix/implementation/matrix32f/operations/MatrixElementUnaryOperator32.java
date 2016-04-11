package de.tuberlin.pserver.types.matrix.implementation.matrix32f.operations;


public interface MatrixElementUnaryOperator32 {

    float apply(final long row, final long col, final float element);
}
