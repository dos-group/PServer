package de.tuberlin.pserver.types.matrix.f32.operations;


public interface MatrixElementUnaryOperator32 {

    float apply(final long row, final long col, final float element);
}
