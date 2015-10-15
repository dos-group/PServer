package de.tuberlin.pserver.math.operations;

import de.tuberlin.pserver.math.matrix.Matrix;

public interface MatrixAggregation<V extends Number> {

    V apply(final Matrix f);
}