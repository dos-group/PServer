package de.tuberlin.pserver.ml.optimization;


import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;

public interface Optimizer {

    Matrix32F optimize(final Matrix32F X, final Matrix32F y, Matrix32F W) throws Exception;
}
