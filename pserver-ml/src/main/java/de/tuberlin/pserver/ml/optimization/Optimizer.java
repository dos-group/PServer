package de.tuberlin.pserver.ml.optimization;

import de.tuberlin.pserver.math.matrix.Matrix;


public interface Optimizer {

    public abstract Matrix optimize(final Matrix X, final Matrix y, Matrix W) throws Exception;
}
