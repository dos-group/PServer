package de.tuberlin.pserver.ml.playground.optimization1;

import de.tuberlin.pserver.math.matrix.Matrix;

public interface Observer {

    public abstract void update(final int epoch, final Matrix weights, final Matrix[] gradientSum);
}
