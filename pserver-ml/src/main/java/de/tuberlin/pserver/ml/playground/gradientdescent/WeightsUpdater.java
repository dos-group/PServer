package de.tuberlin.pserver.ml.playground.gradientdescent;

import de.tuberlin.pserver.math.Matrix;

public interface WeightsUpdater {

    public abstract void updateWeights(final int epoch, final Matrix weights);
}
