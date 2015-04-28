package de.tuberlin.pserver.ml.optimization.gradientdescent;


import de.tuberlin.pserver.math.DMatrix;

public interface WeightsUpdater {

    public abstract void updateWeights(final int epoch, final DMatrix weights);
}
