package de.tuberlin.pserver.ml.optimization.gradientdescent;


import de.tuberlin.pserver.experimental.old.DMatrix;

public interface WeightsUpdater {

    public abstract void updateWeights(final int epoch, final DMatrix weights);
}
