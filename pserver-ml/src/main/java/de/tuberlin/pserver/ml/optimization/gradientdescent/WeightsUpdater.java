package de.tuberlin.pserver.ml.optimization.gradientdescent;


import de.tuberlin.pserver.app.types.DMatrix;

public interface WeightsUpdater {

    public abstract void updateWeights(final int epoch, final DMatrix weights);
}
