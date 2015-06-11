package de.tuberlin.pserver.ml.optimization;

import de.tuberlin.pserver.math.Vector;

public interface WeightsObserver {

    public abstract void weightsUpdate(final int epoch, final Vector weights);
}
