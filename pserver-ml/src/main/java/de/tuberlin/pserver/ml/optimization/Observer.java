package de.tuberlin.pserver.ml.optimization;

import de.tuberlin.pserver.math.Vector;

public interface Observer {

    public abstract void update(final int epoch, final Vector weights, final Vector[] gradientSum);
}