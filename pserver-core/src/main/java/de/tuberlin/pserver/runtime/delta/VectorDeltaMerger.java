package de.tuberlin.pserver.runtime.delta;


public interface VectorDeltaMerger {

    public double mergeElement(final long i, final double val, final double remoteVal);
}
