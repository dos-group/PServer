package de.tuberlin.pserver.runtime.delta;


public interface MatrixDeltaMerger {

    public double mergeElement(final long i, final long j, final double val, final double remoteVal);
}
