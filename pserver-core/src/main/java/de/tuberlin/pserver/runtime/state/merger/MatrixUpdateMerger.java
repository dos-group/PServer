package de.tuberlin.pserver.runtime.state.merger;


public interface MatrixUpdateMerger extends UpdateMerger {

    public double mergeElement(final long i, final long j, final double val, final double remoteVal);
}
