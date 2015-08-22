package de.tuberlin.pserver.runtime.state.merger;


public interface VectorUpdateMerger extends UpdateMerger {

    public double mergeElement(final long i, final double val, final double remoteVal);
}
