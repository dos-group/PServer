package de.tuberlin.pserver.runtime.delta;

public interface VectorDeltaFilter {

    public boolean filter(final long i, final double ov, final double nv);
}
