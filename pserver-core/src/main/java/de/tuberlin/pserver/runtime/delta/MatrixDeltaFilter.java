package de.tuberlin.pserver.runtime.delta;


public interface MatrixDeltaFilter {

    public abstract boolean filter(final long i, final long j, final double ov, final double nv);
}
