package de.tuberlin.pserver.runtime.delta;


public interface MatrixDeltaFilter {

    public boolean filter(final long i, final long j, final double ov, final double nv);
}
