package de.tuberlin.pserver.runtime.state.filter;


public interface MatrixUpdateFilter extends UpdateFilter {

    public boolean filter(final long i, final long j, final double ov, final double nv);
}
