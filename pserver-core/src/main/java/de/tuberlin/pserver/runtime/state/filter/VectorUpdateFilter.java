package de.tuberlin.pserver.runtime.state.filter;

public interface VectorUpdateFilter extends UpdateFilter {

    public boolean filter(final long i, final double ov, final double nv);
}
