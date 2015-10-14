package de.tuberlin.pserver.runtime.mcruntime;


public interface ParallelForMatrixBody {

    public void perform(final long i, final long j, final double value) throws Exception;
}
