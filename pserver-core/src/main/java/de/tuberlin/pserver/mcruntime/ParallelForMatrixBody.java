package de.tuberlin.pserver.mcruntime;


public interface ParallelForMatrixBody {

    public void perform(final long i, final long j, final double value) throws Exception;
}
