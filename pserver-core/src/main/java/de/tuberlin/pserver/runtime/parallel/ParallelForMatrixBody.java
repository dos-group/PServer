package de.tuberlin.pserver.runtime.parallel;


public interface ParallelForMatrixBody {

    void perform(final long i, final long j, final float value) throws Exception;
}
