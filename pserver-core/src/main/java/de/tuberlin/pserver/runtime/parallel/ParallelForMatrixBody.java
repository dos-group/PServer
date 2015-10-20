package de.tuberlin.pserver.runtime.parallel;


public interface ParallelForMatrixBody<V extends Number> {

    public void perform(final long i, final long j, final V value) throws Exception;
}
