package de.tuberlin.pserver.runtime.parallel;


public interface ParallelForRowMatrixBody {

    public void perform(final long row) throws Exception;
}
