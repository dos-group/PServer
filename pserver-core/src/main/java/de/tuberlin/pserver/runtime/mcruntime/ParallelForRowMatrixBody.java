package de.tuberlin.pserver.runtime.mcruntime;


public interface ParallelForRowMatrixBody {

    public void perform(final long row) throws Exception;
}
