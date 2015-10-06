package de.tuberlin.pserver.runtime.mcruntime;


public interface ParallelForBody<T> {

    void perform(final T e) throws Exception;
}
