package de.tuberlin.pserver.mcruntime;

public interface ParallelForBody<T> {

    void perform(final T e) throws Exception;
}
