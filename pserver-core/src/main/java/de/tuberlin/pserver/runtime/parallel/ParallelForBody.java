package de.tuberlin.pserver.runtime.parallel;


public interface ParallelForBody<T> {

    void perform(final T e) throws Exception;
}
