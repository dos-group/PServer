package de.tuberlin.pserver.dsl.controlflow.iteration;

public interface IterationBody {

    public abstract void body(final long epoch) throws Exception;
}
