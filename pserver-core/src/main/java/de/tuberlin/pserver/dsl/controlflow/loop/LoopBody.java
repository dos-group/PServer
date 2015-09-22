package de.tuberlin.pserver.dsl.controlflow.loop;

public interface LoopBody {

    public abstract void body(final long epoch) throws Exception;
}
