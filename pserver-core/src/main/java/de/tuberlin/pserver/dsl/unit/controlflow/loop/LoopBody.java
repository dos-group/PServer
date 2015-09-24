package de.tuberlin.pserver.dsl.unit.controlflow.loop;

public interface LoopBody {

    public abstract void body(final long epoch) throws Exception;
}
