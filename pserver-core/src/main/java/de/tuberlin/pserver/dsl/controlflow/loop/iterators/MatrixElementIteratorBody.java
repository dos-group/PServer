package de.tuberlin.pserver.dsl.controlflow.loop.iterators;

public interface MatrixElementIteratorBody {

    public abstract void body(final long epoch, final long i, final long j, final double v) throws Exception;
}
