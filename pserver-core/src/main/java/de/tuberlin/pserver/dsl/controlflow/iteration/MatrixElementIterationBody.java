package de.tuberlin.pserver.dsl.controlflow.iteration;

public interface MatrixElementIterationBody {

    public abstract void body(final long i, final long j, final double v);
}
