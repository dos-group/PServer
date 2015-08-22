package de.tuberlin.pserver.dsl.controlflow.iteration;


import de.tuberlin.pserver.math.vector.Vector;

public interface VectorElementIterationBody {

    public abstract void body(final long epoch, final Vector.ElementIterator iter) throws Exception;
}
