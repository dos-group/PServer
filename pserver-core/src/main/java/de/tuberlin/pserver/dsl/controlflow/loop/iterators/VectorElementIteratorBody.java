package de.tuberlin.pserver.dsl.controlflow.loop.iterators;


import de.tuberlin.pserver.math.vector.Vector;

public interface VectorElementIteratorBody {

    public abstract void body(final long epoch, final Vector.ElementIterator iter) throws Exception;
}
