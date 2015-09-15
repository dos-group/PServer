package de.tuberlin.pserver.dsl.controlflow.loop.iterators;

import de.tuberlin.pserver.math.matrix.Matrix;

public interface MatrixRowIteratorBody {

    public abstract void body(final long epoch, final Matrix.RowIterator iter) throws Exception;
}
