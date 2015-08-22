package de.tuberlin.pserver.dsl.controlflow.iteration;

import de.tuberlin.pserver.math.matrix.Matrix;

public interface MatrixRowIterationBody {

    public abstract void body(final long epoch, final Matrix.RowIterator iter) throws Exception;
}
