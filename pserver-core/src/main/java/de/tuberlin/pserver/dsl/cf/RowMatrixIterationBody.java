package de.tuberlin.pserver.dsl.cf;

import de.tuberlin.pserver.math.matrix.Matrix;

public interface RowMatrixIterationBody {

    public abstract void body(Matrix.RowIterator iter);
}
