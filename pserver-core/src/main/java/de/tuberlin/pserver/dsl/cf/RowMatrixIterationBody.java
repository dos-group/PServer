package de.tuberlin.pserver.dsl.cf;

import de.tuberlin.pserver.math.Matrix;

public interface RowMatrixIterationBody {

    public abstract void body(Matrix.RowIterator iter);
}
