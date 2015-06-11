package de.tuberlin.pserver.ml.optimization;


import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.Vector;

public interface Optimizer {

    public abstract Vector optimize(final Vector weights, final Matrix.RowIterator dataIterator);
}
