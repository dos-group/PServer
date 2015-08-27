package de.tuberlin.pserver.examples.experiments.liblinear;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.vector.Vector;


public interface TronFunction {

    public double functionValue(final Matrix dataPoints, final Vector w_broad, final Parameter param) throws Exception;

    public Vector gradient(final Matrix dataPoints, final Vector w_broad, final Parameter param) throws Exception;

    public Vector hessianVector(final Matrix dataPoints, final Vector w_broad, final Parameter param, final Vector s) throws Exception;
}
