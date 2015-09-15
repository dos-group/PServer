package de.tuberlin.pserver.examples.experiments.liblinear;

import de.tuberlin.pserver.math.matrix.Matrix;


public interface TronFunction {

    public double functionValue(final Matrix dataPoints, final Matrix w_broad, final Parameter param) throws Exception;

    public Matrix gradient(final Matrix dataPoints, final Matrix w_broad, final Parameter param) throws Exception;

    public Matrix hessianVector(final Matrix dataPoints, final Matrix w_broad, final Parameter param, final Matrix s) throws Exception;
}
