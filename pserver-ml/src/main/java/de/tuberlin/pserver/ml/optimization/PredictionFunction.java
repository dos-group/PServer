package de.tuberlin.pserver.ml.optimization;

import de.tuberlin.pserver.math.matrix.Matrix;

public interface PredictionFunction {

    public abstract double predict(final Matrix X, final Matrix W);


    class LinearPrediction implements PredictionFunction {

        @Override
        public double predict(final Matrix X, final Matrix W) {
            return (Double)X.dot(W);
        }
    }

    class LinearBinaryPrediction implements PredictionFunction {

        @Override
        public double predict(final Matrix X, final Matrix W) {
            return (Double)Math.signum(((Double)X.dot(W)));
        }
    }
}

