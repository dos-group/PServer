package de.tuberlin.pserver.ml.optimization;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.dense.Dense64Matrix;

public interface PredictionFunction {

    public abstract double predict(final Matrix X, final Matrix W);


    class LinearPrediction implements PredictionFunction {

        @Override
        public double predict(final Matrix X, final Matrix W) {
            return X.dot(W);
        }
    }

    class LinearBinaryPrediction implements PredictionFunction {

        @Override
        public double predict(final Matrix X, final Matrix W) {
            return Math.signum(X.dot(W));
        }
    }
}

