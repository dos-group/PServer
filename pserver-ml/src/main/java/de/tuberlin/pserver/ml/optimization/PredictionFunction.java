package de.tuberlin.pserver.ml.optimization;

import de.tuberlin.pserver.math.matrix.Matrix32F;

public interface PredictionFunction {

    public abstract float predict(final Matrix32F X, final Matrix32F W);


    class LinearPrediction implements PredictionFunction {

        @Override
        public float predict(final Matrix32F X, final Matrix32F W) {
            return (float)X.dot(W);
        }
    }

    class LinearBinaryPrediction implements PredictionFunction {

        @Override
        public float predict(final Matrix32F X, final Matrix32F W) {
            return Math.signum((X.dot(W)));
        }
    }
}

