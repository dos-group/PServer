package de.tuberlin.pserver.ml.optimization;

import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;

public interface PredictionFunction {

    float predict(final Matrix32F X, final Matrix32F W);


    class LinearPrediction implements PredictionFunction {

        @Override
        public float predict(final Matrix32F X, final Matrix32F W) {
            return X.dot(W);
        }
    }

    class LinearBinaryPrediction implements PredictionFunction {

        @Override
        public float predict(final Matrix32F X, final Matrix32F W) {
            return Math.signum((X.dot(W)));
        }
    }
}

