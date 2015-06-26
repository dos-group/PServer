package de.tuberlin.pserver.ml.playground.optimization1;

import de.tuberlin.pserver.math.DVector;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.Vector;

public interface PredictionFunction {

    public abstract double predict(final Vector features, final Matrix weights);

    public abstract Vector gradient(final Vector features, final Matrix weights);

    // ---------------------------------------------------

    class LinearPredictionFunction implements PredictionFunction {

        @Override
        public double predict(final Vector features, final Matrix weights) {
            return features.dot(weights.rowAsVector());
        }

        public Vector gradient(final Vector features, final Matrix weights) {
            return new DVector((DVector)weights.rowAsVector());
        }
    }
}
