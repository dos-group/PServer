package de.tuberlin.pserver.ml.optimization;

import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.math.vector.dense.DVector;

public interface PredictionFunction {

    public abstract double predict(final Vector features, final Vector weights);

    public abstract Vector gradient(final Vector features, final Vector weights);

    // ---------------------------------------------------

    class LinearPredictionFunction implements PredictionFunction {

        @Override
        public double predict(final Vector features, final Vector weights) {
            return features.dot(weights);
        }

        public Vector gradient(final Vector features, final Vector weights) {
            return new DVector((DVector)weights);
        }
    }
}

