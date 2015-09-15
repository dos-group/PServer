package de.tuberlin.pserver.ml.optimization;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.dense.Dense64Matrix;

public interface PredictionFunction {

    public abstract double predict(final Matrix features, final Matrix weights);

    public abstract Matrix gradient(final Matrix features, final Matrix weights);

    // ---------------------------------------------------

    class LinearPredictionFunction implements PredictionFunction {

        @Override
        public double predict(final Matrix features, final Matrix weights) {
            return features.dot(weights);
        }

        public Matrix gradient(final Matrix features, final Matrix weights) {
            return new Dense64Matrix((Dense64Matrix)weights);
        }
    }
}

