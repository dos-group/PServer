package de.tuberlin.pserver.ml.optimization;

import de.tuberlin.pserver.math.Vector;

public interface GradientStepFunction {

    public abstract Vector takeStep(final Vector weights, final Vector gradients, final double alpha);

    // ---------------------------------------------------

    class SimpleGradientStep implements GradientStepFunction {

        @Override
        public Vector takeStep(final Vector weights, final Vector gradients, final double alpha) {
            return weights.add(-alpha, gradients);
        }
    }
}
