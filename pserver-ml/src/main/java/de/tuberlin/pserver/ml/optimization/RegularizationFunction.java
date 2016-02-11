package de.tuberlin.pserver.ml.optimization;


import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;

public interface RegularizationFunction {

    float regularize(final Matrix32F W, final float lambda);

    Matrix32F regularizeDerivative(final Matrix32F W, final float lambda);

    float regularizeHessian(final Matrix32F W, final float lambda);

    class L2Regularization implements RegularizationFunction {

        @Override
        public float regularize(final Matrix32F W, final float lambda) {
            // do not include intercept
            final float sq  = W.get(0);
            final float dot = W.dot(W);
            return 0.5f * lambda * (dot - sq * sq);
        }

        @Override
        public Matrix32F regularizeDerivative(final Matrix32F W, final float lambda) {
            Matrix32F regularization = W.scale(lambda);
            // do not regularize intercept
            regularization.set(0, 0, lambda);
            return regularization;
        }

        @Override
        public float regularizeHessian(final Matrix32F W, final float lambda) {
            return lambda;
        }
    }
}
