package de.tuberlin.pserver.ml.optimization;


import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix32F;

public interface RegularizationFunction {

    public abstract float regularize(final Matrix32F W, final float lambda);

    public abstract Matrix32F regularizeDerivative(final Matrix32F W, final float lambda);

    public abstract float regularizeHessian(final Matrix32F W, final float lambda);

    class L2Regularization implements RegularizationFunction {

        @Override
        public float regularize(final Matrix32F W, final float lambda) {
            // do not include intercept
            final float sq  = (float)W.get(0);
            final float dot = (float)W.dot(W);
            return 0.5f * lambda * (dot - sq * sq);
        }

        @Override
        public Matrix32F regularizeDerivative(final Matrix32F W, final float lambda) {
            Matrix32F regularization = ((DenseMatrix32F)W).fastScaleNew(lambda);
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
