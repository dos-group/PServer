package de.tuberlin.pserver.ml.optimization;


import de.tuberlin.pserver.math.matrix.Matrix;

public interface RegularizationFunction {

    public abstract double regularize(final Matrix W, final double lambda);

    public abstract Matrix regularizeDerivative(final Matrix W, final double lambda);

    public abstract double regularizeHessian(final Matrix W, final double lambda);


    class L2Regularization implements RegularizationFunction {

        @Override
        public double regularize(final Matrix W, final double lambda) {
            // do not include intercept
            return 0.5 * lambda * (W.dot(W) - W.get(0) * W.get(0));
        }

        @Override
        public Matrix regularizeDerivative(final Matrix W, final double lambda) {
            Matrix regularization = W.scale(lambda);
            // do not regularize intercept
            regularization.set(0, 0, 0.0);

            return regularization;
        }

        @Override
        public double regularizeHessian(final Matrix W, final double lambda) {
            return lambda;
        }
    }
}
