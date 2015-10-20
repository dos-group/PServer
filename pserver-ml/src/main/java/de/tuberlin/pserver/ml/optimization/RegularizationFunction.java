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
            final double sq  = (Double)W.get(0);
            final double dot = (Double)W.dot(W);
            return 0.5 * lambda * (dot - sq * sq);
        }

        @Override
        public Matrix regularizeDerivative(final Matrix W, final double lambda) {
            Matrix regularization = W.scale(lambda);
            // do not regularize intercept
            regularization.set(0, 0, lambda);
            return regularization;
        }

        @Override
        public double regularizeHessian(final Matrix W, final double lambda) {
            return lambda;
        }
    }
}
