package de.tuberlin.pserver.ml.optimization;

import de.tuberlin.pserver.math.matrix.Matrix;


public interface PartialLossFunction {

    public abstract double loss(final Matrix X, final double y, final double yPredict);

    public abstract Matrix derivative(final Matrix X, final double y, final double yPredict);

    public abstract Matrix hessian(final Matrix X, final double y, final double yPredict);


    class SquareLoss implements PartialLossFunction {

        @Override
        public double loss(final Matrix X, final double y, final double yPredict) {
            return .5 * (y - yPredict) * (y - yPredict);
        }

        @Override
        public Matrix derivative(final Matrix X, final double y, final double yPredict) {
            return X.scale(y - yPredict);
        }

        @Override
        public Matrix hessian(final Matrix X, final double y, final double yPredict) {
            return X.mul(X).scale(y - yPredict);
        }
    }


    class LogLoss implements PartialLossFunction {

        @Override
        public double loss(final Matrix X, final double y, final double yPredict) {
            return (-1. * Math.log(sigmoid(y * yPredict)));
        }

        @Override
        public Matrix derivative(final Matrix X, final double y, final double yPredict) {
            return X.scale(-y * sigmoid(-y * yPredict));
        }

        @Override
        public Matrix hessian(final Matrix X, final double y, final double yPredict) {
            return X.mul(X).scale(-y * yPredict);
        }

        private static double sigmoid(double z) {
            return 1. / (1. + Math.exp(-z));
        }
    }
}
