package de.tuberlin.pserver.ml.optimization;


import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;

public interface PartialLossFunction {

    public abstract float loss(final Matrix32F X, final float y, final float yPredict);

    public abstract Matrix32F derivative(final Matrix32F X, final float y, final float yPredict);

    public abstract Matrix32F hessian(final Matrix32F X, final float y, final float yPredict);


    class SquareLoss implements PartialLossFunction {

        @Override
        public float loss(final Matrix32F X, final float y, final float yPredict) {
            return .5f * (y - yPredict) * (y - yPredict);
        }

        @Override
        public Matrix32F derivative(final Matrix32F X, final float y, final float yPredict) {
            return X.scale(y - yPredict);
        }

        @Override
        public Matrix32F hessian(final Matrix32F X, final float y, final float yPredict) {
            return X.mul(X).scale(y - yPredict);
        }
    }


    class LogLoss implements PartialLossFunction {

        @Override
        public float loss(final Matrix32F X, final float y, final float yPredict) {
            return (float)(-1. * Math.log(sigmoid(y * yPredict)));
        }

        @Override
        public Matrix32F derivative(final Matrix32F X, final float y, final float yPredict) {
            return X.scale(-y * sigmoid(-y * yPredict));
        }

        @Override
        public Matrix32F hessian(final Matrix32F X, final float y, final float yPredict) {
            return X.mul(X).scale(-y * yPredict);
        }

        private static float sigmoid(float z) {
            return (float)(1. / (1. + Math.exp(-z)));
        }
    }
}
