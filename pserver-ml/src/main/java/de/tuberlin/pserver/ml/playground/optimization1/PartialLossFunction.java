package de.tuberlin.pserver.ml.playground.optimization1;


public interface PartialLossFunction {

    public abstract double loss(final double prediction, final double label);

    public abstract double derivative(final double prediction, final double label);

    // ---------------------------------------------------

    class SquareLoss implements PartialLossFunction {

        @Override
        public double loss(final double prediction, final double label) {
            return 0.5 * (prediction - label) * (prediction - label);
        }

        @Override
        public double derivative(final double prediction, final double label) {
            return (prediction - label);
        }
    }
}
