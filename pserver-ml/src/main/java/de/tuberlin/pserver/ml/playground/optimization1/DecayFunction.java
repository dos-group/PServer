package de.tuberlin.pserver.ml.playground.optimization1;


public interface DecayFunction {

    public abstract double decayLearningRate(final int epoch, final double initialAlpha, final double lastAlpha);

    // ---------------------------------------------------

    class SimpleDecay implements DecayFunction {

        @Override
        public double decayLearningRate(final int epoch, final double initialAlpha, final double lastAlpha) {
            return initialAlpha / Math.sqrt(epoch);
        }
    }

    class IterationBasedDecay implements DecayFunction {

        // Sets a simple annealing (alpha / (1 + current_iteration / phi)) where phi
        // is the given parameter here. This will gradually lower the global
        // learning rate after the given amount of iterations.
        private int decayEpoch;

        public IterationBasedDecay(final int decayEpoch) { this.decayEpoch = decayEpoch; }

        @Override
        public double decayLearningRate(final int epoch, final double initialAlpha, final double lastAlpha) {
            return initialAlpha / (1d + epoch / decayEpoch);
        }
    }
}
