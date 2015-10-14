package de.tuberlin.pserver.ml.optimization;


public interface LearningRateFunction {

    public abstract double decayLearningRate(final long epoch, final double initialAlpha);


    class ConstantLearningRate implements LearningRateFunction {

        @Override
        public double decayLearningRate(final long epoch, final double initialAlpha) {
            return initialAlpha;
        }
    }

    class ConstantDecayLearningRate implements LearningRateFunction {

        @Override
        public double decayLearningRate(final long epoch, final double initialAlpha) {
            return initialAlpha / Math.sqrt(epoch);
        }
    }

    class IterativeDecayLearningRate implements LearningRateFunction {

        private int decayEpoch;

        public IterativeDecayLearningRate(final int decayEpoch) {
            this.decayEpoch = decayEpoch;
        }

        @Override
        public double decayLearningRate(final long epoch, final double initialAlpha) {
            return initialAlpha / (1d + epoch / decayEpoch);
        }
    }
}
