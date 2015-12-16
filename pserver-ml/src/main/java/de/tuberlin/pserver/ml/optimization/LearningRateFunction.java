package de.tuberlin.pserver.ml.optimization;


public interface LearningRateFunction {

    public abstract float decayLearningRate(final long epoch, final float initialAlpha);


    class ConstantLearningRate implements LearningRateFunction {

        @Override
        public float decayLearningRate(final long epoch, final float initialAlpha) {
            return initialAlpha;
        }
    }

    class ConstantDecayLearningRate implements LearningRateFunction {

        @Override
        public float decayLearningRate(final long epoch, final float initialAlpha) {
            return initialAlpha / (float)Math.sqrt(epoch);
        }
    }

    class IterativeDecayLearningRate implements LearningRateFunction {

        private int decayEpoch;

        public IterativeDecayLearningRate(final int decayEpoch) {
            this.decayEpoch = decayEpoch;
        }

        @Override
        public float decayLearningRate(final long epoch, final float initialAlpha) {
            return initialAlpha / (float)(1d + epoch / decayEpoch);
        }
    }
}
