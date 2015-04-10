package de.tuberlin.pserver.ml.optimization.naive;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.filesystem.FileDataIterator;
import org.apache.commons.csv.CSVRecord;

public final class GradientDescent {

    // Disallow instantiation.
    private GradientDescent() {}

    // ---------------------------------------------------

    public static interface LossFunction {

        /**
         * Convex Loss Functions:
         *      + Square loss: L(y, y') = 0.5 * (p - y)²
         *      + Hinge loss: L(y, y') = max{0, 1 − y * y'}
         *      + Exponential loss: L(y, y') = exp(− y * y')
         *      + Logistic loss: L(y, y') = log2(1 + exp(−y * y'))
         *
         * (All of these are convex upper bounds on 0-1 loss)
         */

        /** Evaluate the loss function. */
        public abstract double loss(double p, double y);

        /** Evaluate the derivative of the loss function with respect to the prediction `p`. */
        public abstract double dloss(double p, double y);
    }

    // ---------------------------------------------------

    public static class SquaredLossFunction implements LossFunction {

        @Override
        public double loss(double p, double y) { return 0.5 * (p - y) * (p - y); }

        @Override
        public double dloss(double p, double y) { return (p - y); }
    }

    // ---------------------------------------------------

    public static interface WeightsUpdater {

        public abstract void updateWeights(final int epoch, final double[] weights);
    }

    // ---------------------------------------------------

    public static abstract class SGDBase {

        protected LossFunction lossFunction;

        protected double alpha = 0.005;

        protected int numIterations = 1500000;

        // ---------------------------------------------------

        public void setLossFunction(final LossFunction lossFunction) { this.lossFunction = Preconditions.checkNotNull(lossFunction); }

        public LossFunction getLossFunction() { return lossFunction; }

        public void setLearningRate(final double alpha) { this.alpha = alpha; }

        public double getLearningRate() { return alpha; }

        public void setNumIterations(final int numIterations) { this.numIterations = numIterations; }

        public int getNumIterations() { return numIterations; }

        // ---------------------------------------------------

        public abstract double[] fit(final double[] weights,
                                     final FileDataIterator<CSVRecord> dataIterator,
                                     final int labelIndex);

        public abstract double[] predict(final double[] weights,
                                         final double[][] data);
    }

    // ---------------------------------------------------

    public static class SGDRegressor extends SGDBase {

        protected WeightsUpdater weightsUpdater;

        public void setWeightsUpdater(final WeightsUpdater wu) { this.weightsUpdater = Preconditions.checkNotNull(wu); }

        public WeightsUpdater getWeightsUpdater() { return weightsUpdater; }

        @Override
        public double[] predict(final double[] weights,
                                final double[][] data) {
            return null;
        }

        @Override
        public double[] fit(final double[] weights,
                            final FileDataIterator<CSVRecord> dataIterator,
                            final int labelIndex) {

            return doPlainSGD(
                    Preconditions.checkNotNull(weights),
                    Preconditions.checkNotNull(dataIterator),
                    labelIndex,
                    lossFunction,
                    alpha,
                    numIterations
            );
        }

        // ---------------------------------------------------

        private double[] doPlainSGD(final double[] weights,
                                    final FileDataIterator<CSVRecord> dataIterator,
                                    final int labelIndex,
                                    final LossFunction lossFunction,
                                    final double alpha,
                                    final int numIterations) {

            boolean first           = true;
            int numFeatures         = 0;
            int[] featureIndices    = null;

            if (lossFunction instanceof SquaredLossFunction) {
                for (int epoch = 0; epoch < numIterations; ++epoch) {

                    while(dataIterator.hasNext()) {

                        final CSVRecord record = dataIterator.next();

                        if (first) {
                            numFeatures     = record.size() - 1;
                            featureIndices  = new int[numFeatures];
                            for (int j = 0, k = 0; j < numFeatures + 1; ++j)
                                if (j != labelIndex) {
                                    featureIndices[k] = j;
                                    ++k;
                                }

                            first = false;
                        }

                        // -- Compute the prediction (p) --
                        // Dot product of a sample x and the weight vector.
                        int m = 1;
                        double p = weights[0];
                        for (int j : featureIndices) {
                            p += Double.parseDouble(record.get(j)) * weights[m];
                            m++;
                        }
                        // -- Minimize the loss function --
                        // Compute parameter Θ(0).
                        weights[0] = weights[0] - alpha * lossFunction.dloss(p, Double.parseDouble(record.get(labelIndex)));
                        // Compute parameters Θ(1..m).
                        int n = 1;
                        for (int k : featureIndices) {
                            weights[n] = weights[n] - alpha * lossFunction.dloss(p, Double.parseDouble(record.get(labelIndex))) * Double.parseDouble(record.get(k));
                            n++;
                        }
                    }

                    dataIterator.reset();

                    weightsUpdater.updateWeights(epoch, weights);
                }
            } else
                throw new IllegalStateException();

            return weights;
        }
    }
}
