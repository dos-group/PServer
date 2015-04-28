package de.tuberlin.pserver.ml.playground;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.ml.generators.DataGenerator;
import org.apache.commons.lang3.tuple.Pair;

public class TestGradientDescent {



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
                                     final double[][] trainingData,
                                     final int labelIndex);

        public abstract double[] predict(final double[] weights,
                                         final double[][] data);
    }

    // ---------------------------------------------------

    public static class SGDRegressor extends SGDBase {

        @Override
        public double[] predict(final double[] weights,
                                final double[][] data) {
            return null;
        }

        @Override
        public double[] fit(final double[] weights,
                            final double[][] trainingData,
                            final int labelIndex) {

            return doPlainSGD(
                    Preconditions.checkNotNull(weights),
                    Preconditions.checkNotNull(trainingData),
                    labelIndex,
                    lossFunction,
                    alpha,
                    numIterations
            );
        }

        // ---------------------------------------------------

        private double[] doPlainSGD(final double[] weights,
                                    final double[][] trainingData,
                                    final int labelIndex,
                                    final LossFunction lossFunction,
                                    final double alpha,
                                    final int numIterations) {

            final int numFeatures = Preconditions.checkNotNull(trainingData[0]).length - 1;
            final int[] featureIndices = new int [numFeatures];
            for (int j = 0, k = 0; j < numFeatures + 1; ++j)
                if (j != labelIndex) {
                    featureIndices[k] = j;
                    ++k;
                }

            if (lossFunction instanceof SquaredLossFunction) {
                for (int epoch = 0; epoch < numIterations; ++epoch) {
                    for (double[] aTrainingData : trainingData) {
                        // -- Compute the prediction (p) --
                        // Dot product of a sample x and the weight vector.
                        int m = 1;
                        double p = weights[0];
                        for (int j : featureIndices) {
                            p += aTrainingData[j] * weights[m];
                            m++;
                        }
                        // -- Minimize the loss function --
                        // Compute parameter Θ(0).
                        weights[0] = weights[0] - alpha * lossFunction.dloss(p, aTrainingData[labelIndex]);
                        // Compute parameters Θ(1..m).
                        int n = 1;
                        for (int k : featureIndices) {
                            weights[n] = weights[n] - alpha * lossFunction.dloss(p, aTrainingData[labelIndex]) * aTrainingData[k];
                            n++;
                        }
                    }
                }
            } else
                throw new IllegalStateException();

            return weights;
        }
    }
    // ---------------------------------------------------
    // Test.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final int numExamples = 1000000;

        final int labelIndex, numFeatures = labelIndex = 15;

        final SGDRegressor regressor = new SGDRegressor();

        regressor.setLossFunction(new SquaredLossFunction());

        regressor.setLearningRate(0.0005);

        regressor.setNumIterations(100000);

        final Pair<double[][], double[]> res = DataGenerator.generateDataset2(numExamples, numFeatures, 42);

        final double[][] trainingData = res.getLeft();

        final double[] params = res.getRight();

        final double[] weights = regressor.fit(new double[numFeatures + 1], trainingData, labelIndex);

        for (int i = 1; i < weights.length; ++i)
            System.out.print(weights[i] + "|");

        System.out.println();

        for (double param : params)
            System.out.print(param + "|");

        //DataGenerator.generateDatasetAndWriteToFile2(100000, 15, 42, "datasets/data.csv");
    }
}
