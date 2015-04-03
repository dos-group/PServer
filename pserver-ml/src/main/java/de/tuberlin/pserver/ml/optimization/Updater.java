package de.tuberlin.pserver.ml.optimization;

import org.apache.commons.lang3.tuple.Pair;
import org.jblas.DoubleMatrix;
import org.jblas.SimpleBlas;

import java.io.Serializable;

/**
 * Class used to perform steps (weight update) using Gradient Descent methods.
 *
 * For general minimization problems, or for regularized problems of the form
 *         min  L(w) + regParam * R(w),
 * the compute function performs the actual update step, when given some
 * (e.g. stochastic) gradient direction for the loss L(w),
 * and a desired step-size (learning rate).
 *
 * The updater is responsible to also perform the update coming from the
 * regularization term R(w) (if any regularization is used).
 */

public abstract class Updater implements Serializable {

    public abstract Pair<DoubleMatrix, Double> compute(
            final DoubleMatrix weightsOld,
            final DoubleMatrix gradient,
            final double   stepSize,
            final int      iter,
            final double   regParam);


    /**
     * A simple updater for gradient descent *without* any regularization.
     * Uses a step-size decreasing with the square root of the number of iterations.
     */
    public static class SimpleUpdater extends Updater {

        @Override
        public Pair<DoubleMatrix, Double> compute(
                final DoubleMatrix weightsOld,
                final DoubleMatrix gradient,
                final double   stepSize,
                final int      iter,
                final double   regParam) {

            final double thisIterStepSize = stepSize / Math.sqrt(iter);
            final DoubleMatrix brzWeights = SimpleBlas.axpy(-thisIterStepSize, gradient, weightsOld);
            return Pair.of(brzWeights, (double)(0));
        }
    }

    /**
     * Updater for L1 regularized problems.
     *          R(w) = ||w||_1
     * Uses a step-size decreasing with the square root of the number of iterations.

     * Instead of subgradient of the regularizer, the proximal operator for the
     * L1 regularization is applied after the gradient step. This is known to
     * result in better sparsity of the intermediate solution.
     *
     * The corresponding proximal operator for the L1 norm is the soft-thresholding
     * function. That is, each weight component is shrunk towards 0 by shrinkageVal.
     *
     * If w >  shrinkageVal, set weight component to w-shrinkageVal.
     * If w < -shrinkageVal, set weight component to w+shrinkageVal.
     * If -shrinkageVal < w < shrinkageVal, set weight component to 0.
     *
     * Equivalently, set weight component to signum(w) * max(0.0, abs(w) - shrinkageVal)
     */

    public static class L1Updater extends Updater {

        @Override
        public Pair<DoubleMatrix, Double> compute(
                final DoubleMatrix weightsOld,
                final DoubleMatrix gradient,
                final double stepSize,
                final int iter,
                final double regParam) {

            final double thisIterStepSize = stepSize / Math.sqrt(iter);
            // Take gradient step
            final DoubleMatrix brzWeights = SimpleBlas.axpy(-thisIterStepSize, gradient, weightsOld);
            // Apply proximal operator (soft thresholding)
            final double shrinkageVal = regParam * thisIterStepSize;
            int i = 0;
            while (i < brzWeights.length) {
                double wi = brzWeights.get(i);
                brzWeights.put(i, Math.signum(wi) * Math.max(0.0, Math.abs(wi) - shrinkageVal));
                i += 1;
            }
            return Pair.of(brzWeights, brzWeights.norm1() * regParam);
        }
    }

    /**
     * Updater for L2 regularized problems.
     *          R(w) = 1/2 ||w||^2
     * Uses a step-size decreasing with the square root of the number of iterations.
     */
    public static class SquaredL2Updater extends Updater {

        @Override
        public Pair<DoubleMatrix, Double> compute(
                final DoubleMatrix weightsOld,
                final DoubleMatrix gradient,
                final double stepSize,
                final int iter,
                final double regParam) {
            // add up both updates from the gradient of the loss (= step) as well as
            // the gradient of the regularizer (= regParam * weightsOld)
            // w' = w - thisIterStepSize * (gradient + regParam * w)
            // w' = (1 - thisIterStepSize * regParam) * w - thisIterStepSize * gradient
            final double thisIterStepSize = stepSize / Math.sqrt(iter);
            // Take gradient step
            DoubleMatrix brzWeights = new DoubleMatrix(weightsOld.data);
            brzWeights.mul( 1.0 - thisIterStepSize * regParam);
            SimpleBlas.axpy(-thisIterStepSize, gradient, brzWeights);
            double n = brzWeights.norm2();
            return Pair.of(brzWeights, 0.5 * regParam * n * n);
        }
    }
}