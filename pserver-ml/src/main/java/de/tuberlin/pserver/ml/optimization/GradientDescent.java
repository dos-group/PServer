package de.tuberlin.pserver.ml.optimization;

import de.tuberlin.pserver.math.experimental.tuples.Tuple3;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

public class GradientDescent {

    private static void axpy(final double a, final double[] x, final double[] y) {
        //Computes y += x * a
    }

    public static double norm(double[] a, double b) {
        return 0;
    }

    public static void scalarMul(double[] a, double f) {
    }

    public static double dot(double[] a, double[] b) {
        return 0;
    }

    public static double[] copy(double[] a) {
        return null;
    }

    public static void scal(double f, double[] a) {
    }

    public static double[] add(final double[] a, final double[] b) {
        return null;
    }


    public static <T> List<Integer> createSampledIndexSet(final List<T> data, final double fraction, final long seed) {
        final Random rand = new Random();
        rand.setSeed(seed);
        final int num = (int)(data.size() * fraction);
        final List<Integer> sampleIndices = new ArrayList(num);
        for (int i = 0; i < num; ++i)
            sampleIndices.add(rand.nextInt(data.size()));
        return sampleIndices;
    }

    // --------------------------------------------------------------------------------------------------------

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

        public abstract Pair<double[], Double> compute(
                final double[] weightsOld,
                final double[] gradient,
                final double   stepSize,
                final int      iter,
                final double   regParam);
    }

    /**
     * A simple updater for gradient descent *without* any regularization.
     * Uses a step-size decreasing with the square root of the number of iterations.
     */
    public class SimpleUpdater extends Updater {

        @Override
        public Pair<double[], Double> compute(
                final double[] weightsOld,
                final double[] gradient,
                final double   stepSize,
                final int      iter,
                final double   regParam) {

            final double thisIterStepSize = stepSize / Math.sqrt(iter);
            double[] brzWeights = weightsOld;

            axpy(-thisIterStepSize, gradient, brzWeights);

            return Pair.of(brzWeights, new Double(0));
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

    public class L1Updater extends Updater {

        @Override
        public Pair<double[], Double> compute(
                final double[] weightsOld,
                final double[] gradient,
                final double stepSize,
                final int iter,
                final double regParam) {

            final double thisIterStepSize = stepSize / Math.sqrt(iter);
            // Take gradient step
            double[] brzWeights = weightsOld;
            axpy(-thisIterStepSize, gradient, brzWeights);
            // Apply proximal operator (soft thresholding)
            final double shrinkageVal = regParam * thisIterStepSize;
            int i = 0;
            while (i < brzWeights.length) {
                double wi = brzWeights[i];
                brzWeights[i] = Math.signum(wi) * Math.max(0.0, Math.abs(wi) - shrinkageVal);
                i += 1;
            }
            return Pair.of(brzWeights, norm(brzWeights, 1.0) * regParam);
        }
    }

    /**
     * Updater for L2 regularized problems.
     *          R(w) = 1/2 ||w||^2
     * Uses a step-size decreasing with the square root of the number of iterations.
     */
    public class SquaredL2Updater extends Updater {

        @Override
        public Pair<double[], Double> compute(
                final double[] weightsOld,
                final double[] gradient,
                final double stepSize,
                final int iter,
                final double regParam) {
            // add up both updates from the gradient of the loss (= step) as well as
            // the gradient of the regularizer (= regParam * weightsOld)
            // w' = w - thisIterStepSize * (gradient + regParam * w)
            // w' = (1 - thisIterStepSize * regParam) * w - thisIterStepSize * gradient
            final double thisIterStepSize = stepSize / Math.sqrt(iter);
            // Take gradient step
            double[] brzWeights = weightsOld;
            scalarMul(brzWeights, 1.0 - thisIterStepSize * regParam);
            axpy(-thisIterStepSize, gradient, brzWeights);
            double n = norm(brzWeights, 2.0);
            return Pair.of(brzWeights, 0.5 * regParam * n * n);
        }
    }

    // --------------------------------------------------------------------------------------------------------

    public abstract class Gradient implements Serializable {
        /**
         * Compute the gradient and loss given the features of a single data point.
         *
         * @param data features for one data point
         * @param label label for this data point
         * @param weights weights/coefficients corresponding to features
         *
         * @return (gradient: Vector, loss: Double)
         */
        public abstract Pair<double[],Double> compute(
                final double[] data,
                final double label,
                final double[] weights
        );

        /**
         * Compute the gradient and loss given the features of a single data point,
         * add the gradient to a provided vector to avoid creating new objects, and return loss.
         *
         * @param data features for one data point
         * @param label label for this data point
         * @param weights weights/coefficients corresponding to features
         * @param cumGradient the computed gradient will be added to this vector
         *
         * @return loss
         */
        public abstract double compute(
                final double[] data,
                final double label,
                final double[] weights,
                final double[] cumGradient
        );
    }


    /**
     * Compute gradient and loss for a Least-squared loss function, as used in linear regression.
     * This is correct for the averaged least squares loss function (mean squared error)
     *              L = 1/2n ||A weights-y||^2
     * See also the documentation for the precise formulation.
     */
    public class LeastSquaresGradient extends Gradient {

        public Pair<double[],Double> compute(
                final double[] data,
                final double label,
                final double[] weights) {

            double diff = dot(data, weights) - label;
            double loss = diff * diff / 2.0;
            double[] gradient = copy(data);
            scal(diff, gradient);
            return Pair.of(gradient, loss);
        }

        @Override
        public double compute(final double[] data,
                              final double label,
                              final double[] weights,
                              final double[] cumGradient) {
            double diff = dot(data, weights) - label;
            axpy(diff, data, cumGradient);
            return diff * diff / 2.0;
        }
    }

    // --------------------------------------------------------------------------------------------------------

    private Gradient    gradient;
    private Updater     updater;

    private double      stepSize            = 1.0;
    private int         numIterations       = 100;
    private double      regParam            = 0.0;
    private double      miniBatchFraction   = 1.0;
    private double[]    initialWeights;

    /**
     * Run stochastic gradient descent (SGD) in parallel using mini batches.
     * In each iteration, we createSampledIndexSet a subset (fraction miniBatchFraction) of the total data
     * in order to compute a gradient estimate.
     * Sampling, and averaging the subgradients over this subset is performed using one standard
     * spark map-reduce in each iteration.
     *
     *  data - Input data for SGD. RDD of the set of data examples, each of
     *               the form (label, [feature values]).
     *  gradient - Gradient object (used to compute the gradient of the loss function of
     *                   one single data example)
     *  updater - Updater function to actually perform a gradient step in a given direction.
     *  stepSize - initial step size for the first step
     *  numIterations - number of iterations that SGD should be run.
     *  regParam - regularization parameter
     *  miniBatchFraction - fraction of the input data set that should be used for
     *                            one iteration of SGD. Default value 1.0.
     *
     * @return A tuple containing two elements. The first element is a column matrix containing
     *         weights for every feature, and the second element is an array containing the
     *         stochastic loss computed for every iteration.
     */
    public Pair<double[], double[]> runMiniBatchSGD(List<Pair<Double, double[]>> data) {

        double[] stochasticLossHistory = new double[numIterations];
        int numExamples = data.size();

        // if no data, return initial weights to avoid NaNs
        if (numExamples == 0) {
            System.out.println("de.tuberlin.pserver.ml.optimization.GradientDescent.runMiniBatchSGD returning initial weights, no data found");
            return Pair.of(initialWeights, stochasticLossHistory);
        }

        if (numExamples * miniBatchFraction < 1)
            throw new IllegalStateException("The miniBatchFraction is too small");

        // Initialize weights as a column vector
        double[] weights = copy(initialWeights);
        final int n = weights.length;

        // For the first iteration, the regVal will be initialized as sum of weight squares
        // if it's L2 updater; for L1 updater, the same logic is followed.
        double regVal = updater.compute(weights, new double[weights.length], 0, 1, regParam).getRight();

        for (int i = 1;i < numIterations; ++i) {

            //val bcWeights = data.context.broadcast(weights);

            final Tuple3<double[],Double,Long> u = new Tuple3<>(new double[n], (double)0, (long)0);

            final double[] w = weights;

            final Optional<Tuple3<double[],Double,Long>> res =
                    createSampledIndexSet(data, miniBatchFraction, 42 + i).stream()
                    .map(data::get)
                    .map(dp -> {
                        final double l = gradient.compute(dp.getRight(), dp.getLeft(), w, u._1);
                        u._1 = u._1;
                        u._2 = u._2 + l;
                        u._3 = u._3 + 1;
                        return u;
                    })
                    .reduce((u1, u2) ->
                            new Tuple3<>(
                                add(u1._1, u2._1),
                                u1._2 + u2._2,
                                u1._3 + u2._3
                            )
                    );

            if (res.isPresent()) {

                final double[] gradientSum  = res.get()._1;
                final double lossSum        = res.get()._2;
                final double miniBatchSize  = res.get()._3;

                if (miniBatchSize > 0) {

                    // lossSum is computed using the weights from the previous iteration
                    // and regVal is the regularization value computed in the previous iteration as well.
                    stochasticLossHistory[i] = (lossSum / miniBatchSize + regVal);
                    scalarMul(gradientSum, 1 / miniBatchSize);

                    final Pair<double[], Double> update = updater.compute(weights, gradientSum, stepSize, i, regParam);
                    weights = update.getLeft();
                    regVal  = update.getRight();

                } else {
                    System.out.println("Iteration ($i/$numIterations). The size of sampled batch is zero");
                }
            }
        }

        return Pair.of(weights, stochasticLossHistory);
    }
}
