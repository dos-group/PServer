package de.tuberlin.pserver.ml.optimization.sparkml_port;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.experimental.tuples.Tuple2;
import de.tuberlin.pserver.math.experimental.tuples.Tuple3;
import org.apache.commons.lang3.tuple.Pair;
import org.jblas.DoubleMatrix;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

public class GradientDescent {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Gradient        gradient;

    private Updater         updater;

    private DoubleMatrix    initialWeights;

    // ---------------------------------------------------

    private double          stepSize            = 1.0;

    private int             numIterations       = 100;

    private double          regParam            = 0.0;

    private double          miniBatchFraction   = 0.1;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public GradientDescent(final Gradient gradient,
                           final Updater updater,
                           final DoubleMatrix initialWeights) {

        this.gradient       = Preconditions.checkNotNull(gradient);
        this.updater        = Preconditions.checkNotNull(updater);
        this.initialWeights = new DoubleMatrix(Preconditions.checkNotNull(initialWeights).data);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     * Run stochastic gradient descent (SGD) in parallel using mini batches.
     * In each iteration, we createSampledDataIndexSet a subset (fraction miniBatchFraction) of the total data
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
    public Tuple2<DoubleMatrix, double[]> runMiniBatchSGD(List<Tuple2<Double, DoubleMatrix>> data) {
        Preconditions.checkNotNull(data);

        double[] stochasticLossHistory = new double[numIterations];
        int numExamples = data.size();

        // if no data, return initial weights to avoid NaNs
        if (numExamples == 0) {
            System.out.println("de.tuberlin.pserver.ml.optimization.sparkml_port.GradientDescent.runMiniBatchSGD returning initial weights, no data found");
            return new Tuple2<>(initialWeights, stochasticLossHistory);
        }

        if (numExamples * miniBatchFraction < 1)
            throw new IllegalStateException("The miniBatchFraction is too small");

        // Initialize weights as a column vector
        DoubleMatrix weights = new DoubleMatrix(initialWeights.data);
        final int n = weights.length;
        // For the first iteration, the regVal will be initialized as sum of weight squares
        // if it's L2 updater; for L1 updater, the same logic is followed.
        double regVal = updater.compute(weights, new DoubleMatrix(weights.length), 0, 1, regParam).getRight();

        for (int i = 1; i < numIterations; ++i) {

            //val bcWeights = data.context.broadcast(weights);

            final Tuple3<DoubleMatrix,Double,Long> u = new Tuple3<>(new DoubleMatrix(n), (double)0, (long)0);
            final DoubleMatrix w = weights;
            final Optional<Tuple3<DoubleMatrix,Double,Long>> res =
                    createSampledDataIndexSet(data, miniBatchFraction, 42 + i).stream()
                    .map(data::get)
                    .map(dp -> {
                        final double l = gradient.compute(dp._2, dp._1, w, u._1);
                        u._1.copy(u._1);
                        u._2 = u._2 + l;
                        u._3 = u._3 + 1;
                        return u;
                    })
                    .reduce((u1, u2) ->
                            new Tuple3<>(
                                u1._1.add(u2._1),
                                u1._2 + u2._2,
                                u1._3 + u2._3
                            )
                    );

            if (res.isPresent()) {
                final DoubleMatrix gradientSum  = res.get()._1;
                final double lossSum        = res.get()._2;
                final double miniBatchSize  = res.get()._3;
                if (miniBatchSize > 0) {
                    // lossSum is computed using the weights from the previous iteration
                    // and regVal is the regularization value computed in the previous iteration as well.
                    stochasticLossHistory[i] = lossSum / miniBatchSize + regVal;
                    gradientSum.mul(1 / miniBatchSize);
                    final Pair<DoubleMatrix, Double> update = updater.compute(weights, gradientSum, stepSize, i, regParam);
                    weights.copy(update.getLeft());
                    regVal  = update.getRight();
                } else {
                    System.out.println("Iteration ($i/$numIterations). The size of sampled batch is zero");
                }
            }
        }

        return new Tuple2<>(weights, stochasticLossHistory);
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private static <T> List<Integer> createSampledDataIndexSet(final List<T> data,
                                                               final double fraction,
                                                               final long seed) {
        final Random rand = new Random();
        rand.setSeed(seed);
        final int num = (int)(data.size() * fraction);
        final List<Integer> sampleIndices = new ArrayList<>(num);
        for (int i = 0; i < num; ++i)
            sampleIndices.add(rand.nextInt(data.size()));
        return sampleIndices;
    }
}
