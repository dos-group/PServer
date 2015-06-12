package de.tuberlin.pserver.examples.ml;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.Vector;
import de.tuberlin.pserver.ml.optimization.*;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;

public final class AsyncHogwildSGDTestJob extends PServerJob {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final DataManager.Merger<Vector> merger = (l, r) -> {
        for (int j = 0; j < l.size(); ++j) {
            double nv = 0.0;
            for (final Vector v : r)
                nv += v.get(j);
            l.set(j, (nv / r.length));
        }
    };

    private final WeightsObserver observer = (epoch, weights) -> {
        if (epoch % 200 == 0)
            dataManager.mergeVector(weights, merger);
    };

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {

        dataManager.loadDMatrix("datasets/sparse_dataset.csv");

        dataManager.createLocalVector("weight-vector", 1000, Vector.VectorType.ROW_VECTOR);
    }

    @Override
    public void compute() {

        final Vector weights = dataManager.getLocalVector("weight-vector");

        final Matrix trainingData = dataManager.getLocalMatrix("sparse_dataset.csv");

        final PredictionFunction predictionFunction = new PredictionFunction.LinearPredictionFunction();

        final PartialLossFunction partialLossFunction = new PartialLossFunction.SquareLoss();

        final Optimizer optimizer = new SGDOptimizer(SGDOptimizer.TYPE.SGD_SIMPLE)
                .setNumberOfIterations(1000)
                .setLearningRate(0.00005)
                .setLossFunction(new LossFunction.GenericLossFunction(predictionFunction, partialLossFunction))
                .setGradientStepFunction(new GradientStepFunction.AtomicGradientStep())
                .setLearningRateDecayFunction(null)
                .setWeightsObserver(observer)
                .setRandomShuffle(true);

        final Matrix.RowIterator dataIterator = dataManager.threadPartitionedRowIterator(trainingData);

        optimizer.optimize(weights, dataIterator);

        result(weights);
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final List<List<Serializable>> weights = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(AsyncHogwildSGDTestJob.class, 2) // <-- enable multi-threading, 2 threads per compute node.
                .results(weights)
                .done();

        final DecimalFormat numberFormat = new DecimalFormat("0.000");
        weights.forEach(
                r -> r.forEach(
                        w -> {
                            for (double weight : ((Vector)w).toArray())
                                System.out.print(numberFormat.format(weight) + "\t | ");
                            System.out.println();
                        }
                )
        );
    }
}