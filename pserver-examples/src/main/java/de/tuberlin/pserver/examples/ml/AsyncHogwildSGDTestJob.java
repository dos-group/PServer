package de.tuberlin.pserver.examples.ml;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.math.Vector;
import de.tuberlin.pserver.ml.optimization.WeightsObserver;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;

public final class AsyncHogwildSGDTestJob extends PServerJob {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Vector model;

    private final DataManager.Merger<Vector> merger = (l, r) -> {
        for (int j = 0; j < l.size(); ++j) {
            double nv = 0.0;
            for (final Vector v : r)
                nv += v.get(j);
            l.set(j, (nv / r.length));
        }
    };

    private final WeightsObserver observer = (epoch, weights) -> {
        if (epoch % 10 == 0)
            dataManager.mergeVector(model, merger);
    };

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {
        dataManager.loadDMatrix("datasets/sparse_dataset.csv");
    }

    @Override
    public void compute() {

        /// ---------------------------------- DOES NOT WORK ----------------------------------

        /*try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        model = dataManager.createLocalVector("model1", 15, Vector.VectorType.ROW_VECTOR);

        final Matrix trainingData = dataManager.getLocalMatrix("sparse_dataset.csv");

        final Functions.PredictionFunction predictionFunction = new Functions.LinearPredictionFunction();

        final Functions.PartialLossFunction partialLossFunction = new Functions.SquareLoss();

        final Optimizer optimizer = new SGDOptimizer()
                .setNumberOfIterations(12000)
                .setLearningRate(0.005)
                .setLossFunction(new Functions.GenericLossFunction(predictionFunction, partialLossFunction))
                .setGradientStepFunction(new SGDOptimizer.SimpleGradientStep())
                .setLearningRateDecayFunction(new SGDOptimizer.SimpleDecay())
                .setWeightsObserver(observer);

        final Vector weights = optimizer.optimize(model, trainingData.rowIterator());

        result(weights);*/

        /// ---------------------------------- DOES NOT WORK ----------------------------------

    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final List<List<Serializable>> weights = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(AsyncSGDTestJob.class, 2) // <-- enable multi-threading
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