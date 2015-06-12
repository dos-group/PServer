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

public final class AsyncSGDTestJob extends PServerJob {

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

        model = dataManager.createLocalVector("model1", 15, Vector.VectorType.ROW_VECTOR);

        dataManager.loadDMatrix("datasets/demo_dataset.csv");
    }

    @Override
    public void compute() {

        final Matrix trainingData = dataManager.getLocalMatrix("demo_dataset.csv");

        final PredictionFunction predictionFunction = new PredictionFunction.LinearPredictionFunction();

        final PartialLossFunction partialLossFunction = new PartialLossFunction.SquareLoss();

        final Optimizer optimizer = new SGDOptimizer(SGDOptimizer.TYPE.SGD_SIMPLE)
                .setNumberOfIterations(12000)
                .setLearningRate(0.005)
                .setLossFunction(new LossFunction.GenericLossFunction(predictionFunction, partialLossFunction))
                .setGradientStepFunction(new GradientStepFunction.SimpleGradientStep())
                .setLearningRateDecayFunction(new DecayFunction.SimpleDecay())
                .setWeightsObserver(observer);

        optimizer.optimize(model, trainingData.rowIterator());

        result(model);
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final List<List<Serializable>> weights = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(AsyncSGDTestJob.class)
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