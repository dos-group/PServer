package de.tuberlin.pserver.examples;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.experimental.old.DMatrix;
import de.tuberlin.pserver.ml.optimization.gradientdescent.SGDRegressor;
import de.tuberlin.pserver.ml.optimization.gradientdescent.WeightsUpdater;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;

public final class AsyncSGDTestJob extends PServerJob {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private SGDRegressor regressor;

    private DMatrix model;

    private final DataManager.MatrixMerger<DMatrix> matrixMerger = (localModel, remoteModels) -> {
                    for (int i = 0; i < localModel.numRows(); ++i) {
                        for (int j = 0; j < localModel.numCols(); ++j) {
                            double v = 0.0;
                            for (final DMatrix m : remoteModels)
                                v += m.get(i, j);
                            localModel.set(i, j, (v / remoteModels.length));
                        }
                    }
                };

    public final WeightsUpdater weightsUpdater = (epoch, weights) -> {
        if (epoch % 10 == 0)
            dataManager.mergeMatrix(model, matrixMerger);
    };

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {

        model = dataManager.createLocalMatrix("model1", 1, 16);

        dataManager.loadDMatrix("datasets/demo_dataset.csv");

        regressor = new SGDRegressor(getJobContext());
        regressor.setLearningRate(0.005);
        regressor.setNumberOfIterations(15400);
        regressor.setWeightsUpdater(weightsUpdater);
    }

    @Override
    public void compute() {

        final int labelColumnIndex = 15;

        final DMatrix trainingData = dataManager.getLocalMatrix("demo_dataset.csv");

        final DMatrix weights = regressor.fit(model, trainingData, labelColumnIndex);

        result(weights);
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

        final DecimalFormat numberFormat = new DecimalFormat("###.###");
        weights.forEach(
                r -> r.forEach(
                        w -> {
                            for (double weight : ((DMatrix) w).toArray())
                                System.out.print(numberFormat.format(weight) + "\t | ");
                            System.out.println();
                        }
                )
        );
    }
}