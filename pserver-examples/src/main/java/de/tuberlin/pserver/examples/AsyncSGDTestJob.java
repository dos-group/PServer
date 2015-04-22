package de.tuberlin.pserver.examples;

import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.app.types.DMatrix;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.ml.optimization.gradientdescent.SGDRegressor;
import de.tuberlin.pserver.ml.optimization.gradientdescent.WeightsUpdater;

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

        dataManager.loadDMatrix("datasets/data1.csv");

        regressor = new SGDRegressor(getJobContext())
            .setLearningRate(0.005)
            .setNumberOfIterations(400)
            .setWeightsUpdater(weightsUpdater);
    }

    @Override
    public void compute() {

        final int labelColumnIndex = 15;

        final DMatrix trainingData = dataManager.getLocalMatrix("data1.csv");

        final DMatrix weights = regressor.fit(model, trainingData, labelColumnIndex);

        for (double weight : weights.toArray())
            System.out.print(weight + " | ");
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        PServerExecutor.LOCAL
                .run(AsyncSGDTestJob.class)
                .done();
    }
}