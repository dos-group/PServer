package de.tuberlin.pserver.examples.experiments.sgd;

import de.tuberlin.pserver.runtime.Program;

public final class AsyncHogwildSGDTestJob extends Program {
/*
    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final DataManager.Merger<Vector> merger = (l, r) -> {
        for (int j = 0; j < l.length(); ++j) {
            double nv = 0.0;
            for (final Vector v : r)
                nv += v.get(j);
            l.set(j, (nv / r.size()));
        }
    };

    private final Observer observer = (epoch, weights, gradientSum) ->
        dataManager.pullMerge(weights, merger);

    private final GeneralLinearModel model = new GeneralLinearModel("model1", 1000);

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {

        dataManager.loadAsMatrix("datasets/sparse_dataset.csv", GenerateLocalTestData.ROWS_SPARSE_DATASET, GenerateLocalTestData.COLS_SPARSE_DATASET);

        model.createModel(programContext);
    }

    @Override
    public void compute() {

        final Matrix trainingData = dataManager.getObject("datasets/sparse_dataset.csv");

        final PredictionFunction predictionFunction = new PredictionFunction.LinearPredictionFunction();

        final PartialLossFunction partialLossFunction = new PartialLossFunction.SquareLoss();

        final Optimizer optimizer = new SGDOptimizer(programContext, SGDOptimizer.TYPE.SGD_SIMPLE, false)
                .setNumberOfIterations(1000)
                .setLearningRate(0.00005)
                .setLossFunction(new LossFunction.GenericLossFunction(predictionFunction, partialLossFunction))
                .setGradientStepFunction(new GradientStepFunction.AtomicGradientStep())
                .setLearningRateDecayFunction(null)
                .setWeightsObserver(observer, 200, true)
                .setRandomShuffle(true);

        final Matrix.RowIterator dataIterator = executionManager.parallelMatrixRowIterator(trainingData);

        optimizer.optimize(model, dataIterator);

        result(model.getWeights());
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final List<List<Serializable>> res = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(AsyncHogwildSGDTestJob.class, 2) // <-- enable multi-threading, 2 threads per compute node.
                .results(res)
                .done();

        final DecimalFormat numberFormat = new DecimalFormat("0.000");
        res.forEach(
                r -> r.forEach(
                        w -> {
                            for (double weight : ((Vector)w).toArray())
                                System.out.print(numberFormat.format(weight) + "\t | ");
                            System.out.println();
                        }
                )
        );
    }*/
}